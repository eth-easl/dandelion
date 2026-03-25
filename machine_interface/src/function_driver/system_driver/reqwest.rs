use crate::{
    DataItem, DataSet, Position, composition::CompositionSet, function_driver::{
        ComputeResource, Driver, EngineWorkQueue, Metadata, WorkDone, WorkToDo, functions::{Function, FunctionConfig}, system_driver::SystemFunction
    }, machine_config::EngineType, memory_domain::{
        Context, ContextTrait, ContextType, bytes_context::BytesContext,
    }, promise::Debt
};
use bytes::{Bytes};
use core_affinity::set_for_current;
use dandelion_commons::{
    dandelion_err, err_dandelion,
    records::{RecordPoint, Recorder},
    DandelionError, DandelionResult, FunctionRegistryError,
};
use futures::{FutureExt, StreamExt};
use http::{version::Version as HttpVersion, HeaderName, HeaderValue, Method as HttpMethod};
use log::{debug, error, warn};
use memcache::Client as MemcachedClient;
use reqwest::{header::HeaderMap, Client as HttpClient};
use std::{sync::Arc};
use tokio::{runtime::Builder, sync::RwLock};

trait Request
where
    Self: Sized,
{
    fn from_raw(raw_request: Vec<u8>, item_name: String, item_key: u32) -> DandelionResult<Self>;
}

/// Stores requestInformation for http
struct HttpRequest {
    /// name of the request item
    item_name: String,
    /// key of the request item
    item_key: u32,
    method: HttpMethod,
    uri: String,
    version: HttpVersion,
    headermap: HeaderMap,
    body: Vec<u8>,
}

enum MemcachedMethod {
    SET,
    GET,
}

/// Stores requestInformation for memcached request
struct MemcachedRequest {
    /// name of the request item
    item_name: String,
    /// key of the request item
    item_key: u32,
    method: MemcachedMethod,
    uri: String,
    memcached_identifier: String,
    body: Vec<u8>,
}

struct ResponseInformation {
    /// name of the original request data item
    item_name: String,
    // key of the original request data item
    item_key: u32,
    /// contains both the status line as well as all headers
    preamble: String,
    body: bytes::Bytes,
}

impl Request for HttpRequest {
    fn from_raw(
        mut raw_request: Vec<u8>,
        item_name: String,
        item_key: u32,
    ) -> DandelionResult<Self> {
        // read first line to get request line
        let request_index = raw_request
            .iter()
            .position(|character| *character == b'\n')
            .unwrap_or(raw_request.len());
        let request_line = match std::str::from_utf8(&raw_request[0..request_index]) {
            Ok(line) => line,
            Err(_) => {
                return err_dandelion!(DandelionError::InvalidSystemFuncArg(String::from(
                    "Request line not utf8",
                )));
            }
        };
        let mut request_iter = request_line.split_ascii_whitespace();

        let method_item = request_iter.next();
        let method = match method_item {
            Some(method_string) if method_string == "GET" => HttpMethod::GET,
            Some(method_string) if method_string == "POST" => HttpMethod::POST,
            Some(method_string) if method_string == "PUT" => HttpMethod::PUT,
            Some(method_string) => {
                return err_dandelion!(DandelionError::InvalidSystemFuncArg(format!(
                    "Unsupported Method: {}",
                    method_string
                )))
            }
            _ => {
                return err_dandelion!(DandelionError::MalformedSystemFuncArg(String::from(
                    "No method found",
                )))
            }
        };

        let uri = String::from(request_iter.next().ok_or(dandelion_err!(
            DandelionError::MalformedSystemFuncArg(String::from("No uri in request"))
        ))?);

        let version = match request_iter.next() {
            Some(version_string) if version_string == "HTTP/0.9" => HttpVersion::HTTP_09,
            Some(version_string) if version_string == "HTTP/1.0" => HttpVersion::HTTP_10,
            Some(version_string) if version_string == "HTTP/1.1" => HttpVersion::HTTP_11,
            Some(version_string) if version_string == "HTTP/2.0" => HttpVersion::HTTP_2,
            Some(version_string) if version_string == "HTTP/3.0" => HttpVersion::HTTP_3,
            Some(version_string) => {
                return err_dandelion!(DandelionError::InvalidSystemFuncArg(format!(
                    "Unkown http version: {}",
                    version_string,
                )))
            }
            None => {
                return err_dandelion!(DandelionError::MalformedSystemFuncArg(String::from(
                    "No http version found",
                )))
            }
        };

        // read new lines until end of header map
        let mut header_index = request_index + 1;
        let mut headermap = HeaderMap::new();
        while header_index < raw_request.len() {
            let header_line = raw_request[header_index..]
                .iter()
                .position(|character| *character == b'\n')
                .and_then(|header_end| Some(&raw_request[header_index..header_index + header_end]))
                .unwrap_or(&raw_request[header_index..]);
            // skip the \n at the index itself
            header_index += header_line.len() + 1;
            // if the header line is empty there are two consequtive new lines which means the headers are finished
            // or the request is at the end which also means there are not more lines to read
            if header_line.len() == 0 {
                break;
            }
            let split_index = header_line
                .iter()
                .position(|character| *character == b':')
                .ok_or(dandelion_err!(DandelionError::MalformedSystemFuncArg(
                    String::from("Header line does not contain \':\'",)
                )))?;
            let (key, value) = header_line.split_at(split_index);
            let header_key = HeaderName::from_bytes(key).or(err_dandelion!(
                DandelionError::MalformedSystemFuncArg(String::from("Header key not utf-8"))
            ))?;
            let header_value = HeaderValue::from_bytes(&value[1..]).or(err_dandelion!(
                DandelionError::MalformedSystemFuncArg(String::from(
                    "Header value not utf-8 conformant",
                ))
            ))?;
            match headermap.entry(header_key) {
                http::header::Entry::Occupied(mut occupied) => occupied.append(header_value),
                http::header::Entry::Vacant(vacant) => {
                    vacant.insert(header_value);
                }
            }
        }

        let body = if header_index < raw_request.len() {
            // TODO check if this copies the data (also used in other request types)
            raw_request.drain(..header_index);
            raw_request
        } else {
            vec![]
        };
        log::trace!("Reqwest body: {:?}", body);

        Ok(HttpRequest {
            item_name,
            item_key,
            method,
            uri,
            version,
            headermap,
            body,
        })
    }
}

impl Request for MemcachedRequest {
    fn from_raw(
        mut raw_request: Vec<u8>,
        item_name: String,
        item_key: u32,
    ) -> DandelionResult<Self> {
        // read first line to get request line
        let request_index = raw_request
            .iter()
            .position(|character| *character == b'\n')
            .unwrap_or(raw_request.len());
        let request_line = match std::str::from_utf8(&raw_request[0..request_index]) {
            Ok(line) => line,
            Err(_) => {
                return err_dandelion!(DandelionError::InvalidSystemFuncArg(String::from(
                    "Request line not utf8",
                )));
            }
        };
        let mut request_iter = request_line.split_ascii_whitespace();

        let method_item = request_iter.next();
        let method = match method_item {
            Some(method_string) if method_string == "MEMCACHED_GET" => MemcachedMethod::GET,
            Some(method_string) if method_string == "MEMCACHED_SET" => MemcachedMethod::SET,
            Some(method_string) => {
                return err_dandelion!(DandelionError::InvalidSystemFuncArg(format!(
                    "Unsupported Method: {}",
                    method_string
                )))
            }
            _ => {
                return err_dandelion!(DandelionError::MalformedSystemFuncArg(String::from(
                    "No method found",
                )))
            }
        };

        let uri = String::from(request_iter.next().ok_or(dandelion_err!(
            DandelionError::MalformedSystemFuncArg(String::from("No uri in request"))
        ))?);
        let memcached_identifier = match request_iter.next() {
            Some(identifier) => identifier.to_string(),
            None => {
                return err_dandelion!(DandelionError::MalformedSystemFuncArg(String::from(
                    "No memcached identifier found",
                )))
            }
        };

        let body = if request_index + 1 < raw_request.len() {
            raw_request.drain(..=request_index);
            raw_request
        } else {
            vec![]
        };
        log::trace!("Reqwest body: {:?}", body);

        return Ok(Self {
            item_name,
            item_key,
            method,
            uri,
            memcached_identifier,
            body,
        });
    }
}

fn parse_requests<RequestType: Request>(
    composition_set: &CompositionSet,
) -> DandelionResult<Vec<RequestType>> {
    let request_info: DandelionResult<Vec<RequestType>> = composition_set
        .into_iter()
        .map(|(set_index, item_index, context)| {
            let data_item = &context.content[set_index].as_ref().unwrap().buffers[item_index];
            let mut request_buffer = Vec::with_capacity(data_item.data.size);
            request_buffer.resize(data_item.data.size, 0);
            context.read(data_item.data.offset, &mut request_buffer)?;
            // TODO: from raw may also take the vec of refs from the context (via the get_chunk interface), so we don't need to copy the request
            RequestType::from_raw(request_buffer, data_item.ident.clone(), data_item.key)
        })
        .collect();
    return request_info;
}

async fn http_request(
    client: HttpClient,
    request_info: HttpRequest,
) -> DandelionResult<ResponseInformation> {
    let HttpRequest {
        item_name,
        item_key,
        method,
        uri,
        version,
        headermap,
        body,
    } = request_info;

    let request_builder = match method {
        HttpMethod::PUT => client.put(uri.clone()),
        HttpMethod::POST => client.post(uri.clone()),
        HttpMethod::GET => client.get(uri.clone()),
        _ => {
            return err_dandelion!(DandelionError::MalformedSystemFuncArg(String::from(
                "Unsupported Method",
            )))
        }
    };

    let request = match request_builder
        .headers(headermap)
        .version(version)
        .body(body)
        .build()
    {
        Ok(req) => req,
        Err(http_error) => {
            error!("URI: {}", uri);
            return err_dandelion!(DandelionError::MalformedSystemFuncArg(format!(
                "{:?}",
                http_error
            )));
        }
    };
    let response = match client.execute(request).await {
        Ok(resp) => resp,
        Err(_) => return err_dandelion!(DandelionError::SystemFuncResponseError),
    };

    // write the status line
    let mut preamble = format!(
        "{:?} {} {}\n",
        response.version(),
        response.status().as_str(),
        response.status().canonical_reason().unwrap_or("")
    );

    // read the content length in the header
    let content_length = response
        .headers()
        .get("content-length")
        .and_then(|value| value.to_str().ok())
        .and_then(|len_str| len_str.parse::<usize>().ok());

    for (key, value) in response.headers() {
        preamble.push_str(&format!("{}:{}\n", key, value.to_str().unwrap()));
    }

    preamble.push('\n');

    let body = match response.bytes().await {
        Ok(bytes) => bytes,
        Err(_) => return err_dandelion!(DandelionError::SystemFuncResponseError),
    };

    if let Some(content_len) = content_length {
        if content_len != body.len() {
            return err_dandelion!(DandelionError::SystemFuncResponseError);
        }
    }
    let response_info = ResponseInformation {
        item_name,
        item_key,
        preamble,
        body,
    };
    return Ok(response_info);
}

async fn memcached_request(request_info: MemcachedRequest) -> DandelionResult<ResponseInformation> {
    let MemcachedRequest {
        item_name,
        item_key,
        method,
        uri,
        memcached_identifier,
        body,
    } = request_info;

    // For simplicity, we use the same Methods as http.
    // Memcached Basic Text Protocol could have following methods:
    // Set, add (set if not present), replace (set if present), append, prepend, cas
    // Get, gets (get with cas ), delete, incr/decr

    let ip = format!("memcache://{}", uri.clone());
    let memcached_client = match MemcachedClient::connect(ip) {
        Ok(client) => client,
        Err(_) => return err_dandelion!(DandelionError::MemcachedError),
    };

    // Preamble is SUCCESS for success. For non successfull functions, error message will be stored there
    let mut preamble: String;
    let response_body: bytes::Bytes;
    // Default item size limit is 1MB. If item is larger, we ignore it
    match method {
        MemcachedMethod::SET => {
            // Assemble value to set
            // TODO Make timeout a parameter

            let result = tokio::task::spawn_blocking(move || {
                memcached_client.set(&memcached_identifier, &*body, 3600)
            })
            .await;

            match result {
                Ok(Ok(_)) => {
                    preamble = String::from("SUCCESS");
                    response_body = Bytes::from(vec![0u8]);
                }
                Ok(Err(e)) => {
                    // TODO: Use better error
                    warn!("Memcached_request set failed with: {:?}", e);
                    return err_dandelion!(DandelionError::MemcachedError);
                }
                Err(e) => {
                    debug!("Failed to start Memcached_request task with: {:?}", e);
                    return err_dandelion!(DandelionError::MemcachedError);
                }
            }
        }
        MemcachedMethod::GET => {
            // Result<Option<Vec<u8>>, tokio_memcached::Error>
            let result = tokio::task::spawn_blocking(move || {
                memcached_client.get::<Vec<u8>>(&memcached_identifier)
            })
            .await;

            match result {
                Ok(Ok(Some(response))) => {
                    preamble = String::from("SUCCESS");
                    // preamble.push_str(&format!(", {:?}", response.key));
                    // match response.cas {
                    //     Some(value) => preamble.push_str(&format!(", {}", value)),
                    //     None => preamble.push_str(", None"),
                    // }
                    // preamble.push_str(&format!(", {}", response.flags.to_string()));
                    response_body = Bytes::from(response);
                }
                Ok(Ok(None)) => {
                    debug!("Key {} did not exist on memcached server", item_key);
                    preamble = String::from("ABSENT");
                    response_body = Bytes::from(vec![0u8]);
                }
                Ok(Err(e)) => {
                    debug!("Memcached_request get failed with: {:?}", e);
                    return err_dandelion!(DandelionError::MemcachedError);
                }
                Err(e) => {
                    debug!("Failed to start Memcached_request task with: {:?}", e);
                    return err_dandelion!(DandelionError::MemcachedError);
                }
            }
        }
    };

    preamble.push('\n');

    let response_info = ResponseInformation {
        item_name,
        item_key,
        preamble,
        body: response_body,
    };
    return Ok(response_info);
}

fn response_write_to_bytes(context: &mut Context, response: ResponseInformation, offset: &mut usize) -> DandelionResult<()> {
    let ResponseInformation {
        item_name,
        item_key,
        preamble,
        body,
    } = response;

    let preamble_len = preamble.len();
    let body_len = body.len();
    let response_len = preamble_len + body_len;
    
    let preamble_bytes = bytes::Bytes::from(preamble.into_bytes());

    match &mut context.context {
        ContextType::Bytes(destination_ctxt) => {
            destination_ctxt.frames.push(preamble_bytes);
            destination_ctxt.frames.push(body);
        },
        _ => {
            error!("Invalid context type in reponse write");
            return Err(dandelion_commons::DandelionError::ContextMissmatch);
        }
    }

    if let Some(response_set) = &mut context.content[0] {
        response_set.buffers.push(DataItem {
            ident: item_name.clone(),
            key: item_key,
            data: Position {
                offset: *offset,
                size: response_len,
            },
        })
    }
    if let Some(body_set) = &mut context.content[1] {
        body_set.buffers.push(DataItem {
            ident: item_name,
            key: item_key,
            data: Position {
                offset: *offset + preamble_len,
                size: response_len - preamble_len,
            },
        })
    }

    *offset += response_len;

    return Ok(());
    
}

fn responses_write(
    context_size: usize,
    output_set_names: &Vec<String>,
    debt: Debt,
    mut recorder: Recorder,
    responses: Vec<ResponseInformation>,
) {
    let mut out_context = Context::new(
        ContextType::Bytes(Box::new(BytesContext::new(Vec::new()))),
        context_size
    );

    let mut offset = 0;

    if !output_set_names.is_empty() {
        out_context.content = vec![None, None];
        if output_set_names.iter().any(|elem| elem == "response") {
            out_context.content[0] = Some(DataSet {
                ident: String::from("response"),
                buffers: vec![],
            })
        }
        if output_set_names.iter().any(|elem| elem == "body") {
            out_context.content[1] = Some(DataSet {
                ident: String::from("body"),
                buffers: vec![],
            })
        }
        let write_results: DandelionResult<Vec<_>> = responses
            .into_iter()
            .map(|response| response_write_to_bytes(&mut out_context, response, &mut offset))
            .collect();
        if let Err(err) = write_results {
            drop(recorder);
            debt.fulfill(Err(err));
            return;
        }
    }

    recorder.record(RecordPoint::EngineEnd);
    drop(recorder);
    debt.fulfill(Ok(WorkDone::Context(out_context)));
    return;
}

async fn run_http_request(
    context_size: usize,
    composition_set: CompositionSet,
    client: HttpClient,
    metadata: Arc<Metadata>,
    debt: Debt,
    recorder: Recorder,
) -> () {
    let request_vec = match parse_requests(&composition_set) {
        Ok(request) => request,
        Err(err) => {
            debt.fulfill(Err(err));
            return;
        }
    };

    let responses = match futures::future::try_join_all(
        request_vec
            .into_iter()
            .map(|request| http_request(client.clone(), request).boxed()),
    )
    .await
    {
        Ok(resp) => resp,
        Err(err) => {
            debt.fulfill(Err(err));
            return;
        }
    };

    responses_write(
        context_size,
        &metadata.output_sets,
        debt,
        recorder,
        responses,
    );
}

async fn run_memcached_request(
    context_size: usize,
    composition_set: CompositionSet,
    metadata: Arc<Metadata>,
    debt: Debt,
    recorder: Recorder,
) -> () {
    let request_vec = match parse_requests(&composition_set) {
        Ok(request) => request,
        Err(err) => {
            debt.fulfill(Err(err));
            return;
        }
    };

    // get rid of systems context to drop references to old contexts before starting to do requests
    drop(composition_set);

    let responses = match futures::future::try_join_all(
        request_vec
            .into_iter()
            .map(|request| memcached_request(request).boxed()),
    )
    .await
    {
        Ok(resp) => resp,
        Err(err) => {
            debt.fulfill(Err(err));
            return;
        }
    };

    responses_write(
        context_size,
        &metadata.output_sets,
        debt,
        recorder,
        responses,
    );
}

async fn engine_loop(queue: Box<dyn EngineWorkQueue + Send>) -> Debt {
    log::debug!("Reqwest engine Init");
    let http_client = HttpClient::new();

    // TODO FIX! This should not be necessary!
    let mut queue_ref = Box::leak(queue);
    let mut tuple;
    let worker_lock = Arc::new(RwLock::new(()));
    loop {
        (tuple, queue_ref) = queue_ref.into_future().await;
        let (args, debt) = if let Some((tuple_args, tuple_debt)) = tuple {
            (tuple_args, tuple_debt)
        } else {
            panic!("Workqueue poll next returned none")
        };
        match args {
            WorkToDo::FunctionArguments {
                function_id: _,
                function_alternatives,
                mut input_sets,
                metadata,
                caching,
                mut recorder,
            } => {
                debug_assert!(caching, "System functions should always be caching");

                let alternative = match function_alternatives
                    .into_iter()
                    .find(|alt| alt.engine == EngineType::Reqwest)
                {
                    Some(alt) => alt,
                    None => {
                        drop(recorder);
                        debt.fulfill(err_dandelion!(DandelionError::FunctionRegistry(
                            FunctionRegistryError::UnknownFunctionAlternative,
                        )));
                        continue;
                    }
                };
                recorder.record(RecordPoint::EngineStart);

                let function = alternative.load_function(true, &mut recorder).unwrap();

                log::debug!("Reqwest engine running function");
                let system_function = match function.config {
                    FunctionConfig::SysConfig(sys_func) => sys_func,
                    _ => {
                        drop(recorder);
                        debt.fulfill(err_dandelion!(DandelionError::ConfigMissmatch));
                        continue;
                    }
                };

                let input_option = metadata.input_sets[0]
                    .1
                    .as_ref()
                    .and_then(|static_set| Some(static_set.clone()))
                    .or_else(|| input_sets[0].take());
                if let Some(request_set) = input_option {
                    match system_function {
                        SystemFunction::HTTP => {
                            tokio::spawn(run_http_request(
                                alternative.context_size,
                                request_set,
                                http_client.clone(),
                                metadata,
                                debt,
                                recorder,
                            ));
                        }
                        SystemFunction::MEMCACHED => {
                            tokio::spawn(run_memcached_request(
                                alternative.context_size,
                                request_set,
                                metadata,
                                debt,
                                recorder,
                            ));
                        }
                        #[allow(unreachable_patterns)]
                        _ => {
                            drop(recorder);
                            debt.fulfill(err_dandelion!(DandelionError::MalformedConfig));
                        }
                    };
                } else {
                    drop(recorder);
                    debt.fulfill(err_dandelion!(DandelionError::MalformedSystemFuncArg(
                        String::from("No request set",)
                    )));
                }
                continue;
            }
            WorkToDo::Shutdown(_) => {
                let _ = worker_lock.write_owned().await;
                return debt;
            }
        }
    }
}

fn outer_engine(core_id: u8, queue: Box<dyn EngineWorkQueue + Send>) {
    // set core affinity
    if !core_affinity::set_for_current(core_affinity::CoreId { id: core_id.into() }) {
        log::error!("core received core id that could not be set");
        return;
    }
    let runtime = Builder::new_multi_thread()
        .on_thread_start(move || {
            if !set_for_current(core_affinity::CoreId { id: core_id.into() }) {
                return;
            }
        })
        .worker_threads(1)
        .enable_all()
        .build()
        .or(err_dandelion!(DandelionError::EngineError))
        .unwrap();
    let debt = runtime.block_on(engine_loop(queue));
    drop(runtime);
    debt.fulfill(Ok(WorkDone::Resources(vec![ComputeResource::CPU(core_id)])));
}

pub struct ReqwestDriver {}

impl Driver for ReqwestDriver {
    fn start_engine(
        &self,
        resource: ComputeResource,
        queue: Box<dyn EngineWorkQueue + Send>,
    ) -> DandelionResult<()> {
        log::debug!("Starting hyper engine");
        let core_id = match resource {
            ComputeResource::CPU(core) => core,
            _ => return err_dandelion!(DandelionError::EngineResourceError),
        };
        // check that core is available
        let available_cores = match core_affinity::get_core_ids() {
            None => return err_dandelion!(DandelionError::EngineResourceError),
            Some(cores) => cores,
        };
        if !available_cores
            .iter()
            .find(|x| x.id == usize::from(core_id))
            .is_some()
        {
            return err_dandelion!(DandelionError::EngineResourceError);
        }
        std::thread::spawn(move || outer_engine(core_id, queue));
        return Ok(());
    }

    fn parse_function(
        &self,
        function_path: String,
        static_domain: &Box<dyn crate::memory_domain::MemoryDomain>,
    ) -> DandelionResult<Function> {
        if function_path.len() != 0 {
            return err_dandelion!(DandelionError::CalledSystemFuncParser);
        }
        return Ok(Function {
            requirements: crate::DataRequirementList {
                input_requirements: vec![],
                static_requirements: vec![],
            },
            context: Arc::new(static_domain.acquire_context(0)?),
            config: FunctionConfig::SysConfig(SystemFunction::HTTP),
        });
    }
}

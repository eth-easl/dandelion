use crate::memory_domain::{
    system_domain::{system_context_write_from_bytes, SystemContext},
    ContextType,
};
use crate::{
    function_driver::{
        ComputeResource, Driver, Function, FunctionConfig, SystemFunction, WorkDone, WorkQueue,
        WorkToDo,
    },
    memory_domain::{self, Context, ContextTrait},
    promise::Debt,
    DataItem, DataSet, Position,
};
use bytes::Buf;
use bytes::Bytes;
use core_affinity::set_for_current;
use dandelion_commons::{
    records::{RecordPoint, Recorder},
    DandelionError, DandelionResult,
};
use futures::{FutureExt, StreamExt};
use http::{version::Version as HttpVersion, HeaderName, HeaderValue, Method as HttpMethod};
use log::{debug, error, warn};
use memcache::Client as MemcachedClient;
use reqwest::{header::HeaderMap, Client as HttpClient};
use std::{collections::BTreeMap, sync::Arc};
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
                return Err(DandelionError::InvalidSystemFuncArg(String::from(
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
                return Err(DandelionError::InvalidSystemFuncArg(format!(
                    "Unsupported Method: {}",
                    method_string
                )))
            }
            _ => {
                return Err(DandelionError::MalformedSystemFuncArg(String::from(
                    "No method found",
                )))
            }
        };

        let uri = String::from(request_iter.next().ok_or(
            DandelionError::MalformedSystemFuncArg(String::from("No uri in request")),
        )?);

        let version = match request_iter.next() {
            Some(version_string) if version_string == "HTTP/0.9" => HttpVersion::HTTP_09,
            Some(version_string) if version_string == "HTTP/1.0" => HttpVersion::HTTP_10,
            Some(version_string) if version_string == "HTTP/1.1" => HttpVersion::HTTP_11,
            Some(version_string) if version_string == "HTTP/2.0" => HttpVersion::HTTP_2,
            Some(version_string) if version_string == "HTTP/3.0" => HttpVersion::HTTP_3,
            Some(version_string) => {
                return Err(DandelionError::InvalidSystemFuncArg(format!(
                    "Unkown http version: {}",
                    version_string,
                )))
            }
            None => {
                return Err(DandelionError::MalformedSystemFuncArg(String::from(
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
                .ok_or(DandelionError::MalformedSystemFuncArg(String::from(
                    "Header line does not contain \':\'",
                )))?;
            let (key, value) = header_line.split_at(split_index);
            let header_key = HeaderName::from_bytes(key).or(Err(
                DandelionError::MalformedSystemFuncArg(String::from("Header key not utf-8")),
            ))?;
            let header_value = HeaderValue::from_bytes(&value[1..]).or(Err(
                DandelionError::MalformedSystemFuncArg(String::from(
                    "Header value not utf-8 conformant",
                )),
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
                return Err(DandelionError::InvalidSystemFuncArg(String::from(
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
                return Err(DandelionError::InvalidSystemFuncArg(format!(
                    "Unsupported Method: {}",
                    method_string
                )))
            }
            _ => {
                return Err(DandelionError::MalformedSystemFuncArg(String::from(
                    "No method found",
                )))
            }
        };

        let uri = String::from(request_iter.next().ok_or(
            DandelionError::MalformedSystemFuncArg(String::from("No uri in request")),
        )?);
        let memcached_identifier = match request_iter.next() {
            Some(identifier) => identifier.to_string(),
            None => {
                return Err(DandelionError::MalformedSystemFuncArg(String::from(
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

fn parse_requests<RequestType: Request>(context: &Context) -> DandelionResult<Vec<RequestType>> {
    let request_set = match context.content.iter().find(|set_option| {
        if let Some(set) = set_option {
            return set.ident == "request";
        } else {
            return false;
        }
    }) {
        Some(Some(set)) => set,
        _ => {
            return Err(DandelionError::MalformedSystemFuncArg(String::from(
                "No request set",
            )))
        }
    };
    let request_info: DandelionResult<Vec<RequestType>> = request_set
        .buffers
        .iter()
        .map(|set_item| {
            let mut request_buffer = Vec::with_capacity(set_item.data.size);
            request_buffer.resize(set_item.data.size, 0);
            context.read(set_item.data.offset, &mut request_buffer)?;
            RequestType::from_raw(request_buffer, set_item.ident.clone(), set_item.key)
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
            return Err(DandelionError::MalformedSystemFuncArg(String::from(
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
            return Err(DandelionError::MalformedSystemFuncArg(format!(
                "{:?}",
                http_error
            )));
        }
    };
    let response = match client.execute(request).await {
        Ok(resp) => resp,
        Err(_) => return Err(DandelionError::SystemFuncResponseError),
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
        Err(_) => return Err(DandelionError::SystemFuncResponseError),
    };

    if let Some(content_len) = content_length {
        if content_len != body.len() {
            return Err(DandelionError::SystemFuncResponseError);
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
        Err(_) => return Err(DandelionError::MemcachedError),
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
                    return Err(DandelionError::MemcachedError);
                }
                Err(e) => {
                    debug!("Failed to start Memcached_request task with: {:?}", e);
                    return Err(DandelionError::MemcachedError);
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
                    return Err(DandelionError::MemcachedError);
                }
                Err(e) => {
                    debug!("Failed to start Memcached_request task with: {:?}", e);
                    return Err(DandelionError::MemcachedError);
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

fn response_write(context: &mut Context, response: ResponseInformation) -> DandelionResult<()> {
    let ResponseInformation {
        item_name,
        item_key,
        preamble,
        mut body,
    } = response;

    let preamble_len = preamble.len();
    let body_len = body.len();
    let response_len = preamble_len + body_len;
    // allocate space in the context for the entire response
    let response_start = context.get_free_space(response_len, 128)?;

    match &mut context.context {
        ContextType::System(destination_ctxt) => {
            let preamble_bytes = bytes::Bytes::from(preamble.into_bytes());
            system_context_write_from_bytes(
                destination_ctxt,
                preamble_bytes,
                response_start,
                preamble_len,
            );
            system_context_write_from_bytes(
                destination_ctxt,
                body.clone(),
                response_start + preamble_len,
                body_len,
            );
        }
        _ => {
            context.write(response_start, preamble.as_bytes())?;
            let mut bytes_read = 0;
            while bytes_read < body_len {
                let chunk = body.chunk();
                let reading = chunk.len();
                context.write(response_start + preamble_len + bytes_read, chunk)?;
                body.advance(reading);
                bytes_read += reading;
            }
            assert_eq!(
                0,
                body.remaining(),
                "Body should have non remaining as we have read the amount given as len in the beginning"
            );
        }
    }

    if let Some(response_set) = &mut context.content[0] {
        response_set.buffers.push(DataItem {
            ident: item_name.clone(),
            key: item_key,
            data: Position {
                offset: response_start,
                size: response_len,
            },
        })
    }
    if let Some(body_set) = &mut context.content[1] {
        body_set.buffers.push(DataItem {
            ident: item_name,
            key: item_key,
            data: Position {
                offset: response_start + preamble_len,
                size: response_len - preamble_len,
            },
        })
    }

    return Ok(());
}

fn responses_write(
    context_size: usize,
    output_set_names: Arc<Vec<String>>,
    debt: Debt,
    mut recorder: Recorder,
    responses: Vec<ResponseInformation>,
) {
    let mut out_context = Context::new(
        ContextType::System(Box::new(SystemContext {
            local_offset_to_data_position: BTreeMap::new(),
            size: context_size,
        })),
        context_size,
    );

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
            .map(|response| response_write(&mut out_context, response))
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
    context: Context,
    client: HttpClient,
    output_set_names: Arc<Vec<String>>,
    debt: Debt,
    recorder: Recorder,
) -> () {
    let request_vec = match parse_requests(&context) {
        Ok(request) => request,
        Err(err) => {
            debt.fulfill(Err(err));
            return;
        }
    };
    let context_size = context.size;

    // get rid of systems context to drop references to old contexts before starting to do requests
    drop(context);

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

    responses_write(context_size, output_set_names, debt, recorder, responses);
}

async fn run_memcached_request(
    context: Context,
    output_set_names: Arc<Vec<String>>,
    debt: Debt,
    recorder: Recorder,
) -> () {
    let request_vec = match parse_requests(&context) {
        Ok(request) => request,
        Err(err) => {
            debt.fulfill(Err(err));
            return;
        }
    };
    let context_size = context.size;

    // get rid of systems context to drop references to old contexts before starting to do requests
    drop(context);

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

    responses_write(context_size, output_set_names, debt, recorder, responses);
}

async fn engine_loop(queue: Box<dyn WorkQueue + Send>) -> Debt {
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
                config,
                context,
                output_sets,
                mut recorder,
            } => {
                recorder.record(RecordPoint::EngineStart);

                // let result = engine_state.run(config, context, output_sets);
                log::debug!("Reqwest engine running function");
                let function = match config {
                    FunctionConfig::SysConfig(sys_func) => sys_func,
                    _ => {
                        drop(recorder);
                        debt.fulfill(Err(DandelionError::ConfigMissmatch));
                        continue;
                    }
                };
                match function {
                    SystemFunction::HTTP => {
                        tokio::spawn(run_http_request(
                            context,
                            http_client.clone(),
                            output_sets,
                            debt,
                            recorder,
                        ));
                    }
                    SystemFunction::MEMCACHED => {
                        tokio::spawn(run_memcached_request(context, output_sets, debt, recorder));
                    }
                    #[allow(unreachable_patterns)]
                    _ => {
                        drop(recorder);
                        debt.fulfill(Err(DandelionError::MalformedConfig));
                    }
                };
                continue;
            }
            WorkToDo::TransferArguments {
                source,
                mut destination,
                destination_set_index,
                destination_allignment,
                destination_item_index,
                destination_set_name,
                source_set_index,
                source_item_index,
                mut recorder,
            } => {
                recorder.record(RecordPoint::TransferStart);

                let transfer_result = memory_domain::transfer_data_item(
                    &mut destination,
                    source,
                    destination_set_index,
                    destination_allignment,
                    destination_item_index,
                    destination_set_name.as_str(),
                    source_set_index,
                    source_item_index,
                );

                recorder.record(RecordPoint::TransferEnd);

                let transfer_return = transfer_result.and(Ok(WorkDone::Context(destination)));
                drop(recorder);
                debt.fulfill(transfer_return);
                continue;
            }
            WorkToDo::ParsingArguments {
                driver,
                path,
                static_domain,
                mut recorder,
            } => {
                recorder.record(RecordPoint::ParsingStart);
                let function_result = driver.parse_function(path, &static_domain);
                recorder.record(RecordPoint::ParsingEnd);
                drop(recorder);
                match function_result {
                    Ok(function) => debt.fulfill(Ok(WorkDone::Function(function))),
                    Err(err) => debt.fulfill(Err(err)),
                }
                continue;
            }
            WorkToDo::LoadingArguments {
                function,
                domain,
                ctx_size,
                mut recorder,
            } => {
                recorder.record(RecordPoint::LoadStart);
                let load_result = function.load(&domain, ctx_size);
                recorder.record(RecordPoint::LoadEnd);
                drop(recorder);
                match load_result {
                    Ok(context) => debt.fulfill(Ok(WorkDone::Context(context))),
                    Err(err) => debt.fulfill(Err(err)),
                }
                continue;
            }
            WorkToDo::Shutdown() => {
                let _ = worker_lock.write_owned().await;
                return debt;
            }
        }
    }
}

fn outer_engine(core_id: u8, queue: Box<dyn WorkQueue + Send>) {
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
        .or(Err(DandelionError::EngineError))
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
        queue: Box<dyn WorkQueue + Send>,
    ) -> DandelionResult<()> {
        log::debug!("Starting hyper engine with Resource: {:?}", resource);
        let core_id = match resource {
            ComputeResource::CPU(core) => core,
            _ => return Err(DandelionError::EngineResourceError),
        };
        std::thread::spawn(move || outer_engine(core_id, queue));
        return Ok(());
    }

    fn parse_function(
        &self,
        function_path: String,
        static_domain: &Box<dyn crate::memory_domain::MemoryDomain>,
    ) -> DandelionResult<Function> {
        if function_path.len() != 0 {
            return Err(DandelionError::CalledSystemFuncParser);
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

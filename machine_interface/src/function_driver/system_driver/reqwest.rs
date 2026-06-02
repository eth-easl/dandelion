use crate::{
    composition::{CompositionSet, ItemData},
    function_driver::{
        functions::{Function, FunctionConfig},
        system_driver::{IoData, SystemFunction},
        ComputeResource, Driver, EngineWorkQueue, WorkDone, WorkToDo,
    },
    memory_domain::{
        bytes_context::BytesContext, read_only::ReadOnlyContext, Context, ContextTrait, ContextType,
    },
    promise::Debt,
    DataItem, Position,
};
use bytes::Bytes;
use core_affinity::set_for_current;
use dandelion_commons::{
    dandelion_err, err_dandelion, records::RecordPoint, DandelionError, DandelionResult,
};
use futures::{stream::FuturesUnordered, StreamExt};
use http::{version::Version as HttpVersion, HeaderName, HeaderValue, Method as HttpMethod};
use itertools::Itertools;
use log::{debug, error, trace, warn};
use memcache::Client as MemcachedClient;
use reqwest::{header::HeaderMap, Client as HttpClient};
use std::{
    future::Future,
    sync::{Arc, OnceLock},
};
use tokio::{
    runtime::Builder,
    sync::{AcquireError, OwnedSemaphorePermit, RwLock, Semaphore},
};

trait Request
where
    Self: Sized,
{
    fn from_raw(raw_request: Vec<u8>) -> DandelionResult<Self>;
}

/// Stores requestInformation for http
struct HttpRequest {
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
    method: MemcachedMethod,
    uri: String,
    memcached_identifier: String,
    body: Vec<u8>,
}

impl Request for HttpRequest {
    fn from_raw(mut raw_request: Vec<u8>) -> DandelionResult<Self> {
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
        trace!("Reqwest body: {:?}", body);

        Ok(HttpRequest {
            method,
            uri,
            version,
            headermap,
            body,
        })
    }
}

impl Request for MemcachedRequest {
    fn from_raw(mut raw_request: Vec<u8>) -> DandelionResult<Self> {
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
        trace!("Reqwest body: {:?}", body);

        return Ok(Self {
            method,
            uri,
            memcached_identifier,
            body,
        });
    }
}

fn parse_request<RequestType: Request>(
    position: Position,
    context: Arc<Context>,
) -> DandelionResult<RequestType> {
    let Position { offset, size } = position;
    let mut request_buffer = Vec::with_capacity(size);
    let mut bytes_read = 0;
    while bytes_read < size {
        let chunk = context.get_chunk_ref(offset + bytes_read, size - bytes_read)?;
        request_buffer.extend_from_slice(chunk);
        bytes_read += chunk.len();
    }
    RequestType::from_raw(request_buffer)
}

async fn http_request(
    client: HttpClient,
    position: Position,
    context: Arc<Context>,
) -> DandelionResult<Vec<Arc<Context>>> {
    let HttpRequest {
        method,
        uri,
        version,
        headermap,
        body,
    } = parse_request(position, context)?;

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
    let mut response = match client.execute(request).await {
        Ok(resp) => resp,
        Err(repsonse_error) => {
            debug!("response error: {}", repsonse_error);
            return err_dandelion!(DandelionError::SystemFuncResponseError);
        }
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

    let mut body_length = 0;
    let mut body = Vec::new();
    loop {
        match response.chunk().await {
            Ok(Some(frame)) => {
                body_length += frame.len();
                body.push(frame)
            }
            Ok(None) => break,
            Err(_) => return err_dandelion!(DandelionError::SystemFuncResponseError),
        }
    }

    if let Some(content_len) = content_length {
        if content_len != body_length {
            return err_dandelion!(DandelionError::SystemFuncResponseError);
        }
    }

    let header_context = Arc::new(ReadOnlyContext::new(
        preamble.into_bytes().into_boxed_slice(),
    )?);

    let body_context = Arc::new(Context::new(
        ContextType::Bytes(Box::new(BytesContext::new(body))),
        body_length,
    ));

    Ok(vec![header_context, body_context])
}

async fn memcached_request(
    position: Position,
    context: Arc<Context>,
) -> DandelionResult<Vec<Arc<Context>>> {
    let MemcachedRequest {
        method,
        uri,
        memcached_identifier,
        body,
    } = parse_request(position, context)?;

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
    // Default item size limit is 1MB. If item is larger, we ignore it
    let (preamble, response_body) = match method {
        MemcachedMethod::SET => {
            // Assemble value to set
            // TODO Make timeout a parameter

            let result = tokio::task::spawn_blocking(move || {
                memcached_client.set(&memcached_identifier, &*body, 3600)
            })
            .await;

            match result {
                Ok(Ok(_)) => (String::from("SUCCESS"), Bytes::from(vec![0u8])),
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
            let debug_identifier = memcached_identifier.clone();
            let result = tokio::task::spawn_blocking(move || {
                memcached_client.get::<Vec<u8>>(&memcached_identifier)
            })
            .await;

            match result {
                Ok(Ok(Some(response))) => (String::from("SUCCESS"), Bytes::from(response)),
                Ok(Ok(None)) => {
                    debug!("Key {} did not exist on memcached server", debug_identifier);
                    (String::from("ABSENT"), Bytes::from(vec![0u8]))
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

    let body_length = response_body.len();

    let header_context = Arc::new(ReadOnlyContext::new(
        preamble.into_bytes().into_boxed_slice(),
    )?);

    let body_context = Arc::new(Context::new(
        ContextType::Bytes(Box::new(BytesContext::new(vec![response_body]))),
        body_length,
    ));

    Ok(vec![header_context, body_context])
}

async fn resolve_item<Ticket: Future<Output = Result<OwnedSemaphorePermit, AcquireError>>>(
    outer_set_index: usize,
    mut item: DataItem,
    data: ItemData,
    client: HttpClient,
    permit: Option<Ticket>,
) -> DandelionResult<(usize, DataItem, ItemData)> {
    // hold on to ticket so it gets dropped at the end
    let _ticket = if let Some(permit) = permit {
        Some(permit.await.unwrap())
    } else {
        None
    };

    debug!("Resolving item {:?}, data {:?}", item, data);
    match data {
        ItemData::LocalData(_) => Ok((outer_set_index, item, data)),
        ItemData::RemoteData(remote_data) => {
            let _remote_data_clone = remote_data.clone(); // avoid potential drop before resolve finishes
            let client = crate::composition::get_remote_data_client()?;
            let (context, position) = client.resolve_remote_data(remote_data).await?;
            item.data = position;
            Ok((outer_set_index, item, ItemData::LocalData(context)))
        }
        ItemData::IoData(io_data) => {
            let IoData {
                mut original_position,
                original_data,
                resolved,
                function,
                set_index,
            } = io_data;
            // first need to check if original data was local or we still need to fetch that.
            let (mut item, request_input) = match *original_data {
                ItemData::LocalData(context) => (item, context),
                _ => match Box::pin(resolve_item(
                    set_index,
                    item,
                    *original_data,
                    client.clone(),
                    None::<Ticket>,
                ))
                .await?
                {
                    (_, item, ItemData::LocalData(context)) => {
                        original_position = item.data;
                        (item, context)
                    }
                    _ => unreachable!(),
                },
            };
            match function {
                SystemFunction::HTTP => {
                    let outputs = resolved
                        .get_or_init(move || http_request(client, original_position, request_input))
                        .await;
                    let context = match outputs {
                        Ok(context_vec) => context_vec[set_index].clone(),
                        Err(err) => return Err(err.clone()),
                    };
                    item.data.offset = 0;
                    item.data.size = context.size;
                    Ok((outer_set_index, item, ItemData::LocalData(context)))
                }
                SystemFunction::MEMCACHED => {
                    let outputs = resolved
                        .get_or_init(move || memcached_request(original_position, request_input))
                        .await;
                    let context = match outputs {
                        Ok(context_vec) => context_vec[set_index].clone(),
                        Err(err) => return Err(err.clone()),
                    };
                    item.data.offset = 0;
                    item.data.size = context.size;
                    Ok((outer_set_index, item, ItemData::LocalData(context)))
                }
            }
        }
    }
}

async fn resolve_all_sets(
    client: HttpClient,
    input_sets: Vec<Option<CompositionSet>>,
    mut permits: Vec<impl Future<Output = Result<OwnedSemaphorePermit, AcquireError>>>,
) -> DandelionResult<Vec<Option<CompositionSet>>> {
    // check if teh function id is for a system function
    let mut sets_vec = Vec::with_capacity(input_sets.len());
    sets_vec.resize(input_sets.len(), Vec::new());
    let set_names = input_sets
        .iter()
        .map(|set_option| set_option.as_ref().map(|set| set.get_name().clone()))
        .collect_vec();
    let mut new_futures = FuturesUnordered::new();
    for (set_index, set_option) in input_sets.into_iter().enumerate() {
        if let Some(set) = set_option {
            for (item, data) in set.into_iter() {
                let permit = permits.pop();
                assert!(permit.is_some());
                debug!("pushing item {:?} for set {}", item, set_index);
                new_futures.push(resolve_item(set_index, item, data, client.clone(), permit));
            }
        }
    }

    while let Some(item_result) = new_futures.next().await {
        let (set_index, item, data) = item_result?;
        sets_vec[set_index].push((item, data));
    }

    Ok(set_names
        .into_iter()
        .zip(sets_vec.into_iter())
        .map(|(name, items)| name.and_then(|name| CompositionSet::from_item_list(name, items)))
        .collect_vec())
}

/// Number of concurrent requests a single IO core should be handling
pub const DEFAULT_CONCURRENCY_LIMIT: usize = 15;
pub static CONCURRENCY_LIMIT: OnceLock<usize> = OnceLock::new();

async fn engine_loop(queue: impl EngineWorkQueue + Clone + Send + 'static) -> Debt {
    log::debug!("Reqwest engine Init");
    let http_client = HttpClient::new();

    let concurrency_limit = CONCURRENCY_LIMIT.get_or_init(|| DEFAULT_CONCURRENCY_LIMIT);
    let semaphore = Arc::new(Semaphore::new(*concurrency_limit));
    let worker_lock = Arc::new(RwLock::new(()));

    loop {
        let ticket = semaphore.clone().acquire_owned().await.unwrap();
        let (args, debt) = queue.get_io_engine_args().await;

        match args {
            WorkToDo::FunctionArguments {
                function_id,
                function_alternatives,
                input_sets,
                metadata,
                caching, // ignoreing caching for system functions
                mut recorder,
            } => {
                let client_clone = http_client.clone();
                let queue_clone = queue.clone();
                let item_number = input_sets
                    .iter()
                    .filter_map(|set| set.as_ref().map(|set| set.len()))
                    .sum();
                let mut ticket_vec = Vec::with_capacity(item_number);
                for _ in 0..item_number {
                    ticket_vec.push(semaphore.clone().acquire_owned());
                }
                tokio::spawn(async move {
                    debug!("Resolving references for call to {}", function_id);
                    recorder.record(RecordPoint::FetchingStart);
                    let sets_result = resolve_all_sets(client_clone, input_sets, ticket_vec).await;
                    recorder.record(RecordPoint::FetchingEnd);
                    match sets_result {
                        Err(err) => debt.fulfill(Err(err)),
                        Ok(sets) => queue_clone.requeu_engine_args(
                            WorkToDo::FunctionArguments {
                                function_id,
                                function_alternatives,
                                input_sets: sets,
                                metadata,
                                caching,
                                recorder,
                            },
                            debt,
                        ),
                    };
                });
                drop(ticket);
            }
            WorkToDo::SetsToResolve { input_sets } => {
                let client_clone = http_client.clone();
                let item_number = input_sets
                    .iter()
                    .filter_map(|set| set.as_ref().map(|set| set.len()))
                    .sum();
                let mut ticket_vec = Vec::with_capacity(item_number);
                for _ in 0..item_number {
                    ticket_vec.push(semaphore.clone().acquire_owned());
                }
                tokio::spawn(async move {
                    // TODO: check if there is nicer way to do this
                    // Tried futures join_all and OrderedSet, but they seem to hang for a while before resolving, unclear why
                    // Also tried with join handles in a vec, let to the same delay
                    let sets_result = resolve_all_sets(client_clone, input_sets, ticket_vec).await;
                    match sets_result {
                        Ok(sets) => debt.fulfill(Ok(WorkDone::CompositionSet(sets))),
                        Err(err) => debt.fulfill(Err(err)),
                    }
                });
                drop(ticket);
            }
            WorkToDo::RemoteToDelete { remote_data } => {
                tokio::spawn(async move {
                    match crate::composition::get_remote_data_client() {
                        Ok(client) => {
                            let result = client
                                .delete_remote_data(remote_data)
                                .await
                                .map(|_| WorkDone::RemoteDeleted);
                            debt.fulfill(result);
                        }
                        Err(err) => {
                            debt.fulfill(Err(err));
                        }
                    };
                    drop(ticket);
                });
            }
            WorkToDo::Shutdown(_) => {
                let _ = worker_lock.write_owned().await;
                return debt;
            }
        }
    }
}

fn outer_engine(core_id: u8, queue: impl EngineWorkQueue + Clone + Send + 'static) {
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
        queue: impl EngineWorkQueue + Clone + Send + 'static,
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

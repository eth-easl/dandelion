use crate::{
    composition::{CompositionSet, ItemData},
    function_driver::{
        functions::{Function, FunctionConfig},
        system_driver::{
            cache::{CacheRegistry, HttpCacheEntry},
            notify_io_data_cache, IoData, SystemFunction,
        },
        ComputeResource, Driver, EngineWorkQueue, WorkDone, WorkToDo,
    },
    memory_domain::{
        bytes_context::BytesContext, read_only::ReadOnlyContext, Context, ContextTrait, ContextType,
    },
    promise::Debt,
    DataItem, Position,
};
use bytes::Bytes;
use dandelion_commons::{
    dandelion_err, err_dandelion, records::RecordPoint, try_with_capacity, DandelionError,
    DandelionResult,
};
use futures::{stream::FuturesUnordered, StreamExt};
use http::{version::Version as HttpVersion, HeaderName, HeaderValue, Method as HttpMethod};
use itertools::Itertools;
use log::{debug, error, trace, warn};
use memcache::Client as MemcachedClient;
use reqwest::{header::HeaderMap, Client as HttpClient};
use std::{
    hash::{DefaultHasher, Hash, Hasher},
    sync::{Arc, OnceLock},
};
use tokio::{
    spawn,
    sync::{OnceCell, OwnedSemaphorePermit, Semaphore},
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
    let http_request = parse_request::<HttpRequest>(position, context)?;
    let cache_key = http_request.cache_key();
    let HttpRequest {
        method,
        uri,
        version,
        headermap,
        body,
    } = http_request;

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

    if let Some(cache_key) = cache_key {
        notify_io_data_cache(
            cache_key,
            [header_context.clone(), body_context.clone()].to_vec(),
        );
    }
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

async fn resolve_item(
    outer_set_index: usize,
    mut item: DataItem,
    data: ItemData,
    client: HttpClient,
    permit: Option<OwnedSemaphorePermit>,
    resolve_iodata: bool,
) -> DandelionResult<(usize, DataItem, ItemData)> {
    debug!("Resolving item {:?}, data {:?}", item, data);
    let results = match data {
        ItemData::LocalData(_) => Ok((outer_set_index, item, data)),
        ItemData::RemoteData(remote_data) => {
            let _remote_data_clone = remote_data.clone(); // avoid potential drop before resolve finishes
            let client = crate::composition::get_remote_data_client()?;
            let (context, position) = client.resolve_remote_data(remote_data).await?;
            item.data = position;
            Ok((outer_set_index, item, ItemData::LocalData(context)))
        }
        ItemData::IoData(io_data) => {
            if !resolve_iodata {
                return Ok((outer_set_index, item, ItemData::IoData(io_data)));
            }
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
                    None,
                    true,
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
    };
    drop(permit);
    results
}

async fn resolve_all_sets(
    client: HttpClient,
    input_sets: Vec<Option<CompositionSet>>,
    semaphore: Arc<Semaphore>,
    ticket: OwnedSemaphorePermit,
    result_sender: impl FnOnce(DandelionResult<Vec<Option<CompositionSet>>>) + 'static + Send,
    resolve_iodata: bool,
) {
    // drop ticket so at least one will be available for the new tasks we spawn
    drop(ticket);
    // check if the function id is for a system function
    let input_set_number = input_sets.len();
    let mut sets_vec = Vec::with_capacity(input_sets.len());
    let set_names = input_sets
        .iter()
        .map(|set_option| set_option.as_ref().map(|set| set.get_name().clone()))
        .collect_vec();
    let mut new_futures = FuturesUnordered::new();
    for (set_index, set_option) in input_sets.into_iter().enumerate() {
        if let Some(set) = set_option {
            for (item, data) in set.into_iter() {
                let permit = semaphore.clone().acquire_owned().await.unwrap();
                new_futures.push(spawn(resolve_item(
                    set_index,
                    item,
                    data,
                    client.clone(),
                    Some(permit),
                    resolve_iodata,
                )));
            }
        }
    }

    spawn(async move {
        sets_vec.resize(input_set_number, Vec::new());
        let mut result = None;
        while let Some(item_result) = new_futures.next().await {
            let item_tuple = item_result.unwrap();
            match item_tuple {
                Ok((set_index, item, data)) => sets_vec[set_index].push((item, data)),
                Err(err) => {
                    result = Some(err);
                    break;
                }
            }
        }

        if let Some(error) = result {
            result_sender(Err(error));
        } else {
            let sets = set_names
                .into_iter()
                .zip(sets_vec.into_iter())
                .map(|(name, items)| {
                    name.and_then(|name| CompositionSet::from_item_list(name, items))
                })
                .collect_vec();
            result_sender(Ok(sets));
        }
    });
}

/// Number of concurrent requests a single IO core should be handling
pub const DEFAULT_CONCURRENCY_LIMIT: usize = 15;
pub static CONCURRENCY_LIMIT: OnceLock<usize> = OnceLock::new();

async fn engine_loop(queue: impl EngineWorkQueue + Clone + Send + 'static) -> Debt {
    log::debug!("Reqwest engine Init");
    let http_client = HttpClient::new();

    let concurrency_limit = CONCURRENCY_LIMIT.get_or_init(|| DEFAULT_CONCURRENCY_LIMIT);
    let semaphore = Arc::new(Semaphore::new(*concurrency_limit));
    // let worker_lock = Arc::new(RwLock::new(()));

    loop {
        let ticket = semaphore.clone().acquire_owned().await.unwrap();
        debug!("IO engine loop has ticket");
        let (args, debt) = queue.get_io_engine_args().await;
        debug!("IO engine loop has work");
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
                debug!("Resolving references for call to {}", function_id);
                recorder.record(RecordPoint::FetchingStart);

                resolve_all_sets(
                    client_clone,
                    input_sets,
                    semaphore.clone(),
                    ticket,
                    move |sets_result| {
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
                    },
                    true,
                )
                .await;
            }
            WorkToDo::FunctionReferences {
                function,
                input_sets,
                recorder,
            } => {
                let client_clone = http_client.clone();
                let semaphore = semaphore.clone();
                tokio::spawn(async move {
                    match convert_to_references(
                        function,
                        input_sets,
                        recorder,
                        client_clone,
                        semaphore.clone(),
                        ticket,
                    )
                    .await
                    {
                        Ok(sets) => debt.fulfill(Ok(WorkDone::CompositionSet(sets))),
                        Err(err) => debt.fulfill(Err(err)),
                    };
                });
            }
            WorkToDo::SetsToResolve { input_sets } => {
                let client_clone = http_client.clone();
                resolve_all_sets(
                    client_clone,
                    input_sets,
                    semaphore.clone(),
                    ticket,
                    |sets_result| {
                        match sets_result {
                            Ok(sets) => debt.fulfill(Ok(WorkDone::CompositionSet(sets))),
                            Err(err) => debt.fulfill(Err(err)),
                        };
                    },
                    false,
                )
                .await;
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
                // TODO: if we still want this, get the current index from affinity.
                // Then block on the correct blocker
                unimplemented!("Shutdown for IO cores currently not implemented");
            }
        }
    }
}

pub struct ReqwestDriver {}

impl Driver for ReqwestDriver {
    fn start_engine(
        &self,
        resource: ComputeResource,
        queue: impl EngineWorkQueue + Clone + Send + 'static,
    ) -> DandelionResult<()> {
        debug!("Starting hyper engine, by unblocking core on async runtime");
        let core_id = match resource {
            ComputeResource::CPU(core) => core,
            _ => return err_dandelion!(DandelionError::EngineResourceError),
        };
        let global_runtime = &crate::async_runtime::GLOBAL_RUNTIME;
        debug!("have global runtime reference");
        global_runtime.spawn(engine_loop(queue));
        debug!("spawned task on global runtiome");
        global_runtime.add_core(core_id.into());
        debug!("sent wake up to core {}", core_id);
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

static HTTP_CACHE_REGISTRY: OnceLock<CacheRegistry> = OnceLock::new();

pub fn insert_http_cache_entry(key: u64, value: HttpCacheEntry) {
    HTTP_CACHE_REGISTRY
        .get_or_init(CacheRegistry::new)
        .insert(key, value);
}

fn get_http_cache_entry(key: u64) -> Option<HttpCacheEntry> {
    HTTP_CACHE_REGISTRY
        .get_or_init(CacheRegistry::new)
        .get(key)
        .clone()
}

impl HttpRequest {
    fn cache_key(&self) -> Option<u64> {
        let mut hasher = DefaultHasher::new();
        if self.method == HttpMethod::GET {
            self.uri.hash(&mut hasher);
            for (key, value) in self.headermap.iter() {
                key.hash(&mut hasher);
                value.hash(&mut hasher);
            }
            Some(hasher.finish())
        } else {
            None
        }
    }
}

async fn convert_to_references(
    function: SystemFunction,
    mut input_sets: Vec<Option<CompositionSet>>,
    mut recorder: dandelion_commons::records::Recorder,
    client: HttpClient,
    semaphore: Arc<Semaphore>,
    ticket: OwnedSemaphorePermit,
) -> DandelionResult<Vec<Option<CompositionSet>>> {
    // check that the function id contains string correcpsonding to system function
    debug_assert_eq!(
        1,
        input_sets.len(),
        "all current IO functions expect a single input set"
    );

    if let SystemFunction::HTTP = function {
        // resolve all remote data in the input set
        let (result_sender, result_receiver) = tokio::sync::oneshot::channel();
        recorder.record(RecordPoint::ResolveSetsStart);
        resolve_all_sets(
            client,
            input_sets.clone(),
            semaphore,
            ticket,
            |result| {
                result_sender.send(result).unwrap();
            },
            false,
        )
        .await;
        let mut resolved_sets = result_receiver.await.unwrap()?;
        recorder.record(RecordPoint::ResolveSetsEnd);

        let mut output_vec = try_with_capacity!(Vec, 2)?;
        output_vec.resize(2, None);

        if let Some(input_set) = resolved_sets[0].take() {
            recorder.record(RecordPoint::CacheLookupStart);
            let input_set_name = input_set.get_name().clone();
            let mut out_0_list = try_with_capacity!(Vec, input_set.len())?;
            let mut out_1_list = try_with_capacity!(Vec, input_set.len())?;
            let mut cache_hit = true;
            for (item, data) in input_set {
                let new_item = DataItem {
                    data: crate::Position { offset: 0, size: 0 },
                    ident: item.ident.clone(),
                    key: item.key,
                };

                if let ItemData::LocalData(context) = data {
                    let http_request = parse_request::<HttpRequest>(item.data, context)?;
                    // println!(
                    //     "Making HTTP request with method {:?}, uri {}, and headers {:?}",
                    //     http_request.method, http_request.uri, http_request.headermap
                    // );
                    if let Some(cache_key) = http_request.cache_key() {
                        if let Some(entry) = get_http_cache_entry(cache_key) {
                            // println!("Cache hit for request with key {}", cache_key);
                            out_0_list.push((new_item.clone(), ItemData::RemoteData(entry.header)));
                            out_1_list.push((new_item, ItemData::RemoteData(entry.body)));
                        } else {
                            // println!("Cache miss for request with key {}", cache_key);
                            cache_hit = false;
                        }
                    } else {
                        // println!("This request is not cacheable");
                    }
                } else {
                    panic!("should have resolved all data to local");
                }
            }
            recorder.record(RecordPoint::CacheLookupEnd);
            output_vec[0] = CompositionSet::from_item_list(input_set_name.clone(), out_0_list);
            output_vec[1] = CompositionSet::from_item_list(input_set_name, out_1_list);
            if cache_hit {
                return Ok(output_vec);
            }
        }
    }

    // go through all input sets and check if there is already a static one, or on in the input data
    let mut output_vec = try_with_capacity!(Vec, 2)?;
    output_vec.resize(2, None);

    if let Some(input_set) = input_sets[0].take() {
        let input_set_name = input_set.get_name().clone();
        let mut out_0_list = try_with_capacity!(Vec, input_set.len())?;
        let mut out_1_list = try_with_capacity!(Vec, input_set.len())?;
        for (item, data) in input_set {
            let new_item = DataItem {
                data: crate::Position { offset: 0, size: 0 },
                ident: item.ident.clone(),
                key: item.key,
            };
            let set_once = Arc::new(OnceCell::new());
            let header_data = IoData {
                original_position: item.data,
                original_data: Box::new(data.clone()),
                resolved: set_once.clone(),
                function,
                set_index: 0,
            };
            let body_data = IoData {
                original_position: item.data,
                original_data: Box::new(data),
                resolved: set_once,
                function,
                set_index: 1,
            };
            out_0_list.push((new_item.clone(), ItemData::IoData(header_data)));
            out_1_list.push((new_item, ItemData::IoData(body_data)));
        }
        output_vec[0] = CompositionSet::from_item_list(input_set_name.clone(), out_0_list);
        output_vec[1] = CompositionSet::from_item_list(input_set_name, out_1_list);
    }
    Ok(output_vec)
}

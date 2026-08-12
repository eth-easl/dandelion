#[cfg(feature = "exactly-once")]
use crate::composition::{IoResolveInput, IoResolveOutcome, IoResolveRequest};
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
    Position,
};
#[cfg(feature = "at-least-once")]
use crate::{
    composition::{
        IoCompletedOutput, IoCompletionOutcome, IoCoordinationCompletion, IoCoordinationKey,
    },
    function_driver::system_driver::{CoordinatedIoData, IoCoordination},
    DataItem,
};
use bytes::Bytes;
use dandelion_commons::{
    dandelion_err, err_dandelion, records::RecordPoint, DandelionError, DandelionResult,
};
use futures::{stream::FuturesUnordered, StreamExt};
use http::{version::Version as HttpVersion, HeaderName, HeaderValue, Method as HttpMethod};
use log::{debug, error, trace, warn};
use memcache::Client as MemcachedClient;
use reqwest::{header::HeaderMap, Client as HttpClient};
use std::{
    collections::{btree_map::Entry, BTreeMap},
    sync::{Arc, OnceLock},
};
use tokio::{
    spawn,
    sync::{OwnedSemaphorePermit, Semaphore},
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

        let method_item = request_iter.next().ok_or_else(|| {
            dandelion_err!(DandelionError::MalformedSystemFuncArg(String::from(
                "No method found",
            )))
        })?;
        let method = HttpMethod::from_bytes(method_item.as_bytes()).map_err(|parsing_err| {
            dandelion_err!(DandelionError::InvalidSystemFuncArg(format!(
                "Invalid HTTP Method: {}",
                parsing_err
            )))
        })?;

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

    let request_builder = client.request(method, &uri);

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

async fn resolve_original_io_data(
    original_position: Position,
    original_data: ItemData,
    client: HttpClient,
) -> DandelionResult<(Position, Arc<Context>)> {
    // first need to check if original data was local or we still need to fetch that.
    match original_data {
        ItemData::LocalData(context) => Ok((original_position, context)),
        ItemData::RemoteData(remote_data) => {
            let remote_client = crate::composition::get_remote_data_client()?;
            remote_client
                .resolve_remote_data(remote_data)
                .await
                .map(|(context, position)| (position, context))
        }
        ItemData::IoData(nested) => Box::pin(resolve_io_item(nested, client)).await,
        #[cfg(feature = "at-least-once")]
        ItemData::CoordinatedIoData(nested) => {
            Box::pin(resolve_checkpointed_io_item(nested, client)).await
        }
    }
}

async fn execute_io<'a>(
    function: SystemFunction,
    resolved: &'a tokio::sync::OnceCell<DandelionResult<Vec<Arc<Context>>>>,
    client: HttpClient,
    input_position: Position,
    input_context: Arc<Context>,
) -> &'a DandelionResult<Vec<Arc<Context>>> {
    resolved
        .get_or_init(move || async move {
            match function {
                SystemFunction::HTTP => http_request(client, input_position, input_context).await,
                SystemFunction::MEMCACHED => memcached_request(input_position, input_context).await,
            }
        })
        .await
}

#[cfg(feature = "exactly-once")]
fn coordination_error(message: String) -> dandelion_commons::DError {
    dandelion_err!(DandelionError::Multinode(
        dandelion_commons::MultinodeError::RequestFailed(message)
    ))
}

async fn resolve_io_item(
    io_data: IoData,
    client: HttpClient,
) -> DandelionResult<(Position, Arc<Context>)> {
    let IoData {
        original_position,
        original_data,
        resolved,
        function,
        set_index,
    } = io_data;
    let (input_position, input_context) =
        resolve_original_io_data(original_position, *original_data, client.clone()).await?;
    let outputs = execute_io(
        function,
        resolved.as_ref(),
        client,
        input_position,
        input_context,
    )
    .await;
    let context = match outputs {
        Ok(contexts) => contexts[set_index].clone(),
        Err(error) => return Err(error.clone()),
    };
    Ok((
        Position {
            offset: 0,
            size: context.size,
        },
        context,
    ))
}

// store checkpointed I/O completion in the background
#[cfg(all(feature = "at-least-once", not(feature = "exactly-once")))]
async fn resolve_checkpointed_io_item(
    io_data: CoordinatedIoData,
    client: HttpClient,
) -> DandelionResult<(Position, Arc<Context>)> {
    let CoordinatedIoData {
        coordination,
        request,
    } = io_data;
    let IoData {
        original_position,
        original_data,
        resolved,
        function,
        set_index,
    } = request;
    let IoCoordination {
        invocation_id,
        composition_set_id,
        item_identifier,
        item_key,
        owner_node_id,
    } = coordination;

    let (input_position, input_context) =
        resolve_original_io_data(original_position, *original_data, client.clone()).await?;
    let outputs = execute_io(
        function,
        resolved.as_ref(),
        client,
        input_position,
        input_context,
    )
    .await;
    let contexts = match outputs {
        Ok(contexts) => contexts,
        Err(error) => return Err(error.clone()),
    };

    // Checkpointing is deliberately detached from result consumption in at-least-once mode.
    // Failure only loses the restart optimization; it never fails the invocation.
    if let Ok(remote_client) = crate::composition::get_remote_data_client() {
        let owner_node_id = owner_node_id.unwrap_or_else(|| remote_client.local_node_id());
        let completion = IoCoordinationCompletion {
            owner_node_id,
            key: IoCoordinationKey {
                invocation_id,
                composition_set_id,
                function,
                identifier: item_identifier.clone(),
                item_key,
            },
            outcome: IoCompletionOutcome::Completed(
                contexts
                    .iter()
                    .map(|context| IoCompletedOutput {
                        item: DataItem {
                            ident: item_identifier.clone(),
                            key: u32::try_from(item_key).expect("I/O item keys originate as u32"),
                            data: Position {
                                offset: 0,
                                size: context.size,
                            },
                        },
                        context: context.clone(),
                    })
                    .collect(),
            ),
        };
        tokio::spawn(async move {
            if let Err(error) = remote_client.publish_io_completion(completion).await {
                warn!(
                    "Failed to checkpoint at-least-once I/O completion: {}",
                    error
                );
            }
        });
    }

    let context = contexts[set_index].clone();
    Ok((
        Position {
            offset: 0,
            size: context.size,
        },
        context,
    ))
}

#[cfg(feature = "exactly-once")]
async fn resolve_checkpointed_io_item(
    io_data: CoordinatedIoData,
    client: HttpClient,
) -> DandelionResult<(Position, Arc<Context>)> {
    resolve_io_item_exactly_once(io_data, client).await
}

#[cfg(feature = "exactly-once")]
async fn resolve_io_item_exactly_once(
    io_data: CoordinatedIoData,
    client: HttpClient,
) -> DandelionResult<(Position, Arc<Context>)> {
    let CoordinatedIoData {
        coordination,
        request,
    } = io_data;
    let IoData {
        original_position,
        original_data,
        resolved,
        function,
        set_index,
    } = request;
    let IoCoordination {
        invocation_id,
        composition_set_id,
        item_identifier,
        item_key,
        owner_node_id,
    } = coordination;

    let remote_client = crate::composition::get_remote_data_client().ok();
    let coordination_key = IoCoordinationKey {
        invocation_id,
        composition_set_id,
        function,
        identifier: item_identifier.clone(),
        item_key,
    };

    let original_remote = match original_data.as_ref() {
        ItemData::RemoteData(data) => Some((data.clone(), original_position.size)),
        _ => None,
    };

    let coordination_owner =
        owner_node_id.or_else(|| remote_client.as_ref().map(|client| client.local_node_id()));
    let resolution = match (&remote_client, coordination_owner) {
        (Some(remote_client), Some(owner_node_id)) => Some(
            remote_client
                .resolve_io(IoResolveRequest {
                    owner_node_id,
                    key: coordination_key.clone(),
                    set_index,
                    original_data: original_remote,
                })
                .await?,
        ),
        _ => None,
    };

    if let Some(IoResolveOutcome::Completed { data }) = resolution {
        let remote_client = remote_client.expect("completed remote I/O requires a data client");
        return remote_client
            .resolve_remote_data(data)
            .await
            .map(|(context, position)| (position, context));
    }
    if let Some(IoResolveOutcome::Failed(error)) = resolution {
        return Err(coordination_error(error));
    }

    let (resolved_input, won_coordination) = match resolution {
        Some(IoResolveOutcome::Execute { input }) => (input, true),
        None => (None, false),
        Some(IoResolveOutcome::Completed { .. } | IoResolveOutcome::Failed(_)) => unreachable!(),
    };
    let input_result = match resolved_input {
        Some(IoResolveInput::Inline { context, position }) => Ok((position, context)),
        Some(IoResolveInput::Remote { data, .. }) => {
            let remote_client = remote_client
                .as_ref()
                .expect("remote resolution input requires a data client");
            remote_client
                .resolve_remote_data(data)
                .await
                .map(|(context, position)| (position, context))
        }
        None => resolve_original_io_data(original_position, *original_data, client.clone()).await,
    };
    let (input_position, input_context) = match input_result {
        Ok(input) => input,
        Err(error) => {
            let coordinated_error = coordination_error(error.to_string());
            if won_coordination {
                if let (Some(remote_client), Some(owner_node_id)) =
                    (&remote_client, coordination_owner)
                {
                    remote_client
                        .publish_io_resolution(IoCoordinationCompletion {
                            owner_node_id,
                            key: coordination_key,
                            outcome: IoCompletionOutcome::Failed(error.to_string()),
                        })
                        .await?;
                }
            }
            return if won_coordination {
                Err(coordinated_error)
            } else {
                Err(error)
            };
        }
    };

    let outputs = execute_io(
        function,
        resolved.as_ref(),
        client,
        input_position,
        input_context,
    )
    .await;

    if won_coordination {
        let (remote_client, owner_node_id) = (
            remote_client
                .as_ref()
                .expect("coordinated I/O requires a data client"),
            coordination_owner.expect("coordinated I/O requires an owner"),
        );
        let outcome = match outputs {
            Ok(contexts) => IoCompletionOutcome::Completed(
                contexts
                    .iter()
                    .enumerate()
                    .map(|(_, context)| IoCompletedOutput {
                        item: DataItem {
                            ident: item_identifier.clone(),
                            key: u32::try_from(item_key).expect("I/O item keys originate as u32"),
                            data: Position {
                                offset: 0,
                                size: context.size,
                            },
                        },
                        context: context.clone(),
                    })
                    .collect(),
            ),
            Err(error) => IoCompletionOutcome::Failed(error.to_string()),
        };
        remote_client
            .publish_io_resolution(IoCoordinationCompletion {
                owner_node_id,
                key: coordination_key,
                outcome,
            })
            .await?;
    }

    let context = match outputs {
        Ok(contexts) => contexts[set_index].clone(),
        Err(error) if won_coordination => return Err(coordination_error(error.to_string())),
        Err(error) => return Err(error.clone()),
    };
    Ok((
        Position {
            offset: 0,
            size: context.size,
        },
        context,
    ))
}

async fn resolve_all_sets(
    client: HttpClient,
    input_sets: Vec<Option<CompositionSet>>,
    semaphore: Arc<Semaphore>,
    ticket: OwnedSemaphorePermit,
    result_sender: impl FnOnce(DandelionResult<Vec<Option<CompositionSet>>>) + 'static + Send,
) {
    // drop ticket so at least one will be available for the new tasks we spawn
    drop(ticket);

    let input_set_number = input_sets.len();
    let mut output_sets = Vec::with_capacity(input_set_number);
    output_sets.resize(input_set_number, None);
    let mut sets_vec = Vec::with_capacity(input_set_number);
    sets_vec.resize(input_set_number, Vec::new());
    let mut set_names = Vec::with_capacity(input_set_number);
    set_names.resize(input_set_number, None);
    let mut io_futures = FuturesUnordered::new();
    let mut remote_futures = FuturesUnordered::new();

    // collect items that need to be fetched from one node
    let mut fetching_nodes = BTreeMap::new();

    for (set_index, set_option) in input_sets.into_iter().enumerate() {
        if let Some(set) = set_option {
            set_names[set_index] = Some(set.get_name().clone());
            if set.is_local() {
                output_sets[set_index] = Some(set);
                continue;
            }
            for (mut item, data) in set.into_iter() {
                match data {
                    ItemData::LocalData(_) => {
                        // a not entirely local set, directly push the items that are already local
                        sets_vec[set_index].push((item, data));
                    }
                    ItemData::IoData(io_data) => {
                        let permit = semaphore.clone().acquire_owned().await.unwrap();
                        let client_clone = client.clone();
                        io_futures.push(spawn(async move {
                            let (position, context) =
                                resolve_io_item(io_data, client_clone).await?;
                            item.data = position;
                            drop(permit);
                            Ok((set_index, item, ItemData::LocalData(context)))
                        }));
                    }
                    #[cfg(feature = "at-least-once")]
                    ItemData::CoordinatedIoData(io_data) => {
                        let permit = semaphore.clone().acquire_owned().await.unwrap();
                        let client_clone = client.clone();
                        io_futures.push(spawn(async move {
                            let (position, context) =
                                resolve_checkpointed_io_item(io_data, client_clone).await?;
                            item.data = position;
                            drop(permit);
                            Ok((set_index, item, ItemData::LocalData(context)))
                        }));
                    }
                    ItemData::RemoteData(remote_data) => {
                        let node_id = remote_data.node_id;
                        match fetching_nodes.entry(node_id) {
                            Entry::Vacant(vacant) => {
                                vacant.insert((vec![(set_index, item)], vec![remote_data]));
                            }
                            Entry::Occupied(mut occupied) => {
                                occupied.get_mut().0.push((set_index, item));
                                occupied.get_mut().1.push(remote_data);
                            }
                        };
                    }
                }
            }
        }
    }

    // perform grouped fetching for all data from each node
    for (mut item_metadata, remote_items) in fetching_nodes.into_values() {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        remote_futures.push(spawn(async move {
            let client = crate::composition::get_remote_data_client()?;
            let new_context = client
                .resolve_multiple_data(&mut item_metadata, remote_items)
                .await?;
            let items = item_metadata
                .into_iter()
                .map(|(set_index, item)| {
                    (set_index, item, ItemData::LocalData(new_context.clone()))
                })
                .collect::<Vec<_>>();
            drop(permit);
            Ok(items)
        }));
    }

    // reaquire the ticket for the resolution task
    let ticket = semaphore.acquire_owned().await.unwrap();

    spawn(async move {
        while let Some(item_result) = io_futures.next().await {
            let item_tuple = item_result.unwrap();
            match item_tuple {
                Ok((set_index, item, data)) => sets_vec[set_index].push((item, data)),
                Err(err) => {
                    result_sender(Err(err));
                    return;
                }
            }
        }

        while let Some(item_results) = remote_futures.next().await {
            let item_vec = item_results.unwrap();
            match item_vec {
                Err(err) => {
                    result_sender(Err(err));
                    return;
                }
                Ok(items) => {
                    for (set_index, item, data) in items {
                        sets_vec[set_index].push((item, data));
                    }
                }
            }
        }

        for (set_index, (output_set, item_vec)) in
            output_sets.iter_mut().zip(sets_vec.into_iter()).enumerate()
        {
            if let Some(_) = output_set {
                debug_assert!(item_vec.is_empty());
            } else {
                if !item_vec.is_empty() {
                    // if there are items with the set index, it must have had a name
                    *output_set = CompositionSet::from_item_list(
                        set_names[set_index].take().unwrap(),
                        item_vec,
                    )
                }
            }
        }
        result_sender(Ok(output_sets));
        drop(ticket);
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
        let (args, debt, id_option) = queue.get_io_engine_args().await;
        debug!("IO engine loop has work");

        if !debt.is_alive() {
            drop(ticket);
            continue;
        }

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
                let composition_id = id_option.unwrap();

                resolve_all_sets(
                    client_clone,
                    input_sets,
                    semaphore.clone(),
                    ticket,
                    move |sets_result| {
                        recorder.record(RecordPoint::FetchingEnd);
                        match sets_result {
                            // FIXME: this leaks the queue's `prefetching_in_progress` count, since
                            // that is only decremented in `requeu_engine_args`. Repeated fetch
                            // failures permanently inflate the count and can starve the io queue
                            // of taking any more prefetch work.
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
                                composition_id,
                            ),
                        };
                    },
                )
                .await;
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

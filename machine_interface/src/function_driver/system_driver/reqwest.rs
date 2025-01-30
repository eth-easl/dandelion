use crate::memory_domain::{system_domain::system_context_write_from_bytes, ContextType};
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
use core_affinity::set_for_current;
use dandelion_commons::{
    records::{RecordPoint, Recorder},
    DandelionError, DandelionResult,
};
use futures::{StreamExt, FutureExt};
use http::{version::Version, HeaderName, HeaderValue, Method};
use log::{error, warn, debug};
use reqwest::{header::HeaderMap, Client};
use std::sync::Arc;
use tokio::runtime::Builder;
use tokio::time::{timeout, Duration, sleep};
use memcache::Client as MemcachedClient;
use reqwest::Client as HttpClient;
use bytes::Bytes;
use base64::{Engine as _, engine::general_purpose};
use std::env;

#[allow(non_camel_case_types)]
enum RequestMethod {
    HTTP_GET,
    HTTP_POST,
    HTTP_PUT,
    MEMCACHED_SET,
    MEMCACHED_GET,
}

/// Stores requestInformation for http and memcached request
/// For memcached requests, version and headermap are not used and ignored for request building
struct RequestInformation {
    /// name of the request item
    item_name: String,
    /// key of the request item
    item_key: u32,
    method: RequestMethod,
    uri: String,
    version: Version,
    headermap: HeaderMap,
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
fn convert_to_request(
    mut raw_request: Vec<u8>,
    item_name: String,
    item_key: u32,
) -> DandelionResult<RequestInformation> {
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
        Some(method_string) if method_string == "GET" => RequestMethod::HTTP_GET,
        Some(method_string) if method_string == "POST" => RequestMethod::HTTP_POST,
        Some(method_string) if method_string == "MEMCACHED_GET" => RequestMethod::MEMCACHED_GET,
        Some(method_string) if method_string == "MEMCACHED_SET" => RequestMethod::MEMCACHED_SET,
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

    let uri = String::from(
        request_iter
            .next()
            .ok_or(DandelionError::MalformedSystemFuncArg(String::from(
                "No uri in request",
            )))?,
    );
    let mut version: Version;
    let mut headermap = HeaderMap::new();
    let mut body: Vec<u8>;
    match method { 
        RequestMethod::HTTP_GET | RequestMethod::HTTP_POST | RequestMethod::HTTP_PUT
            => {
                version = match request_iter.next() {
                    Some(version_string) if version_string == "HTTP/0.9" => Version::HTTP_09,
                    Some(version_string) if version_string == "HTTP/1.0" => Version::HTTP_10,
                    Some(version_string) if version_string == "HTTP/1.1" => Version::HTTP_11,
                    Some(version_string) if version_string == "HTTP/2.0" => Version::HTTP_2,
                    Some(version_string) if version_string == "HTTP/3.0" => Version::HTTP_3,
                    Some(version_string) => {
                        return Err(DandelionError::InvalidSystemFuncArg(format!(
                            "unkown http version: {}",
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
                body = if header_index < raw_request.len() {
                    raw_request.drain(..header_index);
                    raw_request
                } else {
                    vec![]
                };
                log::trace!("Reqwest body: {:?}", body);
            }
        // For memcached request, the version does not matter, as it is not used
        RequestMethod::MEMCACHED_SET | RequestMethod::MEMCACHED_GET 
            => {
                version = Version::HTTP_10;
                body = vec![];
                body.extend_from_slice(request_iter.next()
                .ok_or(DandelionError::MalformedSystemFuncArg(String::from("No memcached identifier")),
                )?.as_bytes());
                body.push(b' ');
                if request_index + 2 < raw_request.len(){
                    // We jump by two, to ignore the two newline symbols after the header
                    body.extend(raw_request.drain(request_index + 2..));
                }
            } 
        _ => return Err(DandelionError::NotImplemented), 
    }

    return Ok(RequestInformation {
        item_name,
        item_key,
        method,
        uri,
        version,
        headermap,
        body,
    });
}

fn request_setup(context: &Context) -> DandelionResult<Vec<RequestInformation>> {
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
    let request_info: DandelionResult<Vec<RequestInformation>> = request_set
        .buffers
        .iter()
        .map(|set_item| {
            let mut request_buffer = Vec::with_capacity(set_item.data.size);
            request_buffer.resize(set_item.data.size, 0);
            context.read(set_item.data.offset, &mut request_buffer)?;
            convert_to_request(request_buffer, set_item.ident.clone(), set_item.key)
            
        })
        .collect();
    return request_info;
}

async fn http_request(
    client: HttpClient,
    request_info: RequestInformation,
) -> DandelionResult<ResponseInformation> {
    let RequestInformation {
        item_name,
        item_key,
        method,
        uri,
        version,
        headermap,
        body,
    } = request_info;

    let request_builder = match method {
        RequestMethod::HTTP_PUT => client.put(uri.clone()),
        RequestMethod::HTTP_POST => client.post(uri.clone()),
        RequestMethod::HTTP_GET => client.get(uri.clone()),
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
    // TODO also accept chunked data
    let content_length = response
        .headers()
        .get("content-length")
        .and_then(|value| value.to_str().ok())
        .and_then(|len_str| len_str.parse::<usize>().ok())
        .ok_or(DandelionError::SystemFuncResponseError)?;

    for (key, value) in response.headers() {
        preamble.push_str(&format!("{}:{}\n", key, value.to_str().unwrap()));
    }

    preamble.push('\n');

    let body = match response.bytes().await {
        Ok(bytes) => bytes,
        Err(_) => return Err(DandelionError::SystemFuncResponseError),
    };
    // Could potentially done in chunks, like this:
    // let mut body = BytesMut::new();
    // let mut stream = response.bytes_stream();
    // while let Some(chunk) = stream.next().await {
    //     match chunk {
    //         Ok(bytes) => body.extend_from_slice(&bytes),
    //         Err(_) => return Err(DandelionError::SystemFuncResponseError),
    //     }
    // }
    // body.freeze();

    if content_length == body.len() {
        let response_info = ResponseInformation {
            item_name,
            item_key,
            preamble,
            body,
        };
        return Ok(response_info);
    } else {
        return Err(DandelionError::SystemFuncResponseError);
    }
}

async fn memcached_request(
    client: HttpClient,
    request_info: RequestInformation,
) -> DandelionResult<ResponseInformation> {
    let RequestInformation {
        item_name,
        item_key,
        method,
        uri,
        version,
        headermap,
        mut body,
    } = request_info;
    env::set_var("RUST_LOG", "info");
    // Memcached Basic Text Protocol could have following methods: 
    // Set, add (set if not present), replace (set if present), append, prepend, cas
    // Get, gets (get with cas ), delete, incr/decr

    // We try to connect to the server and timeout if not reachable
    const SERVER_TIMEOUT: u64 = 1;
    let ip = format!("memcache://{}", uri.clone());
    // warn!("Trying to connect to {}", uri);
    let mut memcached_client = match timeout(Duration::from_secs(SERVER_TIMEOUT), async {
        tokio::task::spawn_blocking(move || MemcachedClient::connect(ip)).await
    })
    .await
    {
        Ok(Ok(Ok(client))) => client,
        Ok(Ok(Err(e))) => {
            debug!("Could not connect to memcached server with error: {:?}", e);
            return Err(DandelionError::MemcachedServerError(format!("Server Error: {:?}", e)));
        }
        Ok(Err(e)) => {
            debug!("Could not connect to memcached server with error: {:?}", e);
            return Err(DandelionError::MemcachedServerError(format!("Connection tokio task failed: {:?}", e)));
        }
        Err(e) => {
            debug!("Could not connect to memcached server with error: {:?}", e);
            return Err(DandelionError::MemcachedServerError(format!("Server could not be reached within {}s: {:?}", SERVER_TIMEOUT, e)));
        }
    };

    // Preamble is SUCCESS for success. For non successfull functions, error message will be stored there
    // Get on non existed key will return ABSENT
    let mut preamble: String;
    let mut response_body: bytes::Bytes;
    // Default item size limit is 1MB. If item is larger, we return an error
    const MAX_SIZE: usize = 1_048_576;
    // Default memcached max size is 30 days
    const MAX_EXPIRATION_TIME: u32 = 60 * 60 * 24 * 30;
    
    let mut memcached_identifier: String = "".to_string();
    let mut start_index_payload: usize = 0;
    for (i, &byte) in body.iter().enumerate() {
        if byte.is_ascii_whitespace() {
            memcached_identifier = String::from_utf8((body[0..i]).to_vec())
                .map_err(|_| DandelionError::MalformedSystemFuncArg(String::from("Invalid memcached identifier")))?;
            start_index_payload = i + 1;
            break;
        }
    }
    // warn!("Memcached_identifier: {}", memcached_identifier);

    match method {
        RequestMethod::MEMCACHED_SET => {
            // warn!("Starting set");
            let mut expiration_time:u32 = 10800;
            
            // The maximum expiration time in memcached is 30 days, minimum is 0 (never expire)
            let mut start_index_value: usize = start_index_payload;
            for (i, &byte) in body[start_index_payload..].iter().enumerate() {
                if byte.is_ascii_whitespace() {
                    let time_slice = &body[start_index_payload..start_index_payload + i];
                    let time_slice_str = std::str::from_utf8(time_slice).map_err(|_| {
                        DandelionError::MalformedSystemFuncArg(String::from("Invalid UTF-8 in expiration time"))
                    })?;
                    expiration_time = time_slice_str.parse::<u32>().map_err(|_| {
                        DandelionError::MalformedSystemFuncArg(format!("Invalid expiration time format. Slice: {}", time_slice_str))
                    })?;
                    if !(0..=MAX_EXPIRATION_TIME).contains(&expiration_time) {
                        return Err(DandelionError::MalformedSystemFuncArg(String::from(
                            "Expiration time for memcached set is to large",
                        )));
                    }
                    start_index_value = start_index_payload + i + 1;
                    break;
                }
            }
            let value: Vec<u8> = body.split_off(start_index_value);
            if value.is_empty() {
                return Err(DandelionError::MalformedSystemFuncArg(String::from(
                    "Value to store with memcached set is missing",
                )));
            }
            else if value.len() > MAX_SIZE {
                return Err(DandelionError::MalformedSystemFuncArg(String::from(
                    "Memcached value is larger than 1MB",
                )));
            } 

            // warn!("Setting identifier to {:?}\nExpiration time: {:?}", value, expiration_time);
            let encoded_value = general_purpose::STANDARD.encode(&value);

            let result = tokio::task::spawn_blocking(move || memcached_client.set(&memcached_identifier, 
                encoded_value,
                expiration_time)).await;

            match result{
                Ok(Ok(_)) => {
                    preamble = String::from("SUCCESS!");
                    response_body = Bytes::from(vec![0u8]);
                }
                Ok(Err(e)) => {
                    debug!("Memcached_request set failed with: {:?}", e);
                    return Err(DandelionError::MemcachedRequestError(format!(
                        "Set request failed: {:?}", e
                    )));
                }
                Err(e) => {
                    debug!("Memcached_request set failed with: {:?}", e);
                    return Err(DandelionError::MemcachedRequestError(format!(
                        "Set request tokio task failed: {:?}", e
                    )));
                }
            }
            // warn!("Completed memcached set");
        }
        RequestMethod::MEMCACHED_GET => {
            // warn!("Starting get");
            // Result<Option<Vec<u8>>, tokio_memcached::Error>
            let result = tokio::task::spawn_blocking(move || memcached_client.get::<Vec<u8>>(&memcached_identifier)).await;

            match result{
                Ok(Ok(Some(response))) => {
                    preamble = String::from("SUCCESS!");
                    let decoded_response = general_purpose::STANDARD.decode(&response).expect("Failed to decode Base64");
                    response_body = Bytes::from(Bytes::from(decoded_response));
                }
                Ok(Ok(None)) => {
                    debug!("Key {} did not exist on memcached server", item_key);
                    preamble = String::from("ABSENT!!");
                    response_body = Bytes::from(vec![0u8]);
                }
                Ok(Err(e)) => {
                    debug!("Memcached_request get failed with: {:?}", e);
                    return Err(DandelionError::MemcachedRequestError(format!(
                        "Get request failed: {:?}", e
                    )));
                }
                Err(e) => {
                    debug!("Memcached_request get failed with: {:?}", e);
                    return Err(DandelionError::MemcachedRequestError(format!(
                        "Get request tokio task failed: {:?}", e
                    )));
                }
            }
            // warn!("Completed memcached get");
        }
        _ => {
            return Err(DandelionError::MalformedSystemFuncArg(String::from(
                "Unsupported Method",
            )))
        }
    };

    preamble.push('\n');
    preamble.push('\n');

    let response_info = ResponseInformation {
        item_name,
        item_key,
        preamble,
        body: response_body,
    };
    // warn!("Completed memcached_request function");
    return Ok(response_info);
}

fn http_context_write(context: &mut Context, response: ResponseInformation) -> DandelionResult<()> {
    let ResponseInformation {
        item_name,
        item_key,
        preamble,
        mut body,
    } = response;
    // warn!("context_write");
    let preamble_len = preamble.len();
    let body_len = body.len();
    let response_len = preamble_len + body_len;
    // allocate space in the context for the entire response
    let response_start = context.get_free_space(response_len, 128)?;

    // warn!("Preamble: {}", preamble);
    // warn!("body: {:?}", body);

    match &mut context.context {
        ContextType::System(destination_ctxt) => {
            // warn!("Transfering to system_context");
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
    // warn!("Completed transfer. Name and key: {}, {}", item_name.clone(), item_key.clone());
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

async fn request_run(
    mut context: Context,
    client: HttpClient,
    output_set_names: Arc<Vec<String>>,
    debt: Debt,
    mut recorder: Recorder,
) -> () {
    let request_vec = match request_setup(&context) {
        Ok(request) => request,
        Err(err) => {
            debt.fulfill(Err(err));
            return;
        }
    };
    let response_vec = match futures::future::try_join_all(
        request_vec
            .into_iter()
            .map(|request| {
                match request.method {
                    RequestMethod::HTTP_GET | RequestMethod::HTTP_POST | RequestMethod::HTTP_PUT
                        => http_request(client.clone(), request).boxed(), 
                    RequestMethod::MEMCACHED_SET | RequestMethod::MEMCACHED_GET 
                        => memcached_request(client.clone(), request).boxed(),
                    _ => unimplemented!(), 
                }
            }),
    )
    .await
    {
        Ok(resp) => resp,
        Err(err) => {
            debt.fulfill(Err(err));
            return;
        }
    };
    // warn!("request_run with response_vec of length {}", response_vec.len());

    // only clear once for all requests
    context.clear_metadata();

    if !output_set_names.is_empty() {
        context.content = vec![None, None];
        if output_set_names.iter().any(|elem| elem == "response") {
            context.content[0] = Some(DataSet {
                ident: String::from("response"),
                buffers: vec![],
            })
        }
        if output_set_names.iter().any(|elem| elem == "body") {
            context.content[1] = Some(DataSet {
                ident: String::from("body"),
                buffers: vec![],
            })
        }
        let write_results: DandelionResult<Vec<_>> = response_vec
            .into_iter()
            .map(|response| http_context_write(&mut context, response))
            .collect();
        if let Err(err) = write_results {
            warn!("Error: {}", err);
            debt.fulfill(Err(err));
            return;
        }
    }
    if let Err(err) = recorder.record(RecordPoint::EngineEnd) {
        debt.fulfill(Err(err));
        return;
    }
    debt.fulfill(Ok(WorkDone::Context(context)));
    // warn!("Debt fullfilled");
    return;
}

async fn engine_loop(queue: Box<dyn WorkQueue + Send>) -> Debt {
    log::debug!("Reqwest engine Init");
    std::env::set_var("RUST_BACKTRACE", "1");
    let client = HttpClient::new();
    // TODO FIX! This should not be necessary!
    let mut queue_ref = Box::leak(queue);
    let mut tuple;
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
                if let Err(err) = recorder.record(RecordPoint::EngineStart) {
                    debt.fulfill(Err(err));
                    continue;
                }
                // let result = engine_state.run(config, context, output_sets);
                log::debug!("Reqwest engine running function");
                let function = match config {
                    FunctionConfig::SysConfig(sys_func) => sys_func,
                    _ => {
                        debt.fulfill(Err(DandelionError::ConfigMissmatch));
                        continue;
                    }
                };
                match function {
                    SystemFunction::HTTP | SystemFunction::MEMCACHED => {
                        tokio::spawn(request_run(
                            context,
                            client.clone(),
                            output_sets,
                            debt,
                            recorder,
                        ));
                    }
                    #[allow(unreachable_patterns)]
                    _ => {
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
                match recorder.record(RecordPoint::TransferStart) {
                    Ok(()) => (),
                    Err(err) => {
                        debt.fulfill(Err(err));
                        continue;
                    }
                }
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
                match recorder.record(RecordPoint::TransferEnd) {
                    Ok(()) => (),
                    Err(err) => {
                        debt.fulfill(Err(err));
                        continue;
                    }
                }
                let transfer_return = transfer_result.and(Ok(WorkDone::Context(destination)));
                debt.fulfill(transfer_return);
                continue;
            }
            WorkToDo::ParsingArguments {
                driver,
                path,
                static_domain,
                mut recorder,
            } => {
                recorder.record(RecordPoint::ParsingStart).unwrap();
                let function_result = driver.parse_function(path, &static_domain);
                recorder.record(RecordPoint::ParsingEnd).unwrap();
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
                recorder.record(RecordPoint::LoadStart).unwrap();
                let load_result = function.load(&domain, ctx_size);
                recorder.record(RecordPoint::LoadEnd).unwrap();
                match load_result {
                    Ok(context) => debt.fulfill(Ok(WorkDone::Context(context))),
                    Err(err) => debt.fulfill(Err(err)),
                }
                continue;
            }
            WorkToDo::Shutdown() => {
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
        log::debug!("Starting hyper engine");
        let core_id = match resource {
            ComputeResource::CPU(core) => core,
            _ => return Err(DandelionError::EngineResourceError),
        };
        // check that core is available
        let available_cores = match core_affinity::get_core_ids() {
            None => return Err(DandelionError::EngineResourceError),
            Some(cores) => cores,
        };
        if !available_cores
            .iter()
            .find(|x| x.id == usize::from(core_id))
            .is_some()
        {
            return Err(DandelionError::EngineResourceError);
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

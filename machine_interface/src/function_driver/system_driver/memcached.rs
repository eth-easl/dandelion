use crate::{
    function_driver::system_driver::request_commons::{
        parse_requests, responses_write, Request, ResponseInformation,
    },
    memory_domain::Context,
    promise::Debt,
};
use bytes::Bytes;
use dandelion_commons::{records::Recorder, DandelionError, DandelionResult, SysFunctionError};
use futures::FutureExt;
use log::{debug, warn};
use memcache::Client as MemcachedClient;
use std::sync::Arc;

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
                return Err(DandelionError::SysFunctionError(
                    SysFunctionError::InvalidArg(String::from("Request line not utf8")),
                ));
            }
        };
        let mut request_iter = request_line.split_ascii_whitespace();

        let method_item = request_iter.next();
        let method = match method_item {
            Some(method_string) if method_string == "MEMCACHED_GET" => MemcachedMethod::GET,
            Some(method_string) if method_string == "MEMCACHED_SET" => MemcachedMethod::SET,
            Some(method_string) => {
                return Err(DandelionError::SysFunctionError(
                    SysFunctionError::InvalidArg(format!("Unsupported Method: {}", method_string)),
                ))
            }
            _ => {
                return Err(DandelionError::SysFunctionError(
                    SysFunctionError::InvalidArg(String::from("No method found")),
                ))
            }
        };

        let uri = String::from(request_iter.next().ok_or(DandelionError::SysFunctionError(
            SysFunctionError::InvalidArg(String::from("No uri in request")),
        ))?);
        let memcached_identifier = match request_iter.next() {
            Some(identifier) => identifier.to_string(),
            None => {
                return Err(DandelionError::SysFunctionError(
                    SysFunctionError::InvalidArg(String::from("No memcached identifier found")),
                ))
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

pub(super) async fn run_memcached_request(
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

use crate::{
    function_driver::system_driver::request_commons::{
        parse_requests, responses_write, Request, ResponseInformation,
    },
    memory_domain::Context,
    promise::Debt,
};
use dandelion_commons::{records::Recorder, DandelionError, DandelionResult, SysFunctionError};
use futures::FutureExt;
use http::{version::Version as HttpVersion, HeaderName, HeaderValue, Method as HttpMethod};
use log::error;
use reqwest::{header::HeaderMap, Client};
use std::sync::Arc;

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
                return Err(DandelionError::SysFunctionError(
                    SysFunctionError::InvalidArg(String::from("Request line not utf8")),
                ));
            }
        };
        let mut request_iter = request_line.split_ascii_whitespace();

        let method_item = request_iter.next();
        let method = match method_item {
            Some(method_string) if method_string == "GET" => HttpMethod::GET,
            Some(method_string) if method_string == "POST" => HttpMethod::POST,
            Some(method_string) if method_string == "PUT" => HttpMethod::PUT,
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

        let version = match request_iter.next() {
            Some(version_string) if version_string == "HTTP/0.9" => HttpVersion::HTTP_09,
            Some(version_string) if version_string == "HTTP/1.0" => HttpVersion::HTTP_10,
            Some(version_string) if version_string == "HTTP/1.1" => HttpVersion::HTTP_11,
            Some(version_string) if version_string == "HTTP/2.0" => HttpVersion::HTTP_2,
            Some(version_string) if version_string == "HTTP/3.0" => HttpVersion::HTTP_3,
            Some(version_string) => {
                return Err(DandelionError::SysFunctionError(
                    SysFunctionError::InvalidArg(format!(
                        "Unkown http version: {}",
                        version_string,
                    )),
                ))
            }
            None => {
                return Err(DandelionError::SysFunctionError(
                    SysFunctionError::InvalidArg(String::from("No http version found")),
                ))
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
                .ok_or(DandelionError::SysFunctionError(
                    SysFunctionError::InvalidArg(String::from(
                        "Header line does not contain \':\'",
                    )),
                ))?;
            let (key, value) = header_line.split_at(split_index);
            let header_key =
                HeaderName::from_bytes(key).or(Err(DandelionError::SysFunctionError(
                    SysFunctionError::InvalidArg(String::from("Header key not utf-8")),
                )))?;
            let header_value =
                HeaderValue::from_bytes(&value[1..]).or(Err(DandelionError::SysFunctionError(
                    SysFunctionError::InvalidArg(String::from("Header value not utf-8 conformant")),
                )))?;
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

async fn http_request(
    client: Client,
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
            return Err(DandelionError::SysFunctionError(
                SysFunctionError::InvalidArg(String::from("Unsupported Method")),
            ))
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
            return Err(DandelionError::SysFunctionError(
                SysFunctionError::InvalidArg(format!("{:?}", http_error)),
            ));
        }
    };
    let response = match client.execute(request).await {
        Ok(resp) => resp,
        Err(_) => {
            return Err(DandelionError::SysFunctionError(
                SysFunctionError::ResponseError,
            ))
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

    preamble.push('\n');

    let body = match response.bytes().await {
        Ok(bytes) => bytes,
        Err(_) => {
            return Err(DandelionError::SysFunctionError(
                SysFunctionError::ResponseError,
            ))
        }
    };

    if let Some(content_len) = content_length {
        if content_len != body.len() {
            return Err(DandelionError::SysFunctionError(
                SysFunctionError::ResponseError,
            ));
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

pub(super) async fn run_http_request(
    context: Context,
    client: Client,
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

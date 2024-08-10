use crate::function_driver::SystemFunction;

#[cfg(feature = "reqwest_io")]
pub mod reqwest;

#[cfg(feature = "reqwest_io")]
pub mod http;

#[cfg(feature = "reqwest_io")]
pub mod distributed;

#[cfg(feature = "reqwest_io")]
pub mod context_util;

/// HTTP function currently expects one set with requests formated by HTTP standard (in text).
/// This means one line with the reqest method, a space, request url, another space and the protocol version
/// ex.: "PUT /images/logo.png HTTP/1.1"
/// After a line break the headers are one line each with the formatting of key ':' value
/// ex.: "host: www.google.com"
/// After all headers and an empty line the body which can be arbitrary binary data
const HTTP_INPUT_SETS: [&str; 1] = ["request"];

/// HTTP outputs one set with response for each request that was in the input set
/// The reponses start with a status line containing the protocol used, the response code and possible the reason
/// ex.: "HTTP/1.1 200 OK"
/// On the following lines there are the headers in key value formatted with ':' as separator
/// ex.: "Content-Type: text/html; charset=utf-8"
/// After all headers and one empty line is the body, which is arbitrary data
/// Additionally there is a set that only contains the bodies with the same item names as the requests.
const HTTP_OUTPUT_SETS: [&str; 2] = ["response", "body"];

const SEND_INPUT_SETS: [&str; 2] = ["send_meta", "send_data"];

const SEND_OUTPUT_SETS: [&str; 0] = [];

const RECV_INPUT_SETS: [&str; 1] = ["recv_meta"];

const RECV_OUTPUT_SETS: [&str; 1] = ["recv_data"];

/// Provides the input set names for a given system function
pub fn get_system_function_input_sets(function: SystemFunction) -> Vec<String> {
    return match function {
        SystemFunction::HTTP => HTTP_INPUT_SETS.iter().map(|&name| name.to_string()).collect(),
        SystemFunction::SEND => SEND_INPUT_SETS.iter().map(|&name| name.to_string()).collect(),
        SystemFunction::RECV => RECV_INPUT_SETS.iter().map(|&name| name.to_string()).collect()
    }
}

/// Provies the output set names for a given system function
pub fn get_system_function_output_sets(function: SystemFunction) -> Vec<String> {
    return match function {
        SystemFunction::HTTP => HTTP_OUTPUT_SETS.iter().map(|&name| name.to_string()).collect(),
        SystemFunction::SEND => SEND_OUTPUT_SETS.iter().map(|&name| name.to_string()).collect(),
        SystemFunction::RECV => RECV_OUTPUT_SETS.iter().map(|&name| name.to_string()).collect()
    }
}

#[cfg(test)]
mod system_driver_tests;

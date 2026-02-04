#[cfg(feature = "reqwest_io")]
pub mod reqwest;

use crate::{function_driver::functions::SystemFunction, machine_config::EngineType};

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

/// Provides the input set names for a given system function
pub fn get_system_function_input_sets(function: SystemFunction) -> Vec<String> {
    return match function {
        SystemFunction::HTTP => HTTP_INPUT_SETS,
        SystemFunction::MEMCACHED => HTTP_INPUT_SETS,
    }
    .map(|name| name.to_string())
    .to_vec();
}

/// Provies the output set names for a given system function
pub fn get_system_function_output_sets(function: SystemFunction) -> Vec<String> {
    return match function {
        SystemFunction::HTTP => &HTTP_OUTPUT_SETS,
        SystemFunction::MEMCACHED => &HTTP_OUTPUT_SETS,
    }
    .map(|name| name.to_string())
    .to_vec();
}

#[cfg(any(feature = "reqwest_io"))]
const SYS_FUNC_DEFAULT_CONTEXT_SIZE: usize = 0x1_0000_0000;

pub const SYSTEM_FUNCTIONS: &[(EngineType, SystemFunction, usize)] = &[
    #[cfg(feature = "reqwest_io")]
    (
        EngineType::Reqwest,
        SystemFunction::HTTP,
        SYS_FUNC_DEFAULT_CONTEXT_SIZE,
    ),
];

#[cfg(test)]
mod system_driver_tests;

#[cfg(feature = "reqwest_io")]
pub mod reqwest;

use crate::{function_driver::functions::SystemFunction, machine_config::EngineType};

/// HTTP function currently expects one set with requests formated by HTTP standard (in text).
/// This means one line with the reqest method, a space, request url, another space and the protocol version
/// ex.: "PUT /images/logo.png HTTP/1.1"
/// After a line break the headers are one line each with the formatting of key ':' value
/// ex.: "host: www.google.com"
/// After all headers and an empty line the body which can be arbitrary binary data
// TODO: think if we want to also separate this into two sets, one with header one with bodies.
// If we separate, need a way to deal with non matching numbers of bodies and headers and duplicate names.
// Could offer automatic pairing for example for giving a header that can be used with any number of bodies.
// Do not want to overcomplicate things.
const HTTP_INPUT_SETS: [&str; 1] = ["requests"];

/// HTTP outputs two set with response headers and bodies for each request that was in the input set
/// The headers start with a status line containing the protocol used, the response code and possible the reason
/// ex.: "HTTP/1.1 200 OK"
/// On the following lines there are the headers in key value formatted with ':' as separator
/// ex.: "Content-Type: text/html; charset=utf-8"
/// The header and body items all carry the names and keys of the corresponding requests.
/// The user is responsible for ensuring, that requests have names and the names are unique, if they need them to associate
/// the headers with the bodies.
const HTTP_OUTPUT_SETS: [&str; 2] = ["headers", "bodies"];

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

/// The system context only holds references to other contexts, so the size does not matter.
/// System functions can create new contexts to return with appropriate sizes, so this does
/// not matter to them either.
#[cfg(any(feature = "reqwest_io"))]
const SYS_FUNC_DEFAULT_CONTEXT_SIZE: usize = usize::MAX;

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

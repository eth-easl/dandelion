use crate::function_driver::SystemFunction;

#[cfg(feature = "hyper_io")]
mod hyper;

/// HTTP requests currently expect one input set called request with 3 items:
/// - method: the HTTP method, eg. "PUT", "GET", ...
/// - version: the http verson to use eg. "HTTP/0.9", "HTTP/1.1", ...
/// - uri: the uri to query
const HTTP_INPUT_SETS: [&str; 1] = ["request"];

/// HTTP outputs 3 sets "status line", "headers" and "body"
/// - "status line" contains two items:
///     - "status" the status code of the request
///     - "version" the HTTP version used
/// - "headers" set contains one item for each header item with
/// the header key as item identifier and the value as the item.
/// - "body" set contains a single item called body with the body
/// of the request as item
const HTTP_OUTPUT_SETS: [&str; 3] = ["status line", "headers", "body"];

/// Provides the input set names for a given system function
pub fn get_system_function_input_sets(function: SystemFunction) -> Vec<String> {
    return match function {
        SystemFunction::HTTP => HTTP_INPUT_SETS,
    }
    .map(|name| name.to_string())
    .to_vec();
}

/// Provies the output set names for a given system function
pub fn get_system_function_output_sets(function: SystemFunction) -> Vec<String> {
    return match function {
        SystemFunction::HTTP => &HTTP_OUTPUT_SETS,
    }
    .map(|name| name.to_string())
    .to_vec();
}

#[cfg(test)]
mod system_driver_tests;

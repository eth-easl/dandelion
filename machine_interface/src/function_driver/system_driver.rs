use crate::function_driver::SystemFunction;

#[cfg(feature = "hyper_io")]
pub mod hyper;

/// HTTP function currently expects following sets:
/// - request: expetcts to have a single item containing a http request formatted as
/// reqest method, a space, request url, another space and the protocol version
/// ex. "PUT /images/logo.png HTTP/1.1"
/// - headers: a set with specific header key value pairs, the item names are the keys and the item content will be the value
/// - body: a set expected to have one item to send as request body, if there are more than one they will be concatinated
const HTTP_INPUT_SETS: [&str; 3] = ["request", "headers", "body"];

/// HTTP outputs 3 sets "status line", "headers" and "body"
/// - "status line" contains an item with the response line consisting of:
///     - "version" the HTTP version used
///     - "status" the status code of the request
/// - "headers" set contains one item for each header item with
/// the header key as item identifier and the value as the item.
/// - "body" set contains an item with the body of the response
/// of the request as item
const HTTP_OUTPUT_SETS: [&str; 3] = ["status", "headers", "body"];

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

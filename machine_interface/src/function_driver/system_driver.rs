use crate::function_driver::SystemFunction;

#[cfg(feature = "std")]
pub(crate) mod file_system;
mod memcached;
#[cfg(any(feature = "reqwest_io"))]
mod request_commons;
#[cfg(feature = "reqwest_io")]
mod reqwest;
pub mod system;

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

/// The functions file_load and file_metadata load files or their respective metadata
/// accoring to the paths specified in the input set.
/// File load supports partial load, where the path needs to be terminated by a line end,
/// followed by a `<start>-<size>` where `start` and `size` need to valid u64 numbers.
/// This can be ommited and then the range will simply be set to the entire file.
/// For metadata the range is always ignored.
const FILE_INPUT_SETS: [&str; 1] = ["paths"];

/// The file functions return the file data or their respective metadata as items.
/// The items names of the returned items correspond to the item names of the paths,
/// for which data was loaded.
/// The metadata is currently only a 64 bit uint in , since it is unclear what other information makes sense.
const FILE_OUTPUT_SETS: [&str; 1] = ["files"];

/// Provides the input set names for a given system function
pub fn get_system_function_input_sets(function: SystemFunction) -> Vec<String> {
    return match function {
        SystemFunction::HTTP => HTTP_INPUT_SETS,
        SystemFunction::MEMCACHED => HTTP_INPUT_SETS,
        SystemFunction::FileLoad => FILE_INPUT_SETS,
        SystemFunction::FileMedatada => FILE_INPUT_SETS,
    }
    .map(|name| name.to_string())
    .to_vec();
}

/// Provies the output set names for a given system function
pub fn get_system_function_output_sets(function: SystemFunction) -> Vec<String> {
    return match function {
        SystemFunction::HTTP => HTTP_OUTPUT_SETS.as_slice(),
        SystemFunction::MEMCACHED => HTTP_OUTPUT_SETS.as_slice(),
        SystemFunction::FileLoad => FILE_OUTPUT_SETS.as_slice(),
        SystemFunction::FileMedatada => FILE_OUTPUT_SETS.as_slice(),
    }
    .into_iter()
    .map(|&name| name.to_string())
    .collect();
}

#[cfg(test)]
mod system_driver_tests;

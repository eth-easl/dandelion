pub mod reqwest;

use dandelion_commons::{
    err_dandelion, try_with_capacity, DandelionError, DandelionResult, MultinodeError,
    SystemFunction,
};
use memory::data::{DataItem, DataSet, IoData, Position, RemoteData};
use memory::Context;
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, OnceLock},
};
use tokio::sync::OnceCell;

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

/// HTTP outputs two set with response headers and bodies for each request that was in the input set.
/// The response items have the same key as the corresponding request input item.
/// The headers start with a status line containing the protocol used, the response code and possible the reason
/// ex.: "HTTP/1.1 200 OK"
/// On the following lines there are the headers in key value formatted with ':' as separator
/// ex.: "Content-Type: text/html; charset=utf-8"
/// The header and body items all carry the names and keys of the corresponding requests.
/// The user is responsible for ensuring, that requests have names and the names are unique, if they need them to associate
/// the headers with the bodies.
const HTTP_OUTPUT_SETS: [&str; 2] = ["headers", "bodies"];

/// Provides the input set names for a given system function
pub fn get_system_function_input_sets(function: SystemFunction) -> Vec<(String, Option<DataSet>)> {
    match function {
        SystemFunction::HTTP => HTTP_INPUT_SETS,
        SystemFunction::MEMCACHED => HTTP_INPUT_SETS,
    }
    .map(|name| (name.to_string(), None))
    .to_vec()
}

/// Provies the output set names for a given system function
pub fn get_system_function_output_sets(function: SystemFunction) -> Vec<String> {
    match function {
        SystemFunction::HTTP => &HTTP_OUTPUT_SETS,
        SystemFunction::MEMCACHED => &HTTP_OUTPUT_SETS,
    }
    .map(|name| name.to_string())
    .to_vec()
}

pub const SYSTEM_FUNCTIONS: &[SystemFunction] = &[SystemFunction::HTTP];

/// Currently assumes the HTTP_INPUT_SETS and HTTP_OUTPUT_SETS
pub fn convert_to_references(
    function: SystemFunction,
    mut inputs: Vec<DataSet>,
) -> DandelionResult<Vec<Arc<Vec<DataItem>>>> {
    // check that the function id contains string correcpsonding to system function
    debug_assert_eq!(
        1,
        inputs.len(),
        "all current IO functions expect a single input set"
    );

    // go through all input sets and check if there is already a static one, or on in the input data
    let mut output_vec = try_with_capacity!(Vec, 2)?;

    if let Some(input_set) = inputs.pop() {
        let mut out_0_list = try_with_capacity!(Vec, input_set.len())?;
        let mut out_1_list = try_with_capacity!(Vec, input_set.len())?;
        for item in input_set.into_iter() {
            let set_once = Arc::new(OnceCell::new());
            let header_item = DataItem::new_io(
                item.ident.clone(),
                item.key,
                IoData {
                    original_data: Box::new(item.data.clone()),
                    resolved: set_once.clone(),
                    function,
                    set_index: 0,
                },
            );
            let body_item = DataItem::new_io(
                item.ident.clone(),
                item.key,
                IoData {
                    original_data: Box::new(item.data.clone()),
                    resolved: set_once,
                    function,
                    set_index: 1,
                },
            );
            out_0_list.push(header_item);
            out_1_list.push(body_item);
        }

        output_vec.push(Arc::new(out_0_list));
        output_vec.push(Arc::new(out_1_list));
    }
    Ok(output_vec)
}

pub trait RemoteDataClient: Send + Sync {
    fn resolve_remote_data(
        &self,
        // Since this assumes ownership should be careful not to drop until we have the data
        data: RemoteData,
    ) -> Pin<Box<dyn Future<Output = DandelionResult<(Arc<Context>, Position)>> + Send + '_>>;

    /// Returns a single context containing all the items that were fetched and updates
    /// their position metadata accordingly
    fn resolve_multiple_data<'meta>(
        &'meta self,
        // Metadata for data that needs to be fetched
        metadata: &'meta Vec<(usize, DataItem)>,
    ) -> Pin<Box<dyn Future<Output = DandelionResult<Vec<(usize, DataItem)>>> + Send + 'meta>>;

    fn delete_remote_data(
        &self,
        node_id: u64,
        data_id: u64,
    ) -> Pin<Box<dyn Future<Output = DandelionResult<()>> + Send + '_>>;
}

static REMOTE_DATA_CLIENT: OnceLock<Arc<dyn RemoteDataClient>> = OnceLock::new();

pub fn set_remote_data_client(client: Arc<dyn RemoteDataClient>) {
    let _ = REMOTE_DATA_CLIENT.set(client);
}

pub fn get_remote_data_client() -> DandelionResult<Arc<dyn RemoteDataClient>> {
    match REMOTE_DATA_CLIENT.get() {
        Some(client) => Ok(client.clone()),
        None => {
            err_dandelion!(DandelionError::Multinode(MultinodeError::ConfigError(
                "No remote data client configured".to_string(),
            )))
        }
    }
}

#[cfg(test)]
mod system_driver_tests;

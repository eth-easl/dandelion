pub mod reqwest;

use crate::{
    composition::{CompositionSet, ItemData, LocalCompositionSet},
    function_driver::functions::SystemFunction,
    memory_domain::Context,
    DataItem, Position,
};
use dandelion_commons::{try_with_capacity, DandelionResult, InvocationId};
use std::sync::Arc;
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
pub fn get_system_function_input_sets(
    function: SystemFunction,
) -> Vec<(String, Option<LocalCompositionSet>)> {
    return match function {
        SystemFunction::HTTP => HTTP_INPUT_SETS,
        SystemFunction::MEMCACHED => HTTP_INPUT_SETS,
    }
    .map(|name| (name.to_string(), None))
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

pub const SYSTEM_FUNCTIONS: &[SystemFunction] = &[SystemFunction::HTTP];

#[derive(Debug, Clone)]
pub struct IoData {
    pub invocation_id: InvocationId,
    pub composition_node_id: Option<String>,
    pub original_position: Position,
    pub original_data: Box<ItemData>,
    // A vec with the resolved outputs for this IO request
    // one entry for each output set of the function.
    // The output item starts at 0 in the context and goes until the end of the context.
    pub resolved: Arc<OnceCell<DandelionResult<Vec<Arc<Context>>>>>,
    pub function: SystemFunction,
    pub set_index: usize,
    // recorder: Recorder,
}

/// Currently assumes the HTTP_INPUT_SETS and HTTP_OUTPUT_SETS
pub fn convert_to_references(
    function: SystemFunction,
    invocation_id: InvocationId,
    composition_node_id: Option<String>,
    mut inputs: Vec<Option<CompositionSet>>,
    // recorder: Recorder,
) -> DandelionResult<Vec<Option<CompositionSet>>> {
    // check that the function id contains string correcpsonding to system function
    debug_assert_eq!(
        1,
        inputs.len(),
        "all current IO functions expect a single input set"
    );

    // go through all input sets and check if there is already a static one, or on in the input data
    let mut output_vec = try_with_capacity!(Vec, 2)?;
    output_vec.resize(2, None);

    if let Some(input_set) = inputs[0].take() {
        let input_set_name = input_set.get_name().clone();
        let mut out_0_list = try_with_capacity!(Vec, input_set.len())?;
        let mut out_1_list = try_with_capacity!(Vec, input_set.len())?;
        for (item, data) in input_set {
            let new_item = DataItem {
                data: crate::Position { offset: 0, size: 0 },
                ident: item.ident.clone(),
                key: item.key,
            };
            let set_once = Arc::new(OnceCell::new());
            let header_data = IoData {
                invocation_id,
                composition_node_id: composition_node_id.clone(),
                original_position: item.data,
                original_data: Box::new(data.clone()),
                resolved: set_once.clone(),
                function,
                set_index: 0,
            };
            let body_data = IoData {
                invocation_id,
                composition_node_id: composition_node_id.clone(),
                original_position: item.data,
                original_data: Box::new(data),
                resolved: set_once,
                function,
                set_index: 1,
            };
            out_0_list.push((new_item.clone(), ItemData::IoData(header_data)));
            out_1_list.push((new_item, ItemData::IoData(body_data)));
        }
        output_vec[0] = CompositionSet::from_item_list(input_set_name.clone(), out_0_list);
        output_vec[1] = CompositionSet::from_item_list(input_set_name, out_1_list);
    }
    Ok(output_vec)
}

#[cfg(test)]
mod system_driver_tests;

pub mod recovery_log;
pub mod reqwest;

use crate::{
    composition::{CompositionSet, ItemData, LocalCompositionSet},
    function_driver::functions::SystemFunction,
    memory_domain::Context,
    DataItem, Position,
};
use dandelion_commons::{records::Recorder, FunctionId, InvocationId};
use dandelion_commons::{try_with_capacity, DandelionResult};
#[cfg(feature = "at-least-once")]
pub use recovery_log::{
    decode_io_completion_payload, encode_io_completion_payload, IoCompletionData,
    IoCompletionDisposition, IoCompletionItem, IoCompletionOutputSet, IoCompletionRecord,
    RecoveredIoOutput,
};
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

/// Stable recorder ID for one logical lazy-I/O coordination key.
pub fn io_recorder_id(
    function: SystemFunction,
    composition_set_id: usize,
    item_identifier: &str,
    item_key: u32,
) -> FunctionId {
    let identifier_hex: String = item_identifier
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    Arc::new(format!(
        "IO:{}:{}:{}:{:016x}",
        function, composition_set_id, identifier_hex, item_key
    ))
}

#[derive(Debug, Clone)]
pub struct IoData {
    pub original_position: Position,
    pub original_data: Box<ItemData>,
    // A vec with the resolved outputs for this IO request
    // one entry for each output set of the function.
    // The output item starts at 0 in the context and goes until the end of the context.
    pub resolved: Arc<OnceCell<DandelionResult<Vec<Arc<Context>>>>>,
    pub function: SystemFunction,
    pub set_index: usize,
    /// The child recorder for this logical I/O. It is absent after crossing a node boundary
    /// until timestamp transport is added.
    pub recorder: Option<Recorder>,
}

#[cfg(feature = "at-least-once")]
#[derive(Debug, Clone)]
pub struct IoCoordination {
    pub invocation_id: InvocationId,
    pub composition_set_id: usize,
    pub item_identifier: String,
    pub item_key: u64,
    /// Filled when the lazy reference crosses a node boundary; `None` means the current node owns
    /// the coordination registry.
    pub owner_node_id: Option<u64>,
}

#[cfg(feature = "at-least-once")]
#[derive(Debug, Clone)]
pub struct CoordinatedIoData {
    pub coordination: IoCoordination,
    pub request: IoData,
}

pub trait IoReferencePolicy: Copy {
    /// Returns the lazy/reference data and, when recovery substituted a completed output, its
    /// restored item size.
    fn wrap(
        self,
        request: IoData,
        item: &DataItem,
        composition_set_id: usize,
    ) -> (ItemData, Option<usize>);
}

#[derive(Debug, Clone, Copy)]
pub struct UncoordinatedIo;

impl IoReferencePolicy for UncoordinatedIo {
    fn wrap(
        self,
        request: IoData,
        _item: &DataItem,
        _composition_set_id: usize,
    ) -> (ItemData, Option<usize>) {
        (ItemData::IoData(request), None)
    }
}

#[cfg(not(any(feature = "checkpointed-at-least-once", feature = "exactly-once")))]
pub type AsyncIoPolicy = UncoordinatedIo;

#[cfg(not(any(feature = "checkpointed-at-least-once", feature = "exactly-once")))]
pub fn async_io_policy(_invocation_id: InvocationId) -> AsyncIoPolicy {
    UncoordinatedIo
}

#[cfg(feature = "at-least-once")]
#[derive(Debug, Clone, Copy)]
pub struct RecoverableIo {
    pub invocation_id: InvocationId,
}

#[cfg(any(feature = "checkpointed-at-least-once", feature = "exactly-once"))]
pub type AsyncIoPolicy = RecoverableIo;

#[cfg(any(feature = "checkpointed-at-least-once", feature = "exactly-once"))]
pub fn async_io_policy(invocation_id: InvocationId) -> AsyncIoPolicy {
    RecoverableIo { invocation_id }
}

#[cfg(feature = "at-least-once")]
impl IoReferencePolicy for RecoverableIo {
    fn wrap(
        self,
        request: IoData,
        item: &DataItem,
        composition_set_id: usize,
    ) -> (ItemData, Option<usize>) {
        #[cfg(not(feature = "exactly-once"))]
        if let Some(output) = recovery_log::recovered_io_item_location(
            self.invocation_id,
            request.function,
            composition_set_id,
            request.set_index,
            &item.ident,
            item.key.into(),
        ) {
            return match output {
                RecoveredIoOutput::Inline(context) => {
                    let size = context.size;
                    (ItemData::LocalData(context), Some(size))
                }
                RecoveredIoOutput::Remote { data, size } => {
                    (ItemData::RemoteData(data), Some(size))
                }
            };
        }
        (
            ItemData::CoordinatedIoData(CoordinatedIoData {
                coordination: IoCoordination {
                    invocation_id: self.invocation_id,
                    composition_set_id,
                    item_identifier: item.ident.clone(),
                    item_key: item.key.into(),
                    owner_node_id: None,
                },
                request,
            }),
            None,
        )
    }
}

/// Currently assumes the HTTP_INPUT_SETS and HTTP_OUTPUT_SETS
/// Converts a system-function invocation into lazy output references. Both output references for
/// an input item share one `OnceCell`, so resolving either header or body executes the operation
/// once locally. The policy type selects the reference representation at compile time.
pub fn convert_to_references<P: IoReferencePolicy>(
    function: SystemFunction,
    mut inputs: Vec<Option<CompositionSet>>,
    io_policy: P,
    composition_set_id: usize,
    mut recorder: Recorder,
) -> DandelionResult<Vec<Option<CompositionSet>>> {
    // check that the function id contains string correcpsonding to system function
    debug_assert_eq!(
        1,
        inputs.len(),
        "all current IO functions expect a single input set"
    );

    // go through all input sets and check if there is already a static one, or on in the input data
    let output_count = get_system_function_output_sets(function).len();
    let mut output_vec = try_with_capacity!(Vec, output_count)?;
    output_vec.resize(output_count, None);

    if let Some(input_set) = inputs[0].take() {
        let input_set_name = input_set.get_name().clone();
        let mut output_items = (0..output_count)
            .map(|_| try_with_capacity!(Vec, input_set.len()))
            .collect::<DandelionResult<Vec<_>>>()?;
        let mut io_recorders = try_with_capacity!(Vec, input_set.len())?;

        for (item, data) in input_set {
            let resolved = Arc::new(OnceCell::new());
            let io_recorder = Recorder::new_from_parent(
                io_recorder_id(function, composition_set_id, &item.ident, item.key),
                &recorder,
            );
            for (set_index, items) in output_items.iter_mut().enumerate() {
                let (output_data, recovered_size) = io_policy.wrap(
                    IoData {
                        original_position: item.data,
                        original_data: Box::new(data.clone()),
                        resolved: resolved.clone(),
                        function,
                        set_index,
                        recorder: Some(io_recorder.clone()),
                    },
                    &item,
                    composition_set_id,
                );
                items.push((
                    DataItem {
                        data: Position {
                            offset: 0,
                            size: recovered_size.unwrap_or(0),
                        },
                        ident: item.ident.clone(),
                        key: item.key,
                    },
                    output_data,
                ));
            }
            io_recorders.push(io_recorder);
        }

        for (set_index, items) in output_items.into_iter().enumerate() {
            output_vec[set_index] = CompositionSet::from_item_list(input_set_name.clone(), items);
        }
        recorder.add_children(vec![Some(io_recorders)]);
    }
    Ok(output_vec)
}

#[cfg(test)]
mod system_driver_tests;

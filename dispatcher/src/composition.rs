use crate::dispatcher::TransferIndices;
use dandelion_commons::FunctionId;

/// A composition has a composition wide id space that maps ids of
/// the input and output sets and items of individual functions to a unified
/// namespace. The ids in this namespace are used to find out which
/// functions have become ready.
pub struct Composition {
    pub dependencies: Vec<FunctionDependencies>,
}

/// Struct to describe the dependencies of a single function in the composition.
/// The contexts are enumerated in a composition wide name space, with each context posing its own name space for set ids.
/// The inputs of a function are given as a list of context ids and the mappings from that contexts set namespace to the
/// set ids of the functions contexts input set namespaces
pub struct FunctionDependencies {
    pub function: FunctionId,
    /// composition set ids that the function needs to get ready and
    /// the respective mappings to function set ids
    pub input_ids: Vec<(usize, Vec<TransferIndices>)>,
    /// id of the output context of this function
    pub output_id: usize,
}

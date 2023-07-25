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
/// The input ids and output ids discribe the mapping from global set ids to local and vice versa.
pub struct FunctionDependencies {
    pub function: FunctionId,
    /// composition set ids that the function needs to get ready and
    /// the respective mappings to function set ids
    pub input_ids: Vec<TransferIndices>,
    /// ids of output sets within the composition
    pub output_ids: Vec<TransferIndices>,
}

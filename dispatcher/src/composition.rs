use dandelion_commons::FunctionId;

/// A composition has a composition wide id space that maps ids of
/// the input and output sets to sets of individual functions to a unified
/// namespace. The ids in this namespace are used to find out which
/// functions have become ready.
pub struct Composition {
    pub dependencies: Vec<FunctionDependencies>,
}

pub struct FunctionDependencies {
    pub function: FunctionId,
    /// composition set ids that the function needs to get ready and
    /// the mapping to local ids is given implicitly through the index in the vec
    /// if the id is none, that set is not provided by the compostion
    pub input_set_ids: Vec<Option<usize>>,
    /// the composition ids for the output sets of the function,
    /// if the id is none, that set is not needed for the composition
    pub output_set_ids: Vec<Option<usize>>,
}

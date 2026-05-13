use crate::{
    composition::{CompositionSet, LocalCompositionSet},
    machine_config::EngineType,
    memory_domain::MemoryDomain,
};
extern crate alloc;
use alloc::sync::Arc;
use dandelion_commons::{records::Recorder, DandelionResult};

pub mod compute_driver;
pub mod functions;
mod load_utils;
pub mod system_driver;
#[cfg(test)]
mod test_queue;
mod thread_utils;

#[derive(Debug, Clone, Copy)]
pub enum ComputeResource {
    CPU(u8),
    GPU(u8),
}

/// Struct holding general function metadata that is true across all drivers.
#[derive(Debug)]
pub struct Metadata {
    /// The input set names with an optional static composition set. If the static set is set it will
    /// prioritized and any other input for that set is ignored.
    pub input_sets: Vec<(String, Option<LocalCompositionSet>)>,
    /// The output set names.
    pub output_sets: Vec<String>,
}

/// Struct holding function data comming from the dispatcher into the queueing.
pub enum WorkToDo {
    FunctionArguments {
        function_id: Arc<String>,
        function_alternatives: Vec<Arc<functions::FunctionAlternative>>,
        input_sets: Vec<Option<CompositionSet>>,
        metadata: Arc<Metadata>,
        caching: bool,
        recorder: Recorder,
    },
    Shutdown(EngineType),
}

pub enum WorkDone {
    CompositionSet(Vec<Option<CompositionSet>>),
    Resources(Vec<ComputeResource>),
}

impl WorkDone {
    pub fn get_composition(self) -> Vec<Option<CompositionSet>> {
        return match self {
            WorkDone::CompositionSet(sets) => sets,
            _ => panic!("WorkDone is not context when context was expected"),
        };
    }
}

pub trait EngineWorkQueue {
    fn get_engine_args(
        &self,
    ) -> impl std::future::Future<Output = (WorkToDo, crate::promise::Debt)> + Send;
    fn try_get_engine_args(&self) -> Option<(WorkToDo, crate::promise::Debt)>;
}

pub trait Driver: Send + Sync {
    // the resource descirbed by config and make it into an engine of the type
    fn start_engine(
        &self,
        resource: ComputeResource,
        // TODO check out why this can't be impl instead of Box<dyn
        queue: impl EngineWorkQueue + Send + 'static,
    ) -> DandelionResult<()>;

    // parses an executable,
    // returns the layout requirements and a context containing static data,
    //  and a layout description for it
    fn parse_function(
        &self,
        function_path: String,
        static_domain: &Box<dyn MemoryDomain>,
    ) -> DandelionResult<functions::Function>;
}

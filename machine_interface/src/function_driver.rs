use crate::function_driver::system_driver::RemoteData;
use crate::machine_config::EngineType;
use memory::context::MemoryDomain;
extern crate alloc;
use alloc::sync::Arc;
use dandelion_commons::{records::Recorder, DandelionResult};
use memory::data::{DataSet, Metadata};

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

/// Struct holding function data comming from the dispatcher into the queueing.
pub enum WorkToDo {
    FunctionArguments {
        function_id: Arc<String>,
        function_alternatives: Vec<Arc<functions::FunctionAlternative>>,
        input_sets: Vec<Option<DataSet>>,
        metadata: Arc<Metadata>,
        caching: bool,
        recorder: Recorder,
    },
    SetsToResolve {
        input_sets: Vec<Option<DataSet>>,
    },
    RemoteToDelete {
        remote_data: RemoteData,
    },
    Shutdown(EngineType),
}

pub enum WorkDone {
    CompositionSet(Vec<Option<DataSet>>),
    Resources(Vec<ComputeResource>),
    RemoteDeleted,
}

impl WorkDone {
    pub fn get_composition(self) -> Vec<Option<DataSet>> {
        return match self {
            WorkDone::CompositionSet(sets) => sets,
            _ => panic!("WorkDone is not context when context was expected"),
        };
    }
}

pub trait EngineWorkQueue {
    fn get_compute_engine_args(
        &self,
    ) -> impl std::future::Future<Output = (WorkToDo, crate::promise::Debt)> + Send;
    /// Function to get work for the IO engines
    /// Unstable: This is a temproary addition to the interface use with caution
    fn get_io_engine_args(
        &self,
    ) -> impl std::future::Future<Output = (WorkToDo, crate::promise::Debt, Option<usize>)> + Send;
    /// Function to return a Work to do to the queue after fetching all reference sets
    /// Unstable: This is a temproary addition to the interface use with caution
    fn requeu_engine_args(&self, work: WorkToDo, debt: crate::promise::Debt, composition_id: usize);
    fn remove_self_from_queue(&self);
}

pub trait Driver: Send + Sync {
    // the resource descirbed by config and make it into an engine of the type
    fn start_engine(
        &self,
        resource: ComputeResource,
        // TODO check out why this can't be impl instead of Box<dyn
        queue: impl EngineWorkQueue + Clone + Send + 'static,
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

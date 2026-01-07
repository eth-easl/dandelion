use crate::{
    composition::CompositionSet,
    memory_domain::{Context, MemoryDomain},
    DataRequirementList,
};
extern crate alloc;
use alloc::sync::Arc;
use dandelion_commons::{records::Recorder, DandelionResult};

pub mod compute_driver;
mod load_utils;
pub mod system_driver;
#[cfg(test)]
mod test_queue;
mod thread_utils;

#[derive(Clone)]
#[allow(dead_code)]
pub struct ElfConfig {
    // TODO change to positions
    system_data_offset: usize,
    #[cfg(feature = "cheri")]
    return_offset: (usize, usize),
    entry_point: usize,
    #[cfg(feature = "mmu")]
    protection_flags: Arc<Vec<(u32, crate::Position)>>,
}

#[derive(Clone, Copy)]
pub enum SystemFunction {
    HTTP,
    MEMCACHED,
}

impl core::fmt::Display for SystemFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> core::fmt::Result {
        return match self {
            SystemFunction::HTTP => write!(f, "HTTP"),
            SystemFunction::MEMCACHED => write!(f, "MEMCACHED"),
        };
    }
}

#[derive(Clone)]
pub enum FunctionConfig {
    ElfConfig(ElfConfig),
    SysConfig(SystemFunction),
}

pub struct Function {
    pub requirements: DataRequirementList,
    pub context: Arc<Context>,
    pub config: FunctionConfig,
}

impl Function {
    pub fn load(
        &self,
        domain: &Box<dyn MemoryDomain>,
        ctx_size: usize,
    ) -> DandelionResult<Context> {
        return match &self.config {
            FunctionConfig::ElfConfig(_) => {
                load_utils::load_static(domain, self.context.clone(), &self.requirements, ctx_size)
            }
            FunctionConfig::SysConfig(_) => domain.acquire_context(ctx_size),
        };
    }
}

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
    pub input_sets: Vec<(String, Option<CompositionSet>)>,
    /// The output set names.
    pub output_sets: Vec<String>,
}

pub enum WorkToDo {
    FunctionArguments {
        function: Arc<Function>,
        domain: Arc<Box<dyn MemoryDomain>>,
        context_size: usize,
        input_sets: Vec<Option<CompositionSet>>,
        metadata: Arc<Metadata>,
        recorder: Recorder,
    },
    ParsingArguments {
        driver: &'static dyn Driver,
        path: String,
        static_domain: Arc<Box<dyn MemoryDomain>>,
        recorder: Recorder,
    },
    Shutdown(),
}

pub enum WorkDone {
    Context(Context),
    Function(Function),
    Resources(Vec<ComputeResource>),
}

impl WorkDone {
    pub fn get_context(self) -> Context {
        return match self {
            WorkDone::Context(context) => context,
            _ => panic!("WorkDone is not context when context was expected"),
        };
    }
    pub fn get_function(self) -> Function {
        return match self {
            WorkDone::Function(function) => function,
            _ => panic!("WorkDone is not function when function was expected"),
        };
    }
}

pub trait EngineWorkQueue {
    fn get_engine_args(&self) -> (WorkToDo, crate::promise::Debt);
    fn try_get_engine_args(&self) -> Option<(WorkToDo, crate::promise::Debt)>;
}

impl futures::stream::Stream for &mut (dyn EngineWorkQueue + Send) {
    type Item = (WorkToDo, crate::promise::Debt);
    /// By default the behaviour of the work queue on polling is to call try_get_engine_args()
    /// If the call returns Some(tuple), the poll will returns Ready(tuple)
    /// Otherwise the poll function will call the waker and return pending.
    /// The waker is called, because the queue does not know when it becomes ready, it signals to be polled again.
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> core::task::Poll<Option<Self::Item>> {
        if let Some(tuple) = self.try_get_engine_args() {
            return core::task::Poll::Ready(Some(tuple));
        } else {
            cx.waker().wake_by_ref();
            return core::task::Poll::Pending;
        }
    }
}

pub trait Driver: Send + Sync {
    // the resource descirbed by config and make it into an engine of the type
    fn start_engine(
        &self,
        resource: ComputeResource,
        // TODO check out why this can't be impl instead of Box<dyn
        queue: Box<dyn EngineWorkQueue + Send>,
    ) -> DandelionResult<()>;

    // parses an executable,
    // returns the layout requirements and a context containing static data,
    //  and a layout description for it
    fn parse_function(
        &self,
        function_path: String,
        static_domain: &Box<dyn MemoryDomain>,
    ) -> DandelionResult<Function>;
}

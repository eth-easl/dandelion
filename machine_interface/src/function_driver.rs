use crate::{
    memory_domain::{Context, MemoryDomain},
    DataRequirementList, Position,
};
extern crate alloc;
use alloc::sync::Arc;
use dandelion_commons::{records::Recorder, DandelionError, DandelionResult};

#[cfg(feature = "wasm")]
use libloading::Library;

#[cfg(feature = "fpga")]
use std::net::{Ipv4Addr, SocketAddrV4};

pub mod compute_driver;
mod load_utils;
pub mod system_driver;
#[cfg(test)]
mod test_queue;
mod thread_utils;

#[derive(Clone)]
pub struct ElfConfig {
    // TODO change to positions
    system_data_offset: usize,
    #[cfg(feature = "cheri")]
    return_offset: (usize, usize),
    entry_point: usize,
    protection_flags: Arc<Vec<(u32, Position)>>,
}

#[derive(Clone, Copy)]
pub enum SystemFunction {
    HTTP,
}

impl core::fmt::Display for SystemFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> core::fmt::Result {
        return match self {
            SystemFunction::HTTP => write!(f, "HTTP"),
        };
    }
}

#[derive(Clone)]
pub struct WasmConfig {
    #[cfg(feature = "wasm")]
    lib: Arc<Library>,
    wasm_mem_size: usize,
    sdk_heap_base: usize,
    sdk_heap_size: usize,
    system_data_struct_offset: usize,
}

#[derive(Clone)]
pub struct FpgaConfig {
    #[cfg(feature = "fpga")]
    //TODO: other type of identifier?
    dummy_func_num: u16, //exists to differentiate dummy run functions. if 0 then it uses the real function.
}

#[derive(Clone)]
pub enum FunctionConfig {
    ElfConfig(ElfConfig),
    SysConfig(SystemFunction),
    WasmConfig(WasmConfig),
    FpgaConfig(FpgaConfig),
}

pub struct Function {
    pub requirements: DataRequirementList,
    pub context: Context,
    pub config: FunctionConfig,
}

impl Function {
    pub fn load(
        &self,
        domain: &'static dyn MemoryDomain,
        ctx_size: usize,
    ) -> DandelionResult<Context> {
        return match &self.config {
            FunctionConfig::ElfConfig(_) => {
                load_utils::load_static(domain, &self.context, &self.requirements, ctx_size)
            }
            FunctionConfig::SysConfig(_) => domain.acquire_context(ctx_size),
            FunctionConfig::WasmConfig(c) => {
                if ctx_size != c.wasm_mem_size {
                    return Err(DandelionError::WasmContextMemoryMismatch);
                }
                let mut context = domain.acquire_context(c.wasm_mem_size)?;
                context.occupy_space(0, c.sdk_heap_base)?;
                Ok(context)
            }
            FunctionConfig::FpgaConfig(_) => {
                //I don't think I have to do anything here
                domain.acquire_context(ctx_size)
            }
        };
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ComputeResource {
    CPU(u8),
    GPU(u8),
}

pub enum WorkToDo {
    FunctionArguments {
        config: FunctionConfig,
        context: Context,
        output_sets: Arc<Vec<String>>,
        recorder: Recorder,
    },
    TransferArguments {
        destination: Context,
        source: Arc<Context>,
        destination_set_index: usize,
        destination_allignment: usize,
        destination_item_index: usize,
        destination_set_name: String,
        source_set_index: usize,
        source_item_index: usize,
        recorder: Recorder,
    },
    ParsingArguments {
        driver: &'static dyn Driver,
        path: String,
        static_domain: &'static dyn MemoryDomain,
        recorder: Recorder,
    },
    LoadingArguments {
        function: Arc<Function>,
        domain: &'static dyn MemoryDomain,
        ctx_size: usize,
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

pub trait WorkQueue {
    fn get_engine_args(&self) -> (WorkToDo, crate::promise::Debt);
    fn try_get_engine_args(&self) -> Option<(WorkToDo, crate::promise::Debt)>;
}

impl futures::stream::Stream for &mut (dyn WorkQueue + Send) {
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
        queue: Box<dyn WorkQueue + Send>,
    ) -> DandelionResult<()>;

    // parses an executable,
    // returns the layout requirements and a context containing static data,
    //  and a layout description for it
    fn parse_function(
        &self,
        function_path: String,
        static_domain: &'static dyn MemoryDomain,
    ) -> DandelionResult<Function>;
}

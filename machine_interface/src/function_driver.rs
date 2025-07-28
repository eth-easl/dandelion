use std::collections::HashMap;
use crate::{
    memory_domain::{transfer_memory, Context, MemoryDomain},
    interface::DandelionSystemData,
    DataRequirementList,
};
extern crate alloc;
use alloc::sync::Arc;
use dandelion_commons::{records::Recorder, DandelionError, DandelionResult, FunctionId};
use serde::{Deserialize, Serialize};

#[cfg(feature = "wasm")]
use libloading::Library;

#[cfg(feature = "gpu")]
use self::compute_driver::gpu::config_parsing::ExecutionBlueprint;

pub mod compute_driver;
mod load_utils;
pub mod system_driver;

#[cfg(test)]
mod test_queue;
pub mod thread_utils;

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
#[allow(dead_code)]
pub struct WasmConfig {
    #[cfg(feature = "wasm")]
    lib: Arc<Library>,
    wasm_mem_size: usize,
    sdk_heap_base: usize,
    sdk_heap_size: usize,
    system_data_struct_offset: usize,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct GpuConfig {
    pub function_id: FunctionId,
    pub system_data_struct_offset: usize,
    pub code_object_offset: usize,
    pub kernels: Arc<Vec<HashMap<String, String>>>,
    #[cfg(feature = "gpu")]
    pub blueprint: Arc<ExecutionBlueprint>,
}

#[derive(Clone)]
pub enum FunctionConfig {
    ElfConfig(ElfConfig),
    SysConfig(SystemFunction),
    WasmConfig(WasmConfig),
    GpuConfig(GpuConfig),
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
        match &self.config {
            FunctionConfig::ElfConfig(_) => {
                load_utils::load_static(domain, self.context.clone(), &self.requirements, ctx_size)
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
            // no need to occupy space or anything like that as long as context is only inputs/outputs
            FunctionConfig::GpuConfig(cfg) => {
                // TODO : change here. 
                let mut ctxt = domain.acquire_context(ctx_size)?;
                // Make sure sysdata struct isn't overwritten, 0 = system_data_offset
                ctxt.occupy_space(
                    cfg.system_data_struct_offset,
                    std::mem::size_of::<DandelionSystemData<usize, usize>>(),
                )?;
                // Transfer code object
                transfer_memory(
                    &mut ctxt,
                    self.context.clone(),
                    cfg.code_object_offset,
                    0,
                    self.context.size,
                )?;
                // Mark code object storage as occupied
                ctxt.occupy_space(cfg.code_object_offset, self.context.size)?;
                Ok(ctxt)
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ComputeResource {
    CPU(u8),
    GPU(u8, u8, u8), // CPU core, GPU id, number of workers per GPU (only relevant for gpu_process). Eventually the CPU and GPU parts should be split
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
        static_domain: Arc<Box<dyn MemoryDomain>>,
        recorder: Recorder,
    },
    LoadingArguments {
        function: Arc<Function>,
        domain: Arc<Box<dyn MemoryDomain>>,
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
        queue: Box<dyn WorkQueue + Send + Sync>,
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

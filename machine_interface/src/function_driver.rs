use std::{collections::HashMap, sync::Mutex};

use crate::{
    interface::DandelionSystemData,
    memory_domain::{transfer_memory, Context, MemoryDomain},
    DataRequirementList, Position,
};
extern crate alloc;
use alloc::sync::Arc;
use dandelion_commons::{records::Recorder, DandelionError, DandelionResult};

#[cfg(feature = "wasm")]
use libloading::Library;
use serde::{Deserialize, Serialize};

#[cfg(feature = "gpu")]
use self::compute_driver::gpu::{config_parsing::ExecutionBlueprint, hip::FunctionT};

pub mod compute_driver;
mod load_utils;
pub mod system_driver;
// #[cfg(test)]
pub mod test_queue;
pub mod thread_utils;

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

#[derive(Clone, Serialize, Deserialize)]
pub struct GpuConfig {
    pub system_data_struct_offset: usize,
    pub module_offset: usize,
    pub kernels: Arc<Vec<String>>,
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
    pub context: Context,
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
            // no need to occupy space or anything like that as long as context is only inputs/outputs
            FunctionConfig::GpuConfig(cfg) => {
                let mut ctxt = domain.acquire_context(ctx_size)?;
                // Make sure sysdata struct isn't overwritten, 0 = system_data_offset
                ctxt.occupy_space(
                    cfg.system_data_struct_offset,
                    std::mem::size_of::<DandelionSystemData<usize, usize>>(),
                )?;
                transfer_memory(
                    &mut ctxt,
                    &self.context,
                    cfg.module_offset,
                    0,
                    self.context.size,
                )?;
                ctxt.occupy_space(cfg.module_offset, self.context.size)?;
                Ok(ctxt)
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ComputeResource {
    CPU(u8),
    GPU(u8, u8), // TODO change back to GPU(u8) once Driver.start_engine() takes a vec of ComputeResources
}

pub enum EngineArguments {
    FunctionArguments(FunctionArguments),
    TransferArguments(TransferArguments),
    Shutdown(fn(Vec<ComputeResource>) -> ()),
}

pub struct FunctionArguments {
    pub config: FunctionConfig,
    pub context: Context,
    pub output_sets: Arc<Vec<String>>,
    pub recorder: Recorder,
}

pub struct TransferArguments {
    pub destination: Context,
    pub source: Arc<Context>,
    pub destination_set_index: usize,
    pub destination_allignment: usize,
    pub destination_item_index: usize,
    pub destination_set_name: String,
    pub source_set_index: usize,
    pub source_item_index: usize,
    pub recorder: Recorder,
}

pub trait WorkQueue {
    fn get_engine_args(&self) -> (EngineArguments, crate::promise::Debt);
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

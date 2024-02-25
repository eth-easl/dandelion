use crate::{
    memory_domain::{Context, ContextType, MemoryDomain},
    DataRequirementList, Position,
};
use core::pin::Pin;
use dandelion_commons::{records::Recorder, DandelionResult, DandelionError};
use std::{future::Future, sync::Arc};

#[cfg(feature = "wasm")]
use libloading::Library;

#[cfg(any(feature = "wasmtime-jit", feature = "wasmtime-precomp"))]
use wasmtime;
#[cfg(any(feature = "wasmtime-jit", feature = "wasmtime-precomp"))]
use crate::function_driver::compute_driver::wasmtime as wasmtime_driver;

pub mod compute_driver;
mod load_utils;
pub mod system_driver;
mod thread_utils;

#[cfg(any(feature = "mmu", feature = "cheri"))]
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

#[cfg(feature = "wasm")]
#[derive(Clone)]
pub struct WasmConfig {
    lib: Arc<Library>,
    wasm_mem_size: usize,
    sdk_heap_base: usize,
    sdk_heap_size: usize,
    system_data_struct_offset: usize,
}

#[cfg(any(feature = "wasmtime-jit", feature = "wasmtime-precomp"))]
#[derive(Clone)]
pub struct WasmtimeConfig {
    precompiled_module: Vec<u8>,
    total_mem_size: usize,
    sdk_heap_base: usize,
    system_data_struct_offset: usize,
    wasmtime_engine: wasmtime::Engine,  // engine can be shared across threads
}

#[derive(Clone)]
pub enum FunctionConfig {
    #[cfg(any(feature = "mmu", feature = "cheri"))]
    ElfConfig(ElfConfig),
    SysConfig(SystemFunction),
    #[cfg(feature = "wasm")]
    WasmConfig(WasmConfig),
    #[cfg(any(feature = "wasmtime-jit", feature = "wasmtime-precomp"))]
    WasmtimeConfig(WasmtimeConfig),
}

pub struct Function {
    pub requirements: DataRequirementList,
    pub context: Context,
    pub config: FunctionConfig,
}

impl Function {
    pub fn load(&self, domain: &Box<dyn MemoryDomain>) -> DandelionResult<Context> {
        return match &self.config {
            #[cfg(any(feature = "mmu", feature = "cheri"))]
            FunctionConfig::ElfConfig(_) => {
                load_utils::load_static(domain, &self.context, &self.requirements)
            },
            FunctionConfig::SysConfig(_) => domain.acquire_context(self.requirements.size),
            #[cfg(feature = "wasm")]
            FunctionConfig::WasmConfig(c) => {
                let mut context = domain.acquire_context(c.wasm_mem_size)?;
                context.occupy_space(0, c.sdk_heap_base)?;
                Ok(context)
            },
            #[cfg(any(feature = "wasmtime-jit", feature = "wasmtime-precomp"))]
            FunctionConfig::WasmtimeConfig(c) => {
                wasmtime_driver::load_context(c, domain)
            },
        };
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ComputeResource {
    CPU(u8),
    GPU(u8),
}

pub trait Engine: Send {
    fn run(
        &mut self,
        config: &FunctionConfig,
        context: Context,
        output_set_names: &Vec<String>,
        recorder: Recorder,
    ) -> Pin<Box<dyn Future<Output = (DandelionResult<()>, Context)> + '_ + Send>>;
    // TODO make more sensible, as a both functions require self mut, so abort can never be called on a running function
    fn abort(&mut self) -> DandelionResult<()>;
}
// TODO figure out if we could / should enforce proper drop behaviour
// we could add a uncallable function with a private token that is not visible outside,
// but not sure if that is necessary

pub trait Driver: Send + Sync {
    // the resource descirbed by config and make it into an engine of the type
    fn start_engine(&self, resource: ComputeResource) -> DandelionResult<Box<dyn Engine>>;

    // parses an executable,
    // returns the layout requirements and a context containing static data,
    //  and a layout description for it
    fn parse_function(
        &self,
        function_path: String,
        static_domain: &Box<dyn MemoryDomain>,
    ) -> DandelionResult<Function>;
}

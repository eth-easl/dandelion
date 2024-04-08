use crate::{
    memory_domain::{Context, MemoryDomain},
    DataRequirementList, Position,
};
extern crate alloc;
use alloc::sync::Arc;
use dandelion_commons::{records::Recorder, DandelionError, DandelionResult};

#[cfg(feature = "wasm")]
use libloading::Library;

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
pub enum FunctionConfig {
    ElfConfig(ElfConfig),
    SysConfig(SystemFunction),
    WasmConfig(WasmConfig),
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
        };
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ComputeResource {
    CPU(u8),
    GPU(u8),
}

pub enum WorkToDo {
    FunctionArguments(FunctionArguments),
    TransferArguments(TransferArguments),
    ParsingArguments(ParsingArguments),
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

pub struct ParsingArguments {
    pub driver: &'static dyn Driver,
    pub path: String,
    pub static_domain: &'static dyn MemoryDomain,
    pub recorder: Recorder,
}

pub trait WorkQueue {
    fn get_engine_args(&self) -> (WorkToDo, crate::promise::Debt);
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

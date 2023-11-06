use crate::{
    memory_domain::{Context, MemoryDomain},
    DataRequirementList,
};
use core::pin::Pin;
use dandelion_commons::{records::Recorder, DandelionResult};
use std::{future::Future, rc::Rc};

use libloading::Library;

pub mod compute_driver;
pub mod system_driver;
pub mod util;

#[derive(Clone, Copy)]
pub struct ElfConfig {
    // TODO change to positions
    system_data_offset: usize,
    return_offset: (usize, usize),
    entry_point: usize,
}

#[derive(Clone, Copy)]
pub enum SystemFunction {
    HTTPS,
}

// #[derive(Clone, Copy)]
pub struct WasmConfig {
    lib: Rc<Library>,
    system_data_region_base: usize,
    system_data_region_end: usize,
    sdk_heap_base: usize,
    system_data_struct_offset: usize,
}

// #[derive(Clone, Copy)]
pub enum FunctionConfig {
    ElfConfig(ElfConfig),
    SysConfig(SystemFunction),
    WasmConfig(WasmConfig),
}

pub trait Engine: Send {
    fn run(
        &mut self,
        config: &FunctionConfig,
        context: Context,
        output_set_names: &Vec<String>,
        recorder: Recorder,
    ) -> Pin<Box<dyn Future<Output = (DandelionResult<()>, Context)> + '_ + Send>>;
    fn abort(&mut self) -> DandelionResult<()>;
}
// TODO figure out if we could / should enforce proper drop behaviour
// we could add a uncallable function with a private token that is not visible outside,
// but not sure if that is necessary

// TODO maybe combine driver and loader into one trait or replace them completely with function signatrue types
pub trait Driver: Send+Sync {
    // the resource descirbed by config and make it into an engine of the type
    fn start_engine(&self, config: Vec<u8>) -> DandelionResult<Box<dyn Engine>>;

    // parses an executable,
    // returns the layout requirements and a context containing static data,
    //  and a layout description for it
    fn parse_function(
        &self,
        function: Vec<u8>,
        static_domain: &Box<dyn MemoryDomain>,
    ) -> DandelionResult<Function>;
}

// TODO should be private?
pub struct Function {
    pub requirements: DataRequirementList,
    pub context: Context,
    pub config: FunctionConfig,
}

use crate::{
    memory_domain::{Context, MemoryDomain},
    DataRequirementList,
};
use core::pin::Pin;
use dandelion_commons::DandelionResult;
use std::future::Future;

// list of implementations
#[cfg(feature = "cheri")]
pub mod cheri;

#[cfg(feature = "pagetable")]
pub mod pagetable;
pub mod util;

#[derive(Clone, Copy)]
pub struct ElfConfig {
    // TODO change to positions
    input_root: (usize, usize),
    input_number: (usize, usize),
    output_root: (usize, usize),
    output_number: (usize, usize),
    max_output_number: (usize, usize),
    return_offset: (usize, usize),
    entry_point: usize,
}

#[derive(Clone, Copy)]
pub enum FunctionConfig {
    ElfConfig(ElfConfig),
}

pub trait Engine: Send {
    fn run(
        &mut self,
        config: &FunctionConfig,
        context: Context,
    ) -> Pin<Box<dyn Future<Output = (DandelionResult<()>, Context)> + '_ + Send>>;
    fn abort(&mut self) -> DandelionResult<()>;
}
// TODO figure out if we could / should enforce proper drop behaviour
// we could add a uncallable function with a private token that is not visible outside,
// but not sure if that is necessary

pub type DriverFunction = fn(Vec<u8>) -> DandelionResult<Box<dyn Engine>>;

// TODO maybe combine driver and loader into one trait or replace them completely with function signatrue types
pub trait Driver {
    // the resource descirbed by config and make it into an engine of the type
    fn start_engine(config: Vec<u8>) -> DandelionResult<Box<dyn Engine>>;
}

pub type LoaderFunction = fn(
    Vec<u8>,
    &Box<dyn MemoryDomain>,
) -> DandelionResult<(DataRequirementList, Context, FunctionConfig)>;

pub trait Loader {
    // parses an executable,
    // returns the layout requirements and a context containing static data,
    //  and a layout description for it
    fn parse_function(
        function: Vec<u8>,
        static_domain: &Box<dyn MemoryDomain>,
    ) -> DandelionResult<(DataRequirementList, Context, FunctionConfig)>;
}

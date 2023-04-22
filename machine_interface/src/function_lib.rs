use super::memory_domain::{Context, MemoryDomain};
use super::{DataRequirementList, HwResult};

// list of implementations
#[cfg(feature = "cheri")]
mod cheri;

#[cfg(feature = "pagetable")]
mod pagetable;
mod util;

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

pub enum FunctionConfig {
    ElfConfig(ElfConfig),
}

pub trait Engine {
    fn run(&mut self, config: &FunctionConfig, context: Context) -> (HwResult<()>, Context);
    fn abort(&mut self) -> HwResult<()>;
}

// todo find better name
pub trait Driver {
    // required parts of the trait
    type E: Engine;
    fn new(config: Vec<u8>) -> HwResult<Self>
    where
        Self: Sized;
    // take or release one of the available engines
    fn start_engine(&mut self) -> HwResult<Self::E>;
    fn stop_engine(&mut self, engine: Self::E) -> HwResult<()>;
}

pub trait Loader {
    // parses an executable,
    // returns the layout requirements and a context containing static data,
    //  and a layout description for it
    fn parse_function(
        function: Vec<u8>,
        static_domain: &mut dyn MemoryDomain,
    ) -> HwResult<(DataRequirementList, Context, FunctionConfig)>;
}

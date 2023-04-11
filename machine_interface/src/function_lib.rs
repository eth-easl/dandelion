use super::memory_domain::{Context, MemoryDomain};
use super::{DataItem, DataRequirementList, HwResult};

// list of implementations
#[cfg(feature = "cheri")]
mod cheri;

pub struct ElfConfig {
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
    fn run(self, config: FunctionConfig, context: Context) -> (HwResult<()>, Context);
    fn abort(self) -> HwResult<Context>;
}

// todo find better name
pub trait Driver {
    // required parts of the trait
    type E: Engine;
    fn new(config: Vec<u8>) -> HwResult<Self>
    where
        Self: Sized;
    // take or release one of the available engines
    fn start_engine(self, id: u8) -> HwResult<Self::E>;
    fn stop_engine(self, engine: Self::E) -> HwResult<()>;
}

pub trait Navigator {
    // parses an executable,
    // returns the layout requirements and a context containing static data,
    //  and a layout description for it
    fn parse_function(
        function: Vec<u8>,
        static_domain: &dyn MemoryDomain,
    ) -> HwResult<(DataRequirementList, Context, FunctionConfig)>;
}

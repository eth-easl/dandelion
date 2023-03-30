use super::memory_domain::{Context, MemoryDomain};
use super::{DataItem, DataRequirementList, HwResult};

// list of implementations
mod cheri;

pub trait Engine {
    fn run(
        self,
        // code: Self::FunctionConfig,
        context: Context,
        layout: Vec<DataItem>,
        callback: impl FnOnce(HwResult<(Context, Vec<DataItem>)>) -> (),
    ) -> HwResult<()>;
    fn abort(id: u32, callback: impl FnOnce(HwResult<Context>) -> ()) -> HwResult<()>;
}

// todo find better name
pub trait Driver {
    // required parts of the trait
    type E: Engine;
    // take or release one of the available engines
    fn start_engine() -> HwResult<Self::E>;
    fn stop_engine(self) -> HwResult<()>;
}

pub trait Navigator {
    // parses an executable,
    // returns the layout requirements and a context containing static data,
    //  and a layout description for it
    fn parse_function(
        config: Vec<u8>,
        static_domain: &dyn MemoryDomain,
    ) -> HwResult<(DataRequirementList, Context, Vec<DataItem>)>;
}

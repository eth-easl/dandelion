// separate file for memory domain enum definition and transfer functions
pub mod memory_domain;
use self::memory_domain::{Context, MemoryDomain};

// define all interfaces to hardware dependent part of dandelion
use std::vec::Vec;
// TODO define error types, possibly better printing than debug
#[derive(Debug, PartialEq)]
pub enum HardwareError {
    Default,
    // memory errors
    ContextMissmatch, // context handed to context specific function was wrong type
    OutOfMemory,      // domain could not be allocated because there is no space available
    InvalidRead,      // tried to read from domain outside of domain bounds
    InvalidWrite,     // tried to write to domain ouside of domain bounds
}

pub type HwResult<T> = std::result::Result<T, HardwareError>;

enum RequirementType {
    StaticData,
    Input,
}

enum OffsetOrAlignment {
    Offset(isize),
    Allignment(isize),
}

enum SizeRequirement {
    Range(isize, isize),
    ModResidual(isize, isize),
}

struct DataRequirement {
    id: u32,
    req_type: RequirementType,
    position: Option<OffsetOrAlignment>,
    size: Option<SizeRequirement>,
}

struct DataRequirementList {
    domain_id: i32,
    requirements: Vec<DataRequirement>,
}

struct Position {
    offset: isize,
    size: isize,
}

enum DataItemType {
    Item(Position),
    Set(Vec<Position>),
}

pub struct DataItem {
    index: u32,
    item_type: DataItemType,
}

pub trait Engine {
    fn run(
        self,
        // code: Self::FunctionConfig,
        // contexts: Vec<dyn Context>,
        layout: Vec<DataItem>,
        // callback: impl FnOnce(HwResult<(Vec<dyn Context>, Vec<DataItem>)>) -> (),
    ) -> HwResult<()>;
    // fn abort(id: u32, callback: impl FnOnce(HwResult<Vec<dyn Context>>) -> ()) -> HwResult<()>;
}

// todo find better name
pub trait Driver {
    // required parts of the trait
    type E: Engine;
    // take or release one of the available engines
    fn start_engine() -> HwResult<Self::E>;
    fn stop_engine(self) -> HwResult<()>;
}

pub trait Navigator {}

// Todo implement dropping behaviour for compute controller
// impl Drop for ExecutionUnit {
//     fn drop(& mut self){
//         self.removeEU();
//     }
// }

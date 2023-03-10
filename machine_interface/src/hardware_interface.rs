// separate file for memory domain enum definition and transfer functions
pub mod memory_domain;
use self::memory_domain::MemoryDomain;

// define all interfaces to hardware dependent part of dandelion
use std::vec::Vec;
// TODO define error types, possibly better printing than debug
#[derive(Debug, PartialEq)]
pub enum ControllerError {
    Default,
    // domain errors
    InvalidDomain, // trying to perform domain actions on a None domain.
    OutOfMemory,   // domain could not be allocated because there is no space available
    InvalidRead,   // tried to read from domain outside of domain bounds
    InvalidWrite,  // tried to write to domain ouside of domain bounds
}

pub type HwResult<T> = std::result::Result<T, ControllerError>;

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

struct DataRequirementList<'controller> {
    domain_type: MemoryDomain<'controller>,
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

// todo find better name
pub trait ComputeController {
    // required parts of the trait
    type FunctionConfig;
    // initialization and shutdown
    fn start_compute_controller(config: Vec<u8>) -> HwResult<Box<Self>>;
    fn stop_compute_controller(self) -> HwResult<()>;
    // compute unit setup
    fn take_computer(id: u32) -> HwResult<()>;
    fn yield_computer(id: u32) -> HwResult<()>;
    // code setup
    fn setup_function_code(code: Vec<u8>) -> HwResult<Self::FunctionConfig>;
    fn teardown_function_code(config: Self::FunctionConfig) -> HwResult<()>;
    fn run(
        id: u32,
        code: Self::FunctionConfig,
        domain: MemoryDomain,
        input_positions: Vec<DataItem>,
        callback: impl FnOnce(HwResult<(MemoryDomain, Vec<DataItem>)>) -> (),
    ) -> HwResult<()>;
    fn stop(id: u32, callback: impl FnOnce(HwResult<MemoryDomain>) -> ()) -> HwResult<()>;
}

// Todo implement dropping behaviour for compute controller
// impl Drop for ExecutionUnit {
//     fn drop(& mut self){
//         self.removeEU();
//     }
// }

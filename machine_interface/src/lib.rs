pub mod function_lib;
pub mod memory_domain;
mod util;

// TODO define error types, possibly better printing than debug
#[derive(Debug, PartialEq)]
pub enum HardwareError {
    Default,
    // errors in configurations
    MalformedConfig, // configuration vector was malformed
    UnknownSymbol,
    // memory errors
    ContextMissmatch, // context handed to context specific function was wrong type
    OutOfMemory,      // domain could not be allocated because there is no space available
    InvalidRead,      // tried to read from domain outside of domain bounds
    InvalidWrite,     // tried to write to domain ouside of domain bounds
    // engine errors
    ConfigMissmatch, // missmatch between the function config the engine expects and the one given
    NoRunningFunction, // attempted abort when no function was running
}

pub type HwResult<T> = std::result::Result<T, HardwareError>;

#[derive(PartialEq, Debug)]
pub enum OffsetOrAlignment {
    Offset(usize),
    Allignment(usize),
}

#[derive(PartialEq, Debug)]
pub enum SizeRequirement {
    Range(usize, usize),
    ModResidual(usize, usize),
}

#[derive(PartialEq, Debug)]
pub struct DataRequirement {
    pub id: u32,
    pub position: Option<OffsetOrAlignment>,
    pub size: Option<SizeRequirement>,
}

pub struct DataRequirementList {
    // domain_id: i32,
    pub size: usize,
    pub input_requirements: Vec<DataRequirement>,
    pub static_requirements: Vec<Position>,
}

pub struct Position {
    pub offset: usize,
    pub size: usize,
}

pub enum DataItemType {
    Item(Position),
    Set(Vec<Position>),
}

pub struct DataItem {
    pub index: u32,
    pub item_type: DataItemType,
}

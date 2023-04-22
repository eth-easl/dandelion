pub mod function_lib;
pub mod memory_domain;
pub mod util;

use serde::{Serialize, Deserialize};

// TODO define error types, possibly better printing than debug
#[derive(Debug, PartialEq)]
pub enum HardwareError {
    NotImplemented, // trying to use a feature that is not yet implemented
    // errors in configurations
    MalformedConfig, // configuration vector was malformed
    UnknownSymbol,
    // memory errors
    ContextMissmatch, // context handed to context specific function was wrong type
    OutOfMemory,      // domain could not be allocated because there is no space available
    ContextFull,      // context can't fit additional memory
    InvalidRead,      // tried to read from domain outside of domain bounds
    InvalidWrite,     // tried to write to domain ouside of domain bounds
    // engine errors
    ConfigMissmatch, // missmatch between the function config the engine expects and the one given
    NoRunningFunction, // attempted abort when no function was running
    EngineAlreadyRunning, // attempted to run on already busy engine
    EngineError,     // there was a non recoverable issue with the engine
    NoEngineAvailable, // asked driver for engine, but there are no more available
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Position {
    pub offset: usize,
    pub size: usize,
}

#[derive(Debug)]
pub enum DataItemType {
    Item(Position),
    Set(Vec<Position>),
}

#[derive(Debug)]
pub struct DataItem {
    pub index: u32,
    pub item_type: DataItemType,
}

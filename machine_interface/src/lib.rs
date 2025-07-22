pub mod function_driver;
pub mod memory_domain;
pub mod promise;

/// Module contains all the information needed about available engines,
/// contexts, and their compatibility with fast to look up structs
pub mod machine_config;

#[cfg(any(feature = "cheri", feature = "mmu", feature = "kvm", feature = "wasm"))]
mod interface;
pub mod util;

use serde::{Deserialize, Serialize};

#[derive(PartialEq, Debug, Clone, Copy)]
pub enum OffsetOrAlignment {
    Offset(usize),
    Allignment(usize),
}

#[derive(PartialEq, Debug, Clone, Copy)]
pub enum SizeRequirement {
    Range(usize, usize),
    ModResidual(usize, usize),
}

#[derive(PartialEq, Debug, Clone, Copy)]
pub struct DataRequirement {
    pub id: u32,
    pub position: Option<OffsetOrAlignment>,
    pub size: Option<SizeRequirement>,
}

#[derive(Debug, Clone)]
pub struct DataRequirementList {
    // domain_id: i32,
    pub input_requirements: Vec<DataRequirement>,
    /// Mapping of the positions in the Vec<u8> that is returned from parsing,
    /// to where the data needs to go in the function context.
    pub static_requirements: Vec<(Position, Position)>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct Position {
    pub offset: usize,
    pub size: usize,
}

#[derive(Debug)]
pub struct DataSet {
    pub ident: String,
    pub buffers: Vec<DataItem>,
}

#[derive(Debug, Clone)]
pub struct DataItem {
    pub ident: String,
    pub data: Position,
    pub key: u32,
}

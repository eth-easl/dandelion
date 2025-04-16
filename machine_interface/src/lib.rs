pub mod function_driver;
pub mod memory_domain;
pub mod promise;

/// Module contains all the information needed about available engines,
/// contexts, and their compatibility with fast to look up structs
pub mod machine_config;

#[cfg(any(
    feature = "cheri",
    feature = "mmu",
    feature = "wasm",
    feature = "kvm",
    feature = "gpu",
    feature = "reqwest"
))]
mod interface;
pub mod util;

use serde::{Deserialize, Serialize};

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

#[derive(Debug)]
pub struct DataRequirementList {
    // domain_id: i32,
    pub input_requirements: Vec<DataRequirement>,
    pub static_requirements: Vec<Position>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct Position {
    pub offset: usize,
    pub size: usize,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DataSet {
    pub ident: String,
    pub buffers: Vec<DataItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataItem {
    pub ident: String,
    pub data: Position,
    pub key: u32,
}

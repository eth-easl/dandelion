pub mod function_lib;
pub mod memory_domain;
mod util;

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

#[derive(Clone, Copy, Debug)]
pub struct Position {
    pub offset: usize,
    pub size: usize,
}

#[derive(Debug)]
pub struct DataSet {
    pub ident: String,
    pub buffers: Vec<DataItem>,
}

#[derive(Debug)]
pub struct DataItem {
    pub ident: String,
    pub data: Position,
}

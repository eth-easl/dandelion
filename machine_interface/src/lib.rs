pub mod function_driver;
pub mod memory_domain;

#[cfg(any(feature = "cheri"))]
mod interface;
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

#[derive(Debug)]
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

#[derive(Debug, Clone)]
pub struct DataItem {
    pub ident: String,
    pub data: Position,
}

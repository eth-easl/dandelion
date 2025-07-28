use super::gpu_api::{Function, Module};
use crate::function_driver::GpuConfig;
use dandelion_commons::{DandelionError, DandelionResult};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fs::File, io::BufReader, sync::Arc};

pub const SYSDATA_OFFSET: usize = 0usize;

#[derive(Deserialize, Serialize, Debug)]
pub enum Sizing {
    Sizeof(String),
    Absolute(u64),
    FromInput { bufname: String, idx: usize },
}

#[derive(Deserialize, Serialize, Debug)]
pub enum Argument {
    Ptr(String),
    Sizeof(String),
    Constant(i64),
}

#[derive(Deserialize, Serialize, Debug)]
pub struct LaunchConfig {
    pub grid_dim_x: Sizing,
    pub grid_dim_y: Sizing,
    pub grid_dim_z: Sizing,
    pub block_dim_x: Sizing,
    pub block_dim_y: Sizing,
    pub block_dim_z: Sizing,
    pub shared_mem_bytes: Sizing,
}

#[derive(Deserialize, Serialize, Debug)]
pub enum Action {
    ExecKernel(String, Vec<Argument>, Box<LaunchConfig>),
    Repeat(Sizing, Vec<Action>),
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ExecutionBlueprint {
    pub inputs: Vec<String>,
    pub weights: Vec<String>,
    pub buffers: HashMap<String, Sizing>,
    pub outputs: Vec<String>,
    pub control_flow: Vec<Action>,
}

#[derive(Deserialize, Serialize, Debug)]
struct GpuConfigIR {
    modules: Vec<HashMap<String, String>>,
    kernels: Vec<HashMap<String, String>>,
    blueprint: ExecutionBlueprint,
}

impl From<GpuConfigIR> for GpuConfig {
    fn from(value: GpuConfigIR) -> Self {
        Self {
            function_id: u64::MAX,
            system_data_struct_offset: SYSDATA_OFFSET,
            code_object_offset: 0,
            kernels: Arc::new(value.kernels),
            blueprint: Arc::new(value.blueprint),
        }
    }
}

#[derive(Clone)]
pub struct RuntimeGpuConfig {
    pub system_data_struct_offset: usize,
    pub modules: Arc<Vec<Module>>,
    pub kernels: Arc<HashMap<String, Function>>,
    pub blueprint: Arc<ExecutionBlueprint>,
}

pub fn parse_config(path: &str) -> DandelionResult<GpuConfig> {
    let file = File::open(path).map_err(|_| DandelionError::FileError)?;
    let reader = BufReader::new(file);
    let ir: GpuConfigIR = serde_json::from_reader(reader)
        .map_err(|e| DandelionError::ParsingJSONError(format!("{e}")))?;
    Ok(ir.into())
}

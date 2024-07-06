use std::{collections::HashMap, fs::File, io::BufReader, os::raw::c_void, sync::Arc};

use dandelion_commons::{DandelionError, DandelionResult};
use serde::{Deserialize, Serialize};

use crate::function_driver::GpuConfig;

use super::hip::{self, FunctionT, ModuleT};

pub const SYSDATA_OFFSET: usize = 0usize;

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
pub enum Argument {
    Ptr(String),
    Sizeof(String),
    Constant(i64),
}

#[derive(Deserialize, Serialize, Debug)]
pub enum Action {
    ExecKernel(String, Vec<Argument>, Box<LaunchConfig>),
    Repeat(Sizing, Vec<Action>),
}

#[derive(Deserialize, Serialize, Debug)]
pub enum Sizing {
    Sizeof(String),
    Absolute(u64),
    FromInput { bufname: String, idx: usize },
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ExecutionBlueprint {
    pub inputs: Vec<String>,
    /// buffer names to sizes
    pub buffers: HashMap<String, Sizing>,
    pub outputs: Vec<String>, // might not be required
    pub control_flow: Vec<Action>,
}

#[derive(Deserialize, Serialize, Debug)]
struct GpuConfigIR {
    module_path: String,
    kernels: Vec<String>,
    blueprint: ExecutionBlueprint,
}

impl From<GpuConfigIR> for GpuConfig {
    fn from(value: GpuConfigIR) -> Self {
        Self {
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
    pub module: Arc<ModuleT>,
    pub kernels: Arc<HashMap<String, FunctionT>>,
    pub blueprint: Arc<ExecutionBlueprint>,
}

impl GpuConfig {
    pub fn load(self, base: *const u8) -> DandelionResult<RuntimeGpuConfig> {
        let module = unsafe {
            hip::module_load_data(base.wrapping_add(self.code_object_offset) as *const c_void)?
        };
        let kernels = self
            .kernels
            .iter()
            .map(|kname| {
                hip::module_get_function(&module, kname).map(|module| (kname.clone(), module))
            })
            .collect::<Result<HashMap<_, _>, _>>()?; // this is kinda ugly but also pretty cool
        Ok(RuntimeGpuConfig {
            system_data_struct_offset: SYSDATA_OFFSET,
            module: Arc::new(module),
            kernels: Arc::new(kernels),
            blueprint: self.blueprint,
        })
    }
}

pub fn parse_config(path: &str) -> DandelionResult<(GpuConfig, String)> {
    let file = File::open(path).map_err(|_| DandelionError::ConfigMissmatch)?;
    let reader = BufReader::new(file);
    let ir: GpuConfigIR = serde_json::from_reader(reader).map_err(|e| {
        eprintln!("Parsing error: {e}");
        DandelionError::ConfigMissmatch
    })?;
    // Copy kind of unnecessary as ir.into() doesn't need the string, but less bug prone than .drain(..).collect()
    let module_path = ir.module_path.clone();
    Ok((ir.into(), module_path))
}

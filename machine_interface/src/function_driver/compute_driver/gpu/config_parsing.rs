use std::{collections::HashMap, fs::File, io::BufReader, os::raw::c_void, sync::Arc};

use dandelion_commons::{DandelionError, DandelionResult};
use serde::{Deserialize, Serialize};

use crate::function_driver::GpuConfig;

use super::gpu_api::{self, Function, Module};

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
    modules: Vec<HashMap<String, String>>,
    kernels: Vec<HashMap<String, String>>,
    blueprint: ExecutionBlueprint,
}

impl From<GpuConfigIR> for GpuConfig {
    fn from(value: GpuConfigIR) -> Self {
        Self {
            system_data_struct_offset: SYSDATA_OFFSET,
            code_object_offset: 0,
            modules_offsets: Arc::new(HashMap::new()),
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

impl GpuConfig {
    pub fn load(self, base: *const u8) -> DandelionResult<RuntimeGpuConfig> {
        let base = base.wrapping_add(self.code_object_offset);

        let mut to_load: HashMap<String, Vec<String>> = HashMap::new();
        for kernel in self.kernels.iter() {
            let module_name = kernel.get("module_name").ok_or(DandelionError::UnknownSymbol)?;
            let kernel_name = kernel.get("kernel_name").ok_or(DandelionError::UnknownSymbol)?;

            to_load
                .entry(module_name.clone())
                .or_insert_with(Vec::new)
                .push(kernel_name.clone());
        }

        let mut modules = Vec::new();
        let mut kernels = HashMap::new();
        for (module_name, kernels_names) in &to_load {
            let offset = self.modules_offsets.get(module_name).ok_or(DandelionError::UnknownSymbol)?;
            let base_module = base.wrapping_add(offset.clone());

            let module = gpu_api::module_load_data(base_module as *const c_void)?;
            
            for kernel_name in kernels_names.iter() {
                let kernel = gpu_api::module_get_function(&module, kernel_name)?;
                let _ = kernels.insert(kernel_name.to_string(), kernel).ok_or(DandelionError::UnknownSymbol);
            }

            modules.push(module);
        }
        
        Ok(RuntimeGpuConfig {
            system_data_struct_offset: SYSDATA_OFFSET,
            modules: Arc::new(modules),
            kernels: Arc::new(kernels),
            blueprint: self.blueprint,
        })
    }
}

pub fn parse_config(path: &str) -> DandelionResult<(GpuConfig, Vec<HashMap<String, String>>)> {
    let file = File::open(path).map_err(|_| DandelionError::FileError)?;
    let reader = BufReader::new(file);
    let ir: GpuConfigIR = serde_json::from_reader(reader)
        .map_err(|e| DandelionError::ParsingJSONError(format!("{e}")))?;
    // Copy kind of unnecessary as ir.into() doesn't need the string, but less bug prone than .drain(..).collect()
    let modules_info = ir.modules.clone();
    Ok((ir.into(), modules_info))
}

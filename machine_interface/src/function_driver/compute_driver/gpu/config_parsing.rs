use std::{collections::HashMap, fs::File, io::BufReader, os::raw::c_void, sync::Arc};

use dandelion_commons::{DandelionError, DandelionResult};
use serde::{Deserialize, Serialize};

use crate::{
    function_driver::GpuConfig,
    memory_domain::{Context, ContextTrait},
};

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
    Repeat(u32, Vec<Action>),
}

#[derive(Deserialize, Serialize, Debug)]
pub enum Sizing {
    Sizeof(String),
    Absolute(usize),
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
pub struct GpuConfigIR {
    pub module_path: String,
    pub kernels: Vec<String>,
    pub blueprint: ExecutionBlueprint,
}

// impl From<GpuConfigIR> for GpuConfig {
//     fn from(value: GpuConfigIR) -> Self {
//         Self {
//             system_data_struct_offset: SYSDATA_OFFSET,
//             module_offset: 0,
//             kernels: Arc::new(value.kernels),
//             blueprint: Arc::new(value.blueprint),
//         }
//     }
// }

#[derive(Clone)]
pub struct RuntimeGpuConfig {
    pub system_data_struct_offset: usize,
    pub module: Arc<ModuleT>,
    pub kernels: Arc<HashMap<String, FunctionT>>,
    pub blueprint: Arc<ExecutionBlueprint>,
}

impl GpuConfig {
    pub fn load(self, base: *const u8, context: &mut Context) -> DandelionResult<RuntimeGpuConfig> {
        let module =
            hip::module_load_data(base.wrapping_add(self.code_object_offset) as *const c_void)?;
        let kernels = self
            .kernels
            .iter()
            .map(|kname| {
                hip::module_get_function(&module, kname).map(|module| (kname.clone(), module))
            })
            .collect::<Result<HashMap<_, _>, _>>()?; // this is kinda ugly but also pretty cool

        // Deserialise blueprint
        let mut blueprint_buffer = vec![0u8; self.blueprint_size];
        context.read(self.blueprint_offset, &mut blueprint_buffer)?;
        let blueprint: ExecutionBlueprint = serde_json::from_slice(&blueprint_buffer).unwrap();

        Ok(RuntimeGpuConfig {
            system_data_struct_offset: SYSDATA_OFFSET,
            module: Arc::new(module),
            kernels: Arc::new(kernels),
            blueprint: Arc::new(blueprint),
        })
    }
}

pub fn parse_config(path: &str) -> DandelionResult<GpuConfigIR> {
    // TODO: better erros
    let file = File::open(path).map_err(|_| DandelionError::ConfigMissmatch)?;
    let reader = BufReader::new(file);
    let mut ir: GpuConfigIR = serde_json::from_reader(reader).map_err(|e| {
        eprintln!("Parsing error: {e}");
        DandelionError::ConfigMissmatch
    })?;
    Ok(ir)
}

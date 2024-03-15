use std::{collections::HashMap, sync::Arc};

use dandelion_commons::DandelionResult;

use crate::function_driver::GpuConfig;

use super::hip;

pub struct LaunchConfig {
    pub grid_dim_x: u32,
    pub grid_dim_y: u32,
    pub grid_dim_z: u32,
    pub block_dim_x: u32,
    pub block_dim_y: u32,
    pub block_dim_z: u32,
    pub shared_mem_bytes: usize,
}

impl LaunchConfig {
    pub fn one_dimensional(grid_dim: u32, block_dim: u32, shared_mem_bytes: usize) -> Self {
        Self {
            grid_dim_x: grid_dim,
            grid_dim_y: 1,
            grid_dim_z: 1,
            block_dim_x: block_dim,
            block_dim_y: 1,
            block_dim_z: 1,
            shared_mem_bytes,
        }
    }
}

pub enum Argument {
    BufferPtr(String),
    BufferLen(String),
}
enum Condition {}
pub enum Action {
    ExecKernel(String, Vec<Argument>, LaunchConfig),
    RepeatWhile(Condition, Vec<Action>),
}

pub struct ExecutionBlueprint {
    /// buffer names to sizes
    pub inputs: HashMap<String, usize>,
    pub temps: HashMap<String, usize>,
    pub outputs: HashMap<String, usize>,

    pub control_flow: Vec<Action>,
}

pub fn dummy_config() -> DandelionResult<GpuConfig> {
    let module =
        hip::module_load("/home/smithj/dandelion/machine_interface/hip_interface/module.hsaco")?;
    let kernel_set = hip::module_get_function(&module, "set_mem")?;
    let kernel_check = hip::module_get_function(&module, "check_mem")?;

    Ok(GpuConfig {
        system_data_struct_offset: 0,
        module: Arc::new(module),
        kernels: Arc::new(HashMap::from([
            ("set_mem".into(), kernel_set),
            ("check_mem".into(), kernel_check),
        ])),
        blueprint: Arc::new(ExecutionBlueprint {
            inputs: HashMap::new(),
            temps: HashMap::from([("A".into(), 1024)]),
            outputs: HashMap::new(),
            control_flow: vec![
                Action::ExecKernel(
                    "set_mem".into(),
                    vec![
                        Argument::BufferPtr("A".into()),
                        Argument::BufferLen("A".into()),
                    ],
                    LaunchConfig::one_dimensional((256 + 1024 - 1) / 1024, 1024, 0),
                ),
                Action::ExecKernel(
                    "check_mem".into(),
                    vec![
                        Argument::BufferPtr("A".into()),
                        Argument::BufferLen("A".into()),
                    ],
                    LaunchConfig::one_dimensional(1, 1, 0),
                ),
            ],
        }),
    })
}

pub fn parse_config() -> DandelionResult<GpuConfig> {
    todo!()
}

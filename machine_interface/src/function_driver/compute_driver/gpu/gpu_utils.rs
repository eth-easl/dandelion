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
    pub fn two_dimensional(
        grid_dim_x: u32,
        grid_dim_y: u32,
        block_dim_x: u32,
        block_dim_y: u32,
        shared_mem_bytes: usize,
    ) -> Self {
        Self {
            grid_dim_x,
            grid_dim_y,
            grid_dim_z: 1,
            block_dim_x,
            block_dim_y,
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
    pub inputs: Vec<String>,
    /// buffer names to sizes
    pub buffers: HashMap<String, usize>,
    pub outputs: Vec<String>, // might not be required
    pub control_flow: Vec<Action>,
}

pub fn dummy_config() -> DandelionResult<GpuConfig> {
    let module =
        hip::module_load("/home/smithj/dandelion/machine_interface/hip_interface/module.hsaco")?;
    let kernel_set = hip::module_get_function(&module, "set_mem")?;
    let kernel_check = hip::module_get_function(&module, "check_mem")?;

    Ok(GpuConfig {
        system_data_struct_offset: 0x10000usize,
        module: Arc::new(module),
        kernels: Arc::new(HashMap::from([
            ("set_mem".into(), kernel_set),
            ("check_mem".into(), kernel_check),
        ])),
        blueprint: Arc::new(ExecutionBlueprint {
            inputs: vec![],
            buffers: HashMap::from([("A".into(), 1024)]),
            outputs: vec![],
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

pub fn dummy_config2() -> DandelionResult<GpuConfig> {
    let module =
        hip::module_load("/home/smithj/dandelion/machine_interface/hip_interface/module.hsaco")?;
    let kernel = hip::module_get_function(&module, "check_then_write")?;

    Ok(GpuConfig {
        system_data_struct_offset: 0x10000usize,
        module: Arc::new(module),
        kernels: Arc::new(HashMap::from([("check_then_write".into(), kernel)])),
        blueprint: Arc::new(ExecutionBlueprint {
            inputs: vec!["A".into()],
            buffers: HashMap::new(),
            outputs: vec!["A".into()],
            control_flow: vec![Action::ExecKernel(
                "check_then_write".into(),
                vec![Argument::BufferPtr("A".into())],
                LaunchConfig::one_dimensional(1, 1, 0),
            )],
        }),
    })
}

pub fn matmul_dummy(parallel: bool) -> DandelionResult<GpuConfig> {
    let fn_name = if parallel {
        "matmul_para"
    } else {
        "matmul_loop"
    };
    let module =
        hip::module_load("/home/smithj/dandelion/machine_interface/hip_interface/matmul.hsaco")?;
    let kernel = hip::module_get_function(&module, fn_name)?;

    Ok(GpuConfig {
        system_data_struct_offset: 0x10000usize,
        module: Arc::new(module),
        kernels: Arc::new(HashMap::from([(fn_name.into(), kernel)])),
        blueprint: Arc::new(ExecutionBlueprint {
            inputs: vec!["A".into()],
            buffers: HashMap::from([("B".into(), 80)]),
            outputs: vec!["B".into()],
            control_flow: vec![Action::ExecKernel(
                fn_name.into(),
                vec![
                    Argument::BufferPtr("A".into()),
                    Argument::BufferLen("A".into()),
                    Argument::BufferPtr("B".into()),
                ],
                if parallel {
                    LaunchConfig::two_dimensional(1, 1, 3, 3, 0)
                } else {
                    LaunchConfig::one_dimensional(1, 1, 0)
                },
            )],
        }),
    })
}

pub fn parse_config() -> DandelionResult<GpuConfig> {
    todo!()
}

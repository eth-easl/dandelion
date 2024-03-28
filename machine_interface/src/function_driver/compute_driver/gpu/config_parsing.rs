use std::{
    collections::HashMap,
    fs::{self, File},
    io::BufReader,
    sync::Arc,
};

use dandelion_commons::{DandelionError, DandelionResult};
use serde::{Deserialize, Serialize};

use crate::function_driver::GpuConfig;

use super::hip;

const SYSDATA_OFFSET: usize = 0usize;

// This is subject to change; not very happy with it
#[derive(Deserialize, Serialize, Debug)]
pub enum GridSizing {
    CoverBuffer {
        bufname: String,
        dimensionality: u8,
        block_dim: u32,
    },
    Absolute(u32),
}

#[derive(Deserialize, Serialize, Debug)]
pub struct LaunchConfig {
    pub grid_dim_x: GridSizing,
    pub grid_dim_y: GridSizing,
    pub grid_dim_z: GridSizing,
    pub block_dim_x: u32,
    pub block_dim_y: u32,
    pub block_dim_z: u32,
    pub shared_mem_bytes: usize,
}

impl LaunchConfig {
    pub fn one_dimensional(grid_dim: u32, block_dim: u32, shared_mem_bytes: usize) -> Self {
        Self {
            grid_dim_x: GridSizing::Absolute(grid_dim),
            grid_dim_y: GridSizing::Absolute(1),
            grid_dim_z: GridSizing::Absolute(1),
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
            grid_dim_x: GridSizing::Absolute(grid_dim_x),
            grid_dim_y: GridSizing::Absolute(grid_dim_y),
            grid_dim_z: GridSizing::Absolute(1),
            block_dim_x,
            block_dim_y,
            block_dim_z: 1,
            shared_mem_bytes,
        }
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub enum Argument {
    Ptr(String),
    Sizeof(String),
}

#[derive(Deserialize, Serialize, Debug)]
pub enum Condition {
    NTimes(usize),
}

#[derive(Deserialize, Serialize, Debug)]
pub enum Action {
    ExecKernel(String, Vec<Argument>, LaunchConfig),
    Repeat(Condition, Vec<Action>),
}

#[derive(Deserialize, Serialize, Debug)]
pub enum BufferSizing {
    Sizeof(String),
    Absolute(usize),
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ExecutionBlueprint {
    pub inputs: Vec<String>,
    /// buffer names to sizes
    pub buffers: HashMap<String, BufferSizing>,
    pub outputs: Vec<String>, // might not be required
    pub control_flow: Vec<Action>,
}

pub fn dummy_config() -> DandelionResult<GpuConfig> {
    let module =
        hip::module_load("/home/smithj/dandelion/machine_interface/hip_interface/module.hsaco")?;
    let kernel_set = hip::module_get_function(&module, "set_mem")?;
    let kernel_check = hip::module_get_function(&module, "check_mem")?;

    Ok(GpuConfig {
        system_data_struct_offset: SYSDATA_OFFSET,
        module: Arc::new(module),
        kernels: Arc::new(HashMap::from([
            ("set_mem".into(), kernel_set),
            ("check_mem".into(), kernel_check),
        ])),
        blueprint: Arc::new(ExecutionBlueprint {
            inputs: vec![],
            buffers: HashMap::from([("A".into(), BufferSizing::Absolute(1024))]),
            outputs: vec![],
            control_flow: vec![
                Action::ExecKernel(
                    "set_mem".into(),
                    vec![Argument::Ptr("A".into()), Argument::Sizeof("A".into())],
                    LaunchConfig::one_dimensional((256 + 1024 - 1) / 1024, 1024, 0),
                ),
                Action::ExecKernel(
                    "check_mem".into(),
                    vec![Argument::Ptr("A".into()), Argument::Sizeof("A".into())],
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
        system_data_struct_offset: SYSDATA_OFFSET,
        module: Arc::new(module),
        kernels: Arc::new(HashMap::from([("check_then_write".into(), kernel)])),
        blueprint: Arc::new(ExecutionBlueprint {
            inputs: vec!["A".into()],
            buffers: HashMap::new(),
            outputs: vec!["A".into()],
            control_flow: vec![Action::ExecKernel(
                "check_then_write".into(),
                vec![Argument::Ptr("A".into())],
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
        system_data_struct_offset: SYSDATA_OFFSET,
        module: Arc::new(module),
        kernels: Arc::new(HashMap::from([(fn_name.into(), kernel)])),
        blueprint: Arc::new(ExecutionBlueprint {
            inputs: vec!["A".into()],
            buffers: HashMap::from([("B".into(), BufferSizing::Absolute(80))]),
            outputs: vec!["B".into()],
            control_flow: vec![Action::ExecKernel(
                fn_name.into(),
                vec![
                    Argument::Ptr("A".into()),
                    Argument::Sizeof("A".into()),
                    Argument::Ptr("B".into()),
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

#[derive(Deserialize, Serialize, Debug)]
struct GpuConfigIR {
    module_path: String,
    kernels: Vec<String>,
    blueprint: ExecutionBlueprint,
}

impl TryFrom<GpuConfigIR> for GpuConfig {
    type Error = DandelionError;
    fn try_from(value: GpuConfigIR) -> Result<Self, Self::Error> {
        let module = hip::module_load(&value.module_path)?;
        let kernels = value
            .kernels
            .iter()
            .map(|kname| {
                hip::module_get_function(&module, kname).map(|module| (kname.clone(), module))
            })
            .collect::<Result<HashMap<_, _>, _>>()?; // this is kinda ugly but also pretty cool
        Ok(Self {
            module: Arc::new(module),
            system_data_struct_offset: SYSDATA_OFFSET,
            kernels: Arc::new(kernels),
            blueprint: Arc::new(value.blueprint),
        })
    }
}

pub fn parse_config(path: &str) -> DandelionResult<GpuConfig> {
    let file = File::open(path).map_err(|_| DandelionError::ConfigMissmatch)?;
    let reader = BufReader::new(file);
    let ir: GpuConfigIR =
        serde_json::from_reader(reader).map_err(|_| DandelionError::ConfigMissmatch)?;
    ir.try_into()
}

// For me so I could see how the file would look. Keeping it for now until format is finalised
// #[test]
#[allow(unused)]
fn test_parse() {
    let cfg = GpuConfigIR {
        module_path: "/[PATH]/nearest_neigbours.hsaco".into(),
        kernels: vec!["SUB".into(), "ADD".into(), "MUL".into()],
        blueprint: ExecutionBlueprint {
            inputs: vec!["X".into(), "A".into()],
            buffers: HashMap::from([
                ("V".into(), BufferSizing::Sizeof("X".into())),
                ("X_tmp".into(), BufferSizing::Sizeof("X".into())),
            ]),
            outputs: vec!["V".into()],
            control_flow: vec![Action::Repeat(
                Condition::NTimes(25),
                vec![
                    Action::ExecKernel(
                        "SUB".into(),
                        vec![
                            Argument::Ptr("X".into()),
                            Argument::Ptr("V".into()),
                            Argument::Ptr("X_tmp".into()),
                            Argument::Sizeof("X".into()),
                        ],
                        LaunchConfig::one_dimensional(1, 1, 0),
                    ),
                    Action::ExecKernel(
                        "ADD".into(),
                        vec![
                            Argument::Ptr("X".into()),
                            Argument::Ptr("V".into()),
                            Argument::Ptr("V".into()),
                            Argument::Sizeof("X".into()),
                        ],
                        LaunchConfig::one_dimensional(1, 1, 0),
                    ),
                    Action::ExecKernel(
                        "MUL".into(),
                        vec![
                            Argument::Ptr("A".into()),
                            Argument::Ptr("X_tmp".into()),
                            Argument::Ptr("X".into()),
                            Argument::Sizeof("A".into()),
                            Argument::Sizeof("X".into()),
                        ],
                        LaunchConfig::one_dimensional(1, 1, 0),
                    ),
                ],
            )],
        },
    };

    let j = serde_json::to_string(&cfg).unwrap();

    println!("{}", j);
}

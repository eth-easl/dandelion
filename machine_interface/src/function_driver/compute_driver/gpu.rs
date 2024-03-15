// TODO remove unneeded imports; just took everything from wasm.rs
use crate::{
    function_driver::{
        thread_utils::{start_thread, EngineLoop},
        ComputeResource, Driver, Function, FunctionConfig, GpuConfig, WorkQueue,
    },
    interface::{read_output_structs, setup_input_structs},
    memory_domain::{Context, ContextType},
    DataRequirementList, DataSet,
};
use dandelion_commons::{DandelionError, DandelionResult};
use libc::c_void;
use std::{collections::HashMap, ptr::null, thread};

use self::{hip::DEFAULT_STREAM, utils::Action};

pub(crate) mod hip;
pub(crate) mod utils;

pub fn dummy_run(gpu_loop: &mut GpuLoop) -> DandelionResult<()> {
    // set gpu
    hip::set_device(gpu_loop.gpu_id)?;

    // load module
    let module =
        hip::module_load("/home/smithj/dandelion/machine_interface/hip_interface/module.hsaco")?;

    // load kernels
    let kernel_set = hip::module_get_function(&module, "set_mem")?;
    let kernel_check = hip::module_get_function(&module, "check_mem")?;

    // allocate device memory, prepare args
    let arr_elem: usize = 256;
    let elem_size: usize = std::mem::size_of::<f64>();
    let arr_size: usize = arr_elem * elem_size;
    let array = hip::DevicePointer::try_new(arr_size)?;

    let args: [*const c_void; 2] = [
        &array.0 as *const _ as *const c_void,
        &arr_elem as *const _ as *const c_void,
    ];

    // launch them
    let block_width: usize = 1024;
    hip::module_launch_kernel(
        kernel_set,
        ((arr_elem + block_width - 1) / block_width) as u32,
        1,
        1,
        block_width as u32,
        1,
        1,
        0,
        DEFAULT_STREAM,
        args.as_ptr(),
        null(),
    )?;

    hip::module_launch_kernel(
        kernel_check,
        1,
        1,
        1,
        1,
        1,
        1,
        0,
        DEFAULT_STREAM,
        args.as_ptr(),
        null(),
    )?;

    hip::device_synchronize()?;

    Ok(())
}

fn gpu_run(gpu_id: u8, config: GpuConfig, context: Context) -> DandelionResult<Context> {
    let ContextType::Mmu(ref mmu_context) = context.context else {
        return Err(DandelionError::ConfigMissmatch);
    };

    hip::set_device(gpu_id)?;

    let mut buffers = HashMap::new();
    for (name, size) in &config.blueprint.temps {
        buffers.insert(name.clone(), hip::DevicePointer::try_new(*size)?);
    }

    for action in &config.blueprint.control_flow {
        match action {
            Action::ExecKernel(name, argnames, launch_config) => {
                let elem: usize = 256;
                let args = [
                    &buffers.get("A").unwrap().0 as *const _ as *const c_void,
                    &elem as *const _ as *const c_void,
                ];

                hip::module_launch_kernel(
                    *config.kernels.get(name).unwrap(),
                    launch_config.grid_dim_x,
                    1,
                    1,
                    launch_config.block_dim_x,
                    1,
                    1,
                    0,
                    DEFAULT_STREAM,
                    args.as_ptr(),
                    null(),
                )?;
            }
            _ => return Err(DandelionError::NotImplemented),
        }
    }

    Ok(context)
}

// TODO: remove pub at some point
pub struct GpuLoop {
    cpu_slot: u8, // needed to set processes to run on that core
    gpu_id: u8,
    // TODO: runner process pool
}

impl EngineLoop for GpuLoop {
    // TODO: have init take a ComputeResource to set gpu_id
    fn init(core_id: u8) -> DandelionResult<Box<Self>> {
        Ok(Box::new(Self {
            cpu_slot: core_id,
            gpu_id: 0,
        }))
        // this is where the process pool would be launched and the buffer pool initialised
    }

    fn run(
        &mut self,
        config: FunctionConfig,
        mut context: Context,
        output_sets: std::sync::Arc<Vec<String>>,
    ) -> DandelionResult<Context> {
        let FunctionConfig::GpuConfig(config) = config else {
            return Err(DandelionError::ConfigMissmatch);
        };

        setup_input_structs::<usize, usize>(
            &mut context,
            config.system_data_struct_offset,
            &output_sets,
        )?;

        // in thread for now, in process pool eventually; maybe add cpu_slot?
        let gpu_id_clone = self.gpu_id;
        let config_clone = config.clone();
        let handle = thread::spawn(move || gpu_run(gpu_id_clone, config_clone, context));
        let mut context = match handle.join() {
            Ok(res) => res?,
            Err(_) => return Err(DandelionError::EngineError),
        };

        read_output_structs::<usize, usize>(&mut context, config.system_data_struct_offset)?;

        Ok(context)
    }
}

pub struct GpuDriver {}

impl Driver for GpuDriver {
    fn start_engine(
        &self,
        resource: ComputeResource,
        queue: Box<dyn WorkQueue + Send>,
    ) -> dandelion_commons::DandelionResult<()> {
        // extract resources
        let (cpu_slot, gpu_id) = match resource {
            ComputeResource::GPU(cpu, gpu) => (cpu, gpu),
            _ => return Err(DandelionError::EngineResourceError),
        };
        // check that core is available
        let available_cores = match core_affinity::get_core_ids() {
            None => return Err(DandelionError::EngineError),
            Some(cores) => cores,
        };
        if !available_cores
            .iter()
            .any(|x| x.id == usize::from(cpu_slot))
        {
            return Err(DandelionError::EngineResourceError);
        }
        // TODO: check gpu is available

        start_thread::<GpuLoop>(cpu_slot, queue);
        Ok(())
    }

    fn parse_function(
        &self,
        function_path: String,
        static_domain: &Box<dyn crate::memory_domain::MemoryDomain>,
    ) -> dandelion_commons::DandelionResult<crate::function_driver::Function> {
        // Concept for now: function_path gives config file which contains name of module (.hsaco) file
        let config = FunctionConfig::GpuConfig(utils::dummy_config()?);

        // TODO: change this!
        let total_size = 0usize;

        let mut context = static_domain.acquire_context(total_size)?;

        // Taken from wasm.rs; TODO: ask Tom why/if this is the case
        // there must be one data set, which would normally describe the
        // elf sections to be copied
        context.content = vec![Some(DataSet {
            ident: String::from("static"),
            buffers: vec![],
        })];
        let requirements = DataRequirementList {
            static_requirements: vec![],
            input_requirements: vec![],
        };

        Ok(Function {
            requirements,
            context,
            config,
        })
    }
}

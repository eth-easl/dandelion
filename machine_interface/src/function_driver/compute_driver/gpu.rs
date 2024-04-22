use crate::{
    function_driver::{
        load_utils::load_u8_from_file,
        thread_utils::{start_thread, EngineLoop},
        ComputeResource, Driver, Function, FunctionConfig, GpuConfig, WorkQueue,
    },
    interface::{read_output_structs, setup_input_structs, write_gpu_outputs, DandelionSystemData},
    memory_domain::{Context, ContextTrait, ContextType},
    promise::Debt,
    DataItem, DataRequirementList, DataSet, Position,
};
use core_affinity::CoreId;
use dandelion_commons::{DandelionError, DandelionResult};
use libc::c_void;
use std::{
    collections::HashMap,
    mem::size_of,
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
    ptr::null,
    sync::{Arc, Mutex},
    thread::{self, spawn},
};

use self::{
    buffer_pool::BufferPool,
    config_parsing::{Action, Argument, BufferSizing, RuntimeGpuConfig},
    gpu_utils::{copy_data_to_device, get_data_length, get_grid_size, start_gpu_process_pool},
    hip::DEFAULT_STREAM,
};

pub(crate) mod buffer_pool;
pub(crate) mod config_parsing;
pub mod gpu_utils;
pub mod hip;

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
    let array = hip::DeviceAllocation::try_new(arr_size)?;

    let args: [*const c_void; 2] = [
        &array.ptr as *const _ as *const c_void,
        &arr_elem as *const _ as *const c_void,
    ];

    // launch them
    let block_width: usize = 1024;
    hip::module_launch_kernel(
        &kernel_set,
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
        &kernel_check,
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

pub fn gpu_run(
    cpu_slot: usize,
    gpu_id: u8,
    config: GpuConfig,
    buffer_pool: Arc<Mutex<BufferPool>>,
    mut context: Context,
    output_names: Arc<Vec<String>>,
) -> DandelionResult<Context> {
    if !core_affinity::set_for_current(CoreId { id: cpu_slot }) {
        return Err(DandelionError::EngineResourceError);
    }

    let ContextType::Mmu(ref mmu_context) = context.context else {
        return Err(DandelionError::ConfigMissmatch);
    };
    let base = mmu_context.storage.as_ptr();
    let config = config.load(base)?;

    hip::set_device(gpu_id)?;
    // TODO: disable device-side malloc

    let mut buffer_pool = buffer_pool.lock().unwrap();

    // bufname -> (index in buffer pool, local size)
    let mut buffers: HashMap<String, (usize, usize)> = HashMap::new();
    for name in &config.blueprint.inputs {
        let size = get_data_length(name, &context)?;
        let idx = buffer_pool.alloc_buffer(size)?;
        unsafe {
            copy_data_to_device(name, &context, base, &buffer_pool.get(idx)?)?;
        }
        buffers.insert(name.clone(), (idx, size));
    }
    for (name, size) in &config.blueprint.buffers {
        match size {
            BufferSizing::Absolute(bytes) => {
                let idx = buffer_pool.alloc_buffer(*bytes)?;
                buffers.insert(name.clone(), (idx, *bytes));
            }
            BufferSizing::Sizeof(id) => {
                let size = buffers.get(id).ok_or(DandelionError::ConfigMissmatch)?.1;
                let idx = buffer_pool.alloc_buffer(size)?;
                buffers.insert(name.clone(), (idx, size));
            }
        }
    }

    for action in &config.blueprint.control_flow {
        match action {
            Action::ExecKernel(name, args, launch_config) => {
                let mut params: Vec<*const c_void> = Vec::with_capacity(args.len());
                let mut ptrs = vec![];
                for arg in args {
                    match arg {
                        Argument::Ptr(id) => {
                            let idx = buffers.get(id).unwrap().0;
                            let dev_ptr = buffer_pool.get(idx)?;
                            ptrs.push(dev_ptr);
                            let addr = &ptrs.last().unwrap().ptr;
                            params.push(addr as *const _ as *const c_void)
                        }
                        Argument::Sizeof(id) => {
                            params.push(&buffers.get(id).unwrap().1 as *const _ as *const c_void)
                        }
                    };
                }

                hip::module_launch_kernel(
                    config.kernels.get(name).unwrap(),
                    get_grid_size(&launch_config.grid_dim_x, &buffers)?,
                    get_grid_size(&launch_config.grid_dim_y, &buffers)?,
                    get_grid_size(&launch_config.grid_dim_z, &buffers)?,
                    launch_config.block_dim_x,
                    launch_config.block_dim_y,
                    launch_config.block_dim_z,
                    launch_config.shared_mem_bytes,
                    DEFAULT_STREAM,
                    params.as_ptr(),
                    null(),
                )?;
            }
            _ => return Err(DandelionError::NotImplemented),
        }
    }

    // Not required, as hipMemcpy-s synchronise as well
    // hip::device_synchronize()?;

    write_gpu_outputs::<usize, usize>(
        &mut context,
        config.system_data_struct_offset,
        base,
        &output_names,
        &buffers,
        &buffer_pool,
    )?;

    // Mark buffers as useable again
    buffer_pool.dealloc_all();

    Ok(context)
}

pub struct GpuLoop {
    cpu_slot: u8, // needed to set processes to run on that core
    gpu_id: u8,
    buffers: Arc<Mutex<BufferPool>>,
}

impl EngineLoop for GpuLoop {
    fn init(resource: ComputeResource) -> DandelionResult<Box<Self>> {
        let ComputeResource::GPU(cpu_slot, gpu_id) = resource else {
            return Err(DandelionError::ConfigMissmatch);
        };

        Ok(Box::new(Self {
            cpu_slot,
            gpu_id,
            buffers: Arc::new(Mutex::new(BufferPool::try_new(gpu_id)?)),
        }))
    }

    fn run(
        &mut self,
        config: FunctionConfig,
        mut context: Context,
        output_sets: Arc<Vec<String>>,
    ) -> DandelionResult<Context> {
        let FunctionConfig::GpuConfig(config) = config else {
            return Err(DandelionError::ConfigMissmatch);
        };

        let sysdata_offset = config.system_data_struct_offset;
        setup_input_structs::<usize, usize>(&mut context, sysdata_offset, &output_sets)?;

        // in thread in case the kernel crashes
        let cpu_slot_clone = self.cpu_slot as usize;
        let gpu_id_clone = self.gpu_id;
        let config_clone = config.clone();
        let outputs = output_sets.clone();
        let buffer_pool = self.buffers.clone();
        let handle = thread::spawn(move || {
            gpu_run(
                cpu_slot_clone,
                gpu_id_clone,
                config_clone,
                buffer_pool,
                context,
                outputs,
            )
        });
        let mut context = match handle.join() {
            Ok(res) => res?,
            Err(_) => return Err(DandelionError::EngineError),
        };

        let write_buf = vec![12345i64];
        context.write(0, &write_buf)?;

        read_output_structs::<usize, usize>(&mut context, sysdata_offset)?;

        Ok(context)
    }
}

pub struct GpuDriver {}

impl Driver for GpuDriver {
    fn start_engine(
        &self,
        resource: ComputeResource,
        queue: Box<dyn WorkQueue + Send + Sync>,
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
        // check gpu is available
        if usize::from(gpu_id) >= hip::get_device_count()? {
            return Err(DandelionError::EngineResourceError);
        }

        // To switch between single executor and process pool
        // start_thread::<GpuLoop>(cpu_slot, queue);
        spawn(move || start_gpu_process_pool(cpu_slot, gpu_id, queue));
        Ok(())
    }

    fn parse_function(
        &self,
        function_path: String,
        static_domain: &Box<dyn crate::memory_domain::MemoryDomain>,
    ) -> dandelion_commons::DandelionResult<crate::function_driver::Function> {
        // Concept for now: function_path gives config file which contains name of module (.hsaco) file
        // let config = if function_path == "foo" {
        //     FunctionConfig::GpuConfig(config_parsing::dummy_config()?)
        // } else if function_path == "bar" {
        //     FunctionConfig::GpuConfig(config_parsing::dummy_config2()?)
        // } else if function_path == "matmul_loop" {
        //     FunctionConfig::GpuConfig(config_parsing::matmul_dummy(false)?)
        // } else if function_path == "matmul_para" {
        //     FunctionConfig::GpuConfig(config_parsing::matmul_dummy(true)?)
        // } else {
        //     FunctionConfig::GpuConfig(config_parsing::parse_config(&function_path)?)
        // };
        let (mut gpu_config, module_path) = config_parsing::parse_config(&function_path)?;

        let code_object = load_u8_from_file(module_path)?;
        let size = code_object.len() * size_of::<u8>();
        let mut context = static_domain.acquire_context(size)?;
        context.write(0, &code_object)?;
        gpu_config.module_offset = std::mem::size_of::<DandelionSystemData<usize, usize>>();

        let config = FunctionConfig::GpuConfig(gpu_config);
        // Location of code object
        context.content = vec![Some(DataSet {
            ident: String::from("static"),
            buffers: vec![DataItem {
                ident: String::from(""),
                data: Position { offset: 0, size },
                key: 0,
            }],
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

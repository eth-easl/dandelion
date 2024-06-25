use crate::{
    function_driver::{
        load_utils::load_u8_from_file, thread_utils::EngineLoop, ComputeResource, Driver, Function,
        FunctionConfig, GpuConfig, WorkQueue,
    },
    interface::{read_output_structs, setup_input_structs, write_gpu_outputs, DandelionSystemData},
    memory_domain::{Context, ContextTrait, ContextType},
    DataItem, DataRequirementList, DataSet, Position,
};
use config_parsing::SYSDATA_OFFSET;
use core_affinity::CoreId;
use dandelion_commons::{DandelionError, DandelionResult};
use libc::c_void;
use std::{
    borrow::Borrow,
    collections::HashMap,
    mem::size_of,
    ptr::null,
    sync::{
        mpsc::{self, Receiver, Sender},
        Arc, Mutex, MutexGuard,
    },
    thread::{self, spawn},
};

use self::{
    buffer_pool::BufferPool,
    config_parsing::{Action, Argument, RuntimeGpuConfig},
    gpu_utils::{
        copy_data_to_device, get_data_length, get_size, start_gpu_process_pool, start_gpu_thread,
    },
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

fn execute(
    actions: &Vec<Action>,
    buffers: &HashMap<String, (usize, usize)>,
    buffer_pool: &BufferPool,
    context: &Context,
    config: &RuntimeGpuConfig,
) -> DandelionResult<()> {
    for action in actions {
        match action {
            Action::ExecKernel(name, args, launch_config) => {
                // HIP expects arguments as an array of void pointers (pointers to the arguments)
                let mut params: Vec<*const c_void> = Vec::with_capacity(args.len());
                // Initialise them outside of the loop so they live long enough to have valid pointers
                let mut ptrs = vec![];
                let mut constants = vec![];
                for arg in args {
                    match arg {
                        Argument::Ptr(id) => {
                            let idx = buffers.get(id).unwrap().0;
                            let dev_ptr = buffer_pool.get(idx)?;
                            ptrs.push(dev_ptr);
                            let addr = &ptrs.last().unwrap().ptr;
                            params.push(addr as *const _ as *const c_void);
                        }
                        Argument::Sizeof(id) => {
                            params.push(&buffers.get(id).unwrap().1 as *const _ as *const c_void);
                        }
                        Argument::Constant(constant) => {
                            constants.push(*constant);
                            let addr = constants.last().unwrap();
                            params.push(addr as *const _ as *const c_void);
                        }
                    };
                }

                hip::module_launch_kernel(
                    // TODO: throw error here!
                    config.kernels.get(name).unwrap(),
                    get_size(&launch_config.grid_dim_x, buffers, context)? as u32,
                    get_size(&launch_config.grid_dim_y, buffers, context)? as u32,
                    get_size(&launch_config.grid_dim_z, buffers, context)? as u32,
                    get_size(&launch_config.block_dim_x, buffers, context)? as u32,
                    get_size(&launch_config.block_dim_y, buffers, context)? as u32,
                    get_size(&launch_config.block_dim_z, buffers, context)? as u32,
                    get_size(&launch_config.shared_mem_bytes, buffers, context)?,
                    DEFAULT_STREAM,
                    params.as_ptr(),
                    null(),
                )?;
            }
            Action::Repeat(times, actions) => {
                let repetitions = get_size(times, buffers, context)?;
                for _ in 0..repetitions {
                    execute(actions, buffers, buffer_pool, context, config)?;
                }
            }
        }
    }
    Ok(())
}

pub fn gpu_run(
    cpu_slot: usize,
    gpu_id: u8,
    config: GpuConfig,
    buffer_pool: Arc<Mutex<BufferPool>>,
    mut context: Context,
    output_sets: Arc<Vec<String>>,
) -> DandelionResult<Context> {
    // Set affinity of worker thread
    if !core_affinity::set_for_current(CoreId { id: cpu_slot }) {
        return Err(DandelionError::EngineResourceError);
    }

    // TODO: disable device-side malloc
    hip::set_device(gpu_id)?;

    let ContextType::Mmu(ref mmu_context) = context.context else {
        return Err(DandelionError::ConfigMissmatch);
    };
    let base = mmu_context.storage.as_ptr();
    let config = config.load(base)?;

    let mut buffer_pool = buffer_pool.lock().unwrap();

    // Maps from bufname -> (index in buffer pool, size of buffer)
    let mut buffers: HashMap<String, (usize, usize)> = HashMap::new();
    for name in &config.blueprint.inputs {
        let size = get_data_length(name, &context)?;
        let idx = buffer_pool.alloc_buffer(size)?;
        unsafe {
            copy_data_to_device(name, &context, base, &buffer_pool.get(idx)?)?;
        }
        buffers.insert(name.clone(), (idx, size));
    }
    for (name, sizing) in &config.blueprint.buffers {
        let size = get_size(sizing, &buffers, &context)?;
        let idx = buffer_pool.alloc_buffer(size)?;
        buffers.insert(name.clone(), (idx, size));
    }

    execute(
        &config.blueprint.control_flow,
        &buffers,
        buffer_pool.borrow(),
        &context,
        &config,
    )?;

    write_gpu_outputs::<usize, usize>(
        &mut context,
        config.system_data_struct_offset,
        base,
        &output_sets,
        &buffers,
        buffer_pool.borrow(),
    )?;

    // Zero out buffers used by current function
    buffer_pool.dealloc_all()?;

    Ok(context)
}

pub struct GpuLoop {
    cpu_slot: usize,
    gpu_id: u8,
    buffers: Arc<Mutex<BufferPool>>,
    sender: Sender<DandelionResult<Context>>,
    receiver: Receiver<DandelionResult<Context>>,
}

impl EngineLoop for GpuLoop {
    fn init(resource: ComputeResource) -> DandelionResult<Box<Self>> {
        let ComputeResource::GPU(cpu_slot, gpu_id) = resource else {
            return Err(DandelionError::ConfigMissmatch);
        };

        let (sender, receiver) = mpsc::channel();

        Ok(Box::new(Self {
            cpu_slot: cpu_slot as usize,
            gpu_id,
            buffers: Arc::new(Mutex::new(BufferPool::try_new(gpu_id)?)),
            sender,
            receiver,
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

        // Clone for thread
        let buffer_pool = self.buffers.clone();
        let sender = self.sender.clone();
        let cpu_slot = self.cpu_slot;
        let gpu_id = self.gpu_id;
        thread::spawn(move || {
            let result = gpu_run(cpu_slot, gpu_id, config, buffer_pool, context, output_sets);
            sender.send(result).unwrap();
        });

        // Use an mpsc to receive results. If a fault occured, the handler could be registered to put an error on the channel,
        // while the work thread wouldn't return. This means it would have to be shot down
        let mut context = self
            .receiver
            .recv()
            .map_err(|_| DandelionError::EngineError)
            .and_then(|inner| inner)?;

        read_output_structs::<usize, usize>(&mut context, sysdata_offset)?;

        Ok(context)
    }
}

fn common_parse(
    function_path: String,
    static_domain: &'static dyn crate::memory_domain::MemoryDomain,
) -> DandelionResult<crate::function_driver::Function> {
    let (mut gpu_config, module_path) = config_parsing::parse_config(&function_path)?;

    let code_object = load_u8_from_file(module_path)?;
    let size = code_object.len() * size_of::<u8>();
    gpu_config.code_object_offset =
        SYSDATA_OFFSET + std::mem::size_of::<DandelionSystemData<usize, usize>>();

    let mut context = static_domain.acquire_context(size)?;
    // Not including SYSDATA_OFFSET here because SystemData is not in static context
    context.write(0, &code_object)?;
    // Location of code object
    context.content = vec![Some(DataSet {
        ident: String::from("static"),
        buffers: vec![DataItem {
            ident: String::from(""),
            data: Position { offset: 0, size },
            key: 0,
        }],
    })];

    let config = FunctionConfig::GpuConfig(gpu_config);

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

fn common_start(resource: ComputeResource) -> DandelionResult<(u8, u8)> {
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

    Ok((cpu_slot, gpu_id))
}

pub struct GpuThreadDriver {}

impl Driver for GpuThreadDriver {
    fn start_engine(
        &self,
        resource: ComputeResource,
        queue: Box<dyn WorkQueue + Send + Sync>,
    ) -> dandelion_commons::DandelionResult<()> {
        let (cpu_slot, gpu_id) = common_start(resource)?;

        spawn(move || start_gpu_thread(cpu_slot, gpu_id, queue));
        Ok(())
    }

    fn parse_function(
        &self,
        function_path: String,
        static_domain: &'static dyn crate::memory_domain::MemoryDomain,
    ) -> DandelionResult<crate::function_driver::Function> {
        common_parse(function_path, static_domain)
    }
}

pub struct GpuProcessDriver {}

impl Driver for GpuProcessDriver {
    fn start_engine(
        &self,
        resource: ComputeResource,
        queue: Box<dyn WorkQueue + Send + Sync>,
    ) -> dandelion_commons::DandelionResult<()> {
        let (cpu_slot, gpu_id) = common_start(resource)?;

        start_gpu_process_pool(cpu_slot, gpu_id, queue);
        Ok(())
    }

    fn parse_function(
        &self,
        function_path: String,
        static_domain: &'static dyn crate::memory_domain::MemoryDomain,
    ) -> DandelionResult<crate::function_driver::Function> {
        common_parse(function_path, static_domain)
    }
}

use crate::{
    function_driver::{
        load_utils::load_u8_from_file,
        thread_utils::{run_thread, EngineLoop},
        ComputeResource, Driver, Function, FunctionConfig, GpuConfig, WorkQueue,
    },
    interface::{read_output_structs, setup_input_structs, DandelionSystemData},
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
        Arc, Mutex,
    },
    thread::{self, spawn},
};

use self::{
    buffer_pool::BufferPool,
    config_parsing::{Action, Argument, RuntimeGpuConfig},
    gpu_utils::{
        copy_data_to_device, get_data_length, get_size, start_gpu_process_pool, write_gpu_outputs,
    },
    hip::DEFAULT_STREAM,
};

pub(crate) mod buffer_pool;
pub(crate) mod config_parsing;
pub mod gpu_utils;
pub mod hip;

#[cfg(test)]
mod gpu_tests;

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
                // Explanation:
                // HIP expects arguments as an array of void pointers (pointers to the arguments).
                // BufferPool.get() returns a stack allocated struct, so we need to allocate it outside of the loop,
                // otherwise the pointer becomes invalid. In our case we allocate it on the heap using Box. If we
                // just used a Vec<DevicePointer> instead, pointers to its element would become invalid when the Vec
                // resizes. Additionally, Box provides the convenient into_raw function to make sure the data lives
                // long enough, although this means we must manually deallocate at the end.

                let mut params: Vec<*const c_void> = Vec::with_capacity(args.len());
                let mut dev_ptrs = Vec::with_capacity(args.len());
                for arg in args {
                    match arg {
                        Argument::Ptr(id) => {
                            let idx = buffers
                                .get(id)
                                .ok_or(DandelionError::UndeclaredIdentifier(id.to_owned()))?
                                .0;
                            let dev_ptr = buffer_pool.get(idx)?;
                            dev_ptrs.push(Box::into_raw(Box::new(dev_ptr)));
                            params.push(*dev_ptrs.last().unwrap() as *const c_void);
                        }
                        Argument::Sizeof(id) => {
                            params.push(
                                &buffers
                                    .get(id)
                                    .ok_or(DandelionError::UndeclaredIdentifier(id.to_owned()))?
                                    .1 as *const _ as *const c_void,
                            );
                        }
                        Argument::Constant(constant) => {
                            params.push(constant as *const _ as *const c_void);
                        }
                    };
                }

                unsafe {
                    hip::module_launch_kernel(
                        config
                            .kernels
                            .get(name)
                            .ok_or(DandelionError::UndeclaredIdentifier(name.to_owned()))?,
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
                    )?
                };

                // Manually deallocate heap memory we performed into_raw on
                for ptr in dev_ptrs {
                    unsafe {
                        let allocation = Box::from_raw(ptr);
                        drop(allocation); // Not necessary, just do it's very explicit we're dropping the data here
                    }
                }
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

    hip::set_device(gpu_id)?;
    hip::limit_heap_size(0)?;

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
        let size = get_size(sizing, &buffers, &context)? as usize;
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

    // Copy results back into host memory from device memory
    unsafe {
        write_gpu_outputs::<usize, usize>(
            &mut context,
            config.system_data_struct_offset,
            base,
            &output_sets,
            &buffers,
            buffer_pool.borrow(),
        )?
    };

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
            return Err(DandelionError::EngineResourceError);
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

        // TODO: add proper error handling mechanisms
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

// Function parsing logic that can be shared between gpu_thread and gpu_process variants
fn common_parse(
    function_path: String,
    static_domain: &'static dyn crate::memory_domain::MemoryDomain,
) -> DandelionResult<crate::function_driver::Function> {
    // Deserialise user provided config JSON, extract module suffix
    let (mut gpu_config, module_suffix) = config_parsing::parse_config(&function_path)?;

    let mut path = std::env::var("DANDELION_LIBRARY_PATH")
        .unwrap_or(format!("{}/tests/libs/", env!("CARGO_MANIFEST_DIR")));

    path += &module_suffix;

    let code_object = load_u8_from_file(path)?;
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

// Engine start-up logic that can be shared between gpu_thread and gpu_process variants
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

        spawn(move || run_thread::<GpuLoop>(ComputeResource::GPU(cpu_slot, gpu_id), queue));
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

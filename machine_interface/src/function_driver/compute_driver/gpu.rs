use self::{
    buffer_pool::BufferPool,
    config_parsing::{Action, Argument, RuntimeGpuConfig, SYSDATA_OFFSET},
    gpu_utils::{
        copy_data_to_device, get_data_length, get_size, write_gpu_outputs,
    },
};
#[cfg(feature = "gpu_process")]
use self::gpu_utils::start_gpu_process_pool;
use crate::{
    function_driver::{
        thread_utils::{run_thread, EngineLoop},
        ComputeResource, Driver, Function, FunctionConfig, GpuConfig, WorkQueue,
    },
    interface::DandelionSystemData,
    memory_domain::{Context, ContextTrait, ContextType, MemoryDomain},
    DataRequirementList,
};
use core_affinity::CoreId;
use dandelion_commons::{
    records::{RecordPoint, Recorder},
    DandelionError, DandelionResult,
};
use libc::c_void;
use std::{
    borrow::Borrow,
    collections::HashMap,
    ptr::null,
    sync::{
        mpsc::{self, Receiver, Sender},
        Arc, Mutex,
    },
    thread::{self, spawn},
};

pub(crate) mod buffer_pool;
pub(crate) mod config_parsing;
mod gpu_api;
pub mod gpu_utils;

#[cfg(test)]
mod gpu_tests;

fn execute(
    actions: &Vec<Action>,
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
                            let dev_ptr = buffer_pool.get_pointer(id)?;
                            dev_ptrs.push(Box::into_raw(Box::new(dev_ptr)));
                            params.push(*dev_ptrs.last().unwrap() as *const c_void);
                        }
                        Argument::Sizeof(id) => {
                            params.push(
                                &buffer_pool.get_size(id) as *const _ as *const c_void,
                            );
                        }
                        Argument::Constant(constant) => {
                            params.push(constant as *const _ as *const c_void);
                        }
                    };
                }

                gpu_api::module_launch_kernel(
                    config
                        .kernels
                        .get(name)
                        .ok_or(DandelionError::UndeclaredIdentifier(name.to_owned()))?,
                    get_size(&launch_config.grid_dim_x, buffer_pool, context)? as u32,
                    get_size(&launch_config.grid_dim_y, buffer_pool, context)? as u32,
                    get_size(&launch_config.grid_dim_z, buffer_pool, context)? as u32,
                    get_size(&launch_config.block_dim_x, buffer_pool, context)? as u32,
                    get_size(&launch_config.block_dim_y, buffer_pool, context)? as u32,
                    get_size(&launch_config.block_dim_z, buffer_pool, context)? as u32,
                    get_size(&launch_config.shared_mem_bytes, buffer_pool, context)? as u32,
                    gpu_api::DEFAULT_STREAM,
                    params.as_ptr(),
                    null(),
                )?;

                // Manually deallocate heap memory we performed into_raw on
                for ptr in dev_ptrs {
                    unsafe {
                        let allocation = Box::from_raw(ptr);
                        drop(allocation); // Not necessary, just do it's very explicit we're dropping the data here
                    }
                }
            }
            Action::Repeat(times, actions) => {
                let repetitions = get_size(times, buffer_pool, context)?;
                for _ in 0..repetitions {
                    execute(actions, buffer_pool, context, config)?;
                }
            }
        }
    }

    #[cfg(feature = "timestamp")]
    let _ = gpu_api::synchronize();

    Ok(())
}

pub fn gpu_run(
    cpu_slot: usize,
    gpu_id: u8,
    config: GpuConfig,
    buffer_pool: Arc<Mutex<BufferPool>>,
    mut context: Context,
    output_sets: Arc<Vec<String>>,
    mut recorder: Recorder,
) -> DandelionResult<Context> {
    // Set affinity of worker thread
    if !core_affinity::set_for_current(CoreId { id: cpu_slot }) {
        return Err(DandelionError::EngineResourceError);
    }

    gpu_api::set_device(gpu_id)?;

    let read_only_data = match context.context {
        ContextType::Gpu(ref gpu_context) => &gpu_context.read_only,
        _ => return Err(DandelionError::ContextMissmatch),
    };

    // Load modules and kernels
    let mut loaded_modules_map: HashMap<String, usize> = HashMap::new();
    let mut loaded_modules: Vec<gpu_api::Module> = Vec::new();
    let mut loaded_kernels: HashMap<String, gpu_api::Function> = HashMap::new();
    for kernel in config.kernels.iter() {
        let module_name = kernel["module_name"].clone();
        let kernel_name = &kernel["kernel_name"];

        if !loaded_modules_map.contains_key(&module_name) {
            let sub_read_only = read_only_data.get(&module_name).unwrap();
            let data_pointer = sub_read_only
                .context
                .get_chunk_ref(sub_read_only.position.offset, sub_read_only.position.size)
                .unwrap()
                .as_ptr() as *const c_void;

            let loaded_module = gpu_api::module_load_data(data_pointer)?;
            loaded_modules_map.insert(module_name.clone(), loaded_modules.len());
            loaded_modules.push(loaded_module);
        }

        let module = &loaded_modules[loaded_modules_map[&module_name]];
        let loaded_kernel = gpu_api::module_get_function(&module, kernel_name)?;
        let _ = loaded_kernels
            .insert(kernel_name.to_string(), loaded_kernel)
            .ok_or(DandelionError::UnknownSymbol);
    }

    let function_id = config.function_id;
    let mut buffer_pool = buffer_pool.lock().unwrap();

    let config = RuntimeGpuConfig {
        system_data_struct_offset: 0,
        modules: Arc::new(loaded_modules),
        kernels: Arc::new(loaded_kernels),
        blueprint: config.blueprint,
    };

    recorder.record(RecordPoint::GPUTransferStart);
    let mut reload_weights = true;
    #[cfg(feature = "reuse_weights")]
    {
        reload_weights = buffer_pool.prev_function_id != function_id;
    }
    recorder.set_gpu_cache_hit(!reload_weights);

    if reload_weights {
        buffer_pool.prev_function_id = function_id;
        buffer_pool.dealloc_all()?;

        for name in &config.blueprint.weights {
            let sub_read_only = read_only_data.get(name.as_str()).unwrap();
            let data_pointer = sub_read_only
                .context
                .get_chunk_ref(sub_read_only.position.offset, sub_read_only.position.size)
                .unwrap()
                .as_ptr() as *const c_void;
            let size = sub_read_only.position.size;
            
            let _ = buffer_pool.alloc_buffer(name, size, true)?;
            let dev_ptr = buffer_pool.get_pointer(name)?;

            gpu_api::memcpy_h_to_d(&dev_ptr, 0, data_pointer, size)?;
        }
    }

    for name in &config.blueprint.inputs {
        let size = get_data_length(name, &context)?;
        let _ = buffer_pool.alloc_buffer(name, size, false)?;
        let dev_ptr = buffer_pool.get_pointer(name)?;
        copy_data_to_device(name, &context, &dev_ptr)?;
    }
    for (name, sizing) in &config.blueprint.buffers {
        let size = get_size(sizing, &buffer_pool, &context)? as usize;
        let _ = buffer_pool.alloc_buffer(name, size, false)?;
    }
    recorder.record(RecordPoint::GPUTransferEnd);

    recorder.record(RecordPoint::GPUInferenceStart);
    execute(
        &config.blueprint.control_flow,
        buffer_pool.borrow(),
        &context,
        &config,
    )?;
    recorder.record(RecordPoint::GPUInferenceEnd);

    recorder.record(RecordPoint::GPUOutputStart);
    // Copy results back into host memory from device memory
    write_gpu_outputs(&mut context, &output_sets, buffer_pool.borrow())?;
    recorder.record(RecordPoint::GPUOutputEnd);

    // Zero out input, temporary buffers, and output buffers
    buffer_pool.dealloc_tmp_buffers()?;

    Ok(context)
}

pub struct GpuLoop {
    cpu_slot: usize,
    gpu_id: u8,
    buffers: Arc<Mutex<BufferPool>>,
    sender: Sender<DandelionResult<Context>>,
    receiver: Receiver<DandelionResult<Context>>,
}

#[allow(non_upper_case_globals)]
const Gi: usize = 1 << 30;

// TODO: add adaptive amount if other GPUs are used:
// MI210    - 64GiB => 60 * Gi
// RTX 3090 - 24GiB => 23 * Gi
const VRAM_SIZE: usize = 23 * Gi;

impl EngineLoop for GpuLoop {
    fn init(resource: ComputeResource) -> DandelionResult<Box<Self>> {
        let ComputeResource::GPU(cpu_slot, gpu_id, worker_count) = resource else {
            return Err(DandelionError::EngineResourceError);
        };

        let (sender, receiver) = mpsc::channel();

        Ok(Box::new(Self {
            cpu_slot: cpu_slot as usize,
            gpu_id,
            buffers: Arc::new(Mutex::new(BufferPool::try_new(
                gpu_id,
                VRAM_SIZE / worker_count as usize,
            )?)),
            sender,
            receiver,
        }))
    }

    fn run(
        &mut self,
        config: FunctionConfig,
        context: Context,
        output_sets: Arc<Vec<String>>,
        recorder: Recorder,
    ) -> DandelionResult<Context> {
        let FunctionConfig::GpuConfig(config) = config else {
            return Err(DandelionError::ConfigMissmatch);
        };
        let subrecorder = recorder.get_sub_recorder();

        // Clone for thread
        let buffer_pool = self.buffers.clone();
        let sender = self.sender.clone();
        let cpu_slot = self.cpu_slot;
        let gpu_id = self.gpu_id;
        thread::spawn(move || {
            let result = gpu_run(
                cpu_slot,
                gpu_id,
                config,
                buffer_pool,
                context,
                output_sets,
                subrecorder,
            );
            sender.send(result).unwrap();
        });

        // TODO: add proper error handling mechanisms
        // Use an mpsc to receive results. If a fault occured, the handler could be registered to put an error on the channel,
        // while the work thread wouldn't return. This means it would have to be shot down
        let context = self
            .receiver
            .recv()
            .map_err(|_| DandelionError::EngineError)
            .and_then(|inner| inner)?;

        Ok(context)
    }
}

// Function parsing logic that can be shared between gpu_thread and gpu_process variants
fn common_parse(
    function_path: String,
    static_domain: &Box<dyn MemoryDomain>,
) -> DandelionResult<crate::function_driver::Function> {
    let requirements = DataRequirementList {
        static_requirements: vec![],
        input_requirements: vec![],
    };

    let context = Box::new(static_domain.acquire_context(0)?);
    
    let mut gpu_config = config_parsing::parse_config(&function_path)?;
    gpu_config.code_object_offset =
        SYSDATA_OFFSET + std::mem::size_of::<DandelionSystemData<usize, usize>>();
    let config = FunctionConfig::GpuConfig(gpu_config);

    Ok(Function {
        requirements,
        context: Arc::from(context),
        config,
    })
}

// Engine start-up logic that can be shared between gpu_thread and gpu_process variants
fn common_start(resource: ComputeResource) -> DandelionResult<(u8, u8, u8)> {
    // extract resources
    let (cpu_slot, gpu_id, worker_count) = match resource {
        ComputeResource::GPU(cpu, gpu, worker_count) => (cpu, gpu, worker_count),
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
    // gpu_api::limit_heap_size(0)?;
    if usize::from(gpu_id) >= gpu_api::get_device_count()? {
        return Err(DandelionError::EngineResourceError);
    }

    Ok((cpu_slot, gpu_id, worker_count))
}

pub struct GpuThreadDriver {}

impl Driver for GpuThreadDriver {
    fn start_engine(
        &self,
        resource: ComputeResource,
        queue: Box<dyn WorkQueue + Send + Sync>,
    ) -> dandelion_commons::DandelionResult<()> {
        let (cpu_slot, gpu_id, _) = common_start(resource)?;

        // Pass worker_count as 1 to make sure gpu_thread takes full memory region
        spawn(move || run_thread::<GpuLoop>(ComputeResource::GPU(cpu_slot, gpu_id, 1), queue));
        Ok(())
    }

    fn parse_function(
        &self,
        function_path: String,
        static_domain: &Box<dyn MemoryDomain>,
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
        let (cpu_slot, gpu_id, worker_count) = common_start(resource)?;

        #[cfg(feature = "gpu_process")]
        start_gpu_process_pool(cpu_slot, gpu_id, worker_count, queue);
        Ok(())
    }

    fn parse_function(
        &self,
        function_path: String,
        static_domain: &Box<dyn MemoryDomain>,
    ) -> DandelionResult<crate::function_driver::Function> {
        common_parse(function_path, static_domain)
    }
}

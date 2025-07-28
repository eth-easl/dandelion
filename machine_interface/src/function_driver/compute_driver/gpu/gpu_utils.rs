use super::{
    buffer_pool::BufferPool,
    config_parsing::Sizing,
    gpu_api::{self, DevicePointer},
};
use crate::{
    function_driver::{ComputeResource, FunctionConfig, GpuConfig, WorkDone, WorkQueue, WorkToDo},
    interface::read_output_structs,
    memory_domain::{self, Context, ContextState, ContextTrait, ContextType},
    DataItem, DataSet, Position,
};
use dandelion_commons::{records::RecordPoint, DandelionError, DandelionResult};
use libc::c_void;
use log::{debug, error};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    io::{BufRead, BufReader, Write},
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::spawn,
};

#[cfg(feature = "gpu_process")]
use nix::{
    fcntl::OFlag,
    sys::{
        mman::{mmap, shm_open, MapFlags, ProtFlags},
        stat::{fstat, Mode},
    },
};

pub fn get_data_length(ident: &str, context: &Context) -> DandelionResult<usize> {
    let dataset = context
        .content
        .iter()
        .find(|&elem| match elem {
            Some(set) => set.ident == ident,
            _ => false,
        })
        .ok_or(DandelionError::UndeclaredIdentifier(ident.to_owned()))?
        .as_ref()
        .unwrap(); // okay, as we matched successfully

    let length = dataset
        .buffers
        .iter()
        .fold(0usize, |acc, item| acc + item.data.size);

    Ok(length)
}

pub fn copy_data_to_device(
    ident: &str,
    context: &Context,
    dev_ptr: &DevicePointer,
) -> DandelionResult<()> {
    let dataset = context
        .content
        .iter()
        .find(|&elem| match elem {
            Some(set) => set.ident == ident,
            _ => false,
        })
        .ok_or(DandelionError::UndeclaredIdentifier(ident.to_owned()))?
        .as_ref()
        .unwrap(); // okay, as we matched successfully

    let mut total = 0isize;
    for item in &dataset.buffers {
        let size = item.data.size;
        let offset = item.data.offset;
        let src = context.get_chunk_ref(offset, size).unwrap().as_ptr() as *const c_void;
        gpu_api::memcpy_h_to_d(dev_ptr, total, src, size)?;
        total += size as isize;
    }
    Ok(())
}

pub fn write_gpu_outputs(
    output_context: &mut Context,
    output_set_names: &[String],
    // device_buffers: &HashMap<String, (usize, usize)>,
    buffer_pool: &BufferPool,
) -> DandelionResult<()> {
    let base = match &output_context.context {
        ContextType::Gpu(ref mmu_context) => mmu_context.storage.as_ptr(),
        #[cfg(feature = "gpu_process")]
        ContextType::GpuProcess(ref gpu_process_context) => gpu_process_context.as_ptr(),
        _ => return Err(DandelionError::ConfigMissmatch),
    };

    let mut output_sets = vec![];
    let mut buffers = vec![];
    for output_name in output_set_names {
        let dev_ptr = buffer_pool.get_pointer(output_name)?;
        let size = buffer_pool.get_size(output_name)?;

        let buf_offset = output_context.get_free_space(size, 8)?;
        let dst = unsafe { base.byte_offset(buf_offset as isize) } as *const c_void;
        
        gpu_api::memcpy_d_to_h(dst, &dev_ptr, size)?;

        buffers.push(DataItem {
            ident: output_name.clone(),
            data: Position {
                offset: buf_offset,
                size: size,
            },
            key: 0u32,
        });
        output_context.occupy_space(buf_offset, size)?;
    }

    output_sets.push(Some(DataSet {
        ident: "outputs".to_string(),
        buffers: buffers,
    }));
    output_context.content = output_sets;

    Ok(())
}

pub fn get_size(
    sizing: &Sizing,
    buffer_pool: &BufferPool,
    context: &Context,
) -> DandelionResult<u64> {
    match sizing {
        Sizing::Absolute(size) => Ok(*size),
        Sizing::FromInput { bufname, idx } => {
            let dataset = context
                .content
                .iter()
                .find(|&elem| match elem {
                    Some(set) => &set.ident == bufname,
                    _ => false,
                })
                .ok_or(DandelionError::UndeclaredIdentifier(bufname.to_owned()))?
                .as_ref()
                .unwrap(); // okay, as we matched successfully

            let data_item = dataset
                .buffers
                .first()
                .ok_or(DandelionError::FromInputOutOfBounds)?;

            let relative_offset = *idx * std::mem::size_of::<i64>();
            if relative_offset > data_item.data.size {
                return Err(DandelionError::FromInputOutOfBounds);
            }

            let mut buf: [u64; 1] = [0];
            context.read(data_item.data.offset + relative_offset, &mut buf)?;

            Ok(buf[0])
        }
        Sizing::Sizeof(bufname) => Ok(buffer_pool.get_size(bufname)? as u64),
    }
}

struct Worker {
    process: Child,
    pub stdin: ChildStdin,
    pub stdout: BufReader<ChildStdout>,
    // pub available: Notify,
    // pub debt: Mutex<Option<(Debt, Recorder, Context)>>,
}

impl Worker {
    fn new(core_id: u8, gpu_id: u8, worker_count: u8) -> Self {
        // this trick gives the desired path of mmu_worker for packages within the workspace
        // Note: the gpu_worker binary required is assumed to be present (look at README.md)
        let path = std::env::var("PROCESS_WORKER_PATH").unwrap_or(format!(
            "{}/../target/{}-unknown-linux-gnu/{}/gpu_worker",
            env!("CARGO_MANIFEST_DIR"),
            std::env::consts::ARCH,
            if cfg!(debug_assertions) {
                "debug"
            } else {
                "release"
            },
        ));

        let mut child = Command::new(path)
            .arg(core_id.to_string())
            .arg(gpu_id.to_string())
            .arg(worker_count.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("Spawning GPU worker failed");

        // Unwrapping okay, since child is spawned with piped stdin/stdout
        let stdin = child.stdin.take().unwrap();
        let stdout = BufReader::new(child.stdout.take().unwrap());

        Self {
            process: child,
            stdin,
            stdout,
        }
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        if let Err(e) = self.process.kill() {
            error!("Killing Worker process gave: {}", e);
        }
    }
}

// slightly modified Context that can be exchanged between processes
#[derive(Serialize, Deserialize, Debug)]
pub struct SendContext {
    pub context_filename: String,
    pub content: Vec<Option<DataSet>>,
    pub offset: i64,
    pub size: usize,
    pub state: ContextState,
    pub occupation: Vec<Position>,
}

#[cfg(feature = "gpu_process")]
impl TryFrom<SendContext> for Context {
    type Error = DandelionError;

    fn try_from(value: SendContext) -> Result<Self, Self::Error> {
        let filename = &value.context_filename;
        let shmem_fd = match shm_open(filename.as_str(), OFlag::O_RDWR, Mode::S_IRUSR) {
            Err(err) => {
                error!("Error opening shared memory file: {}:{}", err, err.desc());
                return Err(DandelionError::FileError);
            }
            Ok(fd) => fd,
        };

        let size = match fstat(shmem_fd) {
            Err(err) => {
                error!("Error getting file stats: {}:{}", err, err.desc());
                return Err(DandelionError::FileError);
            }
            Ok(stat) => stat.st_size as usize,
        };

        let ptr = unsafe {
            match mmap(
                None,
                NonZeroUsize::new(size).unwrap(),
                ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
                MapFlags::MAP_SHARED,
                shmem_fd,
                value.offset,
            ) {
                Err(err) => {
                    eprintln!(
                        "Error mapping memory from file {} with size {} and offset {}: {}:{}",
                        filename,
                        size,
                        value.offset,
                        err,
                        err.desc()
                    );
                    return Err(DandelionError::MemoryAllocationError);
                }
                Ok(ptr) => ptr as *mut _,
            }
        };

        Ok(Self {
            context: ContextType::GpuProcess(Box::new(GpuProcessContext { ptr, size })),
            content: value.content,
            size: value.size,
            state: value.state,
            occupation: value.occupation,
        })
    }
}

impl TryFrom<&Context> for SendContext {
    type Error = DandelionError;
    fn try_from(value: &Context) -> DandelionResult<Self> {
        let ContextType::Gpu(ref ctxt) = value.context else {
            return Err(DandelionError::ConfigMissmatch);
        };

        // Cloning so much on the hot path is not optimal; a custom serialisation function would be quicker
        Ok(SendContext {
            // unwrap okay, as Mmu memory is always created as shared so a filename exists
            context_filename: ctxt.storage.filename().unwrap().to_string(),
            content: value.content.clone(),
            offset: ctxt.storage.offset(),
            size: ctxt.storage.size(),
            state: value.state.clone(),
            occupation: value.occupation.clone(),
        })
    }
}

#[derive(Serialize, Deserialize)]
pub struct SendFunctionArgs {
    pub config: GpuConfig,
    pub context: SendContext,
    pub output_sets: Arc<Vec<String>>,
}

/*
fn manage_worker(
    resources: (u8, u8, u8),
    core_id: u8,
    gpu_id: u8,
    queue: Arc<dyn WorkQueue + Send + Sync>,
    done: Arc<AtomicBool>,
) {
    // set core affinity
    if !core_affinity::set_for_current(core_affinity::CoreId { id: core_id.into() }) {
        log::error!("core received core id that could not be set");
        return;
    }
    let mut worker = Worker::new(core_id + 1, gpu_id, resources.2);
    let mut line = String::new();

    loop {
        // A different worker thread got the shutdown signal
        if done.load(Ordering::SeqCst) {
            return;
        }

        let (args, debt) = queue.get_engine_args();
        match args {
            WorkToDo::FunctionArguments {
                config,
                mut context,
                output_sets,
                mut recorder,
            } => {
                // transform relevant data into serialisable counterparts
                let FunctionConfig::GpuConfig(config) = config else {
                    debt.fulfill(Err(DandelionError::ConfigMissmatch));
                    return;
                };
                let Ok(send_context) = (&context).try_into() else {
                    debt.fulfill(Err(DandelionError::ConfigMissmatch));
                    return;
                };

                let mut task = serde_json::to_string(&SendFunctionArgs {
                    config,
                    context: send_context,
                    output_sets,
                })
                .unwrap();

                // Very important to add this newline, as the worker reads line by line
                task += "\n";

                recorder.record(RecordPoint::EngineStart);

                // Write task description to worker process stdin
                worker
                    .stdin
                    .write_all(task.as_bytes())
                    .expect("Writing failed");

                loop {
                    line.clear();
                    if worker.stdout.read_line(&mut line).unwrap() == 0 {
                        panic!("Reading Worker output gave EOF!")
                    }
                    if !line.trim().starts_with("__ERROR__") && line.trim() != "__OK__" {
                        // The line is some other output from the GPU, log it
                        debug!("GPU output: {}", line);
                        continue;
                    }

                    recorder.record(RecordPoint::EngineEnd);
                    if line.trim().starts_with("__ERROR__") {
                        error!("GPU error: {}", line);
                        println!("GPU error: {}", line);
                        debt.fulfill(Err(DandelionError::EngineError));
                        break;
                    } else {
                        read_output_structs::<usize, usize>(&mut context, 0).unwrap();
                        debt.fulfill(Ok(WorkDone::Context(context)));
                        break;
                    }
                }
            }
            WorkToDo::TransferArguments {
                source,
                mut destination,
                destination_set_index,
                destination_allignment,
                destination_item_index,
                destination_set_name,
                source_set_index,
                source_item_index,
                mut recorder,
            } => {
                recorder.record(RecordPoint::TransferStart);
                let transfer_result = memory_domain::transfer_data_item(
                    &mut destination,
                    source,
                    destination_set_index,
                    destination_allignment,
                    destination_item_index,
                    destination_set_name.as_str(),
                    source_set_index,
                    source_item_index,
                );
                recorder.record(RecordPoint::TransferEnd);
                let transfer_return = transfer_result.and(Ok(WorkDone::Context(destination)));
                debt.fulfill(transfer_return);
                continue;
            }
            WorkToDo::ParsingArguments {
                driver,
                path,
                static_domain,
                mut recorder,
            } => {
                recorder.record(RecordPoint::ParsingStart);
                let function_result = driver.parse_function(path, &static_domain);
                recorder.record(RecordPoint::ParsingEnd);
                match function_result {
                    Ok(function) => debt.fulfill(Ok(WorkDone::Function(function))),
                    Err(err) => debt.fulfill(Err(err)),
                }
                continue;
            }
            WorkToDo::LoadingArguments {
                function,
                domain,
                ctx_size,
                mut recorder,
            } => {
                recorder.record(RecordPoint::LoadStart);
                let load_result = function.load(&domain, ctx_size);
                recorder.record(RecordPoint::LoadEnd);
                match load_result {
                    Ok(context) => debt.fulfill(Ok(WorkDone::Context(context))),
                    Err(err) => debt.fulfill(Err(err)),
                }
                continue;
            }
            WorkToDo::Shutdown() => {
                // Return original resources that were given to Engine
                debt.fulfill(Ok(WorkDone::Resources(vec![ComputeResource::GPU(
                    resources.0,
                    resources.1,
                    resources.2,
                )])));

                // Inform other threads to shutdown as well when they are done
                done.swap(true, Ordering::SeqCst);
                return;
            }
        }
    }
}

pub fn start_gpu_process_pool(
    core_id: u8,
    gpu_id: u8,
    worker_count: u8,
    queue: Box<dyn WorkQueue + Send + Sync>,
) {
    let done = Arc::new(AtomicBool::new(false));
    let queue: Arc<dyn WorkQueue + Send + Sync> = queue.into();
    for offset in 0..worker_count {
        let queue = queue.clone();
        let done = done.clone();
        spawn(move || {
            manage_worker(
                (core_id, gpu_id, worker_count),
                core_id + 2 * offset,
                gpu_id,
                queue,
                done,
            )
        });
    }
}
*/

use crate::{
    function_driver::{
        ComputeResource, EngineArguments, FunctionArguments, FunctionConfig, GpuConfig,
        TransferArguments, WorkQueue,
    },
    interface::read_output_structs,
    memory_domain::{
        self,
        mmu::{MmuContext, MMAP_BASE_ADDR},
        Context, ContextState, ContextType,
    },
    promise::Debt,
    util::mmapmem::MmapMem,
    DataSet, Position,
};
use dandelion_commons::{
    records::{RecordPoint, Recorder},
    DandelionError, DandelionResult,
};
use libc::c_void;
use nix::sys::mman::ProtFlags;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, process::Stdio, sync::Arc};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::{Mutex, Semaphore},
    task,
};

use self::super::{config_parsing::GridSizing, hip};

use super::hip::DevicePointer;

pub fn get_data_length(ident: &str, context: &Context) -> DandelionResult<usize> {
    let dataset = context
        .content
        .iter()
        .find(|&elem| match elem {
            Some(set) => set.ident == ident,
            _ => false,
        })
        .ok_or(DandelionError::ConfigMissmatch)?
        .as_ref()
        .unwrap(); // okay, as we matched successfully

    let length = dataset
        .buffers
        .iter()
        .fold(0usize, |acc, item| acc + item.data.size);

    Ok(length)
}

pub unsafe fn copy_data_to_device(
    ident: &str,
    context: &Context,
    base: *mut u8,
    dev_ptr: &DevicePointer,
) -> DandelionResult<()> {
    let dataset = context
        .content
        .iter()
        .find(|&elem| match elem {
            Some(set) => set.ident == ident,
            _ => false,
        })
        .ok_or(DandelionError::ConfigMissmatch)?
        .as_ref()
        .unwrap(); // okay, as we matched successfully

    let mut total = 0isize;
    for item in &dataset.buffers {
        let length = item.data.size;
        let offset = item.data.offset;
        let src = unsafe { base.byte_offset((offset) as isize) } as *const c_void;
        hip::memcpy_h_to_d(dev_ptr, total, src, length)?;
        total += length as isize;
    }
    Ok(())
}

// subject to change; not super happy with this
pub fn get_grid_size(
    gs: &GridSizing,
    buffers: &HashMap<String, (usize, usize)>,
) -> DandelionResult<u32> {
    match gs {
        GridSizing::Absolute(size) => Ok(*size),
        GridSizing::CoverBuffer {
            bufname,
            dimensionality,
            block_dim,
        } => {
            if *dimensionality > 3 || *dimensionality == 0 {
                return Err(DandelionError::ConfigMissmatch);
            }
            let bufsize = buffers
                .get(bufname)
                .ok_or(DandelionError::ConfigMissmatch)?
                .1;
            let side_length = (bufsize as f64).powf(1.0 / (*dimensionality as f64)).ceil() as u32;
            Ok((side_length + block_dim - 1) / block_dim)
        }
    }
}

struct Worker {
    _process: Child,
    pub stdin: Mutex<ChildStdin>,
    pub stdout: Mutex<BufReader<ChildStdout>>,
    pub available: Semaphore,
    pub debt: Mutex<Option<(Debt, Recorder, Context)>>,
}

impl Worker {
    fn new(core_id: u8, gpu_id: u8) -> Self {
        // this trick gives the desired path of mmu_worker for packages within the workspace
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
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .expect("Spawning GPU worker failed");

        // Unwrapping okay, since child is spawned with piped stdin/stdout
        let stdin = Mutex::new(child.stdin.take().unwrap());
        let stdout = Mutex::new(BufReader::new(child.stdout.take().unwrap()));

        Self {
            _process: child,
            stdin,
            stdout,
            // TODO write explanation since this is not trivial
            available: Semaphore::new(1),
            debt: Mutex::new(None),
        }
    }
}

async fn process_output(worker: Arc<Worker>) {
    let mut reader = worker.stdout.lock().await;
    let mut line = String::new();

    while reader.read_line(&mut line).await.unwrap() != 0 {
        let mut handle = worker.debt.lock().await;
        if let Some((debt, mut recorder, mut context)) = handle.take() {
            if line.trim() != "__ERROR__" && line.trim() != "__OK__" {
                // The line is some other output from the GPU, log it
                eprintln!("GPU output: {}", line);
                *handle = Some((debt, recorder, context));
                continue;
            }
            if let Err(e) = recorder.record(RecordPoint::EngineEnd) {
                debt.fulfill(Box::new(Err(e)));
            } else if line.trim() == "__ERROR__" {
                debt.fulfill(Box::new(Err(DandelionError::EngineError)));
            } else {
                read_output_structs::<usize, usize>(&mut context, 0).unwrap();
                let results = Box::new(Ok((context, recorder)));
                debt.fulfill(results);
            }
        } else {
            panic!("Got output from GPUWorker but had no debt to fulfill");
        }

        worker.available.add_permits(1);
        line.clear();
    }
}

// slightly modified Context that can be exchanged between processes
#[derive(Serialize, Deserialize, Debug)]
pub struct SendContext {
    pub context_filename: String,
    pub content: Vec<Option<DataSet>>,
    pub size: usize,
    pub state: ContextState,
    pub occupation: Vec<Position>,
}

impl From<SendContext> for Context {
    fn from(value: SendContext) -> Self {
        Self {
            context: ContextType::Mmu(Box::new(MmuContext {
                storage: MmapMem::alt_open(
                    &value.context_filename,
                    ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
                )
                // TODO: actually handle error
                .unwrap(),
            })),
            content: value.content,
            size: value.size,
            state: value.state,
            occupation: value.occupation,
        }
    }
}

impl TryFrom<&Context> for SendContext {
    type Error = DandelionError;
    fn try_from(value: &Context) -> DandelionResult<Self> {
        let ContextType::Mmu(ref ctxt) = value.context else {
            return Err(DandelionError::ConfigMissmatch);
        };

        // TODO: this is kinda inefficient
        Ok(SendContext {
            // unwrap okay, as Mmu memory is always created as shared so a filename exists
            context_filename: ctxt.storage.filename().unwrap().to_string(),
            content: value.content.clone(),
            size: value.size,
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

async fn process_inputs(
    core_id: u8,
    gpu_id: u8,
    queue: Box<dyn WorkQueue + Send + Sync>,
    worker1: Arc<Worker>,
    worker2: Arc<Worker>,
    worker3: Arc<Worker>,
    worker4: Arc<Worker>,
) {
    let queue = Arc::new(queue);
    loop {
        // A bit clumsy but necessary to use spawn_blocking
        let queue = queue.clone();
        let (args, debt) = task::spawn_blocking(move || queue.get_engine_args())
            .await
            .expect("spawn_blocking thread crashed");

        match args {
            EngineArguments::FunctionArguments(func_args) => {
                let FunctionArguments {
                    config,
                    context,
                    output_sets,
                    mut recorder,
                } = func_args;

                // transform relevant data into serialisable counterparts
                let FunctionConfig::GpuConfig(config) = config else {
                    debt.fulfill(Box::new(Err(DandelionError::ConfigMissmatch)));
                    continue;
                };
                let Ok(send_context) = (&context).try_into() else {
                    debt.fulfill(Box::new(Err(DandelionError::ConfigMissmatch)));
                    continue;
                };

                let mut task = serde_json::to_string(&SendFunctionArgs {
                    config,
                    context: send_context,
                    output_sets,
                })
                .unwrap();

                // Very important to add this newline, as the worker reads line by line
                task += "\n";

                // Unfortunate code duplication because of how select works
                tokio::select! {
                    Ok(permit) = worker1.available.acquire() => {
                        if let Err(e) = recorder.record(RecordPoint::EngineStart) {
                            debt.fulfill(Box::new(Err(e)));
                            continue;
                        }

                        // Give debt and recorder to worker
                        *worker1.debt.lock().await = Some((debt, recorder, context));

                        // Write task description to worker process stdin
                        worker1.stdin.lock().await.write_all(task.as_bytes()).await.expect("Writing failed");

                        // unfortunate way to bring ticket count to 0; have to drop first else the forget_permits does nothing
                        drop(permit);
                        worker1.available.forget_permits(1);
                    },
                    Ok(permit) = worker2.available.acquire() => {
                        if let Err(e) = recorder.record(RecordPoint::EngineStart) {
                            debt.fulfill(Box::new(Err(e)));
                            continue;
                        }

                        // Give debt and recorder to worker
                        *worker2.debt.lock().await = Some((debt, recorder, context));

                        // Write task description to worker process stdin
                        worker2.stdin.lock().await.write_all(task.as_bytes()).await.expect("Writing failed");

                        drop(permit);
                        worker2.available.forget_permits(1);
                    },
                    Ok(permit) = worker3.available.acquire() => {
                        if let Err(e) = recorder.record(RecordPoint::EngineStart) {
                            debt.fulfill(Box::new(Err(e)));
                            continue;
                        }

                        // Give debt and recorder to worker
                        *worker3.debt.lock().await = Some((debt, recorder, context));

                        // Write task description to worker process stdin
                        worker3.stdin.lock().await.write_all(task.as_bytes()).await.expect("Writing failed");

                        drop(permit);
                        worker3.available.forget_permits(1);
                    },
                    Ok(permit) = worker4.available.acquire() => {
                        if let Err(e) = recorder.record(RecordPoint::EngineStart) {
                            debt.fulfill(Box::new(Err(e)));
                            continue;
                        }

                        // Give debt and recorder to worker
                        *worker4.debt.lock().await = Some((debt, recorder, context));

                        // Write task description to worker process stdin
                        worker4.stdin.lock().await.write_all(task.as_bytes()).await.expect("Writing failed");

                        drop(permit);
                        worker4.available.forget_permits(1);
                    }
                }
            }
            EngineArguments::TransferArguments(transfer_args) => {
                let TransferArguments {
                    source,
                    mut destination,
                    destination_set_index,
                    destination_allignment,
                    destination_item_index,
                    destination_set_name,
                    source_set_index,
                    source_item_index,
                    recorder,
                } = transfer_args;
                // wait for all pending functions to complete
                if let Ok((_, _, _, _)) = tokio::try_join!(
                    worker1.available.acquire(),
                    worker2.available.acquire(),
                    worker3.available.acquire(),
                    worker4.available.acquire(),
                ) {
                    // execute transfer
                    let transfer_result = memory_domain::transfer_data_item(
                        &mut destination,
                        &source,
                        destination_set_index,
                        destination_allignment,
                        destination_item_index,
                        destination_set_name.as_str(),
                        source_set_index,
                        source_item_index,
                    )
                    .and(Ok((destination, recorder)));
                    debt.fulfill(Box::new(transfer_result));
                    continue;
                } else {
                    // shouldn't really happen unless Workers processes die
                    debt.fulfill(Box::new(Err(DandelionError::EngineError)));
                }
            }
            EngineArguments::Shutdown(resource_returner) => {
                // wait for all pending functions to complete
                if let Ok((_, _, _, _)) = tokio::try_join!(
                    worker1.available.acquire(),
                    worker2.available.acquire(),
                    worker3.available.acquire(),
                    worker4.available.acquire(),
                ) {
                    resource_returner(vec![ComputeResource::GPU(core_id, gpu_id)]);
                    return;
                } else {
                    // shouldn't really happen unless Workers processes die
                    debt.fulfill(Box::new(Err(DandelionError::EngineError)));
                }
            }
        }
    }
}

async fn run_pool(core_id: u8, gpu_id: u8, queue: Box<dyn WorkQueue + Send + Sync>) {
    let worker1 = Arc::new(Worker::new(core_id, gpu_id));
    let worker2 = Arc::new(Worker::new(core_id, gpu_id));
    let worker3 = Arc::new(Worker::new(core_id, gpu_id));
    let worker4 = Arc::new(Worker::new(core_id, gpu_id));

    tokio::spawn(process_output(worker1.clone()));
    tokio::spawn(process_output(worker2.clone()));
    tokio::spawn(process_output(worker3.clone()));
    tokio::spawn(process_output(worker4.clone()));

    tokio::join!(process_inputs(
        core_id,
        gpu_id,
        queue,
        worker1.clone(),
        worker2.clone(),
        worker3.clone(),
        worker4.clone()
    ));
}

pub fn start_gpu_process_pool(core_id: u8, gpu_id: u8, queue: Box<dyn WorkQueue + Send + Sync>) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Runtime building failed");

    rt.block_on(run_pool(core_id, gpu_id, queue));
}

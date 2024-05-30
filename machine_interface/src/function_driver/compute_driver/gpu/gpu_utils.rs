use crate::{
    function_driver::{
        thread_utils::EngineLoop, ComputeResource, FunctionArguments, FunctionConfig, GpuConfig,
        ParsingArguments, TransferArguments, WorkDone, WorkQueue, WorkToDo,
    },
    interface::read_output_structs,
    memory_domain::{self, mmu::MmuContext, Context, ContextState, ContextTrait, ContextType},
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
use std::{
    collections::HashMap,
    process::Stdio,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::{Mutex, Notify},
    task,
};

use self::super::hip;

use super::{config_parsing::Sizing, hip::DevicePointer, GpuLoop};

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

/// # Safety
/// Requires *base* to point to the stat of *context*
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

pub fn get_size(
    sizing: &Sizing,
    buffers: &HashMap<String, (usize, usize)>,
    context: &Context,
) -> DandelionResult<usize> {
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
                .ok_or(DandelionError::ConfigMissmatch)?
                .as_ref()
                .unwrap(); // okay, as we matched successfully

            let data_item = dataset
                .buffers
                .first()
                .ok_or(DandelionError::ConfigMissmatch)?;

            let relative_offset = *idx * std::mem::size_of::<i64>();
            if relative_offset > data_item.data.size {
                return Err(DandelionError::ConfigMissmatch);
            }

            // TODO: make this more portable (not usize)
            let mut buf: [usize; 1] = [0];
            context.read(data_item.data.offset + relative_offset, &mut buf)?;

            Ok(buf[0])
        }
        Sizing::Sizeof(bufname) => Ok(buffers
            .get(bufname)
            .ok_or(DandelionError::ConfigMissmatch)?
            .1),
    }
}

// TODO unify this with thread_utils/run_thead as it's more or less a carbon copy
pub fn start_gpu_thread(core_id: u8, gpu_id: u8, queue: Box<dyn WorkQueue>) {
    // set core affinity
    if !core_affinity::set_for_current(core_affinity::CoreId { id: core_id.into() }) {
        log::error!("core received core id that could not be set");
        return;
    }
    let mut engine_state = GpuLoop::init(ComputeResource::GPU(core_id, gpu_id))
        .expect("Failed to initialize thread state");
    loop {
        // TODO catch unwind so we can always return an error or shut down gracefully
        let (args, debt) = queue.get_engine_args();
        match args {
            WorkToDo::FunctionArguments(func_args) => {
                let FunctionArguments {
                    config,
                    context,
                    output_sets,
                    mut recorder,
                } = func_args;
                if let Err(err) = recorder.record(RecordPoint::EngineStart) {
                    debt.fulfill(Box::new(Err(err)));
                    continue;
                }
                let result = engine_state.run(config, context, output_sets);
                if result.is_ok() {
                    if let Err(err) = recorder.record(RecordPoint::EngineEnd) {
                        debt.fulfill(Box::new(Err(err)));
                        continue;
                    }
                }
                let results = Box::new(result.and_then(|context| Ok(WorkDone::Context(context))));
                debt.fulfill(results);
            }
            WorkToDo::TransferArguments(transfer_args) => {
                let TransferArguments {
                    source,
                    mut destination,
                    destination_set_index,
                    destination_allignment,
                    destination_item_index,
                    destination_set_name,
                    source_set_index,
                    source_item_index,
                    mut recorder,
                } = transfer_args;
                match recorder.record(RecordPoint::TransferStart) {
                    Ok(()) => (),
                    Err(err) => {
                        debt.fulfill(Box::new(Err(err)));
                        continue;
                    }
                }
                let transfer_result = memory_domain::transfer_data_item(
                    &mut destination,
                    &source,
                    destination_set_index,
                    destination_allignment,
                    destination_item_index,
                    destination_set_name.as_str(),
                    source_set_index,
                    source_item_index,
                );
                match recorder.record(RecordPoint::TransferEnd) {
                    Ok(()) => (),
                    Err(err) => {
                        debt.fulfill(Box::new(Err(err)));
                        continue;
                    }
                }
                let transfer_return = transfer_result.and(Ok(WorkDone::Context(destination)));
                debt.fulfill(Box::new(transfer_return));
                continue;
            }
            WorkToDo::ParsingArguments(ParsingArguments {
                driver,
                path,
                static_domain,
                mut recorder,
            }) => {
                recorder.record(RecordPoint::ParsingStart).unwrap();
                let function_result = driver.parse_function(path, static_domain);
                recorder.record(RecordPoint::ParsingEnd).unwrap();
                match function_result {
                    Ok(function) => debt.fulfill(Box::new(Ok(WorkDone::Function(function)))),
                    Err(err) => debt.fulfill(Box::new(Err(err))),
                }
                continue;
            }
            WorkToDo::Shutdown() => {
                debt.fulfill(Box::new(Ok(WorkDone::Resources(vec![
                    ComputeResource::GPU(core_id, gpu_id),
                ]))));
                return;
            }
        }
    }
}

struct Worker {
    _process: Child,
    pub stdin: Mutex<ChildStdin>,
    pub stdout: Mutex<BufReader<ChildStdout>>,
    pub available: Notify,
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

        // We use a Notify (basically a semaphore) to signal availability to work
        let available = Notify::new();
        // Adds a ticket to the semaphore, as the Worker initially is free
        available.notify_one();

        Self {
            _process: child,
            stdin,
            stdout,
            available,
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
            } else if line.trim().starts_with("__ERROR__") {
                eprintln!("GPU error: {}", line);
                debt.fulfill(Box::new(Err(DandelionError::EngineError)));
            } else {
                read_output_structs::<usize, usize>(&mut context, 0).unwrap();
                let results = Box::new(Ok(WorkDone::Context(context)));
                debt.fulfill(results);
            }
        } else {
            panic!("Got output from GPUWorker but had no debt to fulfill");
        }

        worker.available.notify_one();
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
        // Need to use spawn_blocking, else waiting on the queue can deadlock,
        // eg. if new work being added to the queue relies on old work being submitted
        let (args, debt) = task::spawn_blocking(move || queue.get_engine_args())
            .await
            .expect("spawn_blocking thread crashed");

        match args {
            WorkToDo::FunctionArguments(func_args) => {
                // let worker1 = worker1.clone();
                // let worker2 = worker2.clone();
                // let worker3 = worker3.clone();
                // let worker4 = worker4.clone();
                // Spawn in new task to enable more concurrency
                // tokio::spawn(async move {
                let FunctionArguments {
                    config,
                    context,
                    output_sets,
                    mut recorder,
                } = func_args;

                // transform relevant data into serialisable counterparts
                let FunctionConfig::GpuConfig(config) = config else {
                    debt.fulfill(Box::new(Err(DandelionError::ConfigMissmatch)));
                    return;
                };
                let Ok(send_context) = (&context).try_into() else {
                    debt.fulfill(Box::new(Err(DandelionError::ConfigMissmatch)));
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

                // Unfortunate code duplication because of how select works
                tokio::select! {
                    _ = worker1.available.notified() => {
                        if let Err(e) = recorder.record(RecordPoint::EngineStart) {
                            debt.fulfill(Box::new(Err(e)));
                            return;
                        }

                        // Give debt and recorder to worker
                        *worker1.debt.lock().await = Some((debt, recorder, context));

                        // Write task description to worker process stdin
                        worker1.stdin.lock().await.write_all(task.as_bytes()).await.expect("Writing failed");
                    },
                    _ = worker2.available.notified() => {
                        if let Err(e) = recorder.record(RecordPoint::EngineStart) {
                            debt.fulfill(Box::new(Err(e)));
                            return;
                        }

                        // Give debt and recorder to worker
                        *worker2.debt.lock().await = Some((debt, recorder, context));

                        // Write task description to worker process stdin
                        worker2.stdin.lock().await.write_all(task.as_bytes()).await.expect("Writing failed");
                    },
                    _ = worker3.available.notified() => {
                        if let Err(e) = recorder.record(RecordPoint::EngineStart) {
                            debt.fulfill(Box::new(Err(e)));
                            return;
                        }

                        // Give debt and recorder to worker
                        *worker3.debt.lock().await = Some((debt, recorder, context));

                        // Write task description to worker process stdin
                        worker3.stdin.lock().await.write_all(task.as_bytes()).await.expect("Writing failed");
                    },
                    _ = worker4.available.notified() => {
                        if let Err(e) = recorder.record(RecordPoint::EngineStart) {
                            debt.fulfill(Box::new(Err(e)));
                            return;
                        }

                        // Give debt and recorder to worker
                        *worker4.debt.lock().await = Some((debt, recorder, context));

                        // Write task description to worker process stdin
                        worker4.stdin.lock().await.write_all(task.as_bytes()).await.expect("Writing failed");
                    }
                }
                // });
            }
            WorkToDo::TransferArguments(transfer_args) => {
                let TransferArguments {
                    source,
                    mut destination,
                    destination_set_index,
                    destination_allignment,
                    destination_item_index,
                    destination_set_name,
                    source_set_index,
                    source_item_index,
                    mut recorder,
                } = transfer_args;
                // Spawn in new task to enable more concurrency
                tokio::spawn(async move {
                    match recorder.record(RecordPoint::TransferStart) {
                        Ok(()) => (),
                        Err(err) => {
                            debt.fulfill(Box::new(Err(err)));
                            return;
                        }
                    }
                    let transfer_result = memory_domain::transfer_data_item(
                        &mut destination,
                        &source,
                        destination_set_index,
                        destination_allignment,
                        destination_item_index,
                        destination_set_name.as_str(),
                        source_set_index,
                        source_item_index,
                    );
                    match recorder.record(RecordPoint::TransferEnd) {
                        Ok(()) => (),
                        Err(err) => {
                            debt.fulfill(Box::new(Err(err)));
                            return;
                        }
                    }
                    let transfer_return = transfer_result.and(Ok(WorkDone::Context(destination)));
                    debt.fulfill(Box::new(transfer_return));
                });
            }
            WorkToDo::ParsingArguments(ParsingArguments {
                driver,
                path,
                static_domain,
                mut recorder,
            }) => {
                // Spawn in new task to enable more concurrency
                tokio::spawn(async move {
                    recorder.record(RecordPoint::ParsingStart).unwrap();
                    let function_result = driver.parse_function(path, static_domain);
                    recorder.record(RecordPoint::ParsingEnd).unwrap();
                    match function_result {
                        Ok(function) => debt.fulfill(Box::new(Ok(WorkDone::Function(function)))),
                        Err(err) => debt.fulfill(Box::new(Err(err))),
                    }
                });
            }
            WorkToDo::Shutdown() => {
                // wait for all pending functions to complete
                tokio::join!(
                    worker1.available.notified(),
                    worker2.available.notified(),
                    worker3.available.notified(),
                    worker4.available.notified(),
                );
                debt.fulfill(Box::new(Ok(WorkDone::Resources(vec![
                    ComputeResource::GPU(gpu_id, core_id),
                ]))));
                return;
            }
        }
    }
}

async fn run_pool(core_id: u8, gpu_id: u8, queue: Box<dyn WorkQueue + Send + Sync>) {
    let worker1 = Arc::new(Worker::new(core_id + 2, gpu_id));
    let worker2 = Arc::new(Worker::new(core_id + 3, gpu_id));
    let worker3 = Arc::new(Worker::new(core_id + 4, gpu_id));
    let worker4 = Arc::new(Worker::new(core_id + 5, gpu_id));

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
    let counter = AtomicU8::new(0);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .on_thread_start(move || {
            let inc = counter.fetch_add(1, Ordering::SeqCst);
            // set core affinity
            if !core_affinity::set_for_current(core_affinity::CoreId {
                id: (core_id + inc).into(),
            }) {
                panic!("core received core id that could not be set");
            }
        })
        .enable_all()
        .build()
        .expect("Runtime building failed");

    rt.block_on(run_pool(core_id, gpu_id, queue));
}

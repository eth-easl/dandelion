use crate::{
    function_driver::{
        thread_utils::EngineLoop, ComputeResource, FunctionArguments, FunctionConfig, GpuConfig,
        ParsingArguments, TransferArguments, WorkDone, WorkQueue, WorkToDo,
    },
    interface::read_output_structs,
    memory_domain::{self, mmu::MmuContext, Context, ContextState, ContextTrait, ContextType},
    util::mmapmem::MmapMem,
    DataSet, Position,
};
use dandelion_commons::{records::RecordPoint, DandelionError, DandelionResult};
use libc::c_void;
use log::{debug, error};
use nix::sys::mman::ProtFlags;
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
// use tokio::{
//     io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
//     process::{Child, ChildStdin, ChildStdout, Command},
//     sync::{Mutex, Notify, Semaphore},
//     task,
// };

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
        let src = base.byte_offset((offset) as isize) as *const c_void;
        hip::memcpy_h_to_d(dev_ptr, total, src, length)?;
        total += length as isize;
    }
    Ok(())
}

pub fn get_size(
    sizing: &Sizing,
    buffers: &HashMap<String, (usize, usize)>,
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

            let mut buf: [u64; 1] = [0];
            context.read(data_item.data.offset + relative_offset, &mut buf)?;

            Ok(buf[0])
        }
        Sizing::Sizeof(bufname) => Ok(buffers
            .get(bufname)
            .ok_or(DandelionError::ConfigMissmatch)?
            .1 as u64),
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
                let results = Box::new(result.map(WorkDone::Context));
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
    process: Child,
    pub stdin: ChildStdin,
    pub stdout: BufReader<ChildStdout>,
    // pub available: Notify,
    // pub debt: Mutex<Option<(Debt, Recorder, Context)>>,
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
    pub size: usize,
    pub state: ContextState,
    pub occupation: Vec<Position>,
}

impl TryFrom<SendContext> for Context {
    type Error = DandelionError;

    fn try_from(value: SendContext) -> Result<Self, Self::Error> {
        Ok(Self {
            context: ContextType::Mmu(Box::new(MmuContext {
                storage: MmapMem::alt_open(
                    &value.context_filename,
                    ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
                )?,
            })),
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
        let ContextType::Mmu(ref ctxt) = value.context else {
            return Err(DandelionError::ConfigMissmatch);
        };

        // Cloning so much on the hot path is not optimal; a custom serialisation function would be quicker
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

fn manage_worker(
    resources: (u8, u8),
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
    let mut worker = Worker::new(core_id + 1, gpu_id);
    let mut line = String::new();

    loop {
        // A different worker thread got the shutdown signal
        if done.load(Ordering::SeqCst) {
            return;
        }

        let (args, debt) = queue.get_engine_args();
        match args {
            WorkToDo::FunctionArguments(func_args) => {
                let FunctionArguments {
                    config,
                    mut context,
                    output_sets,
                    mut recorder,
                } = func_args;
                if let Err(err) = recorder.record(RecordPoint::EngineStart) {
                    debt.fulfill(Box::new(Err(err)));
                    continue;
                }

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

                if let Err(e) = recorder.record(RecordPoint::EngineStart) {
                    debt.fulfill(Box::new(Err(e)));
                    return;
                }

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

                    if let Err(e) = recorder.record(RecordPoint::EngineEnd) {
                        debt.fulfill(Box::new(Err(e)));
                        break;
                    } else if line.trim().starts_with("__ERROR__") {
                        error!("GPU error: {}", line);
                        debt.fulfill(Box::new(Err(DandelionError::EngineError)));
                        break;
                    } else {
                        read_output_structs::<usize, usize>(&mut context, 0).unwrap();
                        let results = Box::new(Ok(WorkDone::Context(context)));
                        debt.fulfill(results);
                        break;
                    }
                }
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
                // Return original resources that were given to Engine
                debt.fulfill(Box::new(Ok(WorkDone::Resources(vec![
                    ComputeResource::GPU(resources.0, resources.1),
                ]))));

                // Inform other threads to shutdown as well when they are done
                done.swap(true, Ordering::SeqCst);
                return;
            }
        }
    }
}

const NUM_WORKERS: u8 = 2;

pub fn start_gpu_process_pool(core_id: u8, gpu_id: u8, queue: Box<dyn WorkQueue + Send + Sync>) {
    let done = Arc::new(AtomicBool::new(false));
    let queue: Arc<dyn WorkQueue + Send + Sync> = queue.into();
    for offset in 0..NUM_WORKERS {
        let queue = queue.clone();
        let done = done.clone();
        spawn(move || manage_worker((core_id, gpu_id), core_id + 2 * offset, gpu_id, queue, done));
    }
}

use crate::interface::{DandelionSystemData, SizedIntTrait};
use crate::{
    function_driver::{ComputeResource, FunctionConfig, GpuConfig, WorkDone, WorkQueue, WorkToDo},
    interface::read_output_structs,
    memory_domain::{self, gpu::GpuContext, Context, ContextState, ContextTrait, ContextType},
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

use self::super::hip;
use super::buffer_pool::BufferPool;

use super::{config_parsing::Sizing, hip::DevicePointer};

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
        .ok_or(DandelionError::UndeclaredIdentifier(ident.to_owned()))?
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

#[cfg(feature = "gpu")]
/// # Safety
/// Requires *base* to point to the stat of *context*
pub unsafe fn write_gpu_outputs<PtrT: SizedIntTrait, SizeT: SizedIntTrait>(
    context: &mut Context,
    system_data_offset: usize,
    base: *mut u8,
    output_set_names: &[String],
    device_buffers: &HashMap<String, (usize, usize)>,
    buffer_pool: &BufferPool,
) -> DandelionResult<()> {
    // read the system buffer

    use crate::{
        interface::{IoBufferDescriptor, IoSetInfo},
        ptr_t, size_t, usize, usize_ptr,
    };
    let mut system_struct = DandelionSystemData::<PtrT, SizeT>::default();
    context.read(
        system_data_offset,
        core::slice::from_mut(&mut system_struct),
    )?;

    let output_set_number = usize!(system_struct.output_sets_len);
    let mut output_set_info = vec![];
    if output_set_info.try_reserve(output_set_number + 1).is_err() {
        return Err(DandelionError::OutOfMemory);
    }
    let empty_output_set = IoSetInfo::<PtrT, SizeT> {
        ident: ptr_t!(0),
        ident_len: size_t!(0),
        offset: size_t!(0),
    };
    output_set_info.resize_with(output_set_number + 1, || empty_output_set.clone());
    context.read(usize_ptr!(system_struct.output_sets), &mut output_set_info)?;

    let mut output_buffers: Vec<IoBufferDescriptor<PtrT, SizeT>> = Vec::new();
    if output_buffers
        .try_reserve_exact(output_set_names.len())
        .is_err()
    {
        return Err(DandelionError::OutOfMemory);
    }
    for (i, output_name) in output_set_names.iter().enumerate() {
        // alignment shouldn't really make a huge difference
        let (dev_ptr_idx, size) = device_buffers
            .get(output_name)
            .ok_or(DandelionError::ConfigMissmatch)?;
        let buf_offset = context.get_free_space(*size, 8)?;

        let dst = unsafe { base.byte_offset(buf_offset as isize) } as *const c_void;
        let dev_ptr = buffer_pool.get(*dev_ptr_idx)?;
        hip::memcpy_d_to_h(dst, &dev_ptr, *size)?;

        output_buffers.push(IoBufferDescriptor {
            ident: ptr_t!(0),
            ident_len: size_t!(0),
            data: ptr_t!(buf_offset),
            data_len: size_t!(*size),
            key: size_t!(0),
        });
        output_set_info[i].offset = size_t!(i);
    }
    output_set_info[output_set_number].offset = size_t!(output_set_number);

    context.write(usize_ptr!(system_struct.output_sets), &output_set_info)?;

    let output_buffers_offset: PtrT =
        ptr_t!(context.get_free_space_and_write_slice(&output_buffers[..])? as usize);

    system_struct.output_bufs = output_buffers_offset;

    context.write(system_data_offset, core::slice::from_ref(&system_struct))?;
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
        Sizing::Sizeof(bufname) => Ok(buffers
            .get(bufname)
            .ok_or(DandelionError::UndeclaredIdentifier(bufname.to_owned()))?
            .1 as u64),
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
    pub size: usize,
    pub state: ContextState,
    pub occupation: Vec<Position>,
}

impl TryFrom<SendContext> for Context {
    type Error = DandelionError;

    fn try_from(value: SendContext) -> Result<Self, Self::Error> {
        Ok(Self {
            context: ContextType::Gpu(Box::new(GpuContext {
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
        let ContextType::Gpu(ref ctxt) = value.context else {
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
            WorkToDo::ParsingArguments {
                driver,
                path,
                static_domain,
                mut recorder,
            } => {
                recorder.record(RecordPoint::ParsingStart).unwrap();
                let function_result = driver.parse_function(path, static_domain);
                recorder.record(RecordPoint::ParsingEnd).unwrap();
                match function_result {
                    Ok(function) => debt.fulfill(Box::new(Ok(WorkDone::Function(function)))),
                    Err(err) => debt.fulfill(Box::new(Err(err))),
                }
                continue;
            }
            WorkToDo::LoadingArguments {
                function,
                domain,
                ctx_size,
                mut recorder,
            } => {
                recorder.record(RecordPoint::LoadStart).unwrap();
                let load_result = function.load(domain, ctx_size);
                recorder.record(RecordPoint::LoadEnd).unwrap();
                match load_result {
                    Ok(context) => debt.fulfill(Box::new(Ok(WorkDone::Context(context)))),
                    Err(err) => debt.fulfill(Box::new(Err(err))),
                }
                continue;
            }
            WorkToDo::Shutdown() => {
                // Return original resources that were given to Engine
                debt.fulfill(Box::new(Ok(WorkDone::Resources(vec![
                    ComputeResource::GPU(resources.0, resources.1, resources.2),
                ]))));

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

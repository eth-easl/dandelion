use crate::{
    function_lib::{Driver, Engine, FunctionConfig, Loader},
    memory_domain::{cheri::cheri_c_context, Context, ContextTrait, ContextType, MemoryDomain},
    util::elf_parser,
    DataItem, DataRequirement, DataRequirementList, DataSet, Position,
};
use core::{
    future::{ready, Future},
    pin::Pin,
};
use core_affinity;
use dandelion_commons::{DandelionError, DandelionResult};
use futures::{task::Poll, Stream};
use libc::size_t;
use std::{
    collections::HashMap,
    sync::atomic::{AtomicBool, Ordering},
    thread::{spawn, JoinHandle},
};

#[link(name = "cheri_lib")]
extern "C" {
    fn cheri_run_static(
        context: *const cheri_c_context,
        entry_point: size_t,
        return_pair_offset: size_t,
        stack_pointer: size_t,
    ) -> i8;
}

const SYSTEM_STRUCT_SIZE: usize = 72;
const IO_SET_INFO_SIZE: usize = 24;
const IO_BUFFER_SIZE: usize = 32;
const MAX_OUTPUTS: u32 = 16;

fn setup_input_structs(context: &mut Context, system_data_offset: usize) -> DandelionResult<()> {
    // prepare information to set up input sets, output sets and input buffers
    let input_buffer_number = context.dynamic_data.keys().count();
    let input_set_number = context
        .dynamic_data
        .keys()
        .max()
        .and_then(|x| Some(x + 1))
        .unwrap_or(0usize)
        + 1; // +1 for the sentinel set
    let output_set_number = MAX_OUTPUTS as usize + 1; // +1 for sentinel set

    let io_info_size = input_buffer_number * IO_BUFFER_SIZE
        + (input_set_number + output_set_number) * IO_SET_INFO_SIZE;
    let io_info_offset = context.get_free_space(io_info_size, 8)?;
    context.static_data.push(Position {
        offset: io_info_offset,
        size: io_info_size,
    });

    // fill in data for input sets
    // input set number and pointer (offset)
    // TODO replace by single checked allocation and writing to it
    let mut system_buffer = Vec::<u8>::new();
    // heap start and end
    let heap_start = context.get_last_item_end();
    system_buffer.append(&mut usize::to_ne_bytes(heap_start).to_vec());
    system_buffer.append(&mut usize::to_ne_bytes(context.size).to_vec());
    // input set number and offset
    // -1 to exclude sentinel set
    system_buffer.append(&mut usize::to_ne_bytes(input_set_number - 1).to_vec().to_vec());
    system_buffer.append(&mut usize::to_ne_bytes(io_info_offset).to_vec());
    // output sets and offsets
    // -1 to exclude sentinel set
    system_buffer.append(&mut usize::to_ne_bytes(output_set_number - 1).to_vec());
    system_buffer.append(
        &mut usize::to_ne_bytes(io_info_offset + input_set_number * IO_SET_INFO_SIZE).to_vec(),
    );
    // input buffers
    system_buffer.append(
        &mut usize::to_ne_bytes(
            io_info_offset + (input_set_number + output_set_number) * IO_SET_INFO_SIZE,
        )
        .to_vec(),
    );
    // write system buffer after exit code
    context.write(system_data_offset + 8, system_buffer)?;

    // input and output io buffer pointers (output just 0 as the application sets them)
    let mut io_data_buffer = Vec::new();
    // TODO ask for correct size and make it writes instead of appends

    // start writing input set info structs
    let mut buffer_count = 0;
    let mut input_buffers = Vec::<u8>::new();
    for set_index in 0..input_set_number {
        // get name and length
        let (name, buffer_num) = match context.dynamic_data.get(&set_index) {
            Some(set) => (set.ident.clone(), set.buffers.len()),
            None => (String::from(""), 0),
        };
        // find space and write string
        if name != "" {
            let string_offset = context.get_free_space(name.len(), 8)?;
            context.write(string_offset, name.as_bytes().to_vec())?;
            context.static_data.push(Position {
                offset: string_offset,
                size: name.len(),
            });
            io_data_buffer.append(&mut usize::to_ne_bytes(string_offset).to_vec());
            io_data_buffer.append(&mut usize::to_ne_bytes(name.len()).to_vec());
        } else {
            io_data_buffer.append(&mut usize::to_ne_bytes(0).to_vec());
            io_data_buffer.append(&mut usize::to_ne_bytes(0).to_vec());
        }
        // find buffers
        io_data_buffer.append(&mut usize::to_ne_bytes(buffer_count).to_vec());
        buffer_count += buffer_num;
        for buffer_index in 0..buffer_num {
            let (name, offset, size) = match context.dynamic_data.get(&set_index) {
                Some(set) => {
                    let buffer = &set.buffers[buffer_index];
                    (buffer.ident.clone(), buffer.data.offset, buffer.data.size)
                }
                None => (String::from(""), 0, 0),
            };
            let mut string_offset = 0;
            if name != "" {
                string_offset = context.get_free_space(name.len(), 8)?;
                context.write(string_offset, name.as_bytes().to_vec())?;
                context.static_data.push(Position {
                    offset: string_offset,
                    size: name.len(),
                });
            }
            input_buffers.append(&mut usize::to_ne_bytes(string_offset).to_vec());
            input_buffers.append(&mut usize::to_ne_bytes(name.len()).to_vec());
            input_buffers.append(&mut usize::to_ne_bytes(offset).to_vec());
            input_buffers.append(&mut usize::to_ne_bytes(size).to_vec());
        }
    }
    // start writing output set info structs
    for _output_index in 0..output_set_number {
        io_data_buffer.append(&mut usize::to_ne_bytes(0).to_vec());
        io_data_buffer.append(&mut usize::to_ne_bytes(0).to_vec());
        io_data_buffer.append(&mut usize::to_ne_bytes(0).to_vec());
    }

    io_data_buffer.append(&mut input_buffers);
    context.write(io_info_offset, io_data_buffer)?;

    return Ok(());
}

fn get_output_layout(context: &mut Context, system_data_offset: usize) -> DandelionResult<()> {
    // clear out old data
    context.static_data = Vec::new();
    // read the system buffer
    let system_struct = context.read(system_data_offset, SYSTEM_STRUCT_SIZE)?;
    // get exit value
    let _exit_value: i32 = i32::from_ne_bytes(
        system_struct[0..4]
            .try_into()
            .expect("Should be able to convert"),
    );
    // get output set number +1 for sentinel set
    // TODO use as_chunks when it stabilizes
    let output_set_number: usize = usize::from_ne_bytes(
        system_struct[40..48]
            .try_into()
            .expect("Should be able to convert"),
    );
    if output_set_number == 1 {
        context.dynamic_data = HashMap::new();
        return Ok(());
    }
    let output_set_info_offset = usize::from_ne_bytes(
        system_struct[48..56]
            .try_into()
            .expect("Should be able to convert"),
    );
    let output_buffers_offset = usize::from_ne_bytes(
        system_struct[64..72]
            .try_into()
            .expect("Should be able to convert"),
    );
    // load output set info, + 1 to include sentinel set
    let output_set_info = context.read(
        output_set_info_offset,
        (output_set_number + 1) * IO_SET_INFO_SIZE,
    )?;
    let mut output_sets = HashMap::new();
    let output_buffer_number = usize::from_ne_bytes(
        output_set_info
            [output_set_number * IO_SET_INFO_SIZE + 16..output_set_number * IO_SET_INFO_SIZE + 24]
            .try_into()
            .expect("Should be able to convert"),
    );
    let output_buffers =
        context.read(output_buffers_offset, output_buffer_number * IO_BUFFER_SIZE)?;

    for output_set in 0..output_set_number {
        let set_start = output_set * IO_SET_INFO_SIZE;
        let ident_offset = usize::from_ne_bytes(
            output_set_info[set_start..set_start + 8]
                .try_into()
                .expect("Should be able to convert"),
        );
        let ident_length = usize::from_ne_bytes(
            output_set_info[set_start + 8..set_start + 16]
                .try_into()
                .expect("Should be able to convert"),
        );
        let set_ident = context.read(ident_offset, ident_length)?;
        let set_ident_string = String::from_utf8(set_ident).unwrap_or("".to_string());
        let first_buffer = usize::from_ne_bytes(
            output_set_info[set_start + 16..set_start + 24]
                .try_into()
                .expect("Should be able to convert"),
        );
        let one_past_last_buffer = usize::from_ne_bytes(
            output_set_info[set_start + IO_SET_INFO_SIZE + 16..set_start + IO_SET_INFO_SIZE + 24]
                .try_into()
                .expect("Should be able to convert"),
        );
        let buffer_number = one_past_last_buffer - first_buffer;
        let mut buffers = Vec::new();
        if buffers.try_reserve(buffer_number).is_err() {
            return Err(DandelionError::OutOfMemory);
        }
        for buffer_index in 0..buffer_number {
            let buffer_start = buffer_index * IO_BUFFER_SIZE;
            let buffer_ident_offset = usize::from_ne_bytes(
                output_buffers[buffer_start..buffer_start + 8]
                    .try_into()
                    .expect("Should be able to convert"),
            );
            let buffer_ident_length = usize::from_ne_bytes(
                output_buffers[buffer_start + 8..buffer_start + 16]
                    .try_into()
                    .expect("Should be able to convert"),
            );
            let buffer_ident = context.read(buffer_ident_offset, buffer_ident_length)?;
            let data_offset = usize::from_ne_bytes(
                output_buffers[buffer_start + 16..buffer_start + 24]
                    .try_into()
                    .expect("Should be able to convert"),
            );
            let data_length = usize::from_ne_bytes(
                output_buffers[buffer_start + 24..buffer_start + 32]
                    .try_into()
                    .expect("Should be able to convert"),
            );
            let ident_string = String::from_utf8(buffer_ident).unwrap_or("".to_string());
            buffers.push(DataItem {
                ident: ident_string,
                data: Position {
                    offset: data_offset,
                    size: data_length,
                },
            })
        }
        // only add output set if there are actual buffers for it.
        if buffers.len() > 0 {
            output_sets.insert(
                output_set,
                DataSet {
                    ident: set_ident_string,
                    buffers: buffers,
                },
            );
        }
    }

    context.dynamic_data = output_sets;
    Ok(())
}

struct CheriCommand {
    cancel: bool,
    context: *const cheri_c_context,
    entry_point: size_t,
    return_pair_offset: size_t,
    stack_pointer: size_t,
}
unsafe impl Send for CheriCommand {}

pub struct CheriEngine {
    is_running: AtomicBool,
    command_sender: std::sync::mpsc::Sender<CheriCommand>,
    result_receiver: futures::channel::mpsc::Receiver<DandelionResult<()>>,
    thread_handle: Option<JoinHandle<()>>,
}

struct CheriFuture<'a> {
    engine: &'a mut CheriEngine,
    context: Option<Context>,
    system_data_offset: usize,
}

// TODO find better way than take unwrap to return context
// or at least a way to ensure that the future is always initialized with a context,
// so this can only happen when poll is called after it has already returned the context once
impl Future for CheriFuture<'_> {
    type Output = (DandelionResult<()>, Context);
    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut futures::task::Context,
    ) -> futures::task::Poll<Self::Output> {
        match Pin::new(&mut self.engine.result_receiver).poll_next(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(None) => {
                return Poll::Ready((
                    Err(DandelionError::EngineError),
                    self.context.take().unwrap(),
                ))
            }
            Poll::Ready(Some(Err(err))) => {
                return Poll::Ready((Err(err), self.context.take().unwrap()))
            }
            Poll::Ready(Some(Ok(()))) => (),
        }
        let mut context = self.context.take().unwrap();
        // erase all assumptions on context internal layout
        context.dynamic_data.clear();
        context.static_data.clear();
        // read outputs
        let result = get_output_layout(&mut context, self.system_data_offset);
        self.engine.is_running.store(false, Ordering::Release);
        Poll::Ready((result, context))
    }
}

fn run_thread(
    core_id: u8,
    command_receiver: std::sync::mpsc::Receiver<CheriCommand>,
    mut result_sender: futures::channel::mpsc::Sender<DandelionResult<()>>,
) -> () {
    // set core
    if !core_affinity::set_for_current(core_affinity::CoreId { id: core_id.into() }) {
        return;
    };
    'commandloop: for command in command_receiver.iter() {
        if command.cancel {
            break 'commandloop;
        }
        let cheri_error;
        unsafe {
            cheri_error = cheri_run_static(
                command.context,
                command.entry_point,
                command.return_pair_offset,
                command.stack_pointer,
            );
        }
        let message = match cheri_error {
            0 => Ok(()),
            1 => Err(DandelionError::OutOfMemory),
            _ => Err(DandelionError::NotImplemented),
        };
        // try sending until succeeds
        let mut not_sent = true;
        while not_sent {
            not_sent = match result_sender.try_send(message.clone()) {
                Ok(()) => false,
                Err(err) if err.is_full() => true,
                Err(_) => break 'commandloop,
            }
        }
    }
}

impl Engine for CheriEngine {
    fn run(
        &mut self,
        config: &FunctionConfig,
        mut context: Context,
    ) -> Pin<Box<dyn futures::Future<Output = (DandelionResult<()>, Context)> + '_ + Send>> {
        if self.is_running.swap(true, Ordering::AcqRel) {
            return Box::pin(ready((Err(DandelionError::EngineAlreadyRunning), context)));
        }
        let elf_config = match config {
            FunctionConfig::ElfConfig(conf) => conf,
            _ => return Box::pin(ready((Err(DandelionError::ConfigMissmatch), context))),
        };
        let cheri_context = match &context.context {
            ContextType::Cheri(ref cheri_context) => cheri_context,
            _ => return Box::pin(ready((Err(DandelionError::ContextMissmatch), context))),
        };
        let command = CheriCommand {
            cancel: false,
            context: cheri_context.context,
            entry_point: elf_config.entry_point,
            return_pair_offset: elf_config.return_offset.0,
            stack_pointer: cheri_context.size,
        };
        if let Err(err) = setup_input_structs(&mut context, elf_config.system_data_offset) {
            return Box::pin(ready((Err(err), context)));
        }
        match self.command_sender.send(command) {
            Err(_) => return Box::pin(ready((Err(DandelionError::EngineError), context))),
            Ok(_) => (),
        }
        let function_future = Box::<CheriFuture>::pin(CheriFuture {
            engine: self,
            context: Some(context),
            system_data_offset: elf_config.system_data_offset,
        });
        return function_future;
    }
    fn abort(&mut self) -> DandelionResult<()> {
        if !self.is_running.load(Ordering::Acquire) {
            return Err(DandelionError::NoRunningFunction);
        }
        // TODO actually abort
        Ok(())
    }
}

impl Drop for CheriEngine {
    fn drop(&mut self) {
        if let Some(handle) = self.thread_handle.take() {
            // drop channel
            let _res = self.command_sender.send(CheriCommand {
                cancel: true,
                context: std::ptr::null(),
                entry_point: 0,
                return_pair_offset: 0,
                stack_pointer: 0,
            });
            handle.join().expect("Cheri thread should not panic");
        }
    }
}

pub struct CheriDriver {}

impl Driver for CheriDriver {
    // // take or release one of the available engines
    fn start_engine(config: Vec<u8>) -> DandelionResult<Box<dyn Engine>> {
        if config.len() != 1 {
            return Err(DandelionError::ConfigMissmatch);
        }
        let cpu_slot: u8 = config[0];
        // check that core is available
        let available_cores = match core_affinity::get_core_ids() {
            None => return Err(DandelionError::EngineError),
            Some(cores) => cores,
        };
        if !available_cores
            .iter()
            .find(|x| x.id == usize::from(cpu_slot))
            .is_some()
        {
            return Err(DandelionError::MalformedConfig);
        }
        let (command_sender, command_receiver) = std::sync::mpsc::channel();
        let (result_sender, result_receiver) = futures::channel::mpsc::channel(0);
        let thread_handle = spawn(move || run_thread(cpu_slot, command_receiver, result_sender));
        let is_running = AtomicBool::new(false);
        return Ok(Box::new(CheriEngine {
            command_sender,
            result_receiver,
            thread_handle: Some(thread_handle),
            is_running,
        }));
    }
}

const DEFAULT_SPACE_SIZE: usize = 0x40_0000; // 4MiB

pub struct CheriLoader {}
impl Loader for CheriLoader {
    // parses an executable,
    // returns the layout requirements and a context containing static data,
    //  and a layout description for it
    fn parse_function(
        function: Vec<u8>,
        static_domain: &Box<dyn MemoryDomain>,
    ) -> DandelionResult<(DataRequirementList, Context, FunctionConfig)> {
        let elf = elf_parser::ParsedElf::new(&function)?;
        let system_data = elf.get_symbol_by_name(&function, "__dandelion_system_data")?;
        let return_offset = elf.get_symbol_by_name(&function, "__dandelion_return_address")?;
        let entry = elf.get_entry_point();
        let config = FunctionConfig::ElfConfig(super::ElfConfig {
            system_data_offset: system_data.0,
            return_offset: return_offset,
            entry_point: entry,
        });
        let (static_requirements, source_layout) = elf.get_layout_pair();
        // set default size to 128KiB
        let requirements = DataRequirementList {
            size: DEFAULT_SPACE_SIZE,
            input_requirements: Vec::<DataRequirement>::new(),
            static_requirements: static_requirements,
        };
        // sum up all sizes
        let mut total_size = 0;
        for position in source_layout.iter() {
            total_size += position.size;
        }
        let mut context = static_domain.acquire_context(total_size)?;
        // copy all
        let mut write_counter = 0;
        let mut static_layout = Vec::<Position>::new();
        for position in source_layout.iter() {
            context.write(
                write_counter,
                function[position.offset..position.offset + position.size].to_vec(),
            )?;
            static_layout.push(Position {
                offset: write_counter,
                size: position.size,
            });
            write_counter += position.size;
        }
        context.static_data = static_layout;
        return Ok((requirements, context, config));
    }
}

#[cfg(test)]
mod test;

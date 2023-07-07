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

fn setup_input_structs(
    context: &mut Context,
    system_data_offset: usize,
    out_set_names: Vec<String>,
) -> DandelionResult<()> {
    // prepare information to set up input sets, output sets and input buffers
    let input_buffer_number = context
        .content
        .iter()
        .fold(0, |acc, set| acc + set.buffers.len());
    let input_set_number = context.content.len();
    let output_set_number = out_set_names.len();

    let io_info_size = input_buffer_number * IO_BUFFER_SIZE
        + (input_set_number + output_set_number + 2) * IO_SET_INFO_SIZE;
    let io_info_offset = context.get_free_space(io_info_size, 8)?;
    // fill in data for input sets
    // input set number and pointer (offset)
    let mut system_buffer = Vec::<u8>::new();
    if system_buffer.try_reserve_exact(72).is_err() {
        return Err(DandelionError::OutOfMemory);
    }
    system_buffer.resize(72, 0);
    // heap start and end
    let heap_start = context.get_last_item_end();
    system_buffer[8..16].clone_from_slice(&usize::to_ne_bytes(heap_start));
    // TODO set heap start to be lower that expected stack max
    system_buffer[16..24].clone_from_slice(&usize::to_ne_bytes(context.size - 128));
    // input set number and offset
    system_buffer[24..32].clone_from_slice(&usize::to_ne_bytes(input_set_number));
    system_buffer[32..40].clone_from_slice(&usize::to_ne_bytes(io_info_offset));
    // output sets and offsets
    system_buffer[40..48].clone_from_slice(&usize::to_ne_bytes(output_set_number));
    // +1 for sentinel set
    system_buffer[48..56].clone_from_slice(&usize::to_ne_bytes(
        io_info_offset + (input_set_number + 1) * IO_SET_INFO_SIZE,
    ));

    // input buffers
    // + 2 for sentinel sets
    system_buffer[56..64].clone_from_slice(&usize::to_ne_bytes(
        io_info_offset + (input_set_number + output_set_number + 2) * IO_SET_INFO_SIZE,
    ));
    // write system buffer after exit code
    context.write(system_data_offset, system_buffer)?;

    // input and output io buffer pointers (output just 0 as the application sets them)
    let mut io_data_buffer = Vec::new();
    if io_data_buffer.try_reserve_exact(io_info_size).is_err() {
        return Err(DandelionError::OutOfMemory);
    }
    io_data_buffer.resize(io_info_size, 0);
    // start writing input set info structs
    let mut buffer_count = 0;
    let buffer_base = (input_set_number + output_set_number + 2) * IO_SET_INFO_SIZE;
    for set_index in 0..input_set_number {
        // get name and length
        let name = context.content[set_index].ident.as_bytes().to_vec();
        let name_length = name.len();
        let buffer_num = context.content[set_index].buffers.len();
        // find space and write string
        let mut string_offset = 0;
        if name_length != 0 {
            string_offset = context.get_free_space(name.len(), 8)?;
            context.write(string_offset, name)?;
        }
        let set_data_offset = set_index * IO_SET_INFO_SIZE;
        io_data_buffer[set_data_offset..set_data_offset + 8]
            .clone_from_slice(&usize::to_ne_bytes(string_offset));
        io_data_buffer[set_data_offset + 8..set_data_offset + 16]
            .clone_from_slice(&usize::to_ne_bytes(name_length));
        io_data_buffer[set_data_offset + 16..set_data_offset + 24]
            .clone_from_slice(&usize::to_ne_bytes(buffer_count));
        // find buffers
        for buffer_index in 0..buffer_num {
            let buffer = &context.content[set_index].buffers[buffer_index];
            let name = buffer.ident.as_bytes().to_vec();
            let name_length = name.len();
            let offset = buffer.data.offset;
            let size = buffer.data.size;
            let mut string_offset = 0;
            if name_length != 0 {
                string_offset = context.get_free_space(name_length, 8)?;
                context.write(string_offset, name)?;
            }
            let buffer_offset = buffer_base + (buffer_count + buffer_index) * IO_BUFFER_SIZE;
            io_data_buffer[buffer_offset..buffer_offset + 8]
                .clone_from_slice(&usize::to_ne_bytes(string_offset));
            io_data_buffer[buffer_offset + 8..buffer_offset + 16]
                .clone_from_slice(&usize::to_ne_bytes(name_length));
            io_data_buffer[buffer_offset + 16..buffer_offset + 24]
                .clone_from_slice(&usize::to_ne_bytes(offset));
            io_data_buffer[buffer_offset + 24..buffer_offset + 32]
                .clone_from_slice(&usize::to_ne_bytes(size));
        }
        buffer_count += buffer_num;
    }
    // write input sentinel set
    let sentinel_offset = input_set_number * IO_SET_INFO_SIZE;
    io_data_buffer[sentinel_offset + 16..sentinel_offset + 24]
        .clone_from_slice(&usize::to_ne_bytes(buffer_count));

    // start writing output set info structs
    let output_base = sentinel_offset + IO_SET_INFO_SIZE;
    for (index, out_set_name) in out_set_names.iter().enumerate() {
        // insert the name into the context
        let mut string_offset = 0;
        let string_len = out_set_name.len();
        if string_len != 0 {
            string_offset = context.get_free_space(out_set_name.len(), 8)?;
            context.write(string_offset, out_set_name.as_bytes().to_vec())?;
        }
        let output_offset = output_base + index * IO_SET_INFO_SIZE;
        io_data_buffer[output_offset..output_offset + 8]
            .clone_from_slice(&usize::to_ne_bytes(string_offset));
        io_data_buffer[output_offset + 8..output_offset + 16]
            .clone_from_slice(&usize::to_ne_bytes(string_len));
    }

    context.write(io_info_offset, io_data_buffer)?;

    return Ok(());
}

fn get_output_layout(context: &mut Context, system_data_offset: usize) -> DandelionResult<()> {
    // read the system buffer
    let system_struct = context.read(system_data_offset, SYSTEM_STRUCT_SIZE)?;
    // get exit value
    let _exit_value: i32 = i32::from_ne_bytes(
        system_struct[4..8]
            .try_into()
            .expect("Should be able to convert"),
    );
    let _maybe_exit_value: i32 = i32::from_ne_bytes(system_struct[0..4].try_into().expect(""));

    // get output set number +1 for sentinel set
    // TODO use as_chunks when it stabilizes
    let output_set_number: usize = usize::from_ne_bytes(
        system_struct[40..48]
            .try_into()
            .expect("Should be able to convert"),
    );
    if output_set_number == 0 {
        context.content = vec![];
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
    let mut output_sets = vec![];
    if output_sets.try_reserve(output_set_number).is_err() {
        return Err(DandelionError::OutOfMemory);
    }
    let sentinel_offset = output_set_number * IO_SET_INFO_SIZE;
    let output_buffer_number = usize::from_ne_bytes(
        output_set_info[sentinel_offset + 16..sentinel_offset + 24]
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
        for buffer_index in first_buffer..one_past_last_buffer {
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
        output_sets.push(DataSet {
            ident: set_ident_string,
            buffers: buffers,
        });
    }

    context.content = output_sets;
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
        context.content.clear();
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
        output_set_names: Vec<String>,
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
            stack_pointer: cheri_context.size - 32,
        };
        if let Err(err) = setup_input_structs(
            &mut context,
            elf_config.system_data_offset,
            output_set_names,
        ) {
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

const DEFAULT_SPACE_SIZE: usize = 0x800_0000; // 128MiB

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
        let mut new_content = DataSet {
            ident: String::from("static"),
            buffers: vec![],
        };
        let buffers = &mut new_content.buffers;
        for position in source_layout.iter() {
            context.write(
                write_counter,
                function[position.offset..position.offset + position.size].to_vec(),
            )?;
            buffers.push(DataItem {
                ident: String::from(""),
                data: Position {
                    offset: write_counter,
                    size: position.size,
                },
            });
            write_counter += position.size;
        }
        context.content = vec![new_content];
        return Ok((requirements, context, config));
    }
}

#[cfg(test)]
mod test;

use crate::{
    function_lib::{Driver, ElfConfig, Engine, FunctionConfig, Loader},
    memory_domain::{cheri::cheri_c_context, Context, ContextTrait, ContextType, MemoryDomain},
    util::elf_parser,
    DataItem, DataItemType, DataRequirement, DataRequirementList, HardwareError, HwResult,
    Position,
};
use core::{
    future::{ready, Future},
    pin::Pin,
};
use core_affinity;
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

const IO_STRUCT_SIZE: usize = 16;
const MAX_OUTPUTS: u32 = 16;

fn setup_input_structs(context: &mut Context, config: &ElfConfig) -> HwResult<()> {
    // size of array with input struct array
    let input_number_option = context
        .dynamic_data
        .iter()
        .max_by(|a, b| a.index.cmp(&b.index));
    let input_number = match input_number_option {
        Some(num) => num.index + 1,
        None => return Ok(()),
    };
    let input_struct_size = input_number as usize * IO_STRUCT_SIZE;
    let input_offset = context.get_free_space(input_struct_size, 8)?;
    for input in &context.dynamic_data {
        let pos = match &input.item_type {
            DataItemType::Item(position) => position,
            _ => return Err(HardwareError::NotImplemented),
        };
        let struct_offset = input_offset + IO_STRUCT_SIZE * input.index as usize;
        let mut io_struct = Vec::<u8>::new();
        io_struct.append(&mut usize::to_ne_bytes(pos.size).to_vec());
        io_struct.append(&mut usize::to_ne_bytes(pos.offset).to_vec());
        context.context.write(struct_offset, io_struct)?;
    }
    context.static_data.push(Position {
        offset: input_offset,
        size: input_struct_size,
    });
    // find space for output structs
    let output_struct_size = IO_STRUCT_SIZE * MAX_OUTPUTS as usize;
    let output_offset = context.get_free_space(output_struct_size, 8)?;
    context.static_data.push(Position {
        offset: output_offset,
        size: output_struct_size,
    });
    // fill in values
    context.write(
        config.input_root.0,
        usize::to_ne_bytes(input_offset).to_vec(),
    )?;
    context.write(
        config.input_number.0,
        u32::to_ne_bytes(input_number).to_vec(),
    )?;
    context.write(
        config.output_root.0,
        usize::to_ne_bytes(output_offset).to_vec(),
    )?;
    context.write(config.output_number.0, u32::to_ne_bytes(0).to_vec())?;
    context.write(
        config.max_output_number.0,
        u32::to_ne_bytes(MAX_OUTPUTS).to_vec(),
    )?;
    return Ok(());
}

fn get_output_layout(
    context: &mut Context,
    output_root: (usize, usize),
    output_number: (usize, usize),
) -> HwResult<()> {
    // get output number
    let output_number_vec = context.read(output_number.0, output_number.1, false)?;
    // TODO make this dependent on the actual size of the values
    // TODO use as_chunks when it stabilizes
    let output_number_slice: [u8; 4] = output_number_vec[0..4]
        .try_into()
        .expect("Should have correct length");
    let output_number = u32::min(u32::from_ne_bytes(output_number_slice), MAX_OUTPUTS);
    let mut output_structs = Vec::<DataItem>::new();
    let output_root_vec = context.read(output_root.0, 8, false)?;
    let output_root_offset = usize::from_ne_bytes(
        output_root_vec[0..8]
            .try_into()
            .expect("Should have correct length"),
    );
    for output_index in 0..output_number {
        let read_offset = output_root_offset + IO_STRUCT_SIZE * output_index as usize;
        let out_struct = context.read(read_offset, IO_STRUCT_SIZE as usize, false)?;
        let size = usize::from_ne_bytes(
            out_struct[0..8]
                .try_into()
                .expect("Should have correct length"),
        );
        let offset = usize::from_ne_bytes(
            out_struct[8..16]
                .try_into()
                .expect("Should have correct length"),
        );
        output_structs.push(DataItem {
            index: output_index,
            item_type: DataItemType::Item(Position {
                offset: offset,
                size: size,
            }),
        })
    }
    context.dynamic_data = output_structs;
    Ok(())
}

struct CheriCommand {
    context: *const cheri_c_context,
    entry_point: size_t,
    return_pair_offset: size_t,
    stack_pointer: size_t,
}
unsafe impl Send for CheriCommand {}

pub struct CheriEngine {
    is_running: AtomicBool,
    cpu_slot: u8,
    command_sender: std::sync::mpsc::Sender<CheriCommand>,
    result_receiver: futures::channel::mpsc::Receiver<HwResult<()>>,
    thread_handle: JoinHandle<()>,
}

struct CheriFuture<'a> {
    engine: &'a mut CheriEngine,
    context: Option<Context>,
    output_root: (usize, usize),
    output_number: (usize, usize),
}

// TODO find better way than take unwrap to return context
// or at least a way to ensure that the future is always initialized with a context,
// so this can only happen when poll is called after it has already returned the context once
impl Future for CheriFuture<'_> {
    type Output = (HwResult<()>, Context);
    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut futures::task::Context,
    ) -> futures::task::Poll<Self::Output> {
        match Pin::new(&mut self.engine.result_receiver).poll_next(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(None) => {
                return Poll::Ready((
                    Err(HardwareError::EngineError),
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
        let output_root = self.output_root.clone();
        let output_number = self.output_number.clone();
        let result = get_output_layout(&mut context, output_root, output_number);
        self.engine.is_running.store(false, Ordering::Release);
        Poll::Ready((result, context))
    }
}

fn run_thread(
    core_id: u8,
    command_receiver: std::sync::mpsc::Receiver<CheriCommand>,
    mut result_sender: futures::channel::mpsc::Sender<HwResult<()>>,
) -> () {
    // set core
    if !core_affinity::set_for_current(core_affinity::CoreId { id: core_id.into() }) {
        return;
    };
    'commandloop: for command in command_receiver.iter() {
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
            1 => Err(HardwareError::OutOfMemory),
            _ => Err(HardwareError::NotImplemented),
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
    ) -> Pin<Box<dyn futures::Future<Output = (HwResult<()>, Context)> + '_>> {
        if self.is_running.swap(true, Ordering::AcqRel) {
            return Box::pin(ready((Err(HardwareError::EngineAlreadyRunning), context)));
        }
        let elf_config = match config {
            FunctionConfig::ElfConfig(conf) => conf,
            _ => return Box::pin(ready((Err(HardwareError::ConfigMissmatch), context))),
        };
        let cheri_context = match &context.context {
            ContextType::Cheri(ref cheri_context) => cheri_context,
            _ => return Box::pin(ready((Err(HardwareError::ContextMissmatch), context))),
        };
        let command = CheriCommand {
            context: cheri_context.context,
            entry_point: elf_config.entry_point,
            return_pair_offset: elf_config.return_offset.0,
            stack_pointer: cheri_context.size,
        };
        if let Err(err) = setup_input_structs(&mut context, &elf_config) {
            return Box::pin(ready((Err(err), context)));
        }
        match self.command_sender.send(command) {
            Err(_) => return Box::pin(ready((Err(HardwareError::EngineError), context))),
            Ok(_) => (),
        }
        let function_future = Box::<CheriFuture>::pin(CheriFuture {
            engine: self,
            context: Some(context),
            output_root: elf_config.output_root,
            output_number: elf_config.output_number,
        });
        return function_future;
    }
    fn abort(&mut self) -> HwResult<()> {
        if !self.is_running.load(Ordering::Acquire) {
            return Err(HardwareError::NoRunningFunction);
        }
        Ok(())
    }
}

pub struct CheriDriver {
    cpu_slots: Vec<u8>,
}

impl Driver for CheriDriver {
    // required parts of the trait
    type E = CheriEngine;
    fn new(config: Vec<u8>) -> HwResult<Self> {
        // each entry in config is expected to be a core id for a cpu to use
        // check that each one is available
        let available_cores = match core_affinity::get_core_ids() {
            None => return Err(HardwareError::EngineError),
            Some(cores) => cores,
        };
        for core in config.iter() {
            let found = available_cores
                .iter()
                .find(|x| x.id == core.clone().into())
                .is_some();
            if !found {
                return Err(HardwareError::MalformedConfig);
            }
        }
        return Ok(CheriDriver { cpu_slots: config });
    }
    // // take or release one of the available engines
    fn start_engine(&mut self) -> HwResult<Self::E> {
        let cpu_slot = match self.cpu_slots.pop() {
            Some(core_id) => core_id,
            None => return Err(HardwareError::NoEngineAvailable),
        };
        let (command_sender, command_receiver) = std::sync::mpsc::channel();
        let (result_sender, result_receiver) = futures::channel::mpsc::channel(0);
        let thread_handle = spawn(move || run_thread(cpu_slot, command_receiver, result_sender));
        let is_running = AtomicBool::new(false);
        return Ok(CheriEngine {
            cpu_slot,
            command_sender,
            result_receiver,
            thread_handle,
            is_running,
        });
    }
    fn stop_engine(&mut self, engine: Self::E) -> HwResult<()> {
        drop(engine.command_sender);
        drop(engine.result_receiver);
        // TODO check if expect makes sense
        engine
            .thread_handle
            .join()
            .expect("Expecting cheri thread handle to be joinable");
        self.cpu_slots.push(engine.cpu_slot);
        return Ok(());
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
        static_domain: &mut dyn MemoryDomain,
    ) -> HwResult<(DataRequirementList, Context, FunctionConfig)> {
        let elf = elf_parser::ParsedElf::new(&function)?;
        let input_root = elf.get_symbol_by_name(&function, "inputRoot")?;
        let input_number = elf.get_symbol_by_name(&function, "inputNumber")?;
        let output_root = elf.get_symbol_by_name(&function, "outputRoot")?;
        let output_number = elf.get_symbol_by_name(&function, "outputNumber")?;
        let max_output_number = elf.get_symbol_by_name(&function, "maxOutputNumber")?;
        let return_offset = elf.get_symbol_by_name(&function, "returnPair")?;
        let entry = elf.get_entry_point();
        let config = FunctionConfig::ElfConfig(super::ElfConfig {
            input_root: input_root,
            input_number: input_number,
            output_root: output_root,
            output_number: output_number,
            max_output_number: max_output_number,
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

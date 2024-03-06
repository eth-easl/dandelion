use crate::{
    function_driver::{
        load_utils::load_u8_from_file,
        thread_utils::{DefaultState, ThreadCommand, ThreadController, ThreadPayload},
        ComputeResource, Driver, ElfConfig, Engine, Function, FunctionConfig,
    },
    interface::{read_output_structs, setup_input_structs},
    memory_domain::{cheri::cheri_c_context, Context, ContextTrait, ContextType, MemoryDomain},
    util::elf_parser,
    DataItem, DataRequirement, DataRequirementList, DataSet, Position,
};
use core::{
    future::{ready, Future},
    pin::Pin,
};
use core_affinity;
use dandelion_commons::{
    records::{RecordPoint, Recorder},
    DandelionError, DandelionResult,
};
use futures::task::Poll;
use libc::size_t;
use std::sync::Arc;

#[link(name = "cheri_lib")]
extern "C" {
    fn cheri_run_static(
        context: *const cheri_c_context,
        entry_point: size_t,
        return_pair_offset: size_t,
        stack_pointer: size_t,
    ) -> i8;
}

struct CheriCommand {
    context: *const cheri_c_context,
    entry_point: size_t,
    return_pair_offset: size_t,
    stack_pointer: size_t,
}
unsafe impl Send for CheriCommand {}

impl ThreadPayload for CheriCommand {
    type State = DefaultState;
    fn run(self, _state: &mut Self::State) -> DandelionResult<()> {
        let cheri_error;
        unsafe {
            cheri_error = cheri_run_static(
                self.context,
                self.entry_point,
                self.return_pair_offset,
                self.stack_pointer,
            );
        }
        return match cheri_error {
            0 => Ok(()),
            1 => Err(DandelionError::OutOfMemory),
            _ => Err(DandelionError::NotImplemented),
        };
    }
}

pub struct CheriEngine {
    thread_controller: ThreadController<CheriCommand>,
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
        match Pin::new(&mut self.engine.thread_controller).poll_next(cx) {
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
        // read outputs
        let result = read_output_structs::<u64, u64>(&mut context, self.system_data_offset);
        Poll::Ready((result, context))
    }
}

impl Engine for CheriEngine {
    fn run(
        &mut self,
        config: &FunctionConfig,
        mut context: Context,
        output_set_names: &Vec<String>,
        mut recorder: &mut Recorder,
    ) -> Pin<Box<dyn futures::Future<Output = (DandelionResult<()>, Context)> + '_ + Send>> {
        if let Err(err) = recorder.record(RecordPoint::EngineStart) {
            return Box::pin(core::future::ready((Err(err), context)));
        }
        let elf_config = match config {
            FunctionConfig::ElfConfig(conf) => conf,
            _ => return Box::pin(ready((Err(DandelionError::ConfigMissmatch), context))),
        };
        let cheri_context = match &context.context {
            ContextType::Cheri(ref cheri_context) => cheri_context,
            _ => return Box::pin(ready((Err(DandelionError::ContextMissmatch), context))),
        };
        let command = ThreadCommand::Run(
            recorder,
            CheriCommand {
                context: cheri_context.context,
                entry_point: elf_config.entry_point,
                return_pair_offset: elf_config.return_offset.0,
                stack_pointer: cheri_context.size - 32,
            },
        );
        if let Err(err) = setup_input_structs::<u64, u64>(
            &mut context,
            elf_config.system_data_offset,
            output_set_names,
        ) {
            return Box::pin(ready((Err(err), context)));
        }
        match self.thread_controller.send_command(command) {
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
        // TODO actually abort
        todo!();
    }
}

pub struct CheriDriver {}

impl Driver for CheriDriver {
    // // take or release one of the available engines
    fn start_engine(&self, resource: ComputeResource) -> DandelionResult<Box<dyn Engine>> {
        let cpu_slot = match resource {
            ComputeResource::CPU(core) => core,
            _ => return Err(DandelionError::EngineResourceError),
        };
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
            return Err(DandelionError::EngineResourceError);
        }
        return Ok(Box::new(CheriEngine {
            thread_controller: ThreadController::new(cpu_slot),
        }));
    }

    // parses an executable,
    // returns the layout requirements and a context containing static data,
    //  and a layout description for it
    fn parse_function(
        &self,
        function_path: String,
        static_domain: &Box<dyn MemoryDomain>,
    ) -> DandelionResult<Function> {
        let function = load_u8_from_file(function_path)?;
        let elf = elf_parser::ParsedElf::new(&function)?;
        let system_data = elf.get_symbol_by_name(&function, "__dandelion_system_data")?;
        let return_offset = elf.get_symbol_by_name(&function, "__dandelion_return_address")?;
        let entry = elf.get_entry_point();
        let config = FunctionConfig::ElfConfig(ElfConfig {
            system_data_offset: system_data.0,
            return_offset: return_offset,
            entry_point: entry,
            protection_flags: Arc::new(elf.get_memory_protection_layout()),
        });
        let (static_requirements, source_layout) = elf.get_layout_pair();
        let requirements = DataRequirementList {
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
                &function[position.offset..position.offset + position.size],
            )?;
            buffers.push(DataItem {
                ident: String::from(""),
                data: Position {
                    offset: write_counter,
                    size: position.size,
                },
                key: 0,
            });
            write_counter += position.size;
        }
        context.content = vec![Some(new_content)];
        return Ok(Function {
            requirements,
            context,
            config,
        });
    }
}

#[cfg(test)]
mod test;

use crate::{
    function_driver::{
        load_utils::load_u8_from_file,
        thread_utils::{start_thread, EngineLoop},
        ComputeResource, Driver, ElfConfig, Function, FunctionConfig, WorkQueue,
    },
    interface::{read_output_structs, setup_input_structs},
    memory_domain::{cheri::cheri_c_context, Context, ContextTrait, ContextType, MemoryDomain},
    util::elf_parser,
    DataItem, DataRequirement, DataRequirementList, DataSet, Position,
};
use core_affinity;
use dandelion_commons::{DandelionError, DandelionResult};
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

struct CheriLoop {}

impl EngineLoop for CheriLoop {
    fn init(_core_id: u8) -> DandelionResult<Box<Self>> {
        return Ok(Box::new(CheriLoop {}));
    }
    fn run(
        &mut self,
        config: FunctionConfig,
        mut context: Context,
        output_sets: std::sync::Arc<Vec<String>>,
    ) -> DandelionResult<Context> {
        let elf_config = match config {
            FunctionConfig::ElfConfig(conf) => conf,
            _ => return Err(DandelionError::ConfigMissmatch),
        };
        setup_input_structs::<u64, u64>(&mut context, elf_config.system_data_offset, &output_sets)?;
        let cheri_context = match &context.context {
            ContextType::Cheri(cheri_context) => cheri_context,
            _ => return Err(DandelionError::ContextMissmatch),
        };
        let cheri_error = unsafe {
            cheri_run_static(
                cheri_context.context,
                elf_config.entry_point,
                elf_config.return_offset.0,
                cheri_context.size - 32,
            )
        };
        match cheri_error {
            0 => Ok(()),
            1 => Err(DandelionError::OutOfMemory),
            _ => Err(DandelionError::NotImplemented),
        }?;
        read_output_structs::<u64, u64>(&mut context, elf_config.system_data_offset)?;
        return Ok(context);
    }
}

pub struct CheriDriver {}

impl Driver for CheriDriver {
    // // take or release one of the available engines
    fn start_engine(
        &self,
        resource: ComputeResource,
        queue: Box<dyn WorkQueue + Send>,
    ) -> DandelionResult<()> {
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
        start_thread::<CheriLoop>(cpu_slot, queue);
        return Ok(());
    }

    // parses an executable,
    // returns the layout requirements and a context containing static data,
    //  and a layout description for it
    fn parse_function(
        &self,
        function_path: String,
        static_domain: &'static dyn MemoryDomain,
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
            context: Arc::new(context),
            config,
        });
    }
}

#[cfg(test)]
mod test;

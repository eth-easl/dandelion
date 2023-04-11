use libc::size_t;

use super::FunctionConfig;
use crate::function_lib::{DataItem, DataRequirementList, Driver, Engine, Navigator};
use crate::memory_domain::cheri::cheri_c_context;
use crate::memory_domain::{Context, ContextTrait, ContextType, MemoryDomain};
use crate::util::elf_parser;
use crate::{DataRequirement, HardwareError, HwResult, Position};

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

// fn setup_input_structs(context: &mut Context, max_output_number: u32) -> () {
//     // size of array with input struct array
//     let input_array_size = input_layout.len() * IO_STRUCT_SIZE;
// }

struct CheriEngine {
    function_context: Option<Context>,
}

impl Engine for CheriEngine {
    fn run(self, config: FunctionConfig, mut context: Context) -> (HwResult<()>, Context) {
        // let output_layout = Vec::<DataItem>::new();
        // setup input structs
        // setup_input_structs(&mut context, _);
        // TODO maybe reverse order to fail faster?
        let cheri_context = match context.context {
            ContextType::Cheri(ref cheri_context) => cheri_context,
            _ => return (Err(HardwareError::ContextMissmatch), context),
        };
        let elf_config = match config {
            FunctionConfig::ElfConfig(conf) => conf,
            _ => return (Err(HardwareError::ConfigMissmatch), context),
        };
        let cheri_error;
        unsafe {
            cheri_error = cheri_run_static(
                cheri_context.context,
                elf_config.entry_point,
                elf_config.return_offset.0,
                cheri_context.size,
            );
        }
        // TODO handle cheri error
        (Ok(()), context)
    }
    fn abort(self) -> HwResult<Context> {
        match self.function_context {
            Some(context) => Ok(context),
            None => Err(HardwareError::NoRunningFunction),
        }
    }
}

struct CheriDriver {}

impl Driver for CheriDriver {
    // required parts of the trait
    type E = CheriEngine;
    fn new(config: Vec<u8>) -> HwResult<Self> {
        return Ok(CheriDriver {});
    }
    // // take or release one of the available engines
    fn start_engine(self, id: u8) -> HwResult<Self::E> {
        return Ok(CheriEngine {
            function_context: None,
        });
    }
    fn stop_engine(self, engine: Self::E) -> HwResult<()> {
        return Ok(());
    }
}

const DEFAULT_SPACE_SIZE: usize = 0x40_0000; // 4MiB

struct CheriNavigator {}
impl Navigator for CheriNavigator {
    // parses an executable,
    // returns the layout requirements and a context containing static data,
    //  and a layout description for it
    fn parse_function(
        function: Vec<u8>,
        static_domain: &dyn MemoryDomain,
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

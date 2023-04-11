use libc::size_t;

use super::FunctionConfig;
use crate::function_lib::{Driver, ElfConfig, Engine, Navigator};
use crate::memory_domain::cheri::cheri_c_context;
use crate::memory_domain::{Context, ContextTrait, ContextType, MemoryDomain};
use crate::util::elf_parser;
use crate::{
    DataItem, DataItemType, DataRequirement, DataRequirementList, HardwareError, HwResult, Position,
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

fn get_output_layout(context: &mut Context, config: &ElfConfig) -> HwResult<()> {
    // get output number
    let output_number_vec = context.read(config.output_number.0, config.output_number.1, false)?;
    // TODO make this dependent on the actual size of the values
    // TODO use as_chunks when it stabilizes
    let output_number_slice: [u8; 4] = output_number_vec[0..4]
        .try_into()
        .expect("Should have correct length");
    let output_number = u32::min(u32::from_ne_bytes(output_number_slice), MAX_OUTPUTS);
    let mut output_structs = Vec::<DataItem>::new();
    let output_root_vec = context.read(config.output_root.0, 8, false)?;
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

struct CheriEngine {
    function_context: Option<Context>,
}

impl Engine for CheriEngine {
    fn run(self, config: FunctionConfig, mut context: Context) -> (HwResult<()>, Context) {
        let elf_config = match config {
            FunctionConfig::ElfConfig(conf) => conf,
            _ => return (Err(HardwareError::ConfigMissmatch), context),
        };
        if let Err(err) = setup_input_structs(&mut context, &elf_config) {
            return (Err(err), context);
        }
        // TODO maybe reverse order to fail faster?
        let cheri_context = match context.context {
            ContextType::Cheri(ref cheri_context) => cheri_context,
            _ => return (Err(HardwareError::ContextMissmatch), context),
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
        match cheri_error {
            0 => (),
            1 => return (Err(HardwareError::OutOfMemory), context),
            _ => return (Err(HardwareError::NotImplemented), context),
        }
        // TODO handle cheri error
        // erase all assumptions on context internal layout
        context.dynamic_data.clear();
        context.static_data.clear();
        // read outputs
        let result = get_output_layout(&mut context, &elf_config);
        (result, context)
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

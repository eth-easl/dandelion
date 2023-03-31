use super::FunctionConfig;
use crate::function_lib::{DataItem, DataRequirementList, Navigator};
use crate::memory_domain::{Context, ContextTrait, MemoryDomain};
use crate::util::elf_parser;
use crate::HwResult;
pub trait Engine {
    fn run(
        self,
        config: FunctionConfig,
        context: Context,
        layout: Vec<DataItem>,
        callback: impl FnOnce(HwResult<(Context, Vec<DataItem>)>) -> (),
    ) -> HwResult<()>;
    fn abort(id: u32, callback: impl FnOnce(HwResult<Context>) -> ()) -> HwResult<()>;
}

// todo find better name
pub trait Driver {
    // required parts of the trait
    // type E: Engine;
    // // take or release one of the available engines
    // fn start_engine() -> HwResult<Self::E>;
    // fn stop_engine(self) -> HwResult<()>;
}

struct CheriNavigator {}
impl Navigator for CheriNavigator {
    // parses an executable,
    // returns the layout requirements and a context containing static data,
    //  and a layout description for it
    fn parse_function(
        function: Vec<u8>,
        static_domain: &dyn MemoryDomain,
    ) -> HwResult<(DataRequirementList, Context, Vec<DataItem>, FunctionConfig)> {
        let elf = elf_parser::ParsedElf::new(&function)?;
        let input_root = elf.get_symbol_by_name(&function, "inputRoot")?;
        let input_number = elf.get_symbol_by_name(&function, "inputNumber")?;
        let output_root = elf.get_symbol_by_name(&function, "outputRoot")?;
        let output_number = elf.get_symbol_by_name(&function, "outputNumber")?;
        let max_output_number = elf.get_symbol_by_name(&function, "maxOutputNumber")?;
        let entry = elf.get_entry_point();
        let config = FunctionConfig::ElfConfig(super::ElfConfig {
            input_root: input_root,
            input_number: input_number,
            output_root: output_root,
            output_number: output_number,
            max_output_number: max_output_number,
            entry_point: entry,
        });
        let (requirements, source_layout) = elf.get_layout_pair();
        // sum up all sizes
        let mut total_size = 0;
        for position in source_layout.iter() {
            total_size += position.size;
        }
        let mut context = static_domain.acquire_context(total_size)?;
        // copy all
        let mut write_counter = 0;
        let mut static_layout = Vec::<DataItem>::new();
        for (index, position) in source_layout.iter().enumerate() {
            context.write(
                write_counter,
                function[position.offset..position.offset + position.size].to_vec(),
            )?;
            static_layout.push(DataItem {
                index: index as u32,
                item_type: crate::DataItemType::Item(crate::Position {
                    offset: write_counter,
                    size: position.size,
                }),
            });
            write_counter += position.size;
        }
        return Ok((requirements, context, static_layout, config));
    }
}

#[cfg(test)]
mod test;

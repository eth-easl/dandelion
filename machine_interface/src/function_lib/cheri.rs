use super::super::function_lib::{DataItem, DataRequirementList, Navigator};
use crate::memory_domain::{Context, ContextTrait, MemoryDomain};
use crate::util::elf_parser;
use crate::HwResult;
pub trait Engine {
    fn run(
        self,
        // code: Self::FunctionConfig,
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
        config: Vec<u8>,
        static_domain: &dyn MemoryDomain,
    ) -> HwResult<(DataRequirementList, Context, Vec<DataItem>)> {
        let file = elf_parser::ParsedElf::new(&config)?;
        let (requirements, source_layout) = file.get_layout_pair();
        // sum up all sizes
        let mut total_size = 0;
        for position in source_layout.iter() {
            total_size += position.size;
        }
        let mut context = static_domain.acquire_context(total_size)?;
        // copy all
        let mut write_counter = 0;
        for position in source_layout.iter() {
            context.write(
                write_counter,
                config[position.offset..position.offset + position.size].to_vec(),
            )?;
            write_counter += position.size;
        }
        let static_layout = Vec::<DataItem>::new();
        return Ok((requirements, context, static_layout));
    }
}

#[cfg(test)]
mod test;

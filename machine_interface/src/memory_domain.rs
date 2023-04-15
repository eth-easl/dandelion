// list of memory domain implementations
#[cfg(feature = "cheri")]
pub mod cheri;
pub mod malloc;
pub mod pagetable;

use crate::{DataItem, DataItemType, HardwareError, Position};

// import parent for depenencies
use super::HwResult;

pub trait ContextTrait {
    fn write(&mut self, offset: usize, data: Vec<u8>) -> HwResult<()>;
    fn read(&mut self, offset: usize, read_size: usize, sanitize: bool) -> HwResult<Vec<u8>>;
}

// https://docs.rs/enum_dispatch/latest/enum_dispatch/index.html
// check if this would be better way to do it
pub enum ContextType {
    Malloc(Box<malloc::MallocContext>),
    #[cfg(feature = "cheri")]
    Cheri(Box<cheri::CheriContext>),
    Pagetable(Box<pagetable::PagetableContext>),
}

impl ContextTrait for ContextType {
    fn write(&mut self, offset: usize, data: Vec<u8>) -> HwResult<()> {
        match self {
            ContextType::Malloc(context) => context.write(offset, data),
            #[cfg(feature = "cheri")]
            ContextType::Cheri(context) => context.write(offset, data),
            ContextType::Pagetable(context) => context.write(offset, data),
        }
    }
    fn read(&mut self, offset: usize, read_size: usize, sanitize: bool) -> HwResult<Vec<u8>> {
        match self {
            ContextType::Malloc(context) => context.read(offset, read_size, sanitize),
            #[cfg(feature = "cheri")]
            ContextType::Cheri(context) => context.read(offset, read_size, sanitize),
            ContextType::Pagetable(context) => context.read(offset, read_size, sanitize),
        }
    }
}
pub struct Context {
    pub context: ContextType,
    pub dynamic_data: Vec<DataItem>,
    pub static_data: Vec<Position>,
}

impl ContextTrait for Context {
    fn write(&mut self, offset: usize, data: Vec<u8>) -> HwResult<()> {
        self.context.write(offset, data)
    }
    fn read(&mut self, offset: usize, read_size: usize, sanitize: bool) -> HwResult<Vec<u8>> {
        self.context.read(offset, read_size, sanitize)
    }
}

impl Context {
    pub fn get_free_space(&self, size: usize, alignment: usize) -> HwResult<usize> {
        let mut items = Vec::<Position>::new();
        for pos in &self.static_data {
            items.push(pos.clone());
        }
        // make single vector with all positions
        for dyn_item in &self.dynamic_data {
            match &dyn_item.item_type {
                DataItemType::Item(item) => items.push(item.clone()),
                DataItemType::Set(set) => {
                    for pos in set {
                        items.push(pos.clone())
                    }
                }
            }
        }
        items.sort_unstable_by(|a, b| a.offset.cmp(&b.offset));
        // search for smallest space that is bigger than size
        // space start holds previous start
        let mut space_start = Err(HardwareError::ContextFull);
        let mut space_size = usize::MAX;
        let mut last_end = 0;
        if items.len() == 0 {
            return Ok(0);
        };
        for item in items {
            let item_start = item.offset;
            let free_space = item_start - last_end;

            if free_space >= size && free_space < space_size {
                space_size = free_space;
                space_start = Ok(last_end);
            }
            last_end = item_start + item.size;
            // TODO use next_multiple_of as soon as it is stabilized
            if last_end % alignment != 0 {
                last_end += alignment - last_end % alignment;
            }
        }
        return space_start;
    }
}

pub trait MemoryDomain {
    // allocation and distruction
    fn init(config: Vec<u8>) -> HwResult<Self>
    where
        Self: Sized;
    fn acquire_context(&mut self, size: usize) -> HwResult<Context>;
    fn release_context(&mut self, context: Context) -> HwResult<()>;
}

// Code to specialize transfers between different domains
pub fn transefer_memory(
    destination: &mut Context,
    source: &mut Context,
    destination_offset: usize,
    source_offset: usize,
    size: usize,
    sanitize: bool,
) -> HwResult<()> {
    let result = match (&mut destination.context, &mut source.context) {
        (ContextType::Malloc(destination_ctxt), ContextType::Malloc(source_ctxt)) => {
            malloc::malloc_transfer(
                destination_ctxt,
                source_ctxt,
                destination_offset,
                source_offset,
                size,
                sanitize,
            )
        }
        // TODO add cheri transfer
        // default implementation using reads and writes
        (destination, source) => {
            let read_result = source.read(source_offset, size, sanitize);
            match read_result {
                Ok(read_value) => destination.write(destination_offset, read_value),
                Err(err) => Err(err),
            }
        }
    };
    result
}

#[cfg(test)]
mod tests;

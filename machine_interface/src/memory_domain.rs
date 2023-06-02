// list of memory domain implementations
#[cfg(feature = "cheri")]
pub mod cheri;
pub mod malloc;

use crate::{DataItem, DataSet, Position};
use dandelion_commons::{DandelionError, DandelionResult};
use std::collections::HashMap;

pub trait ContextTrait: Send + Sync {
    fn write(&mut self, offset: usize, data: Vec<u8>) -> DandelionResult<()>;
    fn read(&self, offset: usize, read_size: usize) -> DandelionResult<Vec<u8>>;
}

// https://docs.rs/enum_dispatch/latest/enum_dispatch/index.html
// check if this would be better way to do it
pub enum ContextType {
    Malloc(Box<malloc::MallocContext>),
    #[cfg(feature = "cheri")]
    Cheri(Box<cheri::CheriContext>),
}

impl ContextTrait for ContextType {
    fn write(&mut self, offset: usize, data: Vec<u8>) -> DandelionResult<()> {
        match self {
            ContextType::Malloc(context) => context.write(offset, data),
            #[cfg(feature = "cheri")]
            ContextType::Cheri(context) => context.write(offset, data),
        }
    }
    fn read(&self, offset: usize, read_size: usize) -> DandelionResult<Vec<u8>> {
        match self {
            ContextType::Malloc(context) => context.read(offset, read_size),
            #[cfg(feature = "cheri")]
            ContextType::Cheri(context) => context.read(offset, read_size),
        }
    }
}
pub struct Context {
    pub context: ContextType,
    pub dynamic_data: HashMap<usize, DataSet>,
    pub static_data: Vec<Position>,
    pub size: usize,
}

impl ContextTrait for Context {
    fn write(&mut self, offset: usize, data: Vec<u8>) -> DandelionResult<()> {
        self.context.write(offset, data)
    }
    fn read(&self, offset: usize, read_size: usize) -> DandelionResult<Vec<u8>> {
        self.context.read(offset, read_size)
    }
}

impl Context {
    pub fn get_free_space(&self, size: usize, alignment: usize) -> DandelionResult<usize> {
        let static_items = self.static_data.iter();
        let dynamic_items = self
            .dynamic_data
            .values()
            .flat_map(|set| set.buffers.iter().map(|item| &item.data));
        let items = static_items.chain(dynamic_items);
        let mut item_slice: Vec<&Position> = items.collect();
        item_slice.sort_unstable_by(|a, b| a.offset.cmp(&b.offset));
        // search for smallest space that is bigger than size
        // space start holds previous start
        let mut space_start = Err(DandelionError::ContextFull);
        let mut space_size = usize::MAX;
        let mut next_free = 0;
        if item_slice.len() == 0 {
            return Ok(0);
        };
        for item in item_slice {
            let item_start = item.offset;
            let free_space = item_start - next_free;

            if free_space >= size && free_space < space_size {
                space_size = free_space;
                space_start = Ok(next_free);
            }
            next_free = item_start + item.size;
            // TODO use next_multiple_of as soon as it is stabilized
            if next_free % alignment != 0 {
                next_free += alignment - next_free % alignment;
            }
        }
        // check after last item
        if self.size - next_free >= size && self.size - next_free < space_size {
            space_start = Ok(next_free);
        }
        return space_start;
    }
    pub fn get_last_item_end(&self) -> usize {
        let static_items = self.static_data.iter();
        let dynamic_items = self
            .dynamic_data
            .values()
            .flat_map(|set| set.buffers.iter().map(|item| &item.data));
        let items = static_items.chain(dynamic_items);
        return items
            .max_by(|a, b| (a.offset + a.size).cmp(&(b.offset + b.size)))
            .and_then(|a| Some(a.offset + a.size))
            .unwrap_or(0usize);
    }
}

pub trait MemoryDomain: Sync + Send {
    // allocation and distruction
    fn init(config: Vec<u8>) -> DandelionResult<Box<dyn MemoryDomain>>
    where
        Self: Sized;
    fn acquire_context(&self, size: usize) -> DandelionResult<Context>;
    fn release_context(&self, context: Context) -> DandelionResult<()>;
}

// Code to specialize transfers between different domains
pub fn transefer_memory(
    destination: &mut Context,
    source: &Context,
    destination_offset: usize,
    source_offset: usize,
    size: usize,
) -> DandelionResult<()> {
    let result = match (&mut destination.context, &source.context) {
        (ContextType::Malloc(destination_ctxt), ContextType::Malloc(source_ctxt)) => {
            malloc::malloc_transfer(
                destination_ctxt,
                source_ctxt,
                destination_offset,
                source_offset,
                size,
            )
        }
        #[cfg(feature = "cheri")]
        (ContextType::Cheri(destination_ctxt), ContextType::Cheri(source_ctxt)) => {
            cheri::cheri_transfer(
                destination_ctxt,
                source_ctxt,
                destination_offset,
                source_offset,
                size,
            )
        }
        // default implementation using reads and writes
        (destination, source) => {
            let read_result = source.read(source_offset, size);
            match read_result {
                Ok(read_value) => destination.write(destination_offset, read_value),
                Err(err) => Err(err),
            }
        }
    };
    result
}

pub fn transer_data_item(
    destination: &mut Context,
    source: &Context,
    destionation_set_index: usize,
    destination_allignment: usize,
    source_set_index: usize,
    source_item_index: Option<usize>,
) -> DandelionResult<()> {
    // check if source has item
    let source_set = match source.dynamic_data.get(&source_set_index) {
        Some(set) => set,
        None => return Err(DandelionError::InvalidRead),
    };
    if source_set.buffers.is_empty() {
        if source_item_index.is_none() {
            return Ok(());
        } else {
            return Err(DandelionError::EmptyDataSet);
        }
    }
    // get positions where to read from
    let source_positions: Vec<Position> = if let Some(item_index) = source_item_index {
        if source_set.buffers.len() <= item_index {
            return Err(DandelionError::InvalidRead);
        } else {
            vec![source_set.buffers[item_index].data]
        }
    } else {
        source_set.buffers.iter().map(|item| item.data).collect()
    };
    // find positions to write to and insert them into meta data
    let mut new_positions = Vec::new();
    let destination_positions: Vec<Position> = source_positions
        .iter()
        .map(|pos| {
            let offset = destination.get_free_space(pos.size, destination_allignment)?;
            let new_position = Position {
                size: pos.size,
                offset,
            };
            new_positions.push(DataItem {
                ident: String::from(""),
                data: new_position,
            });
            return Ok(new_position);
        })
        .collect::<DandelionResult<Vec<_>>>()?;
    // check if destination already has set that will be appended to or create new set
    let entry = destination
        .dynamic_data
        .entry(destionation_set_index)
        .or_insert(DataSet {
            ident: String::from(""),
            buffers: Vec::new(),
        });
    entry.buffers.append(&mut new_positions);
    // perform transfers
    let position_pair = source_positions
        .iter()
        .zip(destination_positions.into_iter());
    for (source_pos, destination_pos) in position_pair {
        transefer_memory(
            destination,
            source,
            destination_pos.offset,
            source_pos.offset,
            source_pos.size,
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;

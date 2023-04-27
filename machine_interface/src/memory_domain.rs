// list of memory domain implementations
#[cfg(feature = "cheri")]
pub mod cheri;
pub mod malloc;

use crate::{DataItem, Position};
use dandelion_commons::{DandelionError, DandelionResult};
use std::collections::HashMap;

pub trait ContextTrait {
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
    pub dynamic_data: HashMap<usize, DataItem>,
    pub static_data: Vec<Position>,
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
        let mut items = Vec::<Position>::new();
        for pos in &self.static_data {
            items.push(pos.clone());
        }
        // make single vector with all positions
        for dyn_item in self.dynamic_data.values() {
            match dyn_item {
                DataItem::Item(item) => items.push(item.clone()),
                DataItem::Set(set) => {
                    for pos in set {
                        items.push(pos.clone())
                    }
                }
            }
        }
        items.sort_unstable_by(|a, b| a.offset.cmp(&b.offset));
        // search for smallest space that is bigger than size
        // space start holds previous start
        let mut space_start = Err(DandelionError::ContextFull);
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
    fn init(config: Vec<u8>) -> DandelionResult<Box<dyn MemoryDomain>>
    where
        Self: Sized;
    fn acquire_context(&mut self, size: usize) -> DandelionResult<Context>;
    fn release_context(&mut self, context: Context) -> DandelionResult<()>;
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
    destionation_index: usize,
    destination_allignment: usize,
    source_index: usize,
    source_sub_index: Option<usize>,
) -> DandelionResult<()> {
    // check if source has item
    let source_item = match source.dynamic_data.get(&source_index) {
        Some(i) => i,
        None => return Err(DandelionError::InvalidRead),
    };
    // check destination is unoccupied, could be fused with insert in future with try insert
    if destination.dynamic_data.contains_key(&destionation_index) {
        return Err(DandelionError::InvalidWrite);
    }
    // get positions where to read from
    let temp;
    let source_positions = match (source_item, source_sub_index) {
        (DataItem::Item(p), None) => {
            temp = vec![p.clone()];
            &temp
        }
        (DataItem::Item(_), Some(_)) => return Err(DandelionError::InvalidRead),
        (DataItem::Set(s), None) => s,
        (DataItem::Set(s), Some(i)) if i < s.len() => {
            temp = vec![s[i].clone()];
            &temp
        }
        (DataItem::Set(_), Some(_)) => return Err(DandelionError::InvalidRead),
    };
    // find positions to write to
    let destination_positions_result: DandelionResult<Vec<Position>> = source_positions
        .iter()
        .map(|pos| {
            let offset = destination.get_free_space(pos.size, destination_allignment)?;
            Ok(Position {
                size: pos.size,
                offset,
            })
        })
        .collect();
    let destination_positions = destination_positions_result?;
    if destination_positions.len() == 0 {
        return Err(DandelionError::EmptyDataItemSet);
    } else if destination_positions.len() == 1 {
        let transfer_error = transefer_memory(
            destination,
            source,
            destination_positions[0].offset,
            source_positions[0].offset,
            source_positions[0].size,
        );
        destination
            .dynamic_data
            .insert(destionation_index, DataItem::Item(destination_positions[0]));
        return transfer_error;
    } else {
        // perform transfers
        let position_pair = source_positions
            .iter()
            .zip(destination_positions.into_iter());
        let mut destination_set = Vec::new();
        if destination_set.try_reserve(source_positions.len()).is_err() {
            return Err(DandelionError::OutOfMemory);
        }
        for (source_pos, destination_pos) in position_pair {
            let transfer_error = transefer_memory(
                destination,
                source,
                destination_pos.offset,
                source_pos.offset,
                source_pos.size,
            );
            destination_set.push(destination_pos);
            if transfer_error.is_err() {
                break;
            }
        }
        destination
            .dynamic_data
            .insert(destionation_index, DataItem::Set(destination_set));
    }
    Ok(())
}

#[cfg(test)]
mod tests;

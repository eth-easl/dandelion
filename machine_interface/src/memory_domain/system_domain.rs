use crate::{
    memory_domain::{Context, ContextTrait, ContextType, MemoryDomain, MemoryResource},
    Position,
};
use dandelion_commons::{err_dandelion, DandelionError, DandelionResult};
use log::{debug, warn};
use std::{
    cmp::min,
    collections::BTreeMap,
    ops::Bound::{Included, Unbounded},
    sync::Arc,
};

use super::transfer_memory;

/// Type to hold data in a system context.
#[derive(Debug)]
pub(crate) struct DataPosition {
    // The context holding the data
    context: Arc<Context>,
    // The position of the data in the context
    position: Position,
}

#[derive(Debug)]
/// This context does not have any real data in it. It only holds pointers to other contexts, where
/// the data is. Everything else (including the metadata, where every item is) is as in any other context.
/// Thus, offsets stored in Position for DataItems are only "virtual" and don't have actual meaning
/// The size field of a Context does not matter internally for a SystemContext, since it does not hold any data,
/// we only check for accesses that have larger offset than the context size with it
/// The struct does not enforce "feasability" of memory. An item of size 10 can be stored at offset 2,
/// and another item of size 10 can be stored at offset 3 without issue. In such cases, read cannot guarantee desired output
/// This should be managed within type Context, Context.content respectively
pub struct SystemContext {
    /// Links virtual offset to corresponding data position and size of item
    /// Position is currently either in Arc<Context> with corresponding offset or in Bytes
    pub(crate) local_offset_to_data_position: BTreeMap<usize, DataPosition>,
    pub(crate) size: usize,
}

impl ContextTrait for SystemContext {
    fn write<T>(&mut self, offset: usize, _data: &[T]) -> DandelionResult<()> {
        panic!("Tried to write to a SystemContext with offset: {}!", offset);
    }

    /// We assume:
    ///     offset + size of read_buffer <= context size. Will fail otherwise
    ///     The read can span over multiple items
    ///     Offset does not have to be the start of an item
    ///     We don't have to read till the end of an item
    /// Currently, it does not fail while reading data that has never been written to.
    /// If we hit an offset, where we have no data stored, we leave remaining part of buffer empty and return.
    /// Thus, further data, which is separated from other data by uninitialised memory regions, is ignored.
    /// This could be changed but would be difficult to use in practice anyway
    fn read<T>(&self, offset: usize, read_buffer: &mut [T]) -> DandelionResult<()> {
        let mut total_bytes_read = 0;
        let read_buffer_size = core::mem::size_of::<T>() * read_buffer.len();
        let byte_buffer = unsafe {
            core::slice::from_raw_parts_mut(read_buffer.as_ptr() as *mut u8, read_buffer_size)
        };
        if offset + read_buffer_size > self.size {
            debug!("Offset + read_buffer_size are larger than context size (read). Offset: {}, buffer_size: {}, context size: {}",
            offset, read_buffer_size, self.size);
            return err_dandelion!(DandelionError::InvalidRead);
        }

        while total_bytes_read < read_buffer_size {
            let mut range = self
                .local_offset_to_data_position
                .range((Unbounded, Included(offset + total_bytes_read)));
            // get the item that starts closest before the current read start
            if let Some((&item_offset, DataPosition { context, position })) = range.next_back() {
                // know that the current item start is <= offset + total_bytes_read, need to check that it also does not end before the read offset
                if (offset + total_bytes_read) < item_offset + position.size {
                    let offset_in_item = (offset + total_bytes_read) - item_offset;
                    let new_chunk = context.get_chunk_ref(
                        position.offset + offset_in_item,
                        read_buffer_size - total_bytes_read,
                    )?;
                    let new_bytes = new_chunk.len();
                    byte_buffer[total_bytes_read..total_bytes_read + new_bytes]
                        .copy_from_slice(new_chunk);
                    total_bytes_read += new_bytes;
                } else {
                    warn!(
                        "Read offset has no stored data in SystemContext (read). Offset: {}",
                        offset
                    );
                    break;
                }
            } else {
                warn!(
                    "Read offset has no stored data in SystemContext (read). Offset: {}",
                    offset
                );
                break;
            }
        }
        return Ok(());
    }

    fn get_chunk_ref(&self, offset: usize, length: usize) -> DandelionResult<&[u8]> {
        if offset + length > self.size {
            debug!("Offset + length are larger than context size (get_chunk_ref). Offset: {}, length: {}, context size: {}",
            offset, length, self.size);
            return err_dandelion!(DandelionError::InvalidRead);
        }
        let mut range = self
            .local_offset_to_data_position
            .range((Unbounded, Included(offset)));
        if let Some((&item_offset, DataPosition { context, position })) = range.next_back() {
            if offset < item_offset + position.size {
                let offset_in_item = offset - item_offset;
                let max_length = min(length, position.size);
                return context.get_chunk_ref(position.offset + offset_in_item, max_length);
            }
        }
        warn!(
            "Read offset not stored in SystemContext (get_chunk_ref). Offset: {}",
            offset
        );
        return err_dandelion!(DandelionError::InvalidRead);
    }
}

#[derive(Debug)]
pub struct SystemMemoryDomain {}

impl MemoryDomain for SystemMemoryDomain {
    fn init(_config: MemoryResource) -> DandelionResult<Box<dyn MemoryDomain>> {
        Ok(Box::new(SystemMemoryDomain {}))
    }

    fn acquire_context(&self, size: usize) -> DandelionResult<Context> {
        let new_context = Box::new(SystemContext {
            local_offset_to_data_position: BTreeMap::new(),
            size,
        });
        Ok(Context::new(ContextType::System(new_context), size))
    }
}

/// We assume:
///      Only whole items are transfered. Meaning source_offset is the beginning of an item
pub fn system_context_transfer(
    destination: &mut SystemContext,
    source: &SystemContext,
    destination_offset: usize,
    source_offset: usize,
    size: usize,
) -> DandelionResult<()> {
    let mut transfered_bytes = 0;
    while transfered_bytes < size {
        let Some(DataPosition { context, position }) = source
            .local_offset_to_data_position
            .get(&(source_offset + transfered_bytes))
        else {
            warn!(
                "Read offset not stored in SystemContext (system_context_transfer). Offset: {}",
                source_offset + transfered_bytes
            );
            return err_dandelion!(DandelionError::InvalidRead);
        };

        if transfered_bytes + position.size > size {
            warn!(
                "Memory transfer would not involve whole item! (system_context_transfer). 
                destination_offset: {}, transfered_bytes: {}, total_size: {}, item_size: {}",
                destination_offset, transfered_bytes, size, position.size
            );
            return err_dandelion!(DandelionError::InvalidRead);
        }

        destination.local_offset_to_data_position.insert(
            destination_offset + transfered_bytes,
            DataPosition {
                context: context.clone(),
                position: *position,
            },
        );

        transfered_bytes += position.size;
    }
    Ok(())
}

/// We assume:
///     Only whole items are transfered. Meaning source_offset is the beginning of an item
///     Source is not a systems_context. For this, use system_context_transfer.
///     (Using this function could cause inconcistencies with fragmented data)
pub fn into_system_context_transfer(
    destination: &mut SystemContext,
    source: &Arc<Context>,
    destination_offset: usize,
    source_offset: usize,
    size: usize,
) -> DandelionResult<()> {
    if source_offset + size > source.size {
        debug!("Source_offset+size > source.size (into_system_context_transfer)");
        return err_dandelion!(DandelionError::InvalidRead);
    }
    destination.local_offset_to_data_position.insert(
        destination_offset,
        DataPosition {
            context: source.clone(),
            position: Position {
                offset: source_offset,
                size,
            },
        },
    );
    Ok(())
}

/// We assume:
///     Only whole items are transfered. Meaning source_offset is the beginning of an item and size is its size
///     Destination has enough space for transfer. Function will fail otherwise
pub fn out_of_system_context_transfer(
    destination: &mut Context,
    source: &SystemContext,
    destination_offset: usize,
    source_offset: usize,
    size: usize,
) -> DandelionResult<()> {
    let mut transfered_bytes = 0;
    while transfered_bytes < size {
        let Some(DataPosition { context, position }) = source
            .local_offset_to_data_position
            .get(&(source_offset + transfered_bytes))
        else {
            warn!("Read offset not stored in SystemContext (out_of_system_context_transfer). Offset: {}", source_offset + transfered_bytes);
            return err_dandelion!(DandelionError::InvalidRead);
        };

        if transfered_bytes + position.size > size {
            warn!(
                "Memory transfer would not involve whole item! (out_of_system_context_transfer). 
                destination_offset: {}, transfered_bytes: {}, total_size: {}, item_size: {}",
                destination_offset, transfered_bytes, size, position.size
            );
            return err_dandelion!(DandelionError::InvalidRead);
        }

        transfer_memory(
            destination,
            context,
            destination_offset + transfered_bytes,
            position.offset,
            position.size,
        )?;
        transfered_bytes += position.size;
    }
    Ok(())
}

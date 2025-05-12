use crate::memory_domain::{Context, ContextTrait, ContextType, MemoryDomain, MemoryResource};
use bytes::{Buf, Bytes};
use dandelion_commons::{DandelionError, DandelionResult};
use log::{debug, warn};
use std::{
    cmp::min,
    collections::BTreeMap,
    ops::Bound::{Included, Unbounded},
    sync::Arc,
};

use super::transfer_memory;
#[derive(Debug)]
pub enum DataPosition {
    ContextStorage(Arc<Context>, usize),
    ResponseInformationStorage(Bytes),
}

#[derive(Debug)]
/// This context does not have any real data in it. It only holds pointers to other contexts/Bytes, where
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
    pub(crate) local_offset_to_data_position: BTreeMap<usize, (DataPosition, usize)>,
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

        if offset + read_buffer_size > self.size {
            debug!("Offset + read_buffer_size are larger than context size (read). Offset: {}, buffer_size: {}, context size: {}",
            offset, read_buffer_size, self.size);
            return Err(DandelionError::InvalidRead);
        }

        while total_bytes_read < read_buffer_size {
            let mut bytes_read = 0;
            let mut range = self
                .local_offset_to_data_position
                .range((Unbounded, Included(offset + total_bytes_read)));
            if let Some((&item_offset, &(ref item_position, item_size))) = range.next_back() {
                // debug!("Found item with offset {} and size {}", item_offset, item_size);
                if (offset + total_bytes_read) < item_offset + item_size {
                    let offset_in_item = (offset + total_bytes_read) - item_offset;
                    match item_position {
                        DataPosition::ContextStorage(data_arc_context, actual_offset) => {
                            return data_arc_context
                                .read(*actual_offset + offset_in_item, read_buffer);
                        }

                        DataPosition::ResponseInformationStorage(ref body) => {
                            // We clone the Bytes such that we can use the .advance() without changing the actual body
                            let mut cloned_body = body.clone();
                            let body_len = cloned_body.len();

                            if offset % core::mem::align_of::<T>() != 0 {
                                debug!("Misaligned write at offset {}", offset);
                                return Err(DandelionError::ReadMisaligned);
                            }

                            let read_memory = unsafe {
                                core::slice::from_raw_parts_mut(
                                    read_buffer.as_mut_ptr() as *mut u8,
                                    read_buffer_size,
                                )
                            };

                            while bytes_read < read_buffer_size && bytes_read < body_len {
                                let chunk = cloned_body.chunk();
                                if chunk.is_empty() {
                                    return Err(DandelionError::InvalidRead);
                                }
                                let reading = min(
                                    read_buffer_size - (bytes_read + total_bytes_read),
                                    chunk.len(),
                                );
                                read_memory[(bytes_read + total_bytes_read)
                                    ..(bytes_read + total_bytes_read) + reading]
                                    .copy_from_slice(&chunk[..reading]);
                                cloned_body.advance(reading);
                                bytes_read += reading;
                            }
                        }
                    }
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
            total_bytes_read += bytes_read;
        }
        return Ok(());
    }

    fn get_chunk_ref(&self, offset: usize, length: usize) -> DandelionResult<&[u8]> {
        if offset + length > self.size {
            debug!("Offset + length are larger than context size (get_chunk_ref). Offset: {}, length: {}, context size: {}",
            offset, length, self.size);
            return Err(DandelionError::InvalidRead);
        }
        let mut range = self
            .local_offset_to_data_position
            .range((Unbounded, Included(offset)));
        if let Some((&item_offset, &(ref item_position, item_size))) = range.next_back() {
            if offset < item_offset + item_size {
                let offset_in_item = offset - item_offset;
                match item_position {
                    // For contextStorage, we propagate the call down
                    DataPosition::ContextStorage(data_arc_context, actual_offset) => {
                        let max_length = min(length, item_size);
                        return data_arc_context
                            .get_chunk_ref(*actual_offset + offset_in_item, max_length);
                    }
                    // For bytesStorage, we just return the current item at max
                    DataPosition::ResponseInformationStorage(ref body) => {
                        // We calculate the amount of bytes that are definitely contiguous, namely
                        // at max the whole Bytes representing this item
                        let max_length = min(length, item_size);
                        return Ok(&body.as_ref()[offset_in_item..max_length]);
                    }
                }
            }
        }
        warn!(
            "Read offset not stored in SystemContext (get_chunk_ref). Offset: {}",
            offset
        );
        return Err(DandelionError::InvalidRead);
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
///     size = size of source Bytes
/// If another stored item starts at this exact offset, it is overwritten.
pub fn system_context_write_from_bytes(
    destination: &mut SystemContext,
    source: Bytes,
    destination_offset: usize,
    size: usize,
) {
    assert_eq!(
        size,
        source.len(),
        "size is not equal to source size (system_context_write_from_bytes)"
    );
    destination.local_offset_to_data_position.insert(
        destination_offset,
        (
            DataPosition::ResponseInformationStorage(source.clone()),
            size,
        ),
    );
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
        let Some((data_position, item_size)) = source
            .local_offset_to_data_position
            .get(&(source_offset + transfered_bytes))
        else {
            warn!(
                "Read offset not stored in SystemContext (system_context_transfer). Offset: {}",
                source_offset + transfered_bytes
            );
            return Err(DandelionError::InvalidRead);
        };

        if transfered_bytes + item_size > size {
            warn!(
                "Memory transfer would not involve whole item! (system_context_transfer). 
                destination_offset: {}, transfered_bytes: {}, total_size: {}, item_size: {}",
                destination_offset, transfered_bytes, size, item_size
            );
            return Err(DandelionError::InvalidRead);
        }

        match data_position {
            DataPosition::ContextStorage(data_arc_context, actual_offset) => {
                destination.local_offset_to_data_position.insert(
                    destination_offset + transfered_bytes,
                    (
                        DataPosition::ContextStorage(data_arc_context.clone(), *actual_offset),
                        *item_size,
                    ),
                );
            }
            DataPosition::ResponseInformationStorage(ref body) => {
                destination.local_offset_to_data_position.insert(
                    destination_offset + transfered_bytes,
                    (
                        DataPosition::ResponseInformationStorage(body.clone()),
                        *item_size,
                    ),
                );
            }
        }
        transfered_bytes += item_size;
    }
    Ok(())
}

/// We assume:
///     Only whole items are transfered. Meaning source_offset is the beginning of an item
///     Source is not a systems_context. For this, use system_context_transfer.
///     (Using this function could cause inconcistencies with fragmented data)
pub fn into_system_context_transfer(
    destination: &mut SystemContext,
    source: Arc<Context>,
    destination_offset: usize,
    source_offset: usize,
    size: usize,
) -> DandelionResult<()> {
    if source_offset + size > source.size {
        debug!("Source_offset+size > source.size (into_system_context_transfer)");
        return Err(DandelionError::InvalidRead);
    }
    destination.local_offset_to_data_position.insert(
        destination_offset,
        (
            DataPosition::ContextStorage(source.clone(), source_offset),
            size,
        ),
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
        let Some((data_position, item_size)) = source
            .local_offset_to_data_position
            .get(&(source_offset + transfered_bytes))
        else {
            warn!("Read offset not stored in SystemContext (out_of_system_context_transfer). Offset: {}", source_offset + transfered_bytes);
            return Err(DandelionError::InvalidRead);
        };

        if transfered_bytes + item_size > size {
            warn!(
                "Memory transfer would not involve whole item! (out_of_system_context_transfer). 
                destination_offset: {}, transfered_bytes: {}, total_size: {}, item_size: {}",
                destination_offset, transfered_bytes, size, item_size
            );
            return Err(DandelionError::InvalidRead);
        }

        match data_position {
            DataPosition::ContextStorage(data_arc_context, actual_offset) => {
                transfer_memory(
                    destination,
                    data_arc_context.clone(),
                    destination_offset + transfered_bytes,
                    *actual_offset,
                    *item_size,
                )?;
            }
            DataPosition::ResponseInformationStorage(ref body) => {
                let body_len = body.len();

                // We clone the Bytes and make it mutable for the advance method
                let mut cloned_body = body.clone();
                let mut bytes_read = 0;
                while bytes_read < body_len {
                    let chunk = cloned_body.chunk();
                    let reading = chunk.len();
                    destination.write(destination_offset + transfered_bytes + bytes_read, chunk)?;
                    cloned_body.advance(reading);
                    bytes_read += reading;
                }
                assert_eq!(
                    0,
                    cloned_body.remaining(),
                    "Body should have non remaining as we have read the amount given as len in the beginning"
                );
            }
        }
        transfered_bytes += item_size;
    }
    Ok(())
}

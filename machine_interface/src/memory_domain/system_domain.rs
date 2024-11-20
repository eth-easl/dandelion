use crate::{memory_domain::{Context, ContextTrait, ContextType, MemoryDomain, MemoryResource}, util::mmapmem::MmapMem};
use dandelion_commons::{DandelionError, DandelionResult};
// use tokio::sync::RwLock;
use std::{
    collections::BTreeMap,
    sync::Arc,
};
use bytes::Buf;
use std::ops::Bound::{Included, Unbounded};
use log::{debug, warn};
use std::cmp::min;
use bytes::Bytes;

use super::transfer_memory;

// This context does not have any real data in it. It only holds pointers to other contexts/Bytes, where
// the data is. Everything else (including the metadata, where every item is) is as in any other context. 
// Thus, offsets stored in Position for DataItems are only "virtual" and don't have actual meaning 
// The size filed of a Context does not matter internally for a SystemContext, since it does not hold any data
// We only check for accesses that have larger offset than the context size


#[derive(Debug)]
pub enum DataPosition{
    ContextStorage(Arc<Context>, usize),
    ResponseInformationStorage(Bytes),
}

#[derive(Debug)]
pub struct SystemContext {
    // Links virtual offset to corresponding data position and size of item
    // Position is currently either in Arc<Context> with corresponding offset or in Bytes
    local_offset_to_data_position: BTreeMap<usize, (DataPosition, usize)>,
    size: usize,
}

impl ContextTrait for SystemContext {
    fn write<T>(&mut self, offset: usize, data: &[T]) -> DandelionResult<()> {
        panic!("Tried to write to a SystemContext with offset: {}!", offset);
    }

    fn read<T>(&self, offset: usize, read_buffer: &mut [T]) -> DandelionResult<()> {
        // We assume:
        //      The read can span over multiple items
        //      Offset does not have to be the start of an item
        //      We don't have to read till the end of an item

        // Currently, it does not fail while reading data that has never been written to.
        // If we hit an offset, where we have no data stored, we abort. 
        // Thus, further data that is separated by other data by uninitialised memory regions 
        // is ignored.
        // This could be changed but would be difficult to use in practice anyway 
        
        let mut total_bytes_read = 0;
        let read_buffer_size = core::mem::size_of::<T>() * read_buffer.len();

        if offset + read_buffer_size > self.size{
            debug!("Offset + read_buffer_size are larger than context size (read). Offset: {}, buffer_size: {}, context size: {}",
            offset, read_buffer_size, self.size);
            return Err(DandelionError::InvalidRead);
        }

        // debug!("Reading with read buffer size {} from offset {}", read_buffer_size, offset);
        while total_bytes_read < read_buffer_size{
            let mut bytes_read = 0;
            let mut range = self.local_offset_to_data_position.range((Unbounded, Included(offset + total_bytes_read))).rev();
            if let Some((&item_offset, &(ref item_position, item_size))) = range.next() {
                // debug!("Found item with offset {} and size {}", item_offset, item_size);
                if (offset + total_bytes_read) < item_offset + item_size {
                    let offset_in_item = (offset + total_bytes_read) - item_offset;
                    match item_position{
                        DataPosition::ContextStorage(data_arc_context, actual_offset) => {
                            return data_arc_context.read(*actual_offset + offset_in_item, read_buffer);
                        }

                        DataPosition::ResponseInformationStorage(ref body) => {
                            // We clone the Bytes such that we can use the .advance() without changing the actual body
                            let mut cloned_body = body.clone();
            
                            // This check has been copied form other implementations of read
                            if offset % core::mem::align_of::<T>() != 0 {
                                debug!("Misaligned write at offset {}", offset);
                                return Err(DandelionError::ReadMisaligned);
                            }
                            
                            // debug!("Read offset: {}, item_offset: {}, item_size: {}", offset, item_offset, item_size);
                            // debug!("Initial Cloned_body.len: {}", cloned_body.len());
                            unsafe {
                                let read_memory = core::slice::from_raw_parts_mut(read_buffer.as_mut_ptr() as *mut u8, read_buffer_size);
                                
                                while bytes_read < read_buffer_size && bytes_read < cloned_body.len(){
                                    let chunk = cloned_body.chunk();
                                    if chunk.is_empty() {
                                        return Err(DandelionError::InvalidRead);
                                    }
                                    let reading = min(read_buffer_size - (bytes_read + total_bytes_read), chunk.len());
                                    read_memory[(bytes_read + total_bytes_read)..(bytes_read + total_bytes_read) + reading].copy_from_slice(&chunk[..reading]);
                                    cloned_body.advance(reading);
                                    // debug!("Cloned_body.len: {}, bytes read: {}", cloned_body.len(), bytes_read);
                                    // debug!("Remaining bytes in body: {}", cloned_body.remaining());
                                    bytes_read += reading;
                                }
                                // This assertion does not have to be valid, if we assume partial items can be read
                                // assert_eq!(
                                //     0,
                                //     cloned_body.remaining(),
                                //     "Body should have no remaining bytes as we have read the amount given as len in the beginning"
                                // );
                            }
                        }
                        _ =>
                            {warn!("Invalid DataPosition in system_context_transfer");
                            return Err(DandelionError::NotImplemented);}
                    }
                } else {
                    warn!("Read offset has no stored data in SystemContext (read). Offset: {}", offset);
                    break;
                }
            } else {
                warn!("Read offset has no stored data in SystemContext (read). Offset: {}", offset);
                break;
            }
            total_bytes_read += bytes_read;
        }
        return Ok(());
    }

    fn get_chunk_ref(&self, offset: usize, length: usize) -> DandelionResult<&[u8]> {
        if offset + length > self.size{
            debug!("Offset + length are larger than context size (get_chunk_ref). Offset: {}, length: {}, context size: {}",
            offset, length, self.size);
            return Err(DandelionError::InvalidRead);
        }
        let mut range = self.local_offset_to_data_position.range((Unbounded, Included(offset))).rev();
        if let Some((&item_offset, &(ref item_position, item_size))) = range.next() {
            if offset < item_offset + item_size {
                let offset_in_item = offset - item_offset;
                match item_position{
                    // For contextStorage, we propagate the call down
                    DataPosition::ContextStorage(data_arc_context, actual_offset) => {
                        let max_length = min(length, item_size);
                        return data_arc_context.get_chunk_ref(*actual_offset + offset_in_item, max_length);
                    }
                    // For bytesStorage, we just return the current item at max
                    DataPosition::ResponseInformationStorage(ref body) => {
                        // We calculate the amount of bytes that are definitely contiguous, namely
                        // at max the whole Bytes representing this item
                        let max_length = min(length, item_size);
                        return Ok(&body.as_ref()[offset_in_item..max_length]);
                    }
                    _ => {
                        warn!("Invalid DataPosition in system_context_transfer");
                        return Err(DandelionError::NotImplemented);
                    }
                }
            }
        }
        warn!("Read offset not stored in SystemContext (get_chunk_ref). Offset: {}", offset);
        return Err(DandelionError::InvalidRead);
    }
}

#[derive(Debug)]
pub struct SystemMemoryDomain {}

impl MemoryDomain for SystemMemoryDomain {
    fn init(_config: MemoryResource) -> DandelionResult<Box<dyn MemoryDomain>> {
        Ok(Box::new(SystemMemoryDomain{}))
    }

    fn acquire_context(&self, size: usize) -> DandelionResult<Context> {
        let new_context = Box::new(SystemContext{local_offset_to_data_position: BTreeMap::new(), size});
        Ok(Context::new(ContextType::System(new_context), size))
    }
}

pub fn system_context_write_from_bytes(
    destination: &mut SystemContext,
    source: Bytes,
    destination_offset: usize,
    size: usize,
){  
    debug!("Size of Bytes that is inserted at offset {} is {}", destination_offset, source.len());
    // Changed to overwrite value, if already present
    destination.local_offset_to_data_position.insert(destination_offset, (DataPosition::ResponseInformationStorage(source.clone()), size));
}

pub fn system_context_transfer(
    destination: &mut SystemContext,
    source: Arc<Context>,
    destination_offset: usize,
    source_offset: usize,
    size: usize,
) -> DandelionResult<()> {
    // We assume:
    //      Only whole items are transfered. Meaning source_offset is the beginning of an item
    //      Source is Arc<SystemContext>. Function will fail otherwise

    match &source.context{
        ContextType::System(source_ctxt) => {
            let Some((data_position, size)) = source_ctxt.local_offset_to_data_position.get(&source_offset)
            else {
                warn!("Read offset not stored in SystemContext (system_context_transfer). Offset: {}", source_offset);
                return Err(DandelionError::InvalidRead);
            };
            match data_position{
                DataPosition::ContextStorage(data_arc_context, actual_offset) => 
                    {destination.local_offset_to_data_position.insert(destination_offset, (DataPosition::ContextStorage(data_arc_context.clone(), *actual_offset), *size));}
                DataPosition::ResponseInformationStorage(ref body) => 
                    {destination.local_offset_to_data_position.insert(destination_offset, (DataPosition::ResponseInformationStorage(body.clone()), *size));}
                _ =>
                    {warn!("Invalid DataPosition (system_context_transfer). Offset: {}", source_offset);
                    return Err(DandelionError::NotImplemented);}
            }
            
            Ok(())
        }
        _ => {panic!("Wrong source type (system_context_transfer)");}
    }
}

pub fn into_system_context_transfer(
    destination: &mut SystemContext,
    source: Arc<Context>,
    destination_offset: usize,
    source_offset: usize,
    size: usize,
) -> DandelionResult<()> {
    // We assume:
    //      Only whole items are transfered. Meaning source_offset is the beginning of an item

    if source_offset + size > source.size {
        debug!("Source_offset+size > source.size (into_system_context_transfer)");
        return Err(DandelionError::InvalidRead);
    }
    return match &source.context{
        ContextType::System(source_ctxt) => {
            system_context_transfer(destination, source, destination_offset, source_offset, size)
        }
        _ => {
            destination.local_offset_to_data_position.insert(destination_offset, (DataPosition::ContextStorage(source.clone(), source_offset), size));
            Ok(())
        }
    }
}

pub fn out_of_system_context_transfer(
    destination: &mut Context,
    source: Arc<Context>,
    destination_offset: usize,
    source_offset: usize,
    size: usize,
) -> DandelionResult<()> {
    // We assume:
    //      Only whole items are transfered. Meaning source_offset is the beginning of an item and size is item_size
    //      Source is Arc<SystemContext>. Function will fail otherwise
    //      Destination has enough space for transfer. Function will fail otherwise

    return match &mut destination.context{
        ContextType::System(destination_ctxt) => {
            system_context_transfer(destination_ctxt, source, destination_offset, source_offset, size)
        }
        _ => {
            return match &source.context{
                ContextType::System(source_ctxt) => {
                    let Some((data_position, _item_size)) = source_ctxt.local_offset_to_data_position.get(&source_offset)
                    else {
                        warn!("Read offset not stored in SystemContext (out_of_system_context_transfer). Offset: {}", source_offset);
                        return Err(DandelionError::InvalidRead);
                    };

                    match data_position{
                        DataPosition::ContextStorage(data_arc_context, actual_offset) => {
                            transfer_memory(destination, data_arc_context.clone(), destination_offset, *actual_offset, size)
                        }
                        DataPosition::ResponseInformationStorage(ref body) => {
                            let body_len = body.len();

                            // We clone the Bytes and make it mutable for the advance method
                            let mut cloned_body = body.clone();
                            let mut bytes_read = 0;
                            while bytes_read < body_len {
                                let chunk = cloned_body.chunk();
                                let reading = chunk.len();
                                // warn!("Writing part of body at offset {}", response_start + preable_len + bytes_read);
                                destination.write(destination_offset + bytes_read, chunk)?;
                                cloned_body.advance(reading);
                                bytes_read += reading;
                            }
                            assert_eq!(
                                0,
                                cloned_body.remaining(),
                                "Body should have non remaining as we have read the amount given as len in the beginning"
                            );
                            Ok(())  
                        }
                        _ => {
                            warn!("Invalid DataPosition in system_context_transfer");
                            return Err(DandelionError::NotImplemented);
                        }
                    }
                }
                _ => Err(DandelionError::ContextMissmatch)
            };
        }
    }
}
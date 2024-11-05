use crate::{memory_domain::{Context, ContextTrait, ContextType, MemoryDomain, MemoryResource}, util::mmapmem::MmapMem};
use dandelion_commons::{DandelionError, DandelionResult};
// use tokio::sync::RwLock;
use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};
use bytes::{Buf, BytesMut};
use log::{debug, warn};

use super::transfer_memory;

// This context does not have any real data in it. It only holds arcs to other contexts, where
// the data is. Everything else (including the metadata, where every item is) is as any other context. 
// Thus, offsets stored in Position for DataItems are only "virtual" and don't have actual meaning 
// The size filed of a Context does not matter for a SystemContext, since it does not hold any data

// Size currently does not mean anything. I just store the value it was initialised with 

#[derive(Debug)]
pub enum DataPosition{
    ContextStorage(Arc<Context>, usize),
    ResponseInformationStorage(bytes::Bytes, bytes::Bytes),
}

#[derive(Debug)]
pub struct SystemContext {
    // Links virtual offset to corresponding data position
    local_offset_to_data_position: BTreeMap<usize, DataPosition>,
    
    chunk_ref_return_storage: Arc<RwLock<bytes::Bytes>>,
}

impl ContextTrait for SystemContext {
    fn write<T>(&mut self, offset: usize, data: &[T]) -> DandelionResult<()> {
        panic!("Tried to write to a SystemContext with offset: {}!", offset);
    }

    fn read<T>(&self, offset: usize, read_buffer: &mut [T]) -> DandelionResult<()> {
        // warn!("Tried to read from a SystemContext!");
        let Some(data_position) = self.local_offset_to_data_position.get(&offset)
        else {
            warn!("Read offset not stored in SystemContext (read). Offset: {}", offset);
            return Err(DandelionError::InvalidRead);
        };
        match data_position{
            DataPosition::ContextStorage(data_arc_context, actual_offset) => {
                data_arc_context.read(*actual_offset, read_buffer)
            }
            DataPosition::ResponseInformationStorage(ref preamble, ref body) => {
                let cloned_preamble = preamble.clone();
                let mut cloned_body = body.clone();

                if offset % core::mem::align_of::<T>() != 0 {
                    debug!("Misaligned write at offset {}", offset);
                    return Err(DandelionError::ReadMisaligned);
                }
                // ------------------- TODO: Check this? -------------------
                // We assume preamble_len + body_len = size
                // assert!(preamble_len + body_len == size)



                let read_size = core::mem::size_of::<T>() * read_buffer.len();
                unsafe {
                    let read_memory = core::slice::from_raw_parts_mut(read_buffer.as_mut_ptr() as *mut u8, read_size);
                    
                    let preamble_len = cloned_preamble.len();
                    read_memory[..preamble_len].copy_from_slice(cloned_preamble.as_ref());
    
                    let mut bytes_read = 0;
                    while bytes_read < cloned_body.len() {
                        let chunk = cloned_body.chunk();
                        let reading = chunk.len();
                        read_memory[preamble_len + bytes_read..preamble_len + bytes_read + reading].copy_from_slice(chunk); // Write the chunk to the read_memory
                        cloned_body.advance(reading);
                        bytes_read += reading;
                    }
                    assert_eq!(
                        0,
                        cloned_body.remaining(),
                        "Body should have no remaining bytes as we have read the amount given as len in the beginning"
                    );
                }
                Ok(())
            }
            _ =>
                {warn!("Invalid DataPosition in system_context_transfer");
                return Err(DandelionError::NotImplemented);}
        }
    }

    fn get_chunk_ref(&self, offset: usize, length: usize) -> DandelionResult<&[u8]> {
        let Some(data_position) = self.local_offset_to_data_position.get(&offset)
        else {
            warn!("Read offset not stored in SystemContext (read). Offset: {}", offset);
            return Err(DandelionError::InvalidRead);
        };
        match data_position{
            DataPosition::ContextStorage(data_arc_context, actual_offset) => {
                data_arc_context.get_chunk_ref(*actual_offset, length)
            }
            DataPosition::ResponseInformationStorage(ref preamble, ref body) => {
                let cloned_preamble = preamble.clone();
                let mut cloned_body = body.clone();

                let preamble_length = cloned_preamble.len();
                let body_length = cloned_body.len();
                let total_length = preamble_length + body_length;
                let adjusted_length = std::cmp::min(length, total_length);

                let mut combined = BytesMut::with_capacity(length);

                // let mut combined = Vec::with_capacity(adjusted_length);

                if adjusted_length <= preamble_length {
                    combined.extend_from_slice(&cloned_preamble[..adjusted_length]);
                }
                else {
                    combined.extend_from_slice(&cloned_preamble);
                    let remaining_length = adjusted_length - preamble_length;
                    // combined.extend_from_slice(&cloned_body[..remaining_length]);

                    let mut bytes_read = 0;
                    while bytes_read < cloned_body.len() {
                        let chunk = cloned_body.chunk();
                        let reading = chunk.len();
                        let to_copy = std::cmp::min(reading, remaining_length - bytes_read);
                        combined.extend_from_slice(&chunk[..to_copy]);
                        cloned_body.advance(to_copy);
                        bytes_read += to_copy;
                        if bytes_read >= remaining_length {
                            break;
                        }
                    }
                }

                let new_bytes = combined.freeze();

                {
                    let mut external_lock = self.chunk_ref_return_storage.write().unwrap();
                    *external_lock = new_bytes;
                }
        
                // Read from `external` and return a slice
                // Clone the Bytes data so we can produce a slice from it
                let external_clone = {
                    let external_lock = self.chunk_ref_return_storage.read().unwrap();
                    external_lock.clone()
                };
        
                // Return a slice from the cloned data
                // Ok(external_clone.as_ref())
                Err(DandelionError::NotImplemented)
            }
            _ =>
                {warn!("Invalid DataPosition in system_context_transfer");
                return Err(DandelionError::NotImplemented);}
        }
    }
}

#[derive(Debug)]
pub struct SystemMemoryDomain {}

impl MemoryDomain for SystemMemoryDomain {
    fn init(_config: MemoryResource) -> DandelionResult<Box<dyn MemoryDomain>> {
        Ok(Box::new(SystemMemoryDomain{}))
    }

    fn acquire_context(&self, size: usize) -> DandelionResult<Context> {
        let new_context = Box::new(SystemContext{local_offset_to_data_position: BTreeMap::new(), chunk_ref_return_storage: Arc::new(RwLock::new(bytes::Bytes::new()))});
        Ok(Context::new(ContextType::System(new_context), size))
    }
}

pub fn system_context_write_response_information(
    destination: &mut SystemContext,
    source_preamble: bytes::Bytes,
    source_body: bytes::Bytes,
    destination_offset: usize,
){
    destination.local_offset_to_data_position.entry(destination_offset).or_insert(DataPosition::ResponseInformationStorage(source_preamble.clone(), source_body.clone()));
}

pub fn system_context_transfer(
    destination: &mut SystemContext,
    source: Arc<Context>,
    destination_offset: usize,
    source_offset: usize,
    size: usize,
) -> DandelionResult<()> {
    match &source.context{
        ContextType::System(source_ctxt) => {
            let Some(data_position) = source_ctxt.local_offset_to_data_position.get(&source_offset)
            else {
                warn!("Read offset not stored in SystemContext (system_context_transfer). Offset: {}", source_offset);
                return Err(DandelionError::InvalidRead);
            };
            match data_position{
                DataPosition::ContextStorage(data_arc_context, actual_offset) => 
                    {destination.local_offset_to_data_position.entry(destination_offset).or_insert(DataPosition::ContextStorage(data_arc_context.clone(), *actual_offset));}
                DataPosition::ResponseInformationStorage(ref preamble, ref body) => 
                    {destination.local_offset_to_data_position.entry(destination_offset).or_insert(DataPosition::ResponseInformationStorage(preamble.clone(), body.clone()));}
                _ =>
                    {warn!("Invalid DataPosition in system_context_transfer");
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
    if source_offset + size > source.size {
        return Err(DandelionError::InvalidRead);
    }
    return match &source.context{
        ContextType::System(source_ctxt) => {
            system_context_transfer(destination, source, destination_offset, source_offset, size)
        }
        _ => {
            destination.local_offset_to_data_position.entry(destination_offset).or_insert(DataPosition::ContextStorage(source.clone(), source_offset));
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
    // Checks if we have two system contexts. If yes, change system_context_transfer
    // If not, we call transfer_memory with the actual data location

    return match &mut destination.context{
        ContextType::System(destination_ctxt) => {
            system_context_transfer(destination_ctxt, source, destination_offset, source_offset, size)
        }
        _ => {
            return match &source.context{
                ContextType::System(source_ctxt) => {
                    let Some(data_position) = source_ctxt.local_offset_to_data_position.get(&source_offset)
                    else {
                        warn!("Read offset not stored in SystemContext (out_of_system_context_transfer). Offset: {}", source_offset);
                        return Err(DandelionError::InvalidRead);
                    };

                    match data_position{
                        DataPosition::ContextStorage(data_arc_context, actual_offset) => {
                            transfer_memory(destination, data_arc_context.clone(), destination_offset, *actual_offset, size)
                        }
                        DataPosition::ResponseInformationStorage(ref preamble, ref body) => {


                            let preamble_len = preamble.len();
                            let body_len = body.len();
                            // ------------------- TODO: Check this -------------------
                            // We assume preamble_len + body_len = size
                            // assert!(preamble_len + body_len == size)

                            destination.write(destination_offset, preamble)?;

                            // We clone the bytes::Bytes and make it mutable for the advance method
                            let mut cloned_body = body.clone();
                            let mut bytes_read = 0;
                            while bytes_read < body_len {
                                let chunk = cloned_body.chunk();
                                let reading = chunk.len();
                                // warn!("Writing part of body at offset {}", response_start + preable_len + bytes_read);
                                destination.write(destination_offset + preamble_len + bytes_read, chunk)?;
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
                        _ =>
                            {warn!("Invalid DataPosition in system_context_transfer");
                            return Err(DandelionError::NotImplemented);}
                    }

                    
                }
                _ => Err(DandelionError::ContextMissmatch)
            };
        }
    }
}
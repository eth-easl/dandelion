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
    ResponseInformationStorage(bytes::Bytes),
}

#[derive(Debug)]
pub struct SystemContext {
    // Links virtual offset to corresponding data position
    local_offset_to_data_position: BTreeMap<usize, DataPosition>,
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
            DataPosition::ResponseInformationStorage(ref body) => {
                let mut cloned_body = body.clone();

                if offset % core::mem::align_of::<T>() != 0 {
                    debug!("Misaligned write at offset {}", offset);
                    return Err(DandelionError::ReadMisaligned);
                }
                // ------------------- TODO: Check this? -------------------
                // We assume  body_len = size
                // assert!(body_len == size)



                let read_size = core::mem::size_of::<T>() * read_buffer.len();
                unsafe {
                    let read_memory = core::slice::from_raw_parts_mut(read_buffer.as_mut_ptr() as *mut u8, read_size);
    
                    let mut bytes_read = 0;
                    while bytes_read < cloned_body.len() {
                        let chunk = cloned_body.chunk();
                        let reading = chunk.len();
                        read_memory[bytes_read..bytes_read + reading].copy_from_slice(chunk); // Write the chunk to the read_memory
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
            DataPosition::ResponseInformationStorage(ref body) => {
                // ----------------------- TODO -----------------------
                // Maybe make sure that length <= body.length
                Ok(&body.as_ref()[..length])
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
        let new_context = Box::new(SystemContext{local_offset_to_data_position: BTreeMap::new()});
        Ok(Context::new(ContextType::System(new_context), size))
    }
}

pub fn system_context_write_response_information(
    destination: &mut SystemContext,
    source: bytes::Bytes,
    destination_offset: usize,
){
    destination.local_offset_to_data_position.entry(destination_offset).or_insert(DataPosition::ResponseInformationStorage(source.clone()));
    // warn!("Written at {} from system_context_write_response_information", destination_offset);
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
                DataPosition::ResponseInformationStorage(ref body) => 
                    {destination.local_offset_to_data_position.entry(destination_offset).or_insert(DataPosition::ResponseInformationStorage(body.clone()));}
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
            // warn!("Written at {} from into_system_context_transfer", destination_offset);
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
                        DataPosition::ResponseInformationStorage(ref body) => {
                            let body_len = body.len();
                            // ------------------- TODO: Check this -------------------
                            // We assume body_len = size
                            // assert!(body_len == size)

                            // We clone the bytes::Bytes and make it mutable for the advance method
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
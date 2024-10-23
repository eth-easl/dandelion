use crate::{memory_domain::{Context, ContextTrait, ContextType, MemoryDomain, MemoryResource}, util::mmapmem::MmapMem};
use dandelion_commons::{DandelionError, DandelionResult};
use nix::sys::mman::ProtFlags;
use std::{
    collections::BTreeMap,
    sync::Arc,
};
use log::warn;

use super::{bytes_context::BytesContext, mmap::MmapContext, transfer_memory};

// This context does not have any real data in it. It only holds arcs to other contexts, where
// the data is. Everything else (including the metadata, where every item is) is as any other context. 
// Thus, offsets stored in Position for DataItems are only "virtual" and don't have actual meaning 
// The size filed of a Context does not matter for a SystemContext, since it does not hold any data

// Size currently does not mean anything. I just store the value it was initialised with 

#[derive(Debug)]
pub struct SystemContext {
    // Links virtual offset to corresponding arc context and its offset
    local_offset_to_actual_offset: BTreeMap<usize, (Arc<Context>, usize)>,

    // This is a context only acces through this system context. It is used if data is directly writen to the context.
    // This should not be neccessary if only the transfer_memory interface is used. Currently we use an mmuContext for that
    mmap_context: Box<MmapContext>,
    // Stores if item is stored locally
    local_items: BTreeMap<usize, bool>,
}

impl ContextTrait for SystemContext {
    fn write<T>(&mut self, offset: usize, data: &[T]) -> DandelionResult<()> {
        warn!("Tried to write to a SystemContext!");
        self.local_items.entry(offset).or_insert(true);
        self.mmap_context.storage.write(offset, data)
    }

    fn read<T>(&self, offset: usize, read_buffer: &mut [T]) -> DandelionResult<()> {
        // warn!("Tried to read from a SystemContext!");
        let Some((arc_context, actual_offset)) = self.local_offset_to_actual_offset.get(&offset)
        else {
            let Some(_) = self.local_items.get(&offset)
            else {
                panic!("Read offset not stored in SystemContext (read)");
            };
            return self.mmap_context.read(offset, read_buffer);
        };
        arc_context.read(*actual_offset, read_buffer)

    }

    fn get_chunk_ref(&self, offset: usize, length: usize) -> DandelionResult<&[u8]> {
        // warn!("Tried to get chunk ref from a SystemContext!");

        let Some((arc_context, actual_offset)) = self.local_offset_to_actual_offset.get(&offset)
        else {panic!("Read offset not stored in SystemContext (get_chunk_ref)");};
        arc_context.get_chunk_ref(*actual_offset, length)
    }
}

#[derive(Debug)]
pub struct SystemMemoryDomain {}

impl MemoryDomain for SystemMemoryDomain {
    fn init(_config: MemoryResource) -> DandelionResult<Box<dyn MemoryDomain>> {
        Ok(Box::new(SystemMemoryDomain{}))
    }

    fn acquire_context(&self, size: usize) -> DandelionResult<Context> {

        let mem_space =
            match MmapMem::create(size, ProtFlags::PROT_READ | ProtFlags::PROT_WRITE, false) {
                Ok(v) => v,
                Err(_e) => return Err(DandelionError::MemoryAllocationError),
            };

        let sub_context = Box::new(MmapContext { storage: mem_space });

        let new_context = Box::new(SystemContext{local_offset_to_actual_offset: BTreeMap::new(), mmap_context: sub_context, local_items: BTreeMap::new()});
        Ok(Context::new(ContextType::System(new_context), size))
    }
}

// TODO: For these we assume that the function interface has been changed to Arc<Context>

pub fn system_context_transfer(
    destination: &mut SystemContext,
    source: Arc<Context>,
    destination_offset: usize,
    source_offset: usize,
    size: usize,
) -> DandelionResult<()> {
    match &source.context{
        ContextType::System(source_ctxt) => {
            let Some((data_arc_context, actual_offset)) = source_ctxt.local_offset_to_actual_offset.get(&source_offset)
            else {panic!("Read offset not stored in SystemContext (system_context_transfer)");};
            destination.local_offset_to_actual_offset.entry(destination_offset).or_insert((data_arc_context.clone(), *actual_offset));
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
            destination.local_offset_to_actual_offset.entry(destination_offset).or_insert((source.clone(), source_offset));
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
                    let Some((data_arc_context, actual_offset)) = source_ctxt.local_offset_to_actual_offset.get(&source_offset)
                    else {panic!("Read offset not stored in SystemContext (system_context_transfer)");};

                    transfer_memory(destination, data_arc_context.clone(), destination_offset, *actual_offset, size)
                }
                _ => {panic!("Wrong source type (out_of_system_context_transfer)");}
            };
        }
    }
}
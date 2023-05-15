use crate::{
    memory_domain::{Context, ContextTrait, ContextType, MemoryDomain},
    util::shared_mem::SharedMem,
};
use dandelion_commons::{DandelionError, DandelionResult};
use nix::sys::mman::ProtFlags;
use std::collections::HashMap;

#[derive(Debug)]
pub struct PagetableContext {
    pub storage: SharedMem,
}

impl ContextTrait for PagetableContext {
    fn write(&mut self, offset: usize, data: Vec<u8>) -> DandelionResult<()> {
        // check if the write is within bounds
        if offset + data.len() > self.storage.len() {
            return Err(DandelionError::InvalidWrite);
        }

        // write values
        unsafe {
            self.storage.as_slice_mut()[offset..offset + data.len()].copy_from_slice(&data);
        }

        Ok(())
    }

    fn read(&self, offset: usize, read_size: usize) -> DandelionResult<Vec<u8>> {
        if offset + read_size > self.storage.len() {
            return Err(DandelionError::InvalidRead);
        }

        // try to allocate space for read values
        let mut result_vec = Vec::new();
        if let Err(_) = result_vec.try_reserve(read_size) {
            return Err(DandelionError::OutOfMemory);
        }
        result_vec.resize(read_size, 0);

        // read values, sanitize if necessary
        unsafe {
            result_vec.copy_from_slice(&self.storage.as_slice()[offset..offset + read_size]);
        }

        Ok(result_vec)
    }
}

#[derive(Debug)]
pub struct PagetableMemoryDomain {}

impl MemoryDomain for PagetableMemoryDomain {
    fn init(_config: Vec<u8>) -> DandelionResult<Box<dyn MemoryDomain>> {
        Ok(Box::new(PagetableMemoryDomain {}))
    }

    fn acquire_context(&mut self, size: usize) -> DandelionResult<Context> {
        // create and map a shared memory region
        let mem_space = match SharedMem::create(size, ProtFlags::PROT_READ | ProtFlags::PROT_WRITE)
        {
            Ok(v) => v,
            Err(_e) => return Err(DandelionError::OutOfMemory),
        };

        Ok(Context {
            context: ContextType::Pagetable(Box::new(PagetableContext { storage: mem_space })),
            dynamic_data: HashMap::new(),
            static_data: Vec::new(),
            #[cfg(feature = "pagetable")]
            protection_requirements: Vec::new(),
        })
    }

    fn release_context(&mut self, context: Context) -> DandelionResult<()> {
        match context.context {
            ContextType::Pagetable(_) => Ok(()),
            _ => Err(DandelionError::ContextMissmatch),
        }
    }
}

pub fn pagetable_transfer(
    destination: &mut PagetableContext,
    source: &PagetableContext,
    destination_offset: usize,
    source_offset: usize,
    size: usize,
) -> DandelionResult<()> {
    // check if there is space in both contexts
    if source.storage.len() < source_offset + size {
        return Err(DandelionError::InvalidRead);
    }
    if destination.storage.len() < destination_offset + size {
        return Err(DandelionError::InvalidWrite);
    }
    unsafe {
        destination.storage.as_slice_mut()[destination_offset..destination_offset + size]
            .copy_from_slice(&source.storage.as_slice()[source_offset..source_offset + size]);
    }
    Ok(())
}

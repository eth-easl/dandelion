use std::collections::HashMap;

use crate::memory_domain::{Context, ContextTrait, ContextType, MemoryDomain};
use dandelion_commons::{DandelionError, DandelionResult};

#[derive(Debug)]
pub struct MallocContext {
    storage: Vec<u8>,
}

impl ContextTrait for MallocContext {
    fn write(&mut self, offset: usize, data: Vec<u8>) -> DandelionResult<()> {
        // check if the write is within bounds
        let length = self.storage.capacity();
        if offset + data.len() > length {
            return Err(DandelionError::InvalidWrite);
        }
        // write values
        for (pos, value) in data.into_iter().enumerate() {
            self.storage[offset + pos] = value;
        }
        Ok(())
    }
    fn read(&self, offset: usize, read_size: usize) -> DandelionResult<Vec<u8>> {
        let length = self.storage.len();
        if offset + read_size > length {
            return Err(DandelionError::InvalidRead);
        }
        // try to allocate space for read values
        let mut result_vec = Vec::new();
        if let Err(_) = result_vec.try_reserve(read_size) {
            return Err(DandelionError::OutOfMemory);
        }
        result_vec.resize(read_size, 0);
        // read values, sanitize if necessary
        for (index, result) in result_vec.iter_mut().enumerate() {
            *result = self.storage[offset + index];
        }
        Ok(result_vec)
    }
}

#[derive(Debug)]
pub struct MallocMemoryDomain {}

impl MemoryDomain for MallocMemoryDomain {
    fn init(_config: Vec<u8>) -> DandelionResult<Box<dyn MemoryDomain>> {
        Ok(Box::new(MallocMemoryDomain {}))
    }
    fn acquire_context(&self, size: usize) -> DandelionResult<Context> {
        let mut mem_space = Vec::new();
        if (mem_space.try_reserve_exact(size)) != Ok(()) {
            return Err(DandelionError::OutOfMemory);
        }
        mem_space.resize(size, 0);
        Ok(Context {
            context: ContextType::Malloc(Box::new(MallocContext { storage: mem_space })),
            dynamic_data: HashMap::new(),
            static_data: Vec::new(),
            size,
        })
    }
    fn release_context(&self, context: Context) -> DandelionResult<()> {
        match context.context {
            ContextType::Malloc(_) => Ok(()),
            _ => Err(DandelionError::ContextMissmatch),
        }
    }
}

pub fn malloc_transfer(
    destination: &mut MallocContext,
    source: &MallocContext,
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
    destination.storage[destination_offset..destination_offset + size]
        .copy_from_slice(&source.storage[source_offset..source_offset + size]);
    Ok(())
}

use crate::{DataItem, Position};

use super::super::{HardwareError, HwResult};
use super::{Context, ContextTrait, ContextType, MemoryDomain};

#[derive(Debug)]
pub struct MallocContext {
    storage: Vec<u8>,
}

impl ContextTrait for MallocContext {
    fn write(&mut self, offset: usize, data: Vec<u8>) -> HwResult<()> {
        // check if the write is within bounds
        let length = self.storage.capacity();
        if offset + data.len() > length {
            return Err(HardwareError::InvalidWrite);
        }
        // write values
        for (pos, value) in data.into_iter().enumerate() {
            self.storage[offset + pos] = value;
        }
        Ok(())
    }
    fn read(&mut self, offset: usize, read_size: usize, sanitize: bool) -> HwResult<Vec<u8>> {
        let length = self.storage.len();
        if offset + read_size > length {
            return Err(HardwareError::InvalidRead);
        }
        // try to allocate space for read values
        let mut result_vec = Vec::new();
        if let Err(_) = result_vec.try_reserve(read_size) {
            return Err(HardwareError::OutOfMemory);
        }
        result_vec.resize(read_size, 0);
        // read values, sanitize if necessary
        for (index, result) in result_vec.iter_mut().enumerate() {
            *result = self.storage[offset + index];
            if sanitize {
                self.storage[offset + index] = 0;
            }
        }
        Ok(result_vec)
    }
}

#[derive(Debug)]
pub struct MallocMemoryDomain {}

impl MemoryDomain for MallocMemoryDomain {
    fn init(_config: Vec<u8>) -> HwResult<Self> {
        Ok(MallocMemoryDomain {})
    }
    fn acquire_context(&mut self, size: usize) -> HwResult<Context> {
        let mut mem_space = Vec::new();
        if (mem_space.try_reserve_exact(size)) != Ok(()) {
            return Err(HardwareError::OutOfMemory);
        }
        mem_space.resize(size, 0);
        Ok(Context {
            context: ContextType::Malloc(Box::new(MallocContext { storage: mem_space })),
            dynamic_data: Vec::<DataItem>::new(),
            static_data: Vec::<Position>::new(),
        })
    }
    fn release_context(&mut self, context: Context) -> HwResult<()> {
        match context.context {
            ContextType::Malloc(_) => Ok(()),
            _ => Err(HardwareError::ContextMissmatch),
        }
    }
}

pub fn malloc_transfer(
    destination: &mut MallocContext,
    source: &mut MallocContext,
    destination_offset: usize,
    source_offset: usize,
    size: usize,
    sanitize: bool,
) -> HwResult<()> {
    // check if there is space in both contexts
    if source.storage.len() < source_offset + size {
        return Err(HardwareError::InvalidRead);
    }
    if destination.storage.len() < destination_offset + size {
        return Err(HardwareError::InvalidWrite);
    }
    destination.storage[destination_offset..destination_offset + size]
        .copy_from_slice(&source.storage[source_offset..source_offset + size]);
    if sanitize {
        source.storage[source_offset..source_offset + size].fill(0);
    }
    Ok(())
}

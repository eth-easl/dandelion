use super::super::{HardwareError, HwResult};
use super::{Context, ContextTrait, MemoryDomain};

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
    fn init(config: Vec<u8>) -> HwResult<Box<Self>> {
        Ok(Box::new(MallocMemoryDomain {}))
    }
    fn acquire_context(&self, size: usize) -> HwResult<Context> {
        let mut mem_space = Vec::new();
        if (mem_space.try_reserve_exact(size)) != Ok(()) {
            return Err(HardwareError::OutOfMemory);
        }
        mem_space.resize(size, 0);
        Ok(Context::Malloc(Box::new(MallocContext {
            storage: mem_space,
        })))
    }
    fn release_context(&self, context: Context) -> HwResult<()> {
        match context {
            Context::Malloc(_) => Ok(()),
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
    Ok(())
}

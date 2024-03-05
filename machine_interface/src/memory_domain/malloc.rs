use crate::memory_domain::{Context, ContextTrait, ContextType, MemoryDomain, MemoryResource};
use dandelion_commons::{DandelionError, DandelionResult};

#[derive(Debug)]
pub struct MallocContext {
    storage: Vec<u8>,
}

impl ContextTrait for MallocContext {
    fn write<T>(&mut self, offset: usize, data: &[T]) -> DandelionResult<()> {
        // check alignment
        if offset % core::mem::align_of::<T>() != 0 {
            return Err(DandelionError::WriteMisaligned);
        }

        // check if the write is within bounds
        let write_length = data.len() * core::mem::size_of::<T>();
        let length = self.storage.capacity();
        if write_length + offset > length {
            return Err(DandelionError::InvalidWrite);
        }
        let buffer =
            unsafe { core::slice::from_raw_parts(data.as_ptr() as *const u8, write_length) };
        // write values
        for (pos, value) in buffer.iter().enumerate() {
            self.storage[offset + pos] = *value;
        }
        Ok(())
    }
    fn read<T>(&self, offset: usize, read_buffer: &mut [T]) -> DandelionResult<()> {
        // check that buffer has proper allighment
        if offset % core::mem::align_of::<T>() != 0 {
            return Err(DandelionError::ReadMisaligned);
        }

        let length = self.storage.len();
        let read_size = core::mem::size_of::<T>() * read_buffer.len();
        if offset + read_size > length {
            return Err(DandelionError::InvalidRead);
        }

        let read_memory = unsafe {
            core::slice::from_raw_parts_mut(read_buffer.as_mut_ptr() as *mut u8, read_size)
        };

        // read values, sanitize if necessary
        for index in 0..read_size {
            read_memory[index] = self.storage[offset + index];
        }
        return Ok(());
    }
}

#[derive(Debug)]
pub struct MallocMemoryDomain {}

impl MemoryDomain for MallocMemoryDomain {
    fn init(_config: MemoryResource) -> DandelionResult<Box<dyn MemoryDomain>> {
        Ok(Box::new(MallocMemoryDomain {}))
    }
    fn acquire_context(&self, size: usize) -> DandelionResult<Context> {
        let mut mem_space = Vec::new();
        if (mem_space.try_reserve_exact(size)) != Ok(()) {
            return Err(DandelionError::OutOfMemory);
        }
        mem_space.resize(size, 0);
        return Ok(Context::new(
            ContextType::Malloc(Box::new(MallocContext { storage: mem_space })),
            size,
        ));
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

use crate::memory_domain::{Context, ContextTrait, ContextType, MemoryDomain};
use dandelion_commons::{DandelionError, DandelionResult};
use std::alloc::{alloc_zeroed, Layout};

#[derive(Debug)]
pub struct MallocContext {
    layout: Layout,
    storage: Box<u8>,
}

impl ContextTrait for MallocContext {
    fn write<T>(&mut self, offset: usize, data: &[T]) -> DandelionResult<()> {
        // check alignment
        if offset % core::mem::align_of::<T>() != 0 {
            return Err(DandelionError::WriteMisaligned);
        }

        // check if the write is within bounds
        let write_length = data.len() * core::mem::size_of::<T>();
        let length = self.layout.size();
        if write_length + offset > length {
            return Err(DandelionError::InvalidWrite);
        }
        let buffer =
            unsafe { core::slice::from_raw_parts(data.as_ptr() as *const u8, write_length) };
        // copy values
        let storage_slice =
            unsafe { std::slice::from_raw_parts_mut(self.storage.as_mut(), length) };
        let target = &mut storage_slice[offset..offset + write_length];
        target.copy_from_slice(buffer);

        Ok(())
    }
    fn read<T>(&self, offset: usize, read_buffer: &mut [T]) -> DandelionResult<()> {
        // check that buffer has proper allighment
        if offset % core::mem::align_of::<T>() != 0 {
            return Err(DandelionError::ReadMisaligned);
        }

        let length = self.layout.size();
        let read_size = core::mem::size_of::<T>() * read_buffer.len();
        if offset + read_size > length {
            return Err(DandelionError::InvalidRead);
        }

        let read_memory = unsafe {
            core::slice::from_raw_parts_mut(read_buffer.as_mut_ptr() as *mut u8, read_size)
        };
        let storage_slice = unsafe { std::slice::from_raw_parts(self.storage.as_ref(), length) };

        // read values, sanitize if necessary
        for index in 0..read_size {
            read_memory[index] = storage_slice[offset + index];
        }
        return Ok(());
    }
}

#[derive(Debug)]
pub struct MallocMemoryDomain {}

impl MemoryDomain for MallocMemoryDomain {
    fn init(_config: Vec<u8>) -> DandelionResult<Box<dyn MemoryDomain>> {
        Ok(Box::new(MallocMemoryDomain {}))
    }
    fn acquire_context(&self, size: usize) -> DandelionResult<Context> {
        let layout = Layout::from_size_align(size, 64).unwrap();
        let mem_space: *mut u8 = unsafe { alloc_zeroed(layout) };
        if mem_space.is_null() {
            return Err(DandelionError::MemoryAllocationError);
        }
        return Ok(Context::new(
            ContextType::Malloc(Box::new(MallocContext {
                layout,
                storage: unsafe { Box::from_raw(mem_space) },
            })),
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
    if source.layout.size() < source_offset + size {
        return Err(DandelionError::InvalidRead);
    }
    if destination.layout.size() < destination_offset + size {
        return Err(DandelionError::InvalidWrite);
    }

    let dst_slice = unsafe {
        std::slice::from_raw_parts_mut(destination.storage.as_mut(), destination.layout.size())
    };
    let src_slice =
        unsafe { std::slice::from_raw_parts(source.storage.as_ref(), source.layout.size()) };

    dst_slice[destination_offset..destination_offset + size]
        .copy_from_slice(&src_slice[source_offset..source_offset + size]);
    Ok(())
}

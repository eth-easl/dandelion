use crate::{
    memory_domain::{Context, ContextTrait, ContextType, MemoryDomain},
    util::mmap::MmapMem,
};
use dandelion_commons::{DandelionError, DandelionResult};
use log::debug;
use nix::sys::mman::ProtFlags;

#[derive(Debug)]
pub struct IOContext {
    pub storage: MmapMem,
}

impl ContextTrait for IOContext {
    fn write<T>(&mut self, offset: usize, data: &[T]) -> DandelionResult<()> {
        // check alignment
        if offset % core::mem::align_of::<T>() != 0 {
            debug!("Misaligned write at offset {}", offset);
            return Err(DandelionError::WriteMisaligned);
        }

        // check if the write is within bounds
        let write_length = data.len() * core::mem::size_of::<T>();
        if offset + write_length > self.storage.len() {
            debug!("Write out of bounds at offset {}", offset);
            return Err(DandelionError::InvalidWrite);
        }

        // write values
        unsafe {
            let buffer = core::slice::from_raw_parts(data.as_ptr() as *const u8, write_length);
            self.storage.as_slice_mut()[offset..offset + buffer.len()].copy_from_slice(&buffer);
        }

        Ok(())
    }

    fn read<T>(&self, offset: usize, read_buffer: &mut [T]) -> DandelionResult<()> {
        // check that buffer has proper allighment
        if offset % core::mem::align_of::<T>() != 0 {
            debug!("Misaligned write at offset {}", offset);
            return Err(DandelionError::ReadMisaligned);
        }

        let read_size = core::mem::size_of::<T>() * read_buffer.len();
        if offset + read_size > self.storage.len() {
            debug!("Read out of bounds at offset {}", offset);
            return Err(DandelionError::InvalidRead);
        }

        // read values, sanitize if necessary
        unsafe {
            let read_memory =
                core::slice::from_raw_parts_mut(read_buffer.as_mut_ptr() as *mut u8, read_size);
            read_memory.copy_from_slice(&self.storage.as_slice()[offset..offset + read_size]);
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct IOMemoryDomain {}

impl MemoryDomain for IOMemoryDomain {
    fn init(_config: Vec<u8>) -> DandelionResult<Box<dyn MemoryDomain>> {
        Ok(Box::new(IOMemoryDomain {}))
    }

    fn acquire_context(&self, size: usize) -> DandelionResult<Context> {
        // create and map a shared memory region
        let mem_space =
            match MmapMem::create(size, ProtFlags::PROT_READ | ProtFlags::PROT_WRITE, None) {
                Ok(v) => v,
                Err(_e) => return Err(DandelionError::OutOfMemory),
            };

        let new_context = Box::new(IOContext { storage: mem_space });
        Ok(Context::new(ContextType::IO(new_context), size))
    }
}

pub fn io_transfer(
    destination: &mut IOContext,
    source: &IOContext,
    destination_offset: usize,
    source_offset: usize,
    size: usize,
) -> DandelionResult<()> {
    // check if there is space in both contexts
    if source.storage.len() < source_offset + size {
        debug!(
            "Source out of bounds: {} < {} + {}",
            source.storage.len(),
            source_offset,
            size
        );
        return Err(DandelionError::InvalidRead);
    }
    if destination.storage.len() < destination_offset + size {
        debug!(
            "Destination out of bounds: {} < {} + {}",
            destination.storage.len(),
            destination_offset,
            size
        );
        return Err(DandelionError::InvalidWrite);
    }
    unsafe {
        destination.storage.as_slice_mut()[destination_offset..destination_offset + size]
            .copy_from_slice(&source.storage.as_slice()[source_offset..source_offset + size]);
    }
    Ok(())
}

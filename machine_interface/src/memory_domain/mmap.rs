use crate::{
    memory_domain::{Context, ContextTrait, ContextType, MemoryDomain},
    util::mmapmem::{MmapMem, MmapMemPool},
};
use dandelion_commons::{DandelionError, DandelionResult};
use log::debug;
use nix::sys::mman::ProtFlags;

use super::MemoryResource;

#[derive(Debug)]
pub struct MmapContext {
    pub storage: MmapMem,
}

impl ContextTrait for MmapContext {
    fn write<T>(&mut self, offset: usize, data: &[T]) -> DandelionResult<()> {
        self.storage.write(offset, data)
    }

    fn read<T>(&self, offset: usize, read_buffer: &mut [T]) -> DandelionResult<()> {
        self.storage.read(offset, read_buffer)
    }

    fn get_chunk_ref(&self, offset: usize, length: usize) -> DandelionResult<&[u8]> {
        self.storage.get_chunk_ref(offset, length)
    }
}

#[derive(Debug)]
pub struct MmapMemoryDomain {
    memory_pool: MmapMemPool,
}

impl MemoryDomain for MmapMemoryDomain {
    fn init(config: MemoryResource) -> DandelionResult<Box<dyn MemoryDomain>> {
        let size = match config {
            MemoryResource::Anonymous { size } => size,
            _ => {
                return Err(DandelionError::DomainError(
                    dandelion_commons::DomainError::ConfigMissmatch,
                ))
            }
        };
        let memory_pool =
            MmapMemPool::create(size, ProtFlags::PROT_READ | ProtFlags::PROT_WRITE, None)?;
        Ok(Box::new(MmapMemoryDomain { memory_pool }))
    }

    fn acquire_context(&self, size: usize) -> DandelionResult<Context> {
        // create and map a shared memory region
        let (mem_space, actual_size) = self
            .memory_pool
            .get_allocation(size, nix::sys::mman::MmapAdvise::MADV_DONTNEED)?;

        let new_context = Box::new(MmapContext { storage: mem_space });
        Ok(Context::new(ContextType::Mmap(new_context), actual_size))
    }
}

pub fn io_transfer(
    destination: &mut MmapContext,
    source: &MmapContext,
    destination_offset: usize,
    source_offset: usize,
    size: usize,
) -> DandelionResult<()> {
    // check if there is space in both contexts
    if source.storage.size() < source_offset + size {
        debug!(
            "Source out of bounds: {} < {} + {}",
            source.storage.size(),
            source_offset,
            size
        );
        return Err(DandelionError::InvalidRead);
    }
    if destination.storage.size() < destination_offset + size {
        debug!(
            "Destination out of bounds: {} < {} + {}",
            destination.storage.size(),
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

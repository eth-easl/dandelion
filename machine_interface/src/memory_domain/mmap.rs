use crate::{
    memory_domain::{Context, ContextTrait, ContextType, MemoryDomain},
    util::mmapmem::MmapMem,
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
}

#[derive(Debug)]
pub struct MmapMemoryDomain {}

impl MemoryDomain for MmapMemoryDomain {
    fn init(_config: MemoryResource) -> DandelionResult<Box<dyn MemoryDomain>> {
        Ok(Box::new(MmapMemoryDomain {}))
    }

    fn acquire_context(&self, size: usize) -> DandelionResult<Context> {
        // create and map a shared memory region
        let mem_space =
            match MmapMem::create(size, ProtFlags::PROT_READ | ProtFlags::PROT_WRITE, false) {
                Ok(v) => v,
                Err(_e) => return Err(DandelionError::MemoryAllocationError),
            };

        let new_context = Box::new(MmapContext { storage: mem_space });
        Ok(Context::new(ContextType::Mmap(new_context), size))
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

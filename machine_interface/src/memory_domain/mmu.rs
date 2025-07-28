use crate::{
    memory_domain::{Context, ContextTrait, ContextType, MemoryDomain, MemoryResource},
    util::mmapmem::{MmapMem, MmapMemPool},
};
use dandelion_commons::{DandelionError, DandelionResult};
use log::{error, warn, debug};
use nix::sys::mman::ProtFlags;

// TODO: decide this value in a system dependent way
pub const MMAP_BASE_ADDR: usize = 0x10000;

#[derive(Debug)]
pub struct MmuContext {
    pub storage: MmapMem,
}

impl ContextTrait for MmuContext {
    fn write<T>(&mut self, offset: usize, data: &[T]) -> DandelionResult<()> {
        if offset < MMAP_BASE_ADDR {
            debug!("write offset smaller than MMAP_BASE_ADDR")
            // TODO: could be an issue if the context will be used by mmu_worker (function context)
        }
        self.storage.write(offset, data)
    }

    fn read<T>(&self, offset: usize, read_buffer: &mut [T]) -> DandelionResult<()> {
        if offset < MMAP_BASE_ADDR {
            debug!("read offset smaller than MMAP_BASE_ADDR")
            // TODO: could be an issue if the context will be used by mmu_worker (function context)
        }
        self.storage.read(offset, read_buffer)
    }

    fn get_chunk_ref(&self, offset: usize, length: usize) -> DandelionResult<&[u8]> {
        if offset < MMAP_BASE_ADDR {
            debug!("read offset smaller than MMAP_BASE_ADDR")
            // TODO: could be an issue if the context will be used by mmu_worker (function context)
        }
        self.storage.get_chunk_ref(offset, length)
    }
}

#[derive(Debug)]
pub struct MmuMemoryDomain {
    memory_pool: MmapMemPool,
}

impl MemoryDomain for MmuMemoryDomain {
    fn init(config: MemoryResource) -> DandelionResult<Box<dyn MemoryDomain>> {
        let (id, size) = match config {
            MemoryResource::Shared { id, size } => (id, size),
            _ => {
                return Err(DandelionError::DomainError(
                    dandelion_commons::DomainError::ConfigMissmatch,
                ))
            }
        };
        let memory_pool =
            MmapMemPool::create(size, ProtFlags::PROT_READ | ProtFlags::PROT_WRITE, Some(id))?;
        Ok(Box::new(MmuMemoryDomain { memory_pool }))
    }

    fn acquire_context(&self, size: usize) -> DandelionResult<Context> {
        // create and map a shared memory region
        let (mem_space, actual_size) = self
            .memory_pool
            .get_allocation(size, nix::sys::mman::MmapAdvise::MADV_REMOVE)?;

        let new_context = Box::new(MmuContext { storage: mem_space });
        Ok(Context::new(ContextType::Mmu(new_context), actual_size))
    }
}

pub fn mmu_transfer(
    destination: &mut MmuContext,
    source: &MmuContext,
    destination_offset: usize,
    source_offset: usize,
    size: usize,
) -> DandelionResult<()> {
    // check if there is space in both contexts
    if source.storage.size() < source_offset + size {
        error!(
            "Out of bounds: storage_size {}, source_offset {}, size {}",
            source.storage.size(),
            source_offset,
            size
        );
        return Err(DandelionError::InvalidRead);
    }
    if destination.storage.size() < destination_offset + size {
        return Err(DandelionError::InvalidWrite);
    }
    unsafe {
        destination.storage.as_slice_mut()[destination_offset..destination_offset + size]
            .copy_from_slice(&source.storage.as_slice()[source_offset..source_offset + size]);
    }
    Ok(())
}

#[cfg(feature = "bytes_context")]
pub fn bytest_to_mmu_transfer(
    destination: &mut MmuContext,
    source: &crate::memory_domain::bytes_context::BytesContext,
    destination_offset: usize,
    source_offset: usize,
    size: usize,
) -> DandelionResult<()> {
    // check if bounds for mmu context
    if destination.storage.size() < destination_offset + size {
        return Err(DandelionError::InvalidWrite);
    }
    let mmu_slice = &mut destination.storage[destination_offset..destination_offset + size];
    source.read(source_offset, mmu_slice)?;
    Ok(())
}

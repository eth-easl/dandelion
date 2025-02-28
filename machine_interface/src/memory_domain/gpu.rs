use crate::{
    memory_domain::{Context, ContextTrait, ContextType, MemoryDomain, MemoryResource},
    util::mmapmem::{MmapMem, MmapMemPool},
};
use dandelion_commons::{DandelionError, DandelionResult};
use log::error;
use nix::sys::mman::ProtFlags;

#[derive(Debug)]
pub struct GpuContext {
    pub storage: MmapMem,
}

impl ContextTrait for GpuContext {
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
pub struct GpuMemoryDomain {
    memory_pool: MmapMemPool,
}

impl MemoryDomain for GpuMemoryDomain {
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
        Ok(Box::new(GpuMemoryDomain { memory_pool }))
    }

    fn acquire_context(&self, size: usize) -> DandelionResult<Context> {
        // create and map a shared memory region
        let (mem_space, actual_size) = self
            .memory_pool
            .get_allocation(size, nix::sys::mman::MmapAdvise::MADV_DONTNEED)?;

        let new_context = Box::new(GpuContext { storage: mem_space });
        Ok(Context::new(ContextType::Gpu(new_context), actual_size))
    }
}

pub fn gpu_transfer(
    destination: &mut GpuContext,
    source: &GpuContext,
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
pub fn bytest_to_gpu_transfer(
    destination: &mut GpuContext,
    source: &crate::memory_domain::bytes_context::BytesContext,
    destination_offset: usize,
    source_offset: usize,
    size: usize,
) -> DandelionResult<()> {
    // check if bounds for gpu context
    if destination.storage.size() < destination_offset + size {
        return Err(DandelionError::InvalidWrite);
    }
    let mmu_slice = &mut destination.storage[destination_offset..destination_offset + size];
    source.read(source_offset, mmu_slice)?;
    Ok(())
}

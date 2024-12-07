use crate::{
    memory_domain::{Context, ContextTrait, ContextType, MemoryDomain, MemoryResource},
    util::mmapmem::{self, MmapMem, MmapMemPool},
};
use dandelion_commons::{DandelionError, DandelionResult};
use libc::size_t;
use log::debug;
use nix::sys::mman::ProtFlags;

#[link(name = "cheri_lib")]
extern "C" {
    /// returns a mask for how many trailing zeros are needed in allignment
    /// the mask is all ones from the msb to the last bit that does not need to be alligned
    /// and all zero for the bits that need to be 0 in the address for it to be alligned
    fn sandbox_alignment(size: size_t) -> size_t;
}
#[derive(Debug)]
pub struct CheriContext {
    pub storage: MmapMem,
    pub cap_offset: usize,
}
unsafe impl Send for CheriContext {}
unsafe impl Sync for CheriContext {}

impl ContextTrait for CheriContext {
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

pub struct CheriMemoryDomain {
    memory_pool: MmapMemPool,
}

impl MemoryDomain for CheriMemoryDomain {
    fn init(config: MemoryResource) -> DandelionResult<Box<dyn MemoryDomain>> {
        let size = match config {
            MemoryResource::Anonymous { size } => size,
            _ => {
                return Err(DandelionError::DomainError(
                    dandelion_commons::DomainError::ConfigMissmatch,
                ))
            }
        };
        let memory_pool = MmapMemPool::create(
            size,
            ProtFlags::PROT_READ | ProtFlags::PROT_WRITE | ProtFlags::PROT_EXEC,
            None,
        )?;
        Ok(Box::new(CheriMemoryDomain { memory_pool }))
    }

    fn acquire_context(&self, size: usize) -> DandelionResult<Context> {
        // check the capability allighnement requirements
        let allignment = (!unsafe { sandbox_alignment(size) }) + 1;
        let additional_space = if allignment < mmapmem::SLAB_SIZE {
            0
        } else {
            allignment
        };
        let allocation_size = size
            .checked_add(additional_space)
            .ok_or(DandelionError::OutOfMemory)?;
        let (storage, actual_size) = self
            .memory_pool
            .get_allocation(allocation_size, nix::sys::mman::MmapAdvise::MADV_DONTNEED)?;
        // determine how much we need to offset to be cap alligned
        let cap_offset = storage.as_ptr().align_offset(allignment);
        let new_context = Box::new(CheriContext {
            storage,
            cap_offset,
        });
        Ok(Context::new(ContextType::Cheri(new_context), actual_size))
    }
}

pub fn cheri_transfer(
    destination: &mut CheriContext,
    source: &CheriContext,
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

use crate::{
    memory_domain::{Context, ContextTrait, ContextType, MemoryDomain},
    util::mmap::MmapMem,
};
use dandelion_commons::{DandelionError, DandelionResult};
use log::warn;
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
            warn!("write offset smaller than MMAP_BASE_ADDR")
            // TODO: could be an issue if the context will be used by mmu_worker (function context)
        }

        // check alignment
        if offset % core::mem::align_of::<T>() != 0 {
            return Err(DandelionError::WriteMisaligned);
        }

        // check if the write is within bounds
        let write_length = data.len() * core::mem::size_of::<T>();
        if offset + write_length > self.storage.len() {
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
        if offset < MMAP_BASE_ADDR {
            warn!("read offset smaller than MMAP_BASE_ADDR")
            // TODO: could be an issue if the context will be used by mmu_worker (function context)
        }

        // check that buffer has proper allighment
        if offset % core::mem::align_of::<T>() != 0 {
            return Err(DandelionError::ReadMisaligned);
        }

        let read_size = core::mem::size_of::<T>() * read_buffer.len();
        if offset + read_size > self.storage.len() {
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
pub struct MmuMemoryDomain {}

impl MemoryDomain for MmuMemoryDomain {
    fn init(_config: Vec<u8>) -> DandelionResult<Box<dyn MemoryDomain>> {
        Ok(Box::new(MmuMemoryDomain {}))
    }

    fn acquire_context(&self, size: usize) -> DandelionResult<Context> {
        // create and map a shared memory region
        let filename = format!("/shmem_{:X}", rand::random::<u64>());
        let mem_space = match MmapMem::create(
            size,
            ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
            Some(filename),
        ) {
            Ok(v) => v,
            Err(_e) => return Err(DandelionError::OutOfMemory),
        };

        let new_context = Box::new(MmuContext { storage: mem_space });
        Ok(Context::new(ContextType::Mmu(new_context), size))
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
    if source.storage.len() < source_offset + size {
        return Err(DandelionError::InvalidRead);
    }
    if destination.storage.len() < destination_offset + size {
        return Err(DandelionError::InvalidWrite);
    }
    unsafe {
        destination.storage.as_slice_mut()[destination_offset..destination_offset + size]
            .copy_from_slice(&source.storage.as_slice()[source_offset..source_offset + size]);
    }
    Ok(())
}

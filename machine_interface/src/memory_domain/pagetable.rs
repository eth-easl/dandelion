use crate::memory_domain::{Context, ContextTrait, ContextType, MemoryDomain};
use crate::util::shared_mem::SharedMem;
use crate::Position;
use dandelion_commons::{DandelionError, DandelionResult};
use nix::sys::mman::ProtFlags;

// TODO: decide this value in a system dependent way
pub const MMAP_BASE_ADDR: usize = 0x10000;

#[derive(Debug)]
pub struct PagetableContext {
    pub storage: SharedMem,
}

impl ContextTrait for PagetableContext {
    fn write<T>(&mut self, offset: usize, data: &[T]) -> DandelionResult<()> {
        if offset < MMAP_BASE_ADDR {
            // not an issue if this context is not to be used by pagetable_worker
            eprintln!("[WARNING] write to an offset smaller than MMAP_BASE_ADDR");
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
            // not an issue if this context is not to be used by pagetable_worker
            eprintln!("[WARNING] read from an offset smaller than MMAP_BASE_ADDR");
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
pub struct PagetableMemoryDomain {}

impl MemoryDomain for PagetableMemoryDomain {
    fn init(_config: Vec<u8>) -> DandelionResult<Box<dyn MemoryDomain>> {
        Ok(Box::new(PagetableMemoryDomain {}))
    }

    fn acquire_context(&self, size: usize) -> DandelionResult<Context> {
        // create and map a shared memory region
        let mem_space = match SharedMem::create(size, ProtFlags::PROT_READ | ProtFlags::PROT_WRITE)
        {
            Ok(v) => v,
            Err(_e) => return Err(DandelionError::OutOfMemory),
        };

        Ok(Context {
            context: ContextType::Pagetable(Box::new(PagetableContext { storage: mem_space })),
            content: vec![],
            size,
            occupation: vec![
                Position { offset: 0, size: 0 },
                Position {
                    offset: size,
                    size: 0,
                },
            ],
            #[cfg(feature = "pagetable")]
            protection_requirements: Vec::new(),
        })
    }
}

pub fn pagetable_transfer(
    destination: &mut PagetableContext,
    source: &PagetableContext,
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

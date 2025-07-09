use crate::{
    memory_domain::{Context, ContextTrait, ContextType, MemoryDomain, MemoryResource},
    util::mmapmem::{MmapMem, MmapMemPool},
};
use dandelion_commons::{DandelionError, DandelionResult};
use log::error;
use nix::sys::mman::ProtFlags;

use crate::memory_domain::system_domain::SystemContext;
use std::collections::BTreeMap;

#[derive(Debug)]
pub struct GpuContext {
    pub storage: MmapMem,
    pub function_context: SystemContext,
}

impl ContextTrait for GpuContext {
    fn write<T>(&mut self, offset: usize, data: &[T]) -> DandelionResult<()> {
        println!("HERE - write, {offset}");
        self.storage.write(offset, data)
    }

    fn read<T>(&self, offset: usize, read_buffer: &mut [T]) -> DandelionResult<()> {
        println!("HERE - read, {offset}");
        self.storage.read(offset, read_buffer)
        // self.function_context.read(offset, read_buffer)
    }

    fn get_chunk_ref(&self, offset: usize, length: usize) -> DandelionResult<&[u8]> {
        println!("HERE - ref, {offset} - {length}");
        self.storage.get_chunk_ref(offset, length)
        // self.function_context.get_chunk_ref(offset, length)
    }
}

use log::debug;
use nix::sys::mman::shm_open;
use nix::sys::stat::Mode;
use nix::sys::stat::fstat;
use nix::sys::mman::mmap;
use nix::fcntl::OFlag;
use std::num::NonZeroUsize;
use nix::sys::mman::MapFlags;
use dandelion_commons::DandelionError::NotImplemented;
use std::ops::Deref;
use std::ops::DerefMut;

#[derive(Debug)]
pub struct GpuProcessContext {
    // pub storage: MmapMem,
    pub ptr: *mut u8,
    pub size: usize,
}

impl GpuProcessContext {
    fn size(&self) -> usize {
        self.size
    }

    pub fn as_ptr(&self) -> *mut u8 {
        self.ptr
    }

    pub unsafe fn as_slice(&self) -> &[u8] {
        std::slice::from_raw_parts(self.as_ptr(), self.size())
    }

    pub unsafe fn as_slice_mut(&mut self) -> &mut [u8] {
        std::slice::from_raw_parts_mut(self.as_ptr(), self.size())
    }
}

impl ContextTrait for GpuProcessContext {
    fn write<T>(&mut self, offset: usize, data: &[T]) -> DandelionResult<()> {
        // check alignment
        if offset % core::mem::align_of::<T>() != 0 {
            debug!("Misaligned write at offset {}", offset);
            return Err(DandelionError::WriteMisaligned);
        }

        // check if the write is within bounds
        let write_length = data.len() * core::mem::size_of::<T>();
        if offset + write_length > self.size() {
            debug!("Write out of bounds at offset {}", offset);
            return Err(DandelionError::InvalidWrite);
        }

        // write values
        unsafe {
            let buffer = core::slice::from_raw_parts(data.as_ptr() as *const u8, write_length);
            self.as_slice_mut()[offset..offset + buffer.len()].copy_from_slice(&buffer);
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
        if offset + read_size > self.size() {
            eprintln!(
                "InvalidRead in MMM: len {}, offset {}, size {}",
                self.size(),
                offset,
                read_size
            );
            debug!("Read out of bounds at offset {}", offset);
            return Err(DandelionError::InvalidRead);
        }

        // read values, sanitize if necessary
        unsafe {
            let read_memory =
                core::slice::from_raw_parts_mut(read_buffer.as_mut_ptr() as *mut u8, read_size);
            read_memory.copy_from_slice(&self.as_slice()[offset..offset + read_size]);
        }

        Ok(())
    }

    fn get_chunk_ref(&self, offset: usize, length: usize) -> DandelionResult<&[u8]> {
        if offset + length > self.size() {
            return Err(DandelionError::InvalidRead);
        }
        return Ok(unsafe { &self.as_slice()[offset..offset + length] });
    }
}

unsafe impl Send for GpuProcessContext {}
unsafe impl Sync for GpuProcessContext {}

impl Deref for GpuProcessContext {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { self.as_slice() }
    }
}

impl DerefMut for GpuProcessContext {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.as_slice_mut() }
    }
}

/*pub const SLAB_SIZE: usize = 1 << 20;
impl Drop for GpuProcessContext {
    fn drop(&mut self) {
        let start = usize::try_from(unsafe { self.ptr.offset_from(self.origin.ptr) }).unwrap();
        let start_slab = u32::try_from(start / SLAB_SIZE).unwrap();
        let slab_number = u32::try_from(self.size / SLAB_SIZE).unwrap();
        self.origin
            .occupation
            .lock()
            .unwrap()
            .insert(start_slab, start_slab + slab_number);
    }
}*/


#[derive(Debug)]
pub struct GpuMemoryDomain {
    memory_pool: MmapMemPool,
}

/*use crate::memory_domain::system_domain::SystemContext;
use crate::memory_domain::system_domain::SystemMemoryDomain;
use std::collections::BTreeMap;*/

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

        let function_context = SystemContext {
            local_offset_to_data_position: BTreeMap::new(),
            size, // TODO : change this size
        };

        let cum_size = actual_size + size;

        let new_context = Box::new(GpuContext { storage: mem_space, function_context });
        Ok(Context::new(ContextType::Gpu(new_context), cum_size))
    }

    /*fn init(_config: MemoryResource) -> DandelionResult<Box<dyn MemoryDomain>> {
        Ok(Box::new(SystemMemoryDomain {}))
    }

    fn acquire_context(&self, size: usize) -> DandelionResult<Context> {
        let new_context = Box::new(SystemContext {
            local_offset_to_data_position: BTreeMap::new(),
            size,
        });
        Ok(Context::new(ContextType::System(new_context), size))
    }*/
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

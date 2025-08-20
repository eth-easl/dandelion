use crate::{
    memory_domain::{Context, ContextTrait, ContextType, MemoryDomain, MemoryResource},
    util::mmapmem::{MmapMem, MmapMemPool},
    Position,
};
use dandelion_commons::{
    DandelionError,
    DandelionResult,
};
use log::{debug, error};
use nix::sys::mman::ProtFlags;
use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
    sync::Arc,
};

#[derive(Debug)]
pub struct SubReadOnly {
    pub context: Arc<Context>,
    pub position: Position,
}

#[derive(Debug)]
pub struct SubBytes {
    pub context: Arc<Context>,
    pub position: Position,
}

#[derive(Debug)]
pub struct GpuContext {
    pub storage: MmapMem,
    pub read_only: HashMap<String, SubReadOnly>,
    pub inputs: HashMap<String, SubBytes>,
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

        let new_context = Box::new(GpuContext {
            storage: mem_space,
            read_only: HashMap::new(),
            inputs: HashMap::new(),
        });
        Ok(Context::new(ContextType::Gpu(new_context), actual_size))
    }
}

pub fn read_only_to_gpu_transfer(
    destination_ctxt: &mut GpuContext,
    source: Arc<Context>,
    destination_offset: usize,
    source_offset: usize,
    size: usize,
) -> DandelionResult<()> {
    let Some(ref data_set) = source.content[0] else { todo!() };
    let ident = &data_set.ident.to_string();
    
    #[cfg(feature = "weights_from_disk")]
    {
        use crate::{DataSet, DataItem};
        use crate::memory_domain::read_only::ReadOnlyContext;
        let ContextType::ReadOnly(source_ctxt) = &source.context else { todo!() };
        let disk_path = source_ctxt.disk_path.clone().unwrap();
        let split_path = disk_path.split("/").collect::<Vec<&str>>();
        let name = split_path[split_path.len() - 1].to_string();
        let data_vec = std::fs::read(&disk_path).unwrap();
        let item_size = data_vec.len();
        let mut new_context =
            ReadOnlyContext::new(data_vec.into_boxed_slice()).unwrap();
        new_context.content.push(Some(DataSet {
            ident: ident.clone(),
            buffers: vec![DataItem {
                ident: ident.clone(),
                data: Position {
                    offset: 0,
                    size: item_size,
                },
                key: 0,
            }],
        }));
        let new_source = Arc::new(new_context);

        destination_ctxt.read_only.insert(
            ident.clone(),
            SubReadOnly {
                context: new_source,
                position: Position {
                    offset: source_offset,
                    size
                }
            }
        );
    }

    #[cfg(not(feature = "weights_from_disk"))]
    {
        destination_ctxt.read_only.insert(
            ident.clone(),
            SubReadOnly {
                context: source,
                position: Position {
                    offset: source_offset,
                    size
                }
            }
        );
    }
    
    Ok(())
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
pub fn bytes_to_gpu_transfer(
    destination_ctxt: &mut GpuContext,
    source: Arc<Context>,
    destination_offset: usize,
    source_offset: usize,
    size: usize,
) -> DandelionResult<()> {
    let Some(ref data_set) = source.content[0] else { todo!() };
    let ident = &data_set.ident.to_string();

    destination_ctxt.inputs.insert(
        ident.clone(),
        SubBytes {
            context: source,
            position: Position {
                offset: source_offset,
                size
            }
        }
    );
    Ok(())
}

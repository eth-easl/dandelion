use crate::memory_domain::{Context, ContextTrait, ContextType, MemoryDomain};
use dandelion_commons::{DandelionError, DandelionResult};

mod wasm_memory_allocation {
    use dandelion_commons::DandelionError;
    use libc::{mmap, munmap};
    use std::ops::Deref;
    use std::ops::DerefMut;

    /// A smart pointer for memory mapped memory.
    /// It makes sure that the memory is unmapped when 
    /// it is dropped, and prevents Rust from trying to
    /// free the memory through the global allocator.
    #[derive(Debug)]
    pub struct MmapBox {
        ptr: *mut u8,
        size: usize,
    }

    impl MmapBox {
        pub fn new(size: usize) -> Result<MmapBox, DandelionError> {
            let addr = unsafe { mmap(
                0 as *mut _,
                size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            ) };
            match addr {
                libc::MAP_FAILED => {
                    return Err(DandelionError::OutOfMemory);
                },
                _ => {}
            }
            Ok(MmapBox { ptr: addr as *mut u8, size })
        }
    }

    impl Deref for MmapBox {
        type Target = [u8];

        fn deref(&self) -> &Self::Target {
            unsafe { std::slice::from_raw_parts(self.ptr, self.size) }
        }
    }

    impl DerefMut for MmapBox {
        fn deref_mut(&mut self) -> &mut Self::Target {
            unsafe { std::slice::from_raw_parts_mut(self.ptr, self.size) }
        }
    }

    impl Drop for MmapBox {
        fn drop(&mut self) {
            unsafe { munmap(self.ptr as *mut _, self.size); }
        }
    }
}

use wasm_memory_allocation::MmapBox;

#[derive(Debug)]
pub struct WasmContext {
    pub mem: MmapBox,
}

unsafe impl Send for WasmContext {}
unsafe impl Sync for WasmContext {}

impl ContextTrait for WasmContext {
    fn write<T>(&mut self, offset: usize, data: &[T]) -> DandelionResult<()> {
        let write_size = data.len() * std::mem::size_of::<T>();
        if offset + write_size > self.mem.len() {
            return Err(DandelionError::InvalidWrite);
        }
        let data_bytes: &[u8] = unsafe {
            std::slice::from_raw_parts(data.as_ptr() as *const u8, write_size)
        };
        self.mem[offset..offset+write_size].copy_from_slice(data_bytes);
        Ok(())        
    }
    fn read<T>(&self, offset: usize, read_buffer: &mut [T]) -> DandelionResult<()> {
        let self_data = &self.mem;
        let read_size = read_buffer.len() * std::mem::size_of::<T>();
        if offset + read_size <= self_data.len() {
            let data_bytes: &[u8] = &self_data[offset..offset+read_size];
            unsafe {
                (read_buffer.as_mut_ptr() as *mut u8).copy_from(data_bytes.as_ptr(), read_size)
            };
            Ok(())
        } else {
            Err(DandelionError::InvalidRead)
        }
    }
}

#[derive(Debug)]
pub struct WasmMemoryDomain {}

impl MemoryDomain for WasmMemoryDomain {
    fn init(_config: Vec<u8>) -> DandelionResult<Box<dyn MemoryDomain>> {
        Ok(Box::new(WasmMemoryDomain {}))
    }
    fn acquire_context(&self, size: usize) -> DandelionResult<Context> {
        Ok(Context::new(
            ContextType::Wasm(Box::new(
                WasmContext {
                    mem: MmapBox::new(size)?,
                }
            )), 
            size
        ))
    }
}

pub fn wasm_transfer(
    destination: &mut WasmContext,
    source: &WasmContext,
    destination_offset: usize,
    source_offset: usize,
    size: usize,
) -> DandelionResult<()> {
    if source_offset + size > source.mem.len() {
        return Err(DandelionError::InvalidRead);
    }
    destination.mem[destination_offset..destination_offset + size]
        .copy_from_slice(&source.mem[source_offset..source_offset + size]);
    Ok(())
}

use crate::memory_domain::{Context, ContextTrait, ContextType, MemoryDomain};
use crate::util::mmapmem::MmapMem;
use dandelion_commons::{DandelionError, DandelionResult};
use nix::sys::mman::ProtFlags;

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
    pub mem: MmapMem,
}

unsafe impl Send for WasmContext {}
unsafe impl Sync for WasmContext {}

impl ContextTrait for WasmContext {
    fn write<T>(&mut self, offset: usize, data: &[T]) -> DandelionResult<()> {
        self.mem.write(offset, data)
    }
    fn read<T>(&self, offset: usize, read_buffer: &mut [T]) -> DandelionResult<()> {
        self.mem.read(offset, read_buffer)
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
            ContextType::Wasm(Box::new(WasmContext {
                mem: MmapMem::create(size, ProtFlags::PROT_READ | ProtFlags::PROT_WRITE, false)?,
            })),
            size,
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
    if source_offset + size > source.mem.size() {
        return Err(DandelionError::InvalidRead);
    }
    destination.mem[destination_offset..destination_offset + size]
        .copy_from_slice(&source.mem[source_offset..source_offset + size]);
    Ok(())
}

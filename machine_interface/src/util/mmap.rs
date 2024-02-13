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

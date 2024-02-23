use dandelion_commons::{DandelionError, DandelionResult};
use log::{debug, error};
use nix::{
    fcntl::OFlag,
    sys::{
        mman::{mmap, munmap, shm_open, shm_unlink, MapFlags, ProtFlags},
        stat::{fstat, Mode},
    },
    unistd::{close, ftruncate},
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::{
    num::NonZeroUsize,
    ops::{Deref, DerefMut},
    os::fd::RawFd,
};

/// A smart pointer for memory mapped memory.
/// It makes sure that the memory is unmapped when
/// it is dropped, and prevents Rust from trying to
/// free the memory through the global allocator.
#[derive(Debug)]
pub struct MmapMem {
    ptr: *mut u8,
    size: usize,
    fd: RawFd,
    filename: Option<String>,
}

static COUNTER: AtomicU64 = AtomicU64::new(0);

impl MmapMem {
    // Create a memory-mapped file with the given size and protection flags.
    // If a filename is given, memory will be backed by that file,
    // otherwise it will be backed by an anonymous file.
    pub fn create(size: usize, prot: ProtFlags, shared: bool) -> Result<Self, DandelionError> {
        let mut filename = None;
        let (fd, map_flags) = if shared {
            filename = Some(format!("/shm_{:X}", COUNTER.fetch_add(1, Ordering::SeqCst)));
            log::trace!("ctx filename: {:?}", filename);
            let fd = match shm_open(
                filename.as_ref().unwrap().as_str(),
                OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_RDWR,
                Mode::S_IRUSR | Mode::S_IWUSR,
            ) {
                Err(err) => {
                    error!("Error creating shared memory file: {}", err);
                    return Err(DandelionError::MemoryAllocationError);
                }
                fd => fd.unwrap(),
            };

            match ftruncate(fd, size as _) {
                Err(err) => {
                    close(fd).unwrap();
                    shm_unlink(filename.unwrap().as_str()).unwrap();
                    error!("Error creating shared memory file: {}", err);
                    return Err(DandelionError::MemoryAllocationError);
                }
                _ => {}
            };

            (fd, MapFlags::MAP_SHARED)
        } else {
            (-1, MapFlags::MAP_PRIVATE | MapFlags::MAP_ANONYMOUS)
        };

        // Map the memory.
        let ptr = unsafe {
            match mmap(
                None,
                NonZeroUsize::new(size).unwrap(),
                prot,
                map_flags,
                fd,
                0,
            ) {
                Err(err) => {
                    if shared {
                        close(fd).unwrap();
                        shm_unlink(filename.unwrap().as_str()).unwrap();
                    }
                    error!("Error mapping memory: {}:{}", err, err.desc());
                    return Err(DandelionError::MemoryAllocationError);
                }
                Ok(ptr) => ptr as *mut _,
            }
        };

        let shmem = MmapMem {
            ptr,
            size,
            fd,
            filename,
        };

        Ok(shmem)
    }

    // Memory-maps a file and protection flags, at the given address.
    pub fn open(filename: &str, prot: ProtFlags, addr: usize) -> Result<MmapMem, DandelionError> {
        let shmem_fd = match shm_open(filename, OFlag::O_RDWR, Mode::S_IRUSR) {
            Err(err) => {
                error!("Error opening shared memory file: {}:{}", err, err.desc());
                return Err(DandelionError::FileError);
            }
            Ok(fd) => fd,
        };

        let size = match fstat(shmem_fd) {
            Err(err) => {
                error!("Error getting file stats: {}:{}", err, err.desc());
                return Err(DandelionError::FileError);
            }
            Ok(stat) => stat.st_size as usize,
        };

        let ptr = unsafe {
            match mmap(
                NonZeroUsize::new(addr),
                NonZeroUsize::new(size - addr).unwrap(),
                prot,
                MapFlags::MAP_SHARED | MapFlags::MAP_FIXED_NOREPLACE,
                shmem_fd,
                addr.try_into().unwrap(),
            ) {
                Err(err) => {
                    error!(
                        "Error mapping memory from file {} at address {}: {}:{}",
                        filename,
                        addr,
                        err,
                        err.desc()
                    );
                    return Err(DandelionError::MemoryAllocationError);
                }
                Ok(ptr) => ptr as *mut _,
            }
        };

        let shmem = MmapMem {
            ptr,
            size,
            fd: shmem_fd,
            filename: Some(filename.to_string()),
        };

        Ok(shmem)
    }

    // Returns the complete path of the file backing the memory.
    pub fn filename(&self) -> Option<&str> {
        match &self.filename {
            Some(filename) => Some(filename),
            None => None,
        }
    }

    pub fn size(&self) -> usize {
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

    pub fn write<T>(&mut self, offset: usize, data: &[T]) -> DandelionResult<()> {
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

    pub fn read<T>(&self, offset: usize, read_buffer: &mut [T]) -> DandelionResult<()> {
        // check that buffer has proper allighment
        if offset % core::mem::align_of::<T>() != 0 {
            debug!("Misaligned write at offset {}", offset);
            return Err(DandelionError::ReadMisaligned);
        }

        let read_size = core::mem::size_of::<T>() * read_buffer.len();
        if offset + read_size > self.size() {
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
}

unsafe impl Send for MmapMem {}
unsafe impl Sync for MmapMem {}

impl Deref for MmapMem {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { self.as_slice() }
    }
}

impl DerefMut for MmapMem {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.as_slice_mut() }
    }
}

impl Drop for MmapMem {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe { munmap(self.ptr as *mut _, self.size).unwrap() };
        }
        if let Some(filename) = &self.filename {
            close(self.fd).unwrap();
            shm_unlink(filename.as_str()).unwrap();
        }
    }
}

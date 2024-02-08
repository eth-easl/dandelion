use dandelion_commons::DandelionError;
use log::error;
use nix::{
    fcntl::OFlag,
    sys::{
        mman::{mmap, munmap, shm_open, shm_unlink, MapFlags, ProtFlags},
        stat::{fstat, Mode},
    },
    unistd::{close, ftruncate},
};
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

impl MmapMem {
    // Create a memory-mapped file with the given size and protection flags.
    // If a filename is given, memory will be backed by that file,
    // otherwise it will be backed by an anonymous file.
    pub fn create(
        size: usize,
        prot: ProtFlags,
        filename: Option<String>,
    ) -> Result<Self, DandelionError> {
        // If a filename is given, create a shared memory file with the given name.
        // Otherwise, skip this step.
        let (fd, file, map_flags) = match filename {
            Some(filename) => {
                let file = filename.to_string();
                let fd = match shm_open(
                    file.as_str(),
                    OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_RDWR,
                    Mode::S_IRUSR | Mode::S_IWUSR,
                ) {
                    Err(err) => {
                        error!("Error creating shared memory file: {}:{}", err, err.desc());
                        return Err(DandelionError::MemoryAllocationError);
                    }
                    fd => fd.unwrap(),
                };

                match ftruncate(fd, size as _) {
                    Err(err) => {
                        error!("Error creating shared memory file: {}:{}", err, err.desc());
                        return Err(DandelionError::MemoryAllocationError);
                    }
                    _ => {}
                };

                (fd, Some(file), MapFlags::MAP_SHARED)
            }
            None => (
                -1 as RawFd,
                None,
                MapFlags::MAP_PRIVATE | MapFlags::MAP_ANONYMOUS,
            ),
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
            filename: file.clone(),
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

        if self.fd != -1 {
            close(self.fd).unwrap();
            let file = self.filename.clone();
            shm_unlink(file.unwrap().as_str()).unwrap();
        }
    }
}

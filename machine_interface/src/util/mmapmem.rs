use dandelion_commons::{range_pool::RangePool, DandelionError, DandelionResult, DomainError};
use log::{debug, error};
use nix::{
    fcntl::OFlag,
    sys::{
        mman::{madvise, mmap, munmap, shm_open, shm_unlink, MapFlags, MmapAdvise, ProtFlags},
        stat::Mode,
    },
    unistd::{close, ftruncate},
};
use std::{
    num::NonZeroUsize,
    ops::{Deref, DerefMut},
    os::{fd::RawFd, raw::c_void},
    sync::{Arc, Mutex},
};

/// Internal structure for the memory pool memory to own the allocation.
/// This is created when mapping and will unmap when dropped.
#[derive(Debug)]
struct MmapMemPoolInternal {
    ptr: *mut u8,
    size: usize,
    fd: RawFd,
    filename: Option<String>,
    occupation: Mutex<RangePool<u32>>,
}
unsafe impl Sync for MmapMemPoolInternal {}
unsafe impl Send for MmapMemPoolInternal {}

impl Drop for MmapMemPoolInternal {
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

#[derive(Debug)]
pub struct MmapMemPool {
    internal: Arc<MmapMemPoolInternal>,
}

// Minimum allocation granularity
pub const SLAB_SIZE: usize = 1 << 20;

impl MmapMemPool {
    // Create a memory-mapped file with the given size and protection flags.
    // If a filename is given, memory will be backed by that file,
    // otherwise it will be backed by an anonymous file.
    pub fn create(
        size: usize,
        prot: ProtFlags,
        shared: Option<u64>,
    ) -> Result<Self, DandelionError> {
        // use 1MB for minimal allocation granularity and u32 to keep track of them,
        assert!(size < (u32::MAX as usize) * SLAB_SIZE);
        let upper_end = u32::try_from(size / SLAB_SIZE)
            .expect("Total memory pool should be smaller for current u32 setup");
        let occupation = Mutex::new(RangePool::new(0..upper_end));
        if size == 0 {
            return Ok(MmapMemPool {
                internal: Arc::new(MmapMemPoolInternal {
                    ptr: core::ptr::null_mut(),
                    size,
                    fd: -1,
                    filename: None,
                    occupation,
                }),
            });
        }
        let mut filename = None;
        let (fd, map_flags) = if let Some(shared_id) = shared {
            let filename_string = format!("/shm_{:X}", shared_id);
            log::trace!("ctx filename: {:?}", filename);
            let fd = match shm_open(
                filename_string.as_str(),
                OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_RDWR,
                Mode::S_IRUSR | Mode::S_IWUSR,
            ) {
                Err(err) => {
                    error!("Error creating shared memory file: {}", err);
                    return Err(DandelionError::DomainError(DomainError::SharedOpen));
                }
                fd => fd.unwrap(),
            };

            match ftruncate(fd, size as _) {
                Err(err) => {
                    close(fd).unwrap();
                    shm_unlink(filename_string.as_str()).unwrap();
                    error!("Error creating shared memory file: {}", err);
                    return Err(DandelionError::DomainError(DomainError::SharedTrunc));
                }
                _ => {}
            };
            filename = Some(filename_string);
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
                    if let Some(filename_string) = filename {
                        close(fd).unwrap();
                        shm_unlink(filename_string.as_str()).unwrap();
                    }
                    error!("Error mapping memory: {}:{}", err, err.desc());
                    return Err(DandelionError::DomainError(DomainError::Mapping));
                }
                Ok(ptr) => ptr as *mut _,
            }
        };

        let backing_mem = Arc::new(MmapMemPoolInternal {
            ptr,
            size,
            fd,
            filename,
            occupation,
        });

        Ok(MmapMemPool {
            internal: backing_mem,
        })
    }

    pub fn get_allocation(
        &self,
        requested_size: usize,
        cleaning_flags: MmapAdvise,
    ) -> DandelionResult<(MmapMem, usize)> {
        // check requested size is smaller than max avaiable
        if requested_size > self.internal.size {
            return Err(DandelionError::DomainError(DomainError::InvalidMemorySize));
        }
        // check if space is available
        let occupation_size = u32::try_from((requested_size + SLAB_SIZE - 1) / SLAB_SIZE)
            .expect("Allocation size should be within representable sizes");
        let lenght = usize::try_from(occupation_size).unwrap() * SLAB_SIZE;
        let start_slab = usize::try_from({
            let mut occupation = self.internal.occupation.lock().unwrap();
            occupation
                .get(occupation_size, u32::MIN)
                .ok_or(DandelionError::DomainError(DomainError::ReachedCapacity))?
        })
        .unwrap();
        let start_address = unsafe { self.internal.ptr.add(start_slab * SLAB_SIZE) };
        // clean memory
        unsafe { madvise(start_address as *mut c_void, lenght, cleaning_flags) }.or(Err(
            DandelionError::DomainError(DomainError::CleaningFailure),
        ))?;
        return Ok((
            MmapMem {
                ptr: start_address,
                size: lenght,
                origin: self.internal.clone(),
            },
            lenght,
        ));
    }
}

/// A smart pointer for memory mapped memory.
/// It makes sure that the memory is returned to the pool when dropped.
/// Prevents Rust from trying to deallocate through the global allocator.
#[derive(Debug)]
pub struct MmapMem {
    ptr: *mut u8,
    size: usize,
    origin: Arc<MmapMemPoolInternal>,
}

impl MmapMem {
    // Returns the complete path of the file backing the memory.
    pub fn filename(&self) -> Option<&str> {
        match &self.origin.filename {
            Some(filename) => Some(filename.as_str()),
            None => None,
        }
    }

    pub fn offset(&self) -> i64 {
        i64::try_from(unsafe { self.ptr.offset_from(self.origin.ptr) }).unwrap()
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

    pub fn get_chunk_ref(&self, offset: usize, length: usize) -> DandelionResult<&[u8]> {
        if offset + length > self.size() {
            return Err(DandelionError::InvalidRead);
        }
        return Ok(unsafe { &self.as_slice()[offset..offset + length] });
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
        let start = usize::try_from(unsafe { self.ptr.offset_from(self.origin.ptr) }).unwrap();
        let start_slab = u32::try_from(start / SLAB_SIZE).unwrap();
        let slab_number = u32::try_from(self.size / SLAB_SIZE).unwrap();
        self.origin
            .occupation
            .lock()
            .unwrap()
            .insert(start_slab, start_slab + slab_number);
    }
}

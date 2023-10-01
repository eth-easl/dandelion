use nix::errno::Errno;
use nix::fcntl::OFlag;
use nix::sys::mman::{mmap, munmap, shm_open, shm_unlink, MapFlags, ProtFlags};
use nix::sys::stat::{fstat, Mode};
use nix::unistd::{close, ftruncate};
use std::num::NonZeroUsize;
use std::os::fd::RawFd;
use std::ptr::null_mut;

#[derive(Debug)]
pub struct SharedMem {
    unique_id: String,
    size: usize,
    shmem_fd: RawFd,
    map_ptr: *mut u8,
    owner: bool,
}

unsafe impl Send for SharedMem {}
unsafe impl Sync for SharedMem {}

impl SharedMem {
    pub fn create(size: usize, prot: ProtFlags) -> Result<Self, Errno> {
        let unique_id = format!("/shmem_{:X}", rand::random::<u64>());

        let shmem_fd = shm_open(
            unique_id.as_str(),
            OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_RDWR,
            Mode::S_IRUSR | Mode::S_IWUSR,
        )?;

        let mut shmem = SharedMem {
            unique_id,
            size,
            shmem_fd,
            map_ptr: null_mut(),
            owner: true,
        };

        ftruncate(shmem_fd, size as _)?;

        shmem.map_ptr = unsafe {
            mmap(
                None,
                NonZeroUsize::new(size).unwrap(),
                prot,
                MapFlags::MAP_SHARED,
                shmem_fd,
                0,
            )? as *mut _
        };

        Ok(shmem)
    }

    pub fn open(unique_id: &str, prot: ProtFlags, addr:usize) -> Result<SharedMem, Errno> {
        let shmem_fd = shm_open(unique_id, OFlag::O_RDWR, Mode::S_IRUSR)?;

        let mut shmem = SharedMem {
            unique_id: String::from(unique_id),
            size: 0,
            shmem_fd,
            map_ptr: null_mut(),
            owner: false,
        };

        shmem.size = fstat(shmem_fd)?.st_size as usize;

        shmem.map_ptr = unsafe {
            mmap(
                NonZeroUsize::new(addr),
                NonZeroUsize::new(shmem.size).unwrap(),
                prot,
                MapFlags::MAP_SHARED | MapFlags::MAP_FIXED_NOREPLACE,
                shmem_fd,
                addr.try_into().unwrap(),
            )? as *mut _
        };

        Ok(shmem)
    }

    pub fn id(&self) -> &str {
        &self.unique_id
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub fn as_ptr(&self) -> *mut u8 {
        self.map_ptr
    }

    pub unsafe fn as_slice(&self) -> &[u8] {
        std::slice::from_raw_parts(self.as_ptr(), self.len())
    }

    pub unsafe fn as_slice_mut(&mut self) -> &mut [u8] {
        std::slice::from_raw_parts_mut(self.as_ptr(), self.len())
    }
}

impl Drop for SharedMem {
    fn drop(&mut self) {
        if !self.map_ptr.is_null() {
            unsafe { munmap(self.map_ptr as *mut _, self.size).unwrap() };
        }

        if self.shmem_fd != 0 {
            close(self.shmem_fd).unwrap();
            if self.owner {
                shm_unlink(self.unique_id.as_str()).unwrap();
            }
        }
    }
}

use crate::memory_domain::{Context, ContextTrait, ContextType, MemoryDomain};
use dandelion_commons::{range_pool::RangePool, DandelionError, DandelionResult};
use log::debug;
use nix::{
    sys::{
        memfd::{memfd_create, MemFdCreateFlag},
        mman::{MapFlags, ProtFlags},
    },
    unistd::ftruncate,
};
use std::{
    cmp::min,
    collections::BTreeMap,
    ffi::{c_void, CString},
    num::NonZeroUsize,
    os::fd::RawFd,
    str::FromStr,
    sync::{Arc, Mutex},
};

use super::MemoryResource;

// Minimum allocation granularity
pub const SLAB_SIZE: usize = 1 << 12;

#[derive(Debug)]
pub struct OverlayItem {
    pub context: Arc<Context>,
    pub offset: usize,
}

#[derive(Debug)]
pub struct KvmContext {
    /// overlay data structure recording where overlay items END and how big they are.
    /// The end is recorded as start + size (so it is the index 1 past the last byte.
    /// We are using the end, as it makes overlap checks easier, since we can be sure,
    /// there is no overlap if the offset we are looking for is bigger than the end.
    /// The condition to check for no overlap is, that either is strictly before the other,
    /// meaning overlay_start >= check_end || check_start >= overlay_end, so checking for
    /// overlap is equivalent to the negation of that, which can be expressed as:
    /// overlay_start < check_end && check_start < overlay_end
    pub overlay: BTreeMap<usize, (usize, OverlayItem)>,
    pub storage: &'static mut [u8],
    pub fd: RawFd,
    domain: Arc<Mutex<RangePool<u32>>>,
    pub rangepool_start: u32,
}

/// TODO handle overwriting
impl ContextTrait for KvmContext {
    fn write<T>(&mut self, offset: usize, data: &[T]) -> DandelionResult<()> {
        debug!("Write to offset: {}, size: {}", offset, data.len());
        // check alignment
        if offset % core::mem::align_of::<T>() != 0 {
            debug!("Misaligned write at offset {}", offset);
            return Err(DandelionError::WriteMisaligned);
        }

        // check if the write is within bounds
        let write_length = data.len() * core::mem::size_of::<T>();
        if offset + write_length > self.storage.len() {
            debug!("Write out of bounds at offset {}", offset);
            return Err(DandelionError::InvalidWrite);
        }

        // make sure we are not trying to write into overlay space
        // TODO: when upper_bound / lower bound stabilizise use that
        let write_size = data.len() * size_of::<T>();
        let write_end = offset + write_size;
        let write_memory =
            unsafe { core::slice::from_raw_parts(data.as_ptr() as *const u8, write_size) };
        // all overlays that end after the offset starts
        let mut check_rage = self.overlay.range(offset..);
        if let Some((end, (size, _))) = check_rage.next_back() {
            // there is an overlay that ends after the write starts
            // there is overlap if the overlay also starts before the write ends
            let start = end - size;
            if start < write_end {
                debug!("Trying to write at offset: {}, with size: {}, but overlaps with overlay: {},{}", offset, write_size, start, size);
                return Err(DandelionError::InvalidWrite);
            }
        }
        self.storage[offset..offset + write_size].copy_from_slice(write_memory);
        Ok(())
    }

    fn read<T>(&self, mut offset: usize, read_buffer: &mut [T]) -> DandelionResult<()> {
        // check that buffer has proper allighment
        if offset % core::mem::align_of::<T>() != 0 {
            log::debug!("Misaligned write at offset {}", offset);
            return Err(DandelionError::ReadMisaligned);
        }

        let read_size = core::mem::size_of::<T>() * read_buffer.len();
        if offset + read_size > self.storage.len() {
            log::debug!("Read out of bounds at offset {}", offset);
            return Err(DandelionError::InvalidRead);
        }
        let mut read_memory = unsafe {
            core::slice::from_raw_parts_mut(read_buffer.as_mut_ptr() as *mut u8, read_size)
        };

        let mut overlay_range = self.overlay.range(offset..);
        while let Some((overlay_end, (overlay_size, overlay_context))) = overlay_range.next() {
            // check if there is any space before the overlay item that needs to be read first
            let overlay_start = overlay_end - overlay_size;
            if overlay_start > offset {
                // read until we hit either end of read request or the overlay
                let additional_bytes = min(overlay_start - offset, read_memory.len());
                println!("additional_bytes: {}", additional_bytes);
                read_memory[..additional_bytes]
                    .copy_from_slice(&self.storage[offset..offset + additional_bytes]);
                read_memory = &mut read_memory[additional_bytes..];
                offset += additional_bytes;
            }

            // check how much to read from the overlay, know that offset >= overlay start or that read buffer is empty
            if !read_memory.is_empty() {
                // get offset into the overlay
                let overlay_offset = offset - overlay_start;
                let additional_bytes = min(*overlay_size - overlay_offset, read_buffer.len());
                overlay_context.context.read(
                    overlay_context.offset + overlay_offset,
                    &mut read_memory[..additional_bytes],
                )?;
                read_memory = &mut read_memory[additional_bytes..];
                offset += additional_bytes;
                if read_memory.is_empty() {
                    return Ok(());
                }
            } else {
                return Ok(());
            }
        }
        if !read_memory.is_empty() {
            let left_to_read = read_memory.len();
            read_memory.copy_from_slice(&self.storage[offset..offset + left_to_read]);
        }

        Ok(())
    }

    fn get_chunk_ref(&self, offset: usize, length: usize) -> DandelionResult<&[u8]> {
        if offset + length > self.storage.len() {
            return Err(DandelionError::InvalidRead);
        }

        // check if the offset is into an overlayed object
        if let Some((overlay_end, (overlay_size, overlay_context))) =
            self.overlay.range(offset..).next()
        {
            let overlay_start = overlay_end - overlay_size;
            // overlay object ends after offset, so if overlay start is smaller than offset,
            // it reads from inside the overlay, otherise it is from in front of the overlay
            if overlay_start <= offset {
                let overlay_offset = offset - overlay_start;
                let chunk_size = min(overlay_size - overlay_offset, length);
                overlay_context
                    .context
                    .get_chunk_ref(overlay_context.offset + overlay_offset, chunk_size)
            } else {
                // offset is before overlay start, so can read at most up to overlay start
                let chunk_size = min(length, overlay_start - offset);
                Ok(&self.storage[offset..offset + chunk_size])
            }
        } else {
            Ok(&self.storage[offset..offset + length])
        }
    }
}

impl Drop for KvmContext {
    fn drop(&mut self) {
        let size = u32::try_from(self.storage.len() / SLAB_SIZE).unwrap();
        self.domain
            .lock()
            .unwrap()
            .insert(self.rangepool_start, self.rangepool_start + size);
        unsafe {
            nix::sys::mman::madvise(
                self.storage.as_mut_ptr() as *mut c_void,
                self.storage.len(),
                nix::sys::mman::MmapAdvise::MADV_DONTNEED,
            )
            .unwrap();
            nix::sys::mman::munmap(self.storage.as_mut_ptr() as *mut c_void, self.storage.len())
                .unwrap();
        };
    }
}

#[derive(Debug)]
pub struct KvmMemoryDomain {
    // memory_pool: MmapMemPool,
    occupation: Arc<Mutex<RangePool<u32>>>,
    fd: RawFd,
}

impl MemoryDomain for KvmMemoryDomain {
    fn init(config: MemoryResource) -> DandelionResult<Box<dyn MemoryDomain>> {
        let size = match config {
            MemoryResource::Anonymous { size } => size,
            _ => {
                return Err(DandelionError::DomainError(
                    dandelion_commons::DomainError::ConfigMissmatch,
                ))
            }
        };

        // create memfd for anonymous memory file
        let upper_end = u32::try_from(size / SLAB_SIZE)
            .expect("Total memory pool should be smaller for current u32 setup");
        let occupation = Arc::new(Mutex::new(RangePool::new(0..upper_end)));
        let fd = memfd_create(
            &CString::from_str("KvmMemoryDomain").unwrap(),
            MemFdCreateFlag::empty(),
        )
        .unwrap();
        ftruncate(fd, i64::try_from(size).unwrap()).unwrap();
        Ok(Box::new(KvmMemoryDomain { fd, occupation }))
    }

    fn acquire_context(&self, mut size: usize) -> DandelionResult<Context> {
        // round up to next slab size
        if size > (u32::MAX as usize) * SLAB_SIZE {
            return Err(DandelionError::DomainError(
                dandelion_commons::DomainError::InvalidMemorySize,
            ));
        }

        let number_of_slabs = u32::try_from((size + SLAB_SIZE - 1) / SLAB_SIZE).unwrap();
        size = (number_of_slabs as usize) * SLAB_SIZE;
        let slab = self
            .occupation
            .lock()
            .unwrap()
            .get(number_of_slabs, u32::MIN)
            .ok_or(DandelionError::DomainError(
                dandelion_commons::DomainError::ReachedCapacity,
            ))?;
        let file_offset = (slab as usize) * SLAB_SIZE;
        let mapping_pointer = unsafe {
            nix::sys::mman::mmap(
                None,
                NonZeroUsize::new(size).unwrap(),
                ProtFlags::all(),
                MapFlags::MAP_SHARED,
                self.fd,
                i64::try_from(file_offset).unwrap(),
            )
            .or(Err(DandelionError::DomainError(
                dandelion_commons::DomainError::Mapping,
            )))?
        } as *mut u8;
        let storage = unsafe { core::slice::from_raw_parts_mut(mapping_pointer, size) };

        let new_context = Box::new(KvmContext {
            overlay: BTreeMap::new(),
            storage,
            fd: self.fd,
            domain: self.occupation.clone(),
            rangepool_start: slab,
        });
        Ok(Context::new(ContextType::Kvm(new_context), size))
    }
}

pub fn transfer_into(
    destination: &mut KvmContext,
    source: Arc<Context>,
    destination_offset: usize,
    source_offset: usize,
    size: usize,
) -> DandelionResult<()> {
    debug!(
        "Transfer into kvm context to offset: {}, size: {}",
        destination_offset, size
    );

    // check there is space and there is no overlap
    if source_offset + size > source.size {
        return Err(DandelionError::InvalidRead);
    }
    if destination_offset + size > destination.storage.len() {
        return Err(DandelionError::InvalidWrite);
    }
    if let Some((overlay_end, (overlay_size, _))) =
        destination.overlay.range(destination_offset..).next()
    {
        // check if there is overlap, throw error if there is
        if overlay_end - overlay_size < destination_offset + size {
            return Err(DandelionError::InvalidWrite);
        }
    }
    if size < SLAB_SIZE {
        let mut bytes_written = 0;
        while bytes_written < size {
            let chunk =
                source.get_chunk_ref(source_offset + bytes_written, size - bytes_written)?;
            destination.write(destination_offset + bytes_written, chunk)?;
            bytes_written += chunk.len();
        }
    } else {
        // check if not both have the same distance to the next slab, if so, need to copy regularly
        // TODO remove interface for exact control on transfer item, so location can be controlled by transfer function
        if source_offset % SLAB_SIZE != destination_offset % SLAB_SIZE {
            return Err(DandelionError::NotImplemented);
        }

        // insert the parts that can be remapped and copy the rest
        let rounded_start = destination_offset.next_multiple_of(SLAB_SIZE);
        let rounded_size = ((size - (rounded_start - destination_offset)) / SLAB_SIZE) * SLAB_SIZE;
        let rounded_end = rounded_start + rounded_size;
        let source_rounded_start = source_offset.next_multiple_of(SLAB_SIZE);
        let source_rounded_end = source_rounded_start + rounded_size;

        // copy front and back parts
        let mut header_bytes = 0;
        let header_length = rounded_start - destination_offset;
        while header_bytes < header_length {
            let chunk =
                source.get_chunk_ref(source_offset + header_bytes, header_length - header_bytes)?;
            destination.write(destination_offset + header_bytes, chunk)?;
            header_bytes += chunk.len();
        }

        let mut trailer_bytes = 0;
        let trailer_length = (destination_offset + size) - rounded_end;
        while trailer_bytes < trailer_length {
            let chunk = source.get_chunk_ref(
                source_rounded_end + trailer_bytes,
                trailer_length - trailer_bytes,
            )?;
            destination.write(rounded_end + trailer_bytes, chunk)?;
            trailer_bytes += chunk.len();
        }

        if rounded_size != 0 {
            destination.overlay.insert(
                rounded_end,
                (
                    rounded_size,
                    OverlayItem {
                        context: source,
                        offset: source_rounded_start,
                    },
                ),
            );
        }
    }
    Ok(())
}

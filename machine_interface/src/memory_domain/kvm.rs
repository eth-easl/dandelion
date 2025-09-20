use crate::{
    memory_domain::{Context, ContextTrait, ContextType, MemoryDomain},
    util::mmapmem::{MmapMem, MmapMemPool},
};
use dandelion_commons::{DandelionError, DandelionResult};
use nix::sys::mman::ProtFlags;
use std::{cmp::min, collections::BTreeMap, sync::Arc};

use super::MemoryResource;

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
    pub storage: MmapMem,
}

impl ContextTrait for KvmContext {
    fn write<T>(&mut self, offset: usize, data: &[T]) -> DandelionResult<()> {
        // make sure we are not trying to write into overlay space
        // TODO: when upper_bound / lower bound stabilizise use that
        let write_end = offset + data.len() * size_of::<T>();
        // all overlays that end after the offset starts
        let mut check_rage = self.overlay.range(offset..);
        if let Some((end, (size, _))) = check_rage.next_back() {
            // there is an overlay that ends after the write starts
            // there is overlap if the overlay also starts before the write ends
            let start = end - size;
            if start < write_end {
                panic!("trying to write to context at overlayed address")
            }
        }
        self.storage.write(offset, data)
    }

    fn read<T>(&self, mut offset: usize, mut read_buffer: &mut [T]) -> DandelionResult<()> {
        let read_end = offset + read_buffer.len() * size_of::<T>();
        let mut overlay_range = self.overlay.range(..read_end);
        while let Some((overlay_end, (overlay_size, overlay_context))) = overlay_range.next() {
            // check if there is any space before the overlay item that needs to be read first
            let overlay_start = overlay_end - overlay_size;
            if overlay_start > offset {
                // read until we hit either end of read request or the overlay
                let additional_bytes = min(overlay_start - offset, read_buffer.len());
                self.storage
                    .read(offset, &mut read_buffer[..additional_bytes])?;
                read_buffer = &mut read_buffer[additional_bytes..];
                offset += additional_bytes;
            }
            // check how much to read from the overlay, know that offset >= overlay start or that read buffer is empty
            if !read_buffer.is_empty() {
                // get offset into the overlay
                let overlay_offset = offset - overlay_start;
                let additional_bytes = min(*overlay_size - overlay_offset, read_buffer.len());
                overlay_context.context.read(
                    overlay_context.offset + overlay_offset,
                    &mut read_buffer[..additional_bytes],
                )?;
                read_buffer = &mut read_buffer[additional_bytes..];
                offset += additional_bytes;
                if read_buffer.is_empty() {
                    return Ok(());
                }
            } else {
                return Ok(());
            }
        }
        if !read_buffer.is_empty() {
            self.storage.read(offset, read_buffer)
        } else {
            Ok(())
        }
    }

    fn get_chunk_ref(&self, offset: usize, length: usize) -> DandelionResult<&[u8]> {
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
                self.storage.get_chunk_ref(offset, chunk_size)
            }
        } else {
            self.storage.get_chunk_ref(offset, length)
        }
    }
}

#[derive(Debug)]
pub struct KvmMemoryDomain {
    memory_pool: MmapMemPool,
}

impl MemoryDomain for KvmMemoryDomain {
    fn init(config: MemoryResource) -> DandelionResult<Box<dyn MemoryDomain>> {
        let (size, id) = match config {
            // TODO make anonymous memfd
            MemoryResource::Shared { size, id } => (size, id),
            _ => {
                return Err(DandelionError::DomainError(
                    dandelion_commons::DomainError::ConfigMissmatch,
                ))
            }
        };
        let memory_pool =
            MmapMemPool::create(size, ProtFlags::PROT_READ | ProtFlags::PROT_WRITE, Some(id))?;
        Ok(Box::new(KvmMemoryDomain { memory_pool }))
    }

    fn acquire_context(&self, size: usize) -> DandelionResult<Context> {
        // create and map a shared memory region
        let (mem_space, actual_size) = self
            .memory_pool
            .get_allocation(size, nix::sys::mman::MmapAdvise::MADV_DONTNEED)?;

        let new_context = Box::new(KvmContext {
            overlay: BTreeMap::new(),
            storage: mem_space,
        });
        Ok(Context::new(ContextType::Kvm(new_context), actual_size))
    }
}

// pub fn transfer_into(
//     destination: &mut KvmContext,
//     source: Arc<Context>,
//     destination_offset: usize,
//     source_offset: usize,
//     size: usize,
// ) -> DandelionResult<()> {
// }

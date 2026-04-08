pub(super) use crate::function_driver::compute_driver::kvm::PAGE_SIZE;
use crate::{
    function_driver::compute_driver::kvm::round_down_to_page,
    memory_domain::{Context, ContextTrait, ContextType, MemoryDomain},
};

use super::MemoryResource;
use dandelion_commons::{
    dandelion_err, err_dandelion, range_pool::RangePool, DandelionError, DandelionResult,
};
use log::{debug, trace};
use nix::{
    sys::{
        memfd::{memfd_create, MFdFlags},
        mman::{MapFlags, ProtFlags},
    },
    unistd::ftruncate,
};
use std::{
    cmp::min,
    collections::BTreeMap,
    ffi::c_void,
    fmt::Debug,
    num::NonZeroUsize,
    os::fd::OwnedFd,
    ptr::{self, NonNull},
    sync::{Arc, Mutex},
};

#[derive(Clone)]
pub struct OverlayItem {
    pub context: Arc<Context>,
    pub offset: usize,
}

impl Debug for OverlayItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OverlayItem")
            .field("offset", &self.offset)
            .field("context", &self.context.context)
            .finish()
    }
}

pub struct KvmContext {
    /// overlay data structure recording where overlay items END and how big they are.
    /// The end is recorded as start + size - 1 (so it is the index of the last byte).
    /// We are using the end, as it makes overlap checks easier, since we can be sure,
    /// there is no overlap if the offset we are looking for is bigger than the end.
    /// The condition to check for no overlap is, that either is strictly before the other,
    /// meaning overlay_start >= check_end || check_start > overlay_end, so checking for
    /// overlap is equivalent to the negation of that, which can be expressed as:
    /// overlay_start < check_end && check_start =< overlay_end.
    /// Additionally overlay should only contain whole pages.
    /// The values are tuples of the overlay starts and items containing the context,
    /// that is overlayed with the offset into those contexts.
    /// The overlay item can contain a context, which is supposed to be mapped into the
    /// current context, or if it does not, it indicates that the item has been written to
    /// the location the overlay covers.
    pub overlay: BTreeMap<usize, (usize, Option<OverlayItem>)>,
    pub storage: &'static mut [u8],
    pub fd: Arc<OwnedFd>,
    domain: Arc<Mutex<RangePool<u32>>>,
    pub rangepool_start: u32,
}

impl KvmContext {
    /// Function that inserts a new item into the overlay.
    /// Expects the start to be page aligned and the end to point to 1 past the last byte in the overlay
    /// (equal to start + size of the overlay)
    pub(crate) fn insert_into_overlay(
        &mut self,
        mut new_start: usize,
        new_end: usize,
        new_item: Option<OverlayItem>,
    ) {
        let mut to_remove = Vec::new();
        let mut new_insert_opt = Some((new_end - 1, (new_start, new_item.clone())));
        let mut insert_before_opt = None;
        // TODO replace with cursor for easy removal / insert, as soon as it stabilizes,
        // so we can keep one cursor accross iterations
        // TODO when replaced with cursor, could think about handling first page differently,
        // since it is the only one that can hang off the front
        // also think about writing interface to overlays to centralize overlay manupulation, since it happens in multiple places
        for (&overlay_end, (overlay_start, item_option)) in
            self.overlay.range_mut(new_start.saturating_sub(1)..)
        {
            // if the overlay starts after the write ends, either there is nothing left to do for this range or we can simply append to the front of the range
            if *overlay_start >= new_end {
                if new_item.is_none() && item_option.is_none() && *overlay_start == new_end {
                    *overlay_start = new_start;
                    new_insert_opt = None
                }
                break;
            }

            // check if we need to keep part of the overlay before the write, that is at least one page
            if *overlay_start < new_start {
                if item_option.is_some() || new_item.is_some() {
                    let new_front = (new_start - 1, (*overlay_start, item_option.clone()));
                    assert!(insert_before_opt.replace(new_front).is_none(), "Should never find a second overlay item, that overlays past the start of the write");
                } else {
                    // if the item is none, can just merge it with current one
                    new_start = *overlay_start;
                }
            }
            // check if we need to shorten or remove the current part of the overlay
            if new_end - 1 <= overlay_end {
                // shorten the current overlay if it is a separate item, otherwise just append the new space to the old
                if item_option.is_none() && new_item.is_none() {
                    *overlay_start = new_start;
                    new_insert_opt = None;
                } else {
                    if let Some(item) = item_option {
                        item.offset += new_end - *overlay_start;
                    };
                    *overlay_start = new_end;
                }
                // if it ends before this overlay end, then this was the last one that was relevant
                break;
            } else {
                // remove the current overlay
                to_remove.push(overlay_end);
                // update the new insert opt to the new start and end in case they changed
                new_insert_opt.as_mut().unwrap().0 = new_end - 1;
                new_insert_opt.as_mut().unwrap().1 .0 = new_start;
                // new_insert_opt = Some((new_end - 1, (new_start, new_item)));
            }
        }
        for remove_key in to_remove {
            self.overlay.remove(&remove_key);
        }
        if let Some((key, value)) = insert_before_opt {
            self.overlay.insert(key, value);
        }
        if let Some((key, value)) = new_insert_opt {
            self.overlay.insert(key, value);
        }
    }
}

impl Debug for KvmContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KvmContext")
            .field("overlay", &self.overlay)
            .field(
                "storage",
                &format_args!(
                    "addr: {:?}, len: {}",
                    self.storage.as_ptr(),
                    self.storage.len()
                ),
            )
            .field("fd", &self.fd)
            .field("rangepool_start", &self.rangepool_start)
            .finish()
    }
}

impl ContextTrait for KvmContext {
    fn write<T>(&mut self, offset: usize, data: &[T]) -> DandelionResult<()> {
        // check alignment
        if offset % core::mem::align_of::<T>() != 0 {
            debug!("Misaligned write at offset {}", offset);
            return err_dandelion!(DandelionError::WriteMisaligned);
        }

        // check if the write is within bounds
        let bytes_to_write = data.len() * size_of::<T>();
        let write_end = offset + bytes_to_write;
        if write_end > self.storage.len() {
            debug!(
                "Write out of bounds at offset {} with size: {} for context size: {}",
                offset,
                bytes_to_write,
                self.storage.len()
            );
            return err_dandelion!(DandelionError::InvalidWrite);
        }
        trace!(
            "Write into kvm context at offset: {}, size: {}",
            offset,
            bytes_to_write
        );

        // need to round to the pages that get touched
        let mut rounded_start = round_down_to_page(offset);
        let rounded_end = write_end.next_multiple_of(PAGE_SIZE);
        // find all overlays that end after the offset starts
        // if there is overlap, need remove all the overlapping pages, and copy the parts that are not overwritten
        let mut to_remove = Vec::new();
        let mut new_insert_opt = Some((rounded_end - 1, (rounded_start, None)));
        let mut insert_before_opt = None;
        let mut zero_header = rounded_start;
        let mut zero_trailer = rounded_end;
        // TODO replace with cursor for easy removal / insert, as soon as it stabilizes
        // TODO when replaced with cursor, could think about handling first page differently,
        // since it is the only one that can hang off the front
        // look for rounded start - 1, to catch merging opporunities where one ends exactly at the new overlay start
        for (&overlay_end, (overlay_start, item_option)) in
            self.overlay.range_mut(rounded_start.saturating_sub(1)..)
        {
            // check if the we found one that end right as this one starts and that also has no item
            if overlay_end == rounded_start.saturating_sub(1) {
                if item_option.is_none() {
                    // remove old item
                    to_remove.push(overlay_end);
                    // extend new one
                    new_insert_opt = Some((rounded_end - 1, (*overlay_start, None)));
                    rounded_start = *overlay_start;
                    // do not set zero header to offset, since the old overlay did not pre initialize the page
                    // we are writing to (it ended just before)
                }
                continue;
            }
            // if the overlay starts after the write ends, either there is nothing left to do for this range or we can simply append to the front of the range
            if *overlay_start >= rounded_end {
                if item_option.is_none() && *overlay_start == rounded_end {
                    *overlay_start = rounded_start;
                    new_insert_opt = None;
                }
                break;
            }

            // check if we need to copy parts of a page for a partially overwritten page at the start of the write
            // can be the first page in the overlay, need to compare to offset, because rounded_start may have moved,
            // to absorb overlay before the new write (can only happen once, when we are processing the one overlay,
            // that overlaps with the start of the write)
            if rounded_start < offset && *overlay_start <= offset {
                if let Some(item) = item_option {
                    let offset_page_base = round_down_to_page(offset);
                    let read_offset = item.offset + (offset_page_base - *overlay_start);
                    item.context
                        .read(read_offset, &mut self.storage[offset_page_base..offset])?;
                }
                zero_header = offset;
            }
            // check if we need to keep part of the overlay before the write, that is at least one page
            if *overlay_start < rounded_start {
                if item_option.is_some() {
                    let new_front = (rounded_start - 1, (*overlay_start, item_option.clone()));
                    assert!(insert_before_opt.replace(new_front).is_none(), "Should never find a second overlay item, that overlays past the start of the write");
                } else {
                    // if the item is none, can just merge it with current one
                    rounded_start = *overlay_start;
                }
            }

            // check if we need to shorten copy parts of the page for partially overwritten page at the end of the write
            if rounded_end - 1 <= overlay_end && write_end < rounded_end {
                if let Some(item) = item_option {
                    let read_offset = item.offset + (write_end - *overlay_start);
                    item.context
                        .read(read_offset, &mut self.storage[write_end..rounded_end])?;
                }
                zero_trailer = write_end;
            }
            // check if we need to shorten or remove the current part of the overlay
            if rounded_end - 1 <= overlay_end {
                // shorten the current overlay if it is a separate item, otherwise just append the new space to the old
                if let Some(item) = item_option {
                    item.offset += rounded_end - *overlay_start;
                    *overlay_start = rounded_end;
                    // if rounded_end -1 == overlay_end, the current one is replace with this one, when it is inserted
                    new_insert_opt = Some((rounded_end - 1, (rounded_start, None)));
                } else {
                    *overlay_start = rounded_start;
                    // want to keep using the old one, so no insert
                    new_insert_opt = None;
                }
                // if it ends before this overlay end, then this was the last one that was relevant
                break;
            } else {
                // remove the current overlay
                to_remove.push(overlay_end);
                // update the new insert opt, in case this is the last iteration
                new_insert_opt = Some((rounded_end - 1, (rounded_start, None)));
            }
        }
        for remove_key in to_remove {
            self.overlay.remove(&remove_key);
        }
        if let Some((key, value)) = insert_before_opt {
            self.overlay.insert(key, value);
        }
        if let Some((key, value)) = new_insert_opt {
            self.overlay.insert(key, value);
        }
        trace!("Overlay after write: {:?}", self.overlay);

        let write_memory =
            unsafe { core::slice::from_raw_parts(data.as_ptr() as *const u8, bytes_to_write) };
        // if necessary, insert zeros before the write on the first touched paged and after the write on the last one
        self.storage[zero_header..offset].fill(0);
        self.storage[offset..write_end].copy_from_slice(write_memory);
        self.storage[write_end..zero_trailer].fill(0);
        Ok(())
    }

    fn read<T>(&self, mut offset: usize, read_buffer: &mut [T]) -> DandelionResult<()> {
        // check that buffer has proper allighment
        if offset % core::mem::align_of::<T>() != 0 {
            log::debug!("Misaligned write at offset {}", offset);
            return err_dandelion!(DandelionError::ReadMisaligned);
        }

        let read_size = core::mem::size_of::<T>() * read_buffer.len();
        if offset + read_size > self.storage.len() {
            log::debug!("Read out of bounds at offset {}", offset);
            return err_dandelion!(DandelionError::InvalidRead);
        }
        let mut read_memory = unsafe {
            core::slice::from_raw_parts_mut(read_buffer.as_mut_ptr() as *mut u8, read_size)
        };

        trace!(
            "Read from kvm context at offset: {}, size: {}, with overlay: {:?}",
            offset,
            read_size,
            self.overlay
        );

        if read_size == 0 {
            return Ok(());
        }

        let mut overlay_range = self.overlay.range(offset..);
        while let Some((overlay_end, (overlay_start, overlay_option))) = overlay_range.next() {
            if *overlay_start > offset {
                return err_dandelion!(DandelionError::InvalidRead);
            }

            // check how much to read from the overlay, know that offset >= overlay start or that read buffer is empty
            if !read_memory.is_empty() {
                // get offset into the overlay
                let overlay_offset = offset - overlay_start;
                let additional_bytes = min(*overlay_end - offset + 1, read_memory.len());
                if let Some(overlay_context) = overlay_option {
                    overlay_context.context.read(
                        overlay_context.offset + overlay_offset,
                        &mut read_memory[..additional_bytes],
                    )?;
                } else {
                    read_memory[..additional_bytes]
                        .copy_from_slice(&self.storage[offset..offset + additional_bytes]);
                }
                read_memory = &mut read_memory[additional_bytes..];
                offset += additional_bytes;
                if read_memory.is_empty() {
                    return Ok(());
                }
            } else {
                return Ok(());
            }
        }
        return err_dandelion!(DandelionError::InvalidRead);
    }

    fn get_chunk_ref(&self, offset: usize, length: usize) -> DandelionResult<&[u8]> {
        if offset + length > self.storage.len() {
            return err_dandelion!(DandelionError::InvalidRead);
        }

        // check if the offset is into an overlayed object
        trace!(
            "Trying to get chunk at offset: {} with length {} from overlay: {:?}",
            offset,
            length,
            self.overlay
        );
        if let Some((&overlay_end, (overlay_start, overlay_option))) =
            self.overlay.range(offset..).next()
        {
            // overlay object ends after offset, so if overlay start is smaller than offset,
            // it reads from inside the overlay, otherise it is from in front of the overlay
            if *overlay_start <= offset {
                if let Some(overlay_context) = overlay_option {
                    let overlay_offset = offset - overlay_start;
                    let chunk_size = min(overlay_end + 1 - offset, length);
                    overlay_context
                        .context
                        .get_chunk_ref(overlay_context.offset + overlay_offset, chunk_size)
                } else {
                    // offset is before overlay start, so can read at most up to overlay start
                    let chunk_end = min(offset + length, overlay_end + 1);
                    Ok(&self.storage[offset..chunk_end])
                }
            } else {
                err_dandelion!(DandelionError::InvalidRead)
            }
        } else {
            err_dandelion!(DandelionError::InvalidRead)
        }
    }
}

impl Drop for KvmContext {
    fn drop(&mut self) {
        unsafe {
            nix::sys::mman::munmap(
                NonNull::new_unchecked(self.storage.as_mut_ptr() as *mut c_void),
                self.storage.len(),
            )
            .unwrap();
        };
        let size = u32::try_from(self.storage.len() / PAGE_SIZE).unwrap();
        self.domain
            .lock()
            .unwrap()
            .insert(self.rangepool_start, self.rangepool_start + size);
    }
}

#[derive(Debug)]
pub struct KvmMemoryDomain {
    occupation: Arc<Mutex<RangePool<u32>>>,
    fd: Arc<OwnedFd>,
}

impl MemoryDomain for KvmMemoryDomain {
    fn init(config: MemoryResource) -> DandelionResult<Box<dyn MemoryDomain>> {
        let size = match config {
            MemoryResource::Anonymous { size } => size,
            _ => {
                return err_dandelion!(DandelionError::DomainError(
                    dandelion_commons::DomainError::ConfigMissmatch,
                ))
            }
        };

        // create memfd for anonymous memory file
        let upper_end = u32::try_from(size / PAGE_SIZE)
            .expect("Total memory pool should be smaller for current u32 setup");
        let occupation = Arc::new(Mutex::new(RangePool::new(0..upper_end)));
        let fd = memfd_create("KvmMemoryDomain", MFdFlags::empty()).unwrap();
        ftruncate(&fd, i64::try_from(size).unwrap()).unwrap();
        Ok(Box::new(KvmMemoryDomain {
            fd: Arc::new(fd),
            occupation,
        }))
    }

    fn acquire_context(&self, mut size: usize) -> DandelionResult<Context> {
        // round up to next page size
        if size > (u32::MAX as usize) * PAGE_SIZE {
            return err_dandelion!(DandelionError::DomainError(
                dandelion_commons::DomainError::InvalidMemorySize,
            ));
        }

        let number_of_pages = u32::try_from((size + PAGE_SIZE - 1) / PAGE_SIZE).unwrap();
        size = (number_of_pages as usize) * PAGE_SIZE;
        let page = self
            .occupation
            .lock()
            .unwrap()
            .get(number_of_pages, u32::MIN)
            .ok_or(dandelion_err!(DandelionError::DomainError(
                dandelion_commons::DomainError::ReachedCapacity,
            )))?;
        let file_offset = (page as usize) * PAGE_SIZE;
        // TODO replace casting with NonNull::as_mut_ptr() when it stabilizes
        let mapping_pointer = unsafe {
            ptr::from_mut(
                nix::sys::mman::mmap(
                    None,
                    NonZeroUsize::new(size).unwrap(),
                    ProtFlags::all(),
                    MapFlags::MAP_SHARED,
                    &self.fd,
                    i64::try_from(file_offset).unwrap(),
                )
                .or(err_dandelion!(DandelionError::DomainError(
                    dandelion_commons::DomainError::Mapping,
                )))?
                .as_mut(),
            )
        } as *mut u8;
        let storage = unsafe { core::slice::from_raw_parts_mut(mapping_pointer, size) };

        let new_context = Box::new(KvmContext {
            overlay: BTreeMap::new(),
            storage,
            fd: self.fd.clone(),
            domain: self.occupation.clone(),
            rangepool_start: page,
        });
        Ok(Context::new(ContextType::Kvm(new_context), size))
    }
}

/// Function to find a destination offset, which allows to zero copy pages in the transfer
/// Return the index after which to insert the new occupation and the destination address
pub fn get_transfer_offset(
    occupation: &Vec<crate::Position>,
    source_offset: usize,
    context_size: usize,
    size: usize,
) -> DandelionResult<(usize, usize)> {
    // search for smallest space that is bigger than size
    // space start holds previous start
    // check how far the source is offset from the next page boundry and try to get a spot that has the same
    let page_offset = source_offset % PAGE_SIZE;
    let mut space_size = context_size + 1;
    let mut index = 0;
    let mut start_address = 0;
    for (window_index, occupied) in occupation.windows(2).enumerate() {
        let lower_end = occupied[0].offset + occupied[0].size;
        // find next address that has the same page alignment
        let on_next_page = usize::from(lower_end % PAGE_SIZE > page_offset) * PAGE_SIZE;
        let start = round_down_to_page(lower_end) + on_next_page + page_offset;
        let end = occupied[1].offset;
        let available = end.saturating_sub(start);
        if available >= size && available < space_size {
            space_size = available;
            index = window_index;
            start_address = start;
        }
    }
    trace!(
        "found a place to insert index {}, start address: {}, space_size {}, context size {}",
        index,
        start_address,
        space_size,
        context_size
    );
    if context_size + 1 == space_size {
        return err_dandelion!(DandelionError::ContextFull);
    }
    return Ok((index, start_address));
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
        return err_dandelion!(DandelionError::InvalidRead);
    }
    if destination_offset + size > destination.storage.len() {
        debug!(
            "Trying to transfer into KVM context with destination {} + size {} > context size {}",
            destination_offset,
            size,
            destination.storage.len()
        );
        return err_dandelion!(DandelionError::InvalidWrite);
    }
    // don't need to check if transfers may partially overlap, since the occupation checks for that.
    // if occupation check was fine, can overwrite here (may happen because of planned overwrite or
    // because of page rounding)

    if let ContextType::Kvm(kvm_source_context) = &source.context {
        if size < PAGE_SIZE {
            let mut bytes_written = 0;
            while bytes_written < size {
                let chunk =
                    source.get_chunk_ref(source_offset + bytes_written, size - bytes_written)?;
                debug_assert_ne!(0, chunk.len(), "Chunks should never be zero");
                destination.write(destination_offset + bytes_written, chunk)?;
                bytes_written += chunk.len();
            }
        } else if source_offset % PAGE_SIZE != destination_offset % PAGE_SIZE {
            trace!("starting to transfer large item with non equal offset");
            // check if not both have the same distance to the next page, if so, need to copy regularly
            // TODO remove interface for exact control on transfer item, so location can be controlled by transfer function
            // if that is true, can force destination_offset to be same allignment
            let mut bytes_written = 0;
            while bytes_written < size {
                let chunk =
                    source.get_chunk_ref(source_offset + bytes_written, size - bytes_written)?;
                debug_assert_ne!(0, chunk.len(), "Chunks should never be zero");
                destination.write(destination_offset + bytes_written, chunk)?;
                bytes_written += chunk.len();
            }
        } else {
            // insert the parts that can be remapped and copy the rest
            let rounded_start = destination_offset.next_multiple_of(PAGE_SIZE);
            let rounded_end = round_down_to_page(destination_offset + size);
            let rounded_size = rounded_end - rounded_start;
            let source_rounded_start = source_offset.next_multiple_of(PAGE_SIZE);
            let source_rounded_end = source_rounded_start + rounded_size;

            // copy front and back parts
            let mut header_bytes = 0;
            let header_length = rounded_start - destination_offset;
            while header_bytes < header_length {
                let chunk = source
                    .get_chunk_ref(source_offset + header_bytes, header_length - header_bytes)?;
                debug_assert_ne!(0, chunk.len(), "Chunks should never be zero");
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
                debug_assert_ne!(0, chunk.len(), "Chunks should never be zero");
                destination.write(rounded_end + trailer_bytes, chunk)?;
                trailer_bytes += chunk.len();
            }

            // TODO: may want to keep track on range level with a single file, then we simplify the overlay handling
            if rounded_size != 0 {
                let mut overlayed_bytes = 0;
                // in case the original context has multiple overlays in the transferred range
                for (source_overlay_end, (source_overlay_start, source_item_option)) in
                    kvm_source_context.overlay.range(source_rounded_start..)
                {
                    if overlayed_bytes >= rounded_size
                        || *source_overlay_start >= source_rounded_end
                    {
                        break;
                    }
                    assert!(
                        *source_overlay_start <= source_rounded_start + overlayed_bytes,
                        "Expect to always find an overlay that starts before the start of the transfer,\
                        source overlay: {:?}, source_rounded_start: {}, overlayed_bytes {}",
                        kvm_source_context.overlay, source_rounded_start, overlayed_bytes
                    );

                    let new_end = min(*source_overlay_end + 1, source_rounded_end);
                    let new_bytes = new_end - source_rounded_start + overlayed_bytes;
                    // if this already has been zero copied from another context, use the reference to that context, to avoid dependency chains
                    let new_item = if let Some(OverlayItem {
                        offset,
                        context: nested_context,
                    }) = source_item_option
                    {
                        let new_offset = offset + source_rounded_start - *source_overlay_start;
                        OverlayItem {
                            context: nested_context.clone(),
                            offset: new_offset,
                        }
                    } else {
                        OverlayItem {
                            context: source.clone(),
                            offset: source_rounded_start + overlayed_bytes,
                        }
                    };
                    destination.insert_into_overlay(
                        rounded_start + overlayed_bytes,
                        rounded_start + overlayed_bytes + new_bytes,
                        Some(new_item),
                    );
                    overlayed_bytes += new_bytes;
                }
                debug_assert_eq!(overlayed_bytes, rounded_size);
            }
        }
    } else {
        trace!("Transfer into KVM context from other type of context");
        let mut bytes_written = 0;
        while bytes_written < size {
            let chunk =
                source.get_chunk_ref(source_offset + bytes_written, size - bytes_written)?;
            debug_assert_ne!(0, chunk.len(), "Chunks should never be zero");
            destination.write(destination_offset + bytes_written, chunk)?;
            bytes_written += chunk.len();
        }
    }
    trace!("Overlay after transfer {:?}", destination.overlay);
    Ok(())
}

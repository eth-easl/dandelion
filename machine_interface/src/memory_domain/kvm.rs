pub(super) use crate::function_driver::compute_driver::kvm::PAGE_SIZE;
use crate::{
    function_driver::compute_driver::kvm::round_down_to_page,
    memory_domain::{Context, ContextTrait, ContextType, MemoryDomain},
};

use super::MemoryResource;
use dandelion_commons::{dandelion_err, err_dandelion, DandelionError, DandelionResult};
use log::{debug, trace};
use nix::sys::mman::{MapFlags, ProtFlags};
use std::{
    collections::{
        btree_map::Entry::{Occupied, Vacant},
        BTreeMap,
    },
    fmt::Debug,
    num::NonZeroUsize,
    ops::Deref,
    ptr::{self},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

#[derive(Clone, Debug)]
pub enum PageType {
    CoW,
    Page,
}

#[derive(Clone, Debug)]
pub struct Page {
    physical_addr: Arc<u64>,
}

// TODO implement as_slice or something to directly get the mutable slice of the backing page
impl Page {
    fn new(physical_addr: u64) -> Self {
        Self {
            physical_addr: Arc::new(physical_addr),
        }
    }
    // TODO: page should probably hold reference to the memory, so this is just a method on self
    pub fn get_slice<'memory>(&self, memory: &'memory mut [u8]) -> &'memory mut [u64] {
        let page_start = **self as usize;
        unsafe {
            core::slice::from_raw_parts_mut(
                memory.as_mut_ptr().add(page_start) as *mut u64,
                PAGE_SIZE / size_of::<u64>(),
            )
        }
    }
}

impl Deref for Page {
    type Target = u64;
    fn deref(&self) -> &Self::Target {
        &self.physical_addr
    }
}

// impl Debug for OverlayItem {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         match self {
//             OverlayItem::CoW { context, offset } => f
//                 .debug_struct("OverlayItem::CoW")
//                 .field("offset", offset)
//                 .field("context", &context.context)
//                 .finish(),
//             OverlayItem::Pages {} => todo!(),
//         }
//     }
// }

pub struct KvmContext {
    /// The overlay stores information about valid data to read in the context.
    /// the data structure records where overlay items END and how big they are.
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
    pub overlay: BTreeMap<usize, (PageType, Page)>,
    // pub storage: &'static mut [u8],
    // pub fd: Arc<OwnedFd>,
    // domain: Arc<Mutex<RangePool<u32>>>,
    // TODO: document lifetime dependencies somewhere, to make sure it is clear that unmapping the memory can cause issues.
    pub domain_slice: &'static mut [u8],
    pub context_size: usize,
}

unsafe impl Sync for KvmContext {}
unsafe impl Send for KvmContext {}

/// At the mapping at the 0 address, keep the root pointer for the free page list.
/// This function takes pages from that list and makes them into a PageRange.
fn get_page(memory: &[u8]) -> DandelionResult<Page> {
    let u64_reference = unsafe {
        std::slice::from_raw_parts_mut(memory.as_ptr() as *mut u64, memory.len() / size_of::<u64>())
    };
    let atomic_slice = AtomicU64::from_mut_slice(u64_reference);

    // load where the current root is pointing
    let mut next_free = atomic_slice[0].load(Ordering::SeqCst);
    loop {
        // make sure we are not trying to take the current root
        if next_free == 0 {
            trace!("Dandelion error: Reached Capacity");
            return err_dandelion!(DandelionError::DomainError(
                dandelion_commons::DomainError::ReachedCapacity
            ));
        }
        // load where the next free page is after the current root page
        let potential_root =
            atomic_slice[next_free as usize / size_of::<u64>()].load(Ordering::SeqCst);
        next_free = match atomic_slice[0].compare_exchange(
            next_free,
            potential_root,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            Ok(root) => {
                return Ok(Page::new(root));
            }
            Err(root) => root,
        };
    }
}

impl KvmContext {
    /// At the mapping at the 0 address, keep the root pointer for the free page list.
    /// This function takes pages from that list and makes them into a PageRange.
    pub fn get_page(&self) -> DandelionResult<Page> {
        get_page(self.domain_slice)
    }

    pub fn add_page(&mut self, offset: usize) -> DandelionResult<(u64, &mut [u8])> {
        match self.overlay.entry(offset) {
            Occupied(_) => err_dandelion!(DandelionError::InvalidWrite),
            Vacant(vacant) => {
                let page = get_page(self.domain_slice)?;
                let page_start = *page as usize;
                vacant.insert_entry((PageType::Page, page));
                Ok((
                    page_start as u64,
                    &mut self.domain_slice[page_start..page_start + PAGE_SIZE],
                ))
            }
        }
    }

    // Function that inserts a new item into the overlay.
    // Expects the start to be page aligned and the end to point to 1 past the last byte in the overlay
    // (equal to start + size of the overlay)
    // pub(crate) fn insert_into_overlay(
    //     &mut self,
    //     mut new_start: usize,
    //     new_end: usize,
    //     new_item: Option<PageType>,
    // ) {
    //     let mut to_remove = Vec::new();
    //     let mut new_insert_opt = Some((new_end - 1, (new_start, new_item.clone())));
    //     let mut insert_before_opt = None;
    //     // TODO replace with cursor for easy removal / insert, as soon as it stabilizes,
    //     // so we can keep one cursor accross iterations
    //     // TODO when replaced with cursor, could think about handling first page differently,
    //     // since it is the only one that can hang off the front
    //     // also think about writing interface to overlays to centralize overlay manupulation, since it happens in multiple places
    //     for (&overlay_end, (overlay_start, item_option)) in
    //         self.overlay.range_mut(new_start.saturating_sub(1)..)
    //     {
    //         // if the overlay starts after the write ends, either there is nothing left to do for this range or we can simply append to the front of the range
    //         if *overlay_start >= new_end {
    //             if new_item.is_none() && item_option.is_none() && *overlay_start == new_end {
    //                 *overlay_start = new_start;
    //                 new_insert_opt = None
    //             }
    //             break;
    //         }

    //         // check if we need to keep part of the overlay before the write, that is at least one page
    //         if *overlay_start < new_start {
    //             if item_option.is_some() || new_item.is_some() {
    //                 let new_front = (new_start - 1, (*overlay_start, item_option.clone()));
    //                 assert!(insert_before_opt.replace(new_front).is_none(), "Should never find a second overlay item, that overlays past the start of the write");
    //             } else {
    //                 // if the item is none, can just merge it with current one
    //                 new_start = *overlay_start;
    //             }
    //         }
    //         // check if we need to shorten or remove the current part of the overlay
    //         if new_end - 1 <= overlay_end {
    //             // shorten the current overlay if it is a separate item, otherwise just append the new space to the old
    //             if item_option.is_none() && new_item.is_none() {
    //                 *overlay_start = new_start;
    //                 new_insert_opt = None;
    //             } else {
    //                 if let Some(item) = item_option {
    //                     item.offset += new_end - *overlay_start;
    //                 };
    //                 *overlay_start = new_end;
    //             }
    //             // if it ends before this overlay end, then this was the last one that was relevant
    //             break;
    //         } else {
    //             // remove the current overlay
    //             to_remove.push(overlay_end);
    //             // update the new insert opt to the new start and end in case they changed
    //             new_insert_opt.as_mut().unwrap().0 = new_end - 1;
    //             new_insert_opt.as_mut().unwrap().1 .0 = new_start;
    //             // new_insert_opt = Some((new_end - 1, (new_start, new_item)));
    //         }
    //     }
    //     for remove_key in to_remove {
    //         self.overlay.remove(&remove_key);
    //     }
    //     if let Some((key, value)) = insert_before_opt {
    //         self.overlay.insert(key, value);
    //     }
    //     if let Some((key, value)) = new_insert_opt {
    //         self.overlay.insert(key, value);
    //     }
    // }
}

impl Debug for KvmContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KvmContext")
            .field("overlay", &self.overlay)
            // .field(
            //     "storage",
            //     &format_args!(
            //         "addr: {:?}, len: {}",
            //         self.storage.as_ptr(),
            //         self.storage.len()
            //     ),
            // )
            // .field("fd", &self.fd)
            // .field("rangepool_start", &self.rangepool_start)
            .finish()
    }
}

impl ContextTrait for KvmContext {
    fn write<T>(&mut self, offset: usize, data: &[T]) -> DandelionResult<()> {
        // check alignment
        if !offset.is_multiple_of(core::mem::align_of::<T>()) {
            debug!("Misaligned write at offset {}", offset);
            return err_dandelion!(DandelionError::WriteMisaligned);
        }

        // check if the write is within bounds
        let bytes_to_write = size_of_val(data);
        let write_end = offset + bytes_to_write;
        if write_end > self.context_size {
            debug!(
                "Write out of bounds at offset {} with size: {} for context size: {}",
                offset, bytes_to_write, self.context_size
            );
            return err_dandelion!(DandelionError::InvalidWrite);
        }
        trace!(
            "Write into kvm context at offset: {}, size: {}",
            offset,
            bytes_to_write
        );

        // convert data into u8 slice
        let mut write_memory =
            unsafe { core::slice::from_raw_parts(data.as_ptr() as *const u8, bytes_to_write) };

        // handle first page with possible partial write or write smaller than 1 page
        let write_offset = offset % PAGE_SIZE;
        if write_offset != 0 || bytes_to_write <= PAGE_SIZE {
            let page_write_size = if write_offset + bytes_to_write > PAGE_SIZE {
                PAGE_SIZE - write_offset
            } else {
                bytes_to_write
            };
            match self.overlay.entry(round_down_to_page(offset)) {
                Vacant(vacant) => {
                    let new_page = get_page(self.domain_slice)?;
                    let page_start = *new_page as usize;
                    let page = &mut self.domain_slice[page_start..page_start + PAGE_SIZE];
                    page[0..write_offset].fill(0);
                    page[write_offset..write_offset + page_write_size]
                        .copy_from_slice(&write_memory[..page_write_size]);
                    page[write_offset + page_write_size..].fill(0);
                    vacant.insert((PageType::Page, new_page));
                }
                Occupied(mut occupied) => {
                    let (page_type, page) = occupied.get_mut();
                    match page_type {
                        PageType::CoW => {
                            let new_page = get_page(self.domain_slice)?;
                            let page_start = *new_page as usize;
                            let old_page_start = **page as usize;
                            self.domain_slice
                                .copy_within(page_start..page_start + write_offset, old_page_start);
                            self.domain_slice[page_start + write_offset
                                ..page_start + write_offset + page_write_size]
                                .copy_from_slice(&write_memory[..page_write_size]);
                            self.domain_slice.copy_within(
                                page_start + write_offset + page_write_size..page_start + PAGE_SIZE,
                                old_page_start + write_offset + page_write_size,
                            );
                            occupied.insert((PageType::Page, new_page));
                        }
                        PageType::Page => {
                            let page_start = **page as usize;
                            let page = &mut self.domain_slice[page_start..page_start + PAGE_SIZE];
                            page[write_offset..write_offset + page_write_size]
                                .copy_from_slice(&write_memory[..page_write_size]);
                        }
                    };
                }
            }
            // advance the write memory
            write_memory = &write_memory[page_write_size..];
            // if we are done writing return
            if write_memory.is_empty() {
                return Ok(());
            }
        }

        // handle all pages with full copy
        // need to round to the pages that get touched
        let mut rounded_start = offset.next_multiple_of(PAGE_SIZE);
        let rounded_end = round_down_to_page(write_end);
        // find all pages that end after the offset starts
        // if there is overlap, need remove all the overlapping pages, and copy the parts that are not overwritten
        let mut to_insert = Vec::new();

        // TODO replace with cursor for easy removal / insert, as soon as it stabilizes
        // using rounded start, should get the first page that overlaps with the current write, and care about all of them up to
        // rounded end, which should be correctly exlucded using this range operation.
        for (&virtual_page_address, (page_type, page)) in
            self.overlay.range_mut(rounded_start..rounded_end)
        {
            // check if the current page is already after the next write address, if so insert pages between write start and current page
            // Don't need to check that rounded_start < write_end, since rounded_end is < write_end, and thus the virtual address also is < write_end.
            while rounded_start < virtual_page_address {
                // get new page
                let new_page = get_page(self.domain_slice)?;
                // fill page
                let start = *new_page as usize;
                let page = &mut self.domain_slice[start..start + PAGE_SIZE];
                page.copy_from_slice(&write_memory[..PAGE_SIZE]);
                write_memory = &write_memory[PAGE_SIZE..];
                to_insert.push((rounded_start, (PageType::Page, new_page)));
                rounded_start += PAGE_SIZE;
            }
            // check the current page
            let page_start = match page_type {
                PageType::Page => **page as usize,
                PageType::CoW => {
                    let new_page = get_page(self.domain_slice)?;
                    let page_start = *new_page as usize;
                    to_insert.push((rounded_start, (PageType::Page, new_page)));
                    page_start
                }
            };
            let page = &mut self.domain_slice[page_start..page_start + PAGE_SIZE];
            page.copy_from_slice(&write_memory[..PAGE_SIZE]);
            write_memory = &write_memory[PAGE_SIZE..];
            rounded_start += PAGE_SIZE;
        }

        // Need to fill in pages in case the last page in the overlay was before the rounded_end.
        while rounded_start < rounded_end {
            // get new page
            let new_page = get_page(self.domain_slice)?;
            // fill page
            let start = *new_page as usize;
            let page = &mut self.domain_slice[start..start + PAGE_SIZE];
            page.copy_from_slice(&write_memory[..PAGE_SIZE]);
            write_memory = &write_memory[PAGE_SIZE..];
            to_insert.push((rounded_start, (PageType::Page, new_page)));
            rounded_start += PAGE_SIZE;
        }

        // Bulk add all new pages
        self.overlay.extend(to_insert);

        // handle last page with possible partial write
        if !write_memory.is_empty() {
            debug_assert!(write_memory.len() < PAGE_SIZE);
            match self.overlay.entry(rounded_start) {
                Vacant(vacant) => {
                    let new_page = get_page(self.domain_slice)?;
                    let page_start = *new_page as usize;
                    let page = &mut self.domain_slice[page_start..page_start + PAGE_SIZE];
                    page[0..write_memory.len()].copy_from_slice(write_memory);
                    page[write_memory.len()..].fill(0);
                    vacant.insert((PageType::Page, new_page));
                }
                Occupied(mut occupied) => {
                    let (page_type, page) = occupied.get_mut();
                    match page_type {
                        PageType::CoW => {
                            let new_page = get_page(self.domain_slice)?;
                            let page_start = *new_page as usize;
                            self.domain_slice[page_start..page_start + write_memory.len()]
                                .copy_from_slice(write_memory);
                            let old_page_start = **page as usize;
                            self.domain_slice.copy_within(
                                page_start + write_memory.len()..page_start + PAGE_SIZE,
                                old_page_start + write_memory.len(),
                            );
                            occupied.insert((PageType::Page, new_page));
                        }
                        PageType::Page => {
                            let page_start = **page as usize;
                            let page =
                                &mut self.domain_slice[page_start..page_start + write_memory.len()];
                            page.copy_from_slice(write_memory);
                        }
                    };
                }
            }
        }

        // trace!("Overlay after write: {:?}", self.overlay);
        Ok(())
    }

    fn read<T>(&self, mut offset: usize, read_buffer: &mut [T]) -> DandelionResult<()> {
        // check that buffer has proper allighment
        if !offset.is_multiple_of(core::mem::align_of::<T>()) {
            log::debug!("Misaligned write at offset {}", offset);
            return err_dandelion!(DandelionError::ReadMisaligned);
        }

        let read_size = core::mem::size_of_val(read_buffer);
        if offset + read_size > self.context_size {
            log::debug!("Read out of bounds at offset {}", offset);
            return err_dandelion!(DandelionError::InvalidRead);
        }
        let mut read_memory = unsafe {
            core::slice::from_raw_parts_mut(read_buffer.as_mut_ptr() as *mut u8, read_size)
        };

        trace!(
            "Read from kvm context at offset: {}, size: {}",
            offset,
            read_size,
        );

        if read_size == 0 {
            return Ok(());
        }

        while !read_memory.is_empty() {
            let new_chunck = self.get_chunk_ref(offset, read_memory.len())?;
            let chunck_size = new_chunck.len();
            trace!("chunck size: {}", chunck_size);
            read_memory[..chunck_size].copy_from_slice(new_chunck);
            offset += chunck_size;
            read_memory = &mut read_memory[chunck_size..];
        }
        Ok(())

        // read parts of a page if not full page:
        // let overlay_range = self.overlay.range(offset..);
        // for (virtual_page_start, (page_type, page)) in
        //     self.overlay.range(round_down_to_page(offset))
        // {
        //     if *overlay_start > offset {
        //         return err_dandelion!(DandelionError::InvalidRead);
        //     }

        //     // check how much to read from the overlay, know that offset >= overlay start or that read buffer is empty
        //     if !read_memory.is_empty() {
        //         // get offset into the overlay
        //         let overlay_offset = offset - overlay_start;
        //         let additional_bytes = min(*overlay_end - offset + 1, read_memory.len());
        //         if let Some(overlay_context) = overlay_option {
        //             overlay_context.context.read(
        //                 overlay_context.offset + overlay_offset,
        //                 &mut read_memory[..additional_bytes],
        //             )?;
        //         } else {
        //             read_memory[..additional_bytes]
        //                 .copy_from_slice(&self.storage[offset..offset + additional_bytes]);
        //         }
        //         read_memory = &mut read_memory[additional_bytes..];
        //         offset += additional_bytes;
        //         if read_memory.is_empty() {
        //             return Ok(());
        //         }
        //     } else {
        //         return Ok(());
        //     }
        // }
        // err_dandelion!(DandelionError::InvalidRead)
    }

    fn get_chunk_ref(&self, offset: usize, length: usize) -> DandelionResult<&[u8]> {
        if offset + length > self.context_size {
            return err_dandelion!(DandelionError::InvalidRead);
        }

        // check if the offset is into an overlayed object
        trace!(
            "Trying to get chunk at offset: {} with length {}",
            offset,
            length,
        );
        let page_start = round_down_to_page(offset);
        let (_, page_arc) = self
            .overlay
            .get(&page_start)
            .ok_or(dandelion_err!(DandelionError::InvalidRead))?;
        let page = **page_arc as usize;
        let page_offset = offset % PAGE_SIZE;
        let read_end = usize::min(page_offset + length, PAGE_SIZE);
        trace!(
            "returning slice from page {} with offset {} to {}",
            page,
            page_offset,
            read_end
        );
        Ok(&self.domain_slice[page + page_offset..page + read_end])
        // if let Some((&overlay_end, (overlay_start, overlay_option))) =
        //     self.overlay.range(offset..).next()
        // {
        //     // overlay object ends after offset, so if overlay start is smaller than offset,
        //     // it reads from inside the overlay, otherise it is from in front of the overlay
        //     if *overlay_start <= offset {
        //         if let Some(overlay_context) = overlay_option {
        //             let overlay_offset = offset - overlay_start;
        //             let chunk_size = min(overlay_end + 1 - offset, length);
        //             overlay_context
        //                 .context
        //                 .get_chunk_ref(overlay_context.offset + overlay_offset, chunk_size)
        //         } else {
        //             // offset is before overlay start, so can read at most up to overlay start
        //             let chunk_end = min(offset + length, overlay_end + 1);
        //             Ok(&self.storage[offset..chunk_end])
        //         }
        //     } else {
        //     }
        // } else {
        //     err_dandelion!(DandelionError::InvalidRead)
        // }
    }
}

// impl Drop for KvmContext {
//     fn drop(&mut self) {
//         unsafe {
//             nix::sys::mman::munmap(
//                 NonNull::new_unchecked(self.storage.as_mut_ptr() as *mut c_void),
//                 self.storage.len(),
//             )
//             .unwrap();
//         };
//         let size = u32::try_from(self.storage.len() / PAGE_SIZE).unwrap();
//         self.domain
//             .lock()
//             .unwrap()
//             .insert(self.rangepool_start, self.rangepool_start + size);
//     }
// }

// TODO: consider counting physical pages available (or approximation thereof) to delay scheduling when there are no pages
#[derive(Debug)]
pub struct KvmMemoryDomain {
    // occupation: Arc<Mutex<RangePool<u32>>>,
    domain_start: *mut u8,
    domain_size: usize,
}
unsafe impl Send for KvmMemoryDomain {}
unsafe impl Sync for KvmMemoryDomain {}

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

        let domain_size = size.next_multiple_of(PAGE_SIZE);
        trace!("Creating KVM domain with size: {}", domain_size);

        let domain_start = unsafe {
            ptr::from_mut(
                nix::sys::mman::mmap_anonymous(
                    None,
                    NonZeroUsize::new(domain_size).unwrap(),
                    ProtFlags::all(),
                    MapFlags::MAP_SHARED,
                )
                .or(err_dandelion!(DandelionError::DomainError(
                    dandelion_commons::DomainError::Mapping,
                )))?
                .as_mut(),
            ) as *mut u8
        };

        // to initialize the free page pointer list, need to cast it as u64 slice
        let page_size = PAGE_SIZE / size_of::<u64>();
        let memory = unsafe {
            core::slice::from_raw_parts_mut(
                domain_start as *mut u64,
                domain_size / size_of::<u64>(),
            )
        };
        // step through in page / sizeof(u64) steps and set pointers
        for (page_index, page_start) in memory.iter_mut().step_by(page_size).enumerate() {
            *page_start = ((page_index + 1) * PAGE_SIZE) as u64;
        }
        // set last page to NULL pointer
        memory[(domain_size - PAGE_SIZE) / size_of::<u64>()] = 0;

        Ok(Box::new(KvmMemoryDomain {
            domain_start,
            domain_size,
        }))
    }

    fn acquire_context(&self, size: usize) -> DandelionResult<Context> {
        // round up to next page size
        if size > self.domain_size {
            return err_dandelion!(DandelionError::DomainError(
                dandelion_commons::DomainError::InvalidMemorySize,
            ));
        }

        // let number_of_pages = u32::try_from(size.div_ceil(PAGE_SIZE)).unwrap();
        // size = (number_of_pages as usize) * PAGE_SIZE;
        // let page = self
        //     .occupation
        //     .lock()
        //     .unwrap()
        //     .get(number_of_pages, u32::MIN)
        //     .ok_or(dandelion_err!(DandelionError::DomainError(
        //         dandelion_commons::DomainError::ReachedCapacity,
        //     )))?;
        // let file_offset = (page as usize) * PAGE_SIZE;
        // // TODO replace casting with NonNull::as_mut_ptr() when it stabilizes
        // let mapping_pointer = unsafe {
        //     ptr::from_mut(
        //         nix::sys::mman::mmap(
        //             None,
        //             NonZeroUsize::new(size).unwrap(),
        //             ProtFlags::all(),
        //             MapFlags::MAP_SHARED,
        //             &self.fd,
        //             i64::try_from(file_offset).unwrap(),
        //         )
        //         .or(err_dandelion!(DandelionError::DomainError(
        //             dandelion_commons::DomainError::Mapping,
        //         )))?
        //         .as_mut(),
        //     )
        // } as *mut u8;
        // let storage = unsafe { core::slice::from_raw_parts_mut(mapping_pointer, size) };

        let new_context = Box::new(KvmContext {
            overlay: BTreeMap::new(),
            context_size: size,
            domain_slice: unsafe {
                core::slice::from_raw_parts_mut(self.domain_start, self.domain_size)
            },
        });
        Ok(Context::new(ContextType::Kvm(new_context), size))
    }
}

/// Function to find a destination offset, which allows to zero copy pages in the transfer
/// Return the index after which to insert the new occupation and the destination address
pub fn get_transfer_offset(
    occupation: &[crate::Position],
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
    Ok((index, start_address))
}

pub fn transfer_into(
    destination: &mut KvmContext,
    source: &Arc<Context>,
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
    if destination_offset + size > destination.context_size {
        debug!(
            "Trying to transfer into KVM context with destination {} + size {} > context size {}",
            destination_offset, size, destination.context_size
        );
        return err_dandelion!(DandelionError::InvalidWrite);
    }
    // don't need to check if transfers may partially overlap, since the occupation checks for that.
    // if occupation check was fine, can overwrite here (may happen because of planned overwrite or
    // because of page rounding)

    if let ContextType::Kvm(kvm_source_context) = &source.context {
        // check conditions for mapping to be possible
        if size >= PAGE_SIZE && source_offset % PAGE_SIZE == destination_offset % PAGE_SIZE {
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

            let mut remapped_bytes = 0;
            destination.overlay.extend(
                kvm_source_context
                    .overlay
                    .range(source_rounded_start..source_rounded_end)
                    .map(|(_, (_, page))| {
                        let result = (
                            rounded_start + remapped_bytes,
                            (PageType::CoW, page.clone()),
                        );
                        remapped_bytes += PAGE_SIZE;
                        result
                    }),
            );

            debug_assert_eq!(rounded_size, remapped_bytes);
            return Ok(());
        }
    }
    trace!("Transfer into KVM context from other type of context");
    let mut bytes_written = 0;
    while bytes_written < size {
        let chunk = source.get_chunk_ref(source_offset + bytes_written, size - bytes_written)?;
        debug_assert_ne!(0, chunk.len(), "Chunks should never be zero");
        destination.write(destination_offset + bytes_written, chunk)?;
        bytes_written += chunk.len();
    }

    Ok(())
}

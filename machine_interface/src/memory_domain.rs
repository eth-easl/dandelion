// list of memory domain implementations
#[cfg(feature = "cheri")]
pub mod cheri;
pub mod malloc;
pub mod mmap;
#[cfg(any(feature = "mmu", feature = "gpu"))]
pub mod mmu;
pub mod read_only;
#[cfg(feature = "wasm")]
pub mod wasm;

use crate::{DataItem, DataSet, Position};
use dandelion_commons::{DandelionError, DandelionResult};
use serde::{Deserialize, Serialize};

pub trait ContextTrait: Send + Sync {
    fn write<T>(&mut self, offset: usize, data: &[T]) -> DandelionResult<()>;
    fn read<T>(&self, offset: usize, read_buffer: &mut [T]) -> DandelionResult<()>;
}

// https://docs.rs/enum_dispatch/latest/enum_dispatch/index.html
// check if this would be better way to do it
#[derive(Debug)]
pub enum ContextType {
    Malloc(Box<malloc::MallocContext>),
    Mmap(Box<mmap::MmapContext>),
    ReadOnly(Box<read_only::ReadOnlyContext>),
    #[cfg(feature = "cheri")]
    Cheri(Box<cheri::CheriContext>),
    #[cfg(any(feature = "mmu", feature = "gpu"))]
    Mmu(Box<mmu::MmuContext>),
    #[cfg(feature = "wasm")]
    Wasm(Box<wasm::WasmContext>),
}

impl ContextTrait for ContextType {
    fn write<T>(&mut self, offset: usize, data: &[T]) -> DandelionResult<()> {
        match self {
            ContextType::Malloc(context) => context.write(offset, data),
            ContextType::Mmap(context) => context.write(offset, data),
            ContextType::ReadOnly(context) => context.write(offset, data),
            #[cfg(feature = "cheri")]
            ContextType::Cheri(context) => context.write(offset, data),
            #[cfg(any(feature = "mmu", feature = "gpu"))]
            ContextType::Mmu(context) => context.write(offset, data),
            #[cfg(feature = "wasm")]
            ContextType::Wasm(context) => context.write(offset, data),
        }
    }
    fn read<T>(&self, offset: usize, read_buffer: &mut [T]) -> DandelionResult<()> {
        match self {
            ContextType::Malloc(context) => context.read(offset, read_buffer),
            ContextType::Mmap(context) => context.read(offset, read_buffer),
            ContextType::ReadOnly(context) => context.read(offset, read_buffer),
            #[cfg(feature = "cheri")]
            ContextType::Cheri(context) => context.read(offset, read_buffer),
            #[cfg(any(feature = "mmu", feature = "gpu"))]
            ContextType::Mmu(context) => context.read(offset, read_buffer),
            #[cfg(feature = "wasm")]
            ContextType::Wasm(context) => context.read(offset, read_buffer),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ContextState {
    InPreparation,
    Run(i32),
}

#[derive(Debug)]
pub struct Context {
    pub context: ContextType,
    pub content: Vec<Option<DataSet>>,
    pub size: usize,
    pub state: ContextState,
    pub occupation: Vec<Position>,
}

impl ContextTrait for Context {
    fn write<T>(&mut self, offset: usize, data: &[T]) -> DandelionResult<()> {
        self.context.write(offset, data)
    }
    fn read<T>(&self, offset: usize, read_buffer: &mut [T]) -> DandelionResult<()> {
        self.context.read(offset, read_buffer)
    }
}

impl Context {
    pub fn new(con: ContextType, size: usize) -> Self {
        return Context {
            context: con,
            content: vec![],
            size: size,
            state: ContextState::InPreparation,
            occupation: vec![
                Position { offset: 0, size: 0 },
                Position {
                    offset: size,
                    size: 0,
                },
            ],
        };
    }
    /// Mark area between offset and offset + size as occupied
    /// Start search on index and merge occupation with any overlapping occupation
    /// Assumes offset is larger than or equal to the offset of occupation at index
    fn insert(&mut self, index: usize, offset: usize, size: usize) {
        let mut check_index = index;

        // only merge with previous occupation on seamless insert, leave a hole otherwise
        if (self.occupation[index].offset + self.occupation[index].size) == offset {
            self.occupation[index].size = offset - self.occupation[index].offset + size;
        } else {
            self.occupation.insert(
                index + 1,
                Position {
                    offset: offset,
                    size,
                },
            );
            check_index = index + 1;
        }
        while self.occupation.len() > check_index + 1
            && self.occupation[check_index + 1].offset
                <= (self.occupation[check_index].offset + self.occupation[check_index].size)
        {
            self.occupation[check_index].size = self.occupation[check_index + 1].offset
                - self.occupation[check_index].offset
                + self.occupation[check_index + 1].size;
            self.occupation.remove(check_index + 1);
        }
    }
    /// Make sure all space between offset and size is marked as occupied, ignoring overlap with previous occupation
    pub fn occupy_space(&mut self, offset: usize, size: usize) -> DandelionResult<()> {
        if offset + size > self.size {
            return Err(DandelionError::InvalidWrite);
        }
        let insertion_index = self
            .occupation
            .windows(2)
            .enumerate()
            .find_map(|(index, pos)| {
                if offset >= pos[0].offset && offset < pos[1].offset {
                    return Some(index);
                } else {
                    return None;
                }
            });
        if let Some(index) = insertion_index {
            self.insert(index, offset, size);
        }
        return Ok(());
    }
    pub fn get_free_space(&mut self, size: usize, alignment: usize) -> DandelionResult<usize> {
        // search for smallest space that is bigger than size
        // space start holds previous start
        let mut space_size = self.size + 1;
        let mut index = 0;
        let mut start_address = 0;
        for (window_index, occupied) in self.occupation.windows(2).enumerate() {
            let lower_end = occupied[0].offset + occupied[0].size;
            // TODO use next multiple of when stabilized
            let start = ((lower_end + alignment - 1) / alignment) * alignment;
            let end = occupied[1].offset;
            let available = end - start;
            if available >= size && available < space_size {
                space_size = available;
                index = window_index;
                start_address = start;
            }
        }
        if self.size + 1 == space_size {
            return Err(DandelionError::ContextFull);
        }
        self.insert(index, start_address, size);
        return Ok(start_address);
    }
    pub fn get_free_space_and_write_slice<T>(&mut self, data: &[T]) -> DandelionResult<*const T> {
        let alloc_size = data.len() * core::mem::size_of::<T>();
        let offset = self.get_free_space(alloc_size, core::mem::align_of::<T>())?;
        self.write(offset, data)?;
        Ok(offset as *const T)
    }
    pub fn get_last_item_end(&self) -> usize {
        let last_item = self.occupation[self.occupation.len() - 2];
        return last_item.offset + last_item.size;
    }
    pub fn clear_metadata(&mut self) -> () {
        self.content = vec![];
        self.occupation = vec![
            Position { offset: 0, size: 0 },
            Position {
                offset: self.size,
                size: 0,
            },
        ];
    }
}

/// TODO remove clone / copy once we have an implementation that needs an input
#[derive(Clone, Copy)]
pub enum MemoryResource {
    None,
}

pub trait MemoryDomain: Sync + Send {
    // allocation and distruction
    fn init(resource: MemoryResource) -> DandelionResult<Box<dyn MemoryDomain>>
    where
        Self: Sized;
    fn acquire_context(&self, size: usize) -> DandelionResult<Context>;
}

// Code to specialize transfers between different domains
pub fn transfer_memory(
    destination: &mut Context,
    source: &Context,
    destination_offset: usize,
    source_offset: usize,
    size: usize,
) -> DandelionResult<()> {
    #[allow(clippy::needless_return)]
    return match (&mut destination.context, &source.context) {
        (ContextType::Malloc(destination_ctxt), ContextType::Malloc(source_ctxt)) => {
            malloc::malloc_transfer(
                destination_ctxt,
                source_ctxt,
                destination_offset,
                source_offset,
                size,
            )
        }
        (ContextType::Mmap(destination_ctxt), ContextType::Mmap(source_ctxt)) => mmap::io_transfer(
            destination_ctxt,
            source_ctxt,
            destination_offset,
            source_offset,
            size,
        ),
        #[cfg(feature = "cheri")]
        (ContextType::Cheri(destination_ctxt), ContextType::Cheri(source_ctxt)) => {
            cheri::cheri_transfer(
                destination_ctxt,
                source_ctxt,
                destination_offset,
                source_offset,
                size,
            )
        }
        #[cfg(any(feature = "mmu", feature = "gpu"))]
        (ContextType::Mmu(destination_ctxt), ContextType::Mmu(source_ctxt)) => mmu::mmu_transfer(
            destination_ctxt,
            source_ctxt,
            destination_offset,
            source_offset,
            size,
        ),
        #[cfg(feature = "wasm")]
        (ContextType::Wasm(destination_ctxt), ContextType::Wasm(source_ctxt)) => {
            wasm::wasm_transfer(
                destination_ctxt,
                source_ctxt,
                destination_offset,
                source_offset,
                size,
            )
        }
        // default implementation using reads and writes
        (destination, source) => {
            let mut read_buffer: Vec<u8> = vec![0; size];
            source.read(source_offset, &mut read_buffer)?;
            destination.write(destination_offset, &read_buffer)
        }
    };
}

/// Transfer a complete dataset from one context to another.
/// If there is already a dataset with the given desintation set index present,
/// the items from the source set will be added to that set, keeping the identifier of the previous set.
/// TODO consider removing destination set name and require the set be present instead
pub fn transfer_data_set(
    destination: &mut Context,
    source: &Context,
    destionation_set_index: usize,
    destination_allignment: usize,
    destination_set_name: &str,
    source_set_index: usize,
) -> DandelionResult<()> {
    // check if source has set
    if source.content.len() <= source_set_index {
        return Err(DandelionError::TransferInputNoSetAvailable);
    }
    let source_set = source.content[source_set_index]
        .as_ref()
        .ok_or(DandelionError::EmptyDataSet)?;
    if destination.content.len() <= destionation_set_index {
        destination
            .content
            .resize_with(destionation_set_index + 1, || None);
    }
    let destination_index_offset = destination.content[destionation_set_index]
        .as_ref()
        .and_then(|set| Some(set.buffers.len()))
        .unwrap_or(0);
    for index in 0..source_set.buffers.len() {
        transfer_data_item(
            destination,
            source,
            destionation_set_index,
            destination_allignment,
            destination_index_offset + index,
            destination_set_name,
            source_set_index,
            index,
        )?;
    }
    return Ok(());
}

/// Transfer a data item from one context to another.
/// If the destination does not yet have a set at the index,
/// a new one is created using the set name given.
pub fn transfer_data_item(
    destination: &mut Context,
    source: &Context,
    destination_set_index: usize,
    destination_allignment: usize,
    destination_item_index: usize,
    destination_set_name: &str,
    source_set_index: usize,
    source_item_index: usize,
) -> DandelionResult<()> {
    // check if source has item
    if source.content.len() <= source_set_index {
        return Err(DandelionError::TransferInputNoSetAvailable);
    }
    let source_set = source.content[source_set_index]
        .as_ref()
        .ok_or(DandelionError::EmptyDataSet)?;
    if source_set.buffers.len() <= source_item_index {
        return Err(DandelionError::TransferInputNoSetAvailable);
    }

    if destination.content.len() <= destination_set_index {
        destination
            .content
            .resize_with(destination_set_index + 1, || None)
    }
    let source_item = &source_set.buffers[source_item_index];
    let destination_offset =
        destination.get_free_space(source_item.data.size, destination_allignment)?;
    {
        let destination_set =
            &mut destination.content[destination_set_index].get_or_insert(DataSet {
                ident: destination_set_name.to_string(),
                buffers: vec![],
            });
        if destination_set.buffers.len() <= destination_item_index {
            destination_set
                .buffers
                .resize_with(destination_item_index + 1, || DataItem {
                    ident: String::from(""),
                    data: Position { offset: 0, size: 0 },
                    key: 0,
                });
        } else if destination_set.buffers[destination_item_index].data.size > 0 {
            return Err(DandelionError::TransferItemAlreadyPresent);
        }
        destination_set.buffers[destination_item_index].data.offset = destination_offset;
        destination_set.buffers[destination_item_index].data.size = source_item.data.size;
        destination_set.buffers[destination_item_index].ident = source_item.ident.clone();
    }

    transfer_memory(
        destination,
        source,
        destination_offset,
        source_item.data.offset,
        source_item.data.size,
    )?;
    Ok(())
}

#[cfg(test)]
mod domain_tests;

// list of memory domain implementations
#[cfg(feature = "cheri")]
pub mod cheri;
pub mod malloc;
#[cfg(feature = "pagetable")]
pub mod pagetable;

use crate::{DataItem, DataSet, Position};
use dandelion_commons::{DandelionError, DandelionResult};

pub trait ContextTrait: Send + Sync {
    fn write(&mut self, offset: usize, data: Vec<u8>) -> DandelionResult<()>;
    fn read(&self, offset: usize, read_size: usize) -> DandelionResult<Vec<u8>>;
}

// https://docs.rs/enum_dispatch/latest/enum_dispatch/index.html
// check if this would be better way to do it
pub enum ContextType {
    Malloc(Box<malloc::MallocContext>),
    #[cfg(feature = "cheri")]
    Cheri(Box<cheri::CheriContext>),
    #[cfg(feature = "pagetable")]
    Pagetable(Box<pagetable::PagetableContext>),
}

impl ContextTrait for ContextType {
    fn write(&mut self, offset: usize, data: Vec<u8>) -> DandelionResult<()> {
        match self {
            ContextType::Malloc(context) => context.write(offset, data),
            #[cfg(feature = "cheri")]
            ContextType::Cheri(context) => context.write(offset, data),
            #[cfg(feature = "pagetable")]
            ContextType::Pagetable(context) => context.write(offset, data),
        }
    }
    fn read(&self, offset: usize, read_size: usize) -> DandelionResult<Vec<u8>> {
        match self {
            ContextType::Malloc(context) => context.read(offset, read_size),
            #[cfg(feature = "cheri")]
            ContextType::Cheri(context) => context.read(offset, read_size),
            #[cfg(feature = "pagetable")]
            ContextType::Pagetable(context) => context.read(offset, read_size),
        }
    }
}
pub struct Context {
    pub context: ContextType,
    pub content: Vec<Option<DataSet>>,
    pub size: usize,
    #[cfg(feature = "pagetable")]
    // pub protection_requirements: (Vec<Position>, Vec<Position>),
    pub protection_requirements: Vec<(u32, Position)>,
    occupation: Vec<Position>,
}

impl ContextTrait for Context {
    fn write(&mut self, offset: usize, data: Vec<u8>) -> DandelionResult<()> {
        self.context.write(offset, data)
    }
    fn read(&self, offset: usize, read_size: usize) -> DandelionResult<Vec<u8>> {
        self.context.read(offset, read_size)
    }
}

impl Context {
    pub fn new(con: ContextType, size: usize) -> Self {
        return Context {
            context: con,
            content: vec![],
            size: size,
            occupation: vec![
                Position { offset: 0, size: 0 },
                Position {
                    offset: size,
                    size: 0,
                },
            ],
            #[cfg(feature = "pagetable")]
                protection_requirements: vec![]
        };
    }
    fn insert(&mut self, index: usize, offset: usize, size: usize) {
        if (self.occupation[index].offset + self.occupation[index].size) == offset {
            self.occupation[index].size += size;
        } else {
            self.occupation.insert(
                index + 1,
                Position {
                    offset: offset,
                    size,
                },
            )
        }
    }
    pub fn occupy_space(&mut self, offset: usize, size: usize) -> DandelionResult<()> {
        let insertion_index = self
            .occupation
            .windows(2)
            .enumerate()
            .find_map(|(index, pos)| {
                let start = pos[0].offset + pos[0].size;
                let end = pos[1].offset;
                if offset >= start && offset + size < end {
                    return Some(index);
                } else {
                    return None;
                }
            });
        if let Some(index) = insertion_index {
            self.insert(index, offset, size);
        } else {
            return Err(DandelionError::ContextFull);
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
            if available > size && available < space_size {
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
    pub fn get_last_item_end(&self) -> usize {
        let last_item = self.occupation[self.occupation.len() - 2];
        return last_item.offset + last_item.size;
    }
}

pub trait MemoryDomain: Sync + Send {
    // allocation and distruction
    fn init(config: Vec<u8>) -> DandelionResult<Box<dyn MemoryDomain>>
    where
        Self: Sized;
    fn acquire_context(&self, size: usize) -> DandelionResult<Context>;
    fn release_context(&self, context: Context) -> DandelionResult<()>;
}

// Code to specialize transfers between different domains
pub fn transefer_memory(
    destination: &mut Context,
    source: &Context,
    destination_offset: usize,
    source_offset: usize,
    size: usize,
) -> DandelionResult<()> {
    let result = match (&mut destination.context, &source.context) {
        (ContextType::Malloc(destination_ctxt), ContextType::Malloc(source_ctxt)) => {
            malloc::malloc_transfer(
                destination_ctxt,
                source_ctxt,
                destination_offset,
                source_offset,
                size,
            )
        }
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
        #[cfg(feature = "pagetable")]
        (ContextType::Pagetable(destination_ctxt), ContextType::Pagetable(source_ctxt)) => {
            pagetable::pagetable_transfer(
                destination_ctxt,
                source_ctxt,
                destination_offset,
                source_offset,
                size,
            )
        }
        // default implementation using reads and writes
        (destination, source) => {
            let read_result = source.read(source_offset, size);
            match read_result {
                Ok(read_value) => destination.write(destination_offset, read_value),
                Err(err) => Err(err),
            }
        }
    };
    result
}

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
    for index in 0..source_set.buffers.len() {
        transer_data_item(
            destination,
            source,
            destionation_set_index,
            destination_allignment,
            index,
            destination_set_name,
            source_set_index,
            index,
        )?;
    }
    return Ok(());
}

pub fn transer_data_item(
    destination: &mut Context,
    source: &Context,
    destionation_set_index: usize,
    destination_allignment: usize,
    destination_item_index: usize,
    destination_set_name: &str,
    source_set_index: usize,
    source_item_index: usize,
) -> DandelionResult<()> {
    // check if source has item
    if source.content.len() <= source_set_index {
        return Err(DandelionError::InvalidRead);
    }
    let source_set = source.content[source_set_index]
        .as_ref()
        .ok_or(DandelionError::EmptyDataSet)?;
    if source_set.buffers.len() <= source_item_index {
        return Err(DandelionError::TransferInputNoSetAvailable);
    }

    if destination.content.len() <= destionation_set_index {
        destination
            .content
            .resize_with(destionation_set_index + 1, || None)
    }
    let source_item = &source_set.buffers[source_item_index];
    let destination_offset =
        destination.get_free_space(source_item.data.size, destination_allignment)?;
    {
        let destination_set =
            &mut destination.content[destionation_set_index].get_or_insert(DataSet {
                ident: destination_set_name.to_string(),
                buffers: vec![],
            });
        if destination_set.buffers.len() <= destination_item_index {
            destination_set
                .buffers
                .resize_with(destination_item_index + 1, || DataItem {
                    ident: String::from(""),
                    data: Position { offset: 0, size: 0 },
                });
        } else if destination_set.buffers[destination_item_index].data.size > 0 {
            return Err(DandelionError::TransferItemAlreadyPresent);
        }
        destination_set.buffers[destination_item_index].data.offset = destination_offset;
        destination_set.buffers[destination_item_index].data.size = source_item.data.size;
        destination_set.buffers[destination_item_index].ident = source_item.ident.clone();
    }

    transefer_memory(
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

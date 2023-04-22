use crate::util::shared_mem::SharedMem;
use crate::{DataItem, Position};
use nix::sys::mman::ProtFlags;

use super::super::{HardwareError, HwResult};
use super::{Context, ContextTrait, ContextType, MemoryDomain};

pub struct PagetableContext {
    pub storage: SharedMem,
}

impl ContextTrait for PagetableContext {
    fn write(&mut self, offset: usize, data: Vec<u8>) -> HwResult<()> {
        // check if the write is within bounds
        if offset + data.len() > self.storage.len() {
            return Err(HardwareError::InvalidWrite);
        }

        // write values
        unsafe {
            self.storage.as_slice_mut()[offset..offset + data.len()].copy_from_slice(&data);
        }

        Ok(())
    }

    fn read(&mut self, offset: usize, read_size: usize, sanitize: bool) -> HwResult<Vec<u8>> {
        if offset + read_size > self.storage.len() {
            return Err(HardwareError::InvalidRead);
        }

        // try to allocate space for read values
        let mut result_vec = Vec::new();
        if let Err(_) = result_vec.try_reserve(read_size) {
            return Err(HardwareError::OutOfMemory);
        }
        result_vec.resize(read_size, 0);

        // read values, sanitize if necessary
        unsafe {
            result_vec.copy_from_slice(&self.storage.as_slice()[offset..offset + read_size]);
            if sanitize {
                self.storage.as_slice_mut()[offset..offset + read_size].fill(0);
            }
        }

        Ok(result_vec)
    }
}

#[derive(Debug)]
pub struct PagetableMemoryDomain {}

impl MemoryDomain for PagetableMemoryDomain {
    fn init(config: Vec<u8>) -> HwResult<Self> {
        Ok(PagetableMemoryDomain {})
    }

    fn acquire_context(&mut self, size: usize) -> HwResult<Context> {
        // create and map a shared memory region
        let mem_space = match SharedMem::create(size, ProtFlags::PROT_READ | ProtFlags::PROT_WRITE)
        {
            Ok(v) => v,
            Err(_e) => return Err(HardwareError::OutOfMemory),
        };

        Ok(Context {
            context: ContextType::Pagetable(Box::new(PagetableContext { storage: mem_space })),
            dynamic_data: Vec::<DataItem>::new(),
            static_data: Vec::<Position>::new(),
            #[cfg(feature = "pagetable")]
            protection_requirements: Vec::<(u32, Position)>::new(),
        })
    }

    fn release_context(&mut self, context: Context) -> HwResult<()> {
        match context.context {
            ContextType::Pagetable(_) => Ok(()),
            _ => Err(HardwareError::ContextMissmatch),
        }
    }
}

// pub fn malloc_transfer(
//     destination: &mut PagetableContext,
//     source: &mut PagetableContext,
//     destination_offset: usize,
//     source_offset: usize,
//     size: usize,
//     sanitize: bool,
// ) -> HwResult<()> {
//     Ok(())
// }

use super::super::{ControllerError, HwResult};
use super::{MemoryDomain, MemoryDomainController, MemoryDomainTuple};

#[derive(Debug)]
pub struct MallocMemoryController {}

#[derive(Debug)]
pub struct MallocMemoryDomain {
    storage: Vec<u8>,
}

impl MemoryDomainController for MallocMemoryController {
    type ControllerMemoryDomain = MallocMemoryDomain;
    fn start_domain_controller(_domain_config: Vec<u8>) -> HwResult<Box<Self>> {
        Ok(Box::new(MallocMemoryController {}))
    }
    fn stop_domain_controller(self) -> HwResult<()> {
        Ok(())
    }
    fn alloc_memory_subdomain(&self, size: usize) -> HwResult<MemoryDomain> {
        let mut mem_space = Vec::new();
        if (mem_space.try_reserve_exact(size)) != Ok(()) {
            return Err(ControllerError::OutOfMemory);
        }
        mem_space.resize(size, 0);
        let new_tuple = MemoryDomainTuple {
            controller: self,
            domain: Some(MallocMemoryDomain { storage: mem_space }),
        };
        Ok(MemoryDomain::Malloc(new_tuple))
    }
    fn free_memory_subdomain(&self, _domain: Self::ControllerMemoryDomain) -> HwResult<()> {
        Ok(())
    }
    fn write(&self, domain: &mut MallocMemoryDomain, offset: usize, data: Vec<u8>) -> HwResult<()> {
        // check if the write is within bounds
        let length = domain.storage.capacity();
        if offset + data.len() > length {
            return Err(ControllerError::InvalidWrite);
        }
        // write values
        for (pos, value) in data.into_iter().enumerate() {
            domain.storage[offset + pos] = value;
        }
        Ok(())
    }
    fn read(
        &self,
        domain: &mut MallocMemoryDomain,
        offset: usize,
        read_size: usize,
        sanitize: bool,
    ) -> HwResult<Vec<u8>> {
        let length = domain.storage.len();
        if offset + read_size > length {
            return Err(ControllerError::InvalidRead);
        }
        // try to allocate space for read values
        let mut result_vec = Vec::new();
        if let Err(_) = result_vec.try_reserve(read_size) {
            return Err(ControllerError::OutOfMemory);
        }
        result_vec.resize(read_size, 0);
        // read values, sanitize if necessary
        for (index, result) in result_vec.iter_mut().enumerate() {
            *result = domain.storage[offset + index];
            if sanitize {
                domain.storage[offset + index] = 0;
            }
        }
        Ok(result_vec)
    }
}

pub fn malloc_transfer(
    source_tuple: &MemoryDomainTuple<MallocMemoryController>,
    destination_tuple: &MemoryDomainTuple<MallocMemoryController>,
) -> HwResult<()> {
    match (&source_tuple.domain, &destination_tuple.domain) {
        (Some(_), Some(_)) => Ok(()),
        (Some(_), None) => Ok(()),
        (None, Some(_)) => Ok(()),
        (None, None) => Ok(()),
    }
}

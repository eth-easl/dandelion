// list of memory domain implementations
mod malloc;

// import parent for depenencies
use super::{ControllerError, HwResult};

pub trait MemoryDomainController {
    type ControllerMemoryDomain;
    // initialization and shutdown
    fn start_domain_controller(domain_config: Vec<u8>) -> HwResult<Box<Self>>;
    fn stop_domain_controller(self) -> HwResult<()>;
    // allocation and distruction
    fn alloc_memory_subdomain(&self, size: usize) -> HwResult<MemoryDomain>;
    fn free_memory_subdomain(&self, domain: Self::ControllerMemoryDomain) -> HwResult<()>;
    // direct access
    fn write(
        &self,
        domain: &mut Self::ControllerMemoryDomain,
        offset: usize,
        data: Vec<u8>,
    ) -> HwResult<()>;
    fn read(
        &self,
        domain: &mut Self::ControllerMemoryDomain,
        offset: usize,
        read_size: usize,
        sanitize: bool,
    ) -> HwResult<Vec<u8>>;
}

// Todo implement dropping behaviour for memory controller
// impl Drop for MemoryDomainController {
//     fn drop(& mut self){
//         self.removeEU();
//     }
// }

// structure to hold a domain with a reference to the associated controller
#[derive(Debug)]
pub struct MemoryDomainTuple<'controller, T: MemoryDomainController> {
    controller: &'controller T,
    domain: Option<T::ControllerMemoryDomain>,
}
// make sure domains are dropped correctly by notifying the associated controller
impl<'controller, T: MemoryDomainController> Drop for MemoryDomainTuple<'controller, T> {
    fn drop(&mut self) {
        let mut owned_domain = None;
        core::mem::swap(&mut owned_domain, &mut self.domain);
        let result = match owned_domain {
            Some(domain) => self.controller.free_memory_subdomain(domain),
            None => Ok(()),
        };
        result.unwrap()
    }
}

// Memory domain has a reference to the associated controller and an option to a domain.
#[derive(Debug)]
pub enum MemoryDomain<'controller> {
    Blackhole,
    Malloc(MemoryDomainTuple<'controller, malloc::MallocMemoryController>),
    // CheriDomain(Option<MemoryDomainTuple<cheriController>>),
}

impl<'controller> MemoryDomain<'controller> {
    fn write(&mut self, offset: usize, data: Vec<u8>) -> HwResult<()> {
        match self {
            MemoryDomain::Blackhole => Ok(()),
            MemoryDomain::Malloc(tuple) => {
                if let Some(domain) = &mut tuple.domain {
                    tuple.controller.write(domain, offset, data)
                } else {
                    Err(ControllerError::InvalidDomain)
                }
            }
        }
    }
    fn read(&mut self, offset: usize, read_size: usize, sanitize: bool) -> HwResult<Vec<u8>> {
        match self {
            MemoryDomain::Malloc(tuple) => {
                if let Some(domain) = &mut tuple.domain {
                    tuple.controller.read(domain, offset, read_size, sanitize)
                } else {
                    Err(ControllerError::InvalidDomain)
                }
            }
            MemoryDomain::Blackhole => {
                let mut result = Vec::<u8>::new();
                if result.try_reserve(read_size) == Ok(()) {
                    Ok(result)
                } else {
                    Err(ControllerError::InvalidRead)
                }
            }
        }
    }
}

// Code to specialize transfers between different domains
pub fn transefer_memory(
    mut destination: MemoryDomain,
    mut source: MemoryDomain,
    destination_offset: usize,
    source_offset: usize,
    size: usize,
    sanitize: bool,
    callback: impl FnOnce(HwResult<()>, MemoryDomain, MemoryDomain) -> (),
) {
    let result = match (&mut destination, &mut source) {
        (MemoryDomain::Malloc(destination_tuple), MemoryDomain::Malloc(source_tuple)) => {
            malloc::malloc_transfer(&source_tuple, &destination_tuple)
        }
        // (MemoryDomain::CheriDomain, MemoryDomain::CheriDomain) => println!("Not yet implemented cheri mem transfer"),
        (MemoryDomain::Blackhole, MemoryDomain::Blackhole) => {
            println!("From the hole it comes to the hole it goes");
            Ok(())
        }
        // default implementation using reads and writes
        (dst, src) => {
            let read_result = src.read(source_offset, size, sanitize);
            match read_result {
                Ok(read_value) => dst.write(destination_offset, read_value),
                Err(err) => Err(err),
            }
        }
    };
    callback(result, destination, source);
}

#[cfg(test)]
mod tests;

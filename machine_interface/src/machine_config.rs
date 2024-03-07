use std::collections::BTreeMap;

#[allow(unused_imports)]
use crate::{
    function_driver::Driver,
    memory_domain::{MemoryDomain, MemoryResource},
};

/// Enum for all engine types that allows use in lookup structures
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum EngineType {
    #[cfg(feature = "hyper_io")]
    Hyper,
    #[cfg(feature = "cheri")]
    Cheri,
    #[cfg(feature = "wasm")]
    RWasm,
    #[cfg(feature = "mmu")]
    Process,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub enum DomainType {
    Mmap,
    #[cfg(feature = "cheri")]
    Cheri,
    #[cfg(feature = "wasm")]
    RWasm,
    #[cfg(feature = "mmu")]
    Process,
}

pub fn get_compatibilty_table() -> BTreeMap<EngineType, DomainType> {
    return BTreeMap::from([
        #[cfg(feature = "hyper_io")]
        (EngineType::Hyper, DomainType::Mmap),
        #[cfg(feature = "cheri")]
        (EngineType::Cheri, DomainType::Cheri),
        #[cfg(feature = "wasm")]
        (EngineType::RWasm, DomainType::RWasm),
        #[cfg(feature = "mmu")]
        (EngineType::Process, DomainType::Process),
    ]);
}

pub fn get_available_domains() -> BTreeMap<DomainType, Box<dyn MemoryDomain>> {
    return BTreeMap::from([
        (
            DomainType::Mmap,
            crate::memory_domain::mmap::MmapMemoryDomain::init(MemoryResource::None).unwrap(),
        ),
        #[cfg(feature = "cheri")]
        (
            DomainType::Cheri,
            crate::memory_domain::cheri::CheriMemoryDomain::init(MemoryResource::None).unwrap(),
        ),
        #[cfg(feature = "wasm")]
        (
            DomainType::RWasm,
            crate::memory_domain::wasm::WasmMemoryDomain::init(MemoryResource::None).unwrap(),
        ),
        #[cfg(feature = "mmu")]
        (
            DomainType::Process,
            crate::memory_domain::mmu::MmuMemoryDomain::init(MemoryResource::None).unwrap(),
        ),
    ]);
}

pub fn get_available_drivers() -> BTreeMap<EngineType, Box<dyn Driver>> {
    return BTreeMap::<EngineType, Box<dyn Driver>>::from([
        #[cfg(feature = "hyper_io")]
        (
            EngineType::Hyper,
            Box::new(crate::function_driver::system_driver::hyper::HyperDriver {})
                as Box<dyn Driver>,
        ),
        #[cfg(feature = "cheri")]
        (
            EngineType::Cheri,
            Box::new(crate::function_driver::compute_driver::cheri::CheriDriver {})
                as Box<dyn Driver>,
        ),
        #[cfg(feature = "wasm")]
        (
            EngineType::RWasm,
            Box::new(crate::function_driver::compute_driver::wasm::WasmDriver {})
                as Box<dyn Driver>,
        ),
        #[cfg(feature = "mmu")]
        (
            EngineType::Process,
            Box::new(crate::function_driver::compute_driver::mmu::MmuDriver {}) as Box<dyn Driver>,
        ),
    ]);
}

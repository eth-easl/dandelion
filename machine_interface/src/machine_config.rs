use std::{collections::BTreeMap, sync::Arc};

use crate::function_driver::SystemFunction;
#[allow(unused_imports)]
use crate::{
    function_driver::Driver,
    memory_domain::{MemoryDomain, MemoryResource},
};

/// Enum for all engine types that allows use in lookup structures
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum EngineType {
    #[cfg(feature = "reqwest_io")]
    Reqwest,
    #[cfg(feature = "cheri")]
    Cheri,
    #[cfg(feature = "wasm")]
    RWasm,
    #[cfg(feature = "mmu")]
    Process,
    #[cfg(feature = "kvm")]
    Kvm,
}

// EngineType conversion functions used by grpc (protobuf)
pub fn engine_type_to_i32(engine_type: &EngineType) -> i32 {
    match engine_type {
        #[cfg(feature = "reqwest_io")]
        EngineType::Reqwest => 0,
        #[cfg(feature = "cheri")]
        EngineType::Cheri => 1,
        #[cfg(feature = "wasm")]
        EngineType::RWasm => 2,
        #[cfg(feature = "mmu")]
        EngineType::Process => 3,
        #[cfg(feature = "kvm")]
        EngineType::Kvm => 4,
        #[cfg(not(any(
            feature = "reqwest_io",
            feature = "cheri",
            feature = "wasm",
            feature = "mmu",
            feature = "kvm"
        )))]
        _ => -1,
    }
}
pub fn i32_to_engine_type(val: i32) -> Option<EngineType> {
    match val {
        #[cfg(feature = "reqwest_io")]
        0 => Some(EngineType::Reqwest),
        #[cfg(feature = "cheri")]
        1 => Some(EngineType::Cheri),
        #[cfg(feature = "wasm")]
        2 => Some(EngineType::RWasm),
        #[cfg(feature = "mmu")]
        3 => Some(EngineType::Process),
        #[cfg(feature = "kvm")]
        4 => Some(EngineType::Kvm),
        _ => None,
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub enum DomainType {
    System,
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
        #[cfg(feature = "reqwest_io")]
        (EngineType::Reqwest, DomainType::System),
        #[cfg(feature = "cheri")]
        (EngineType::Cheri, DomainType::Cheri),
        #[cfg(feature = "wasm")]
        (EngineType::RWasm, DomainType::RWasm),
        #[cfg(feature = "mmu")]
        (EngineType::Process, DomainType::Process),
        #[cfg(feature = "kvm")]
        (EngineType::Kvm, DomainType::Mmap),
    ]);
}

#[cfg(any(feature = "reqwest_io"))]
const SYS_FUNC_DEFAULT_CONTEXT_SIZE: usize = 0x200_0000;

pub fn get_system_functions(engine_type: EngineType) -> Vec<(SystemFunction, usize)> {
    return match engine_type {
        #[cfg(feature = "reqwest_io")]
        EngineType::Reqwest => vec![(SystemFunction::HTTP, SYS_FUNC_DEFAULT_CONTEXT_SIZE)],
        #[allow(unreachable_patterns)]
        _ => Vec::new(),
    };
}

pub fn get_available_domains(
    resources: BTreeMap<DomainType, MemoryResource>,
) -> BTreeMap<DomainType, Arc<Box<dyn MemoryDomain>>> {
    let mut default_resources = BTreeMap::from([
        (DomainType::System, MemoryResource::None),
        (DomainType::Mmap, MemoryResource::Anonymous { size: 0 }),
        #[cfg(feature = "cheri")]
        (DomainType::Cheri, MemoryResource::Anonymous { size: 0 }),
        #[cfg(feature = "mmu")]
        (
            DomainType::Process,
            MemoryResource::Shared {
                id: u64::MAX,
                size: 0,
            },
        ),
        #[cfg(feature = "wasm")]
        (DomainType::RWasm, MemoryResource::Anonymous { size: 0 }),
    ]);
    for (dom, resource) in resources {
        default_resources.insert(dom, resource);
    }
    return default_resources
        .into_iter()
        .map(|(dom_type, resource)| match dom_type {
            DomainType::System => (
                dom_type,
                Arc::new(
                    crate::memory_domain::system_domain::SystemMemoryDomain::init(resource)
                        .unwrap(),
                ),
            ),
            DomainType::Mmap => (
                dom_type,
                Arc::new(crate::memory_domain::mmap::MmapMemoryDomain::init(resource).unwrap()),
            ),
            #[cfg(feature = "cheri")]
            DomainType::Cheri => (
                dom_type,
                Arc::new(crate::memory_domain::cheri::CheriMemoryDomain::init(resource).unwrap()),
            ),
            #[cfg(feature = "mmu")]
            DomainType::Process => (
                dom_type,
                Arc::new(crate::memory_domain::mmu::MmuMemoryDomain::init(resource).unwrap()),
            ),
            #[cfg(feature = "wasm")]
            DomainType::RWasm => (
                dom_type,
                Arc::new(crate::memory_domain::wasm::WasmMemoryDomain::init(resource).unwrap()),
            ),
        })
        .collect();
}

pub fn get_available_drivers() -> BTreeMap<EngineType, &'static dyn Driver> {
    return BTreeMap::<EngineType, &'static dyn Driver>::from([
        #[cfg(feature = "reqwest_io")]
        (
            EngineType::Reqwest,
            Box::leak(Box::new(
                crate::function_driver::system_driver::reqwest::ReqwestDriver {},
            )) as &'static dyn Driver,
        ),
        #[cfg(feature = "cheri")]
        (
            EngineType::Cheri,
            Box::leak(Box::new(
                crate::function_driver::compute_driver::cheri::CheriDriver {},
            )) as &'static dyn Driver,
        ),
        #[cfg(feature = "wasm")]
        (
            EngineType::RWasm,
            Box::leak(Box::new(
                crate::function_driver::compute_driver::wasm::WasmDriver {},
            )) as &'static dyn Driver,
        ),
        #[cfg(feature = "mmu")]
        (
            EngineType::Process,
            Box::leak(Box::new(
                crate::function_driver::compute_driver::mmu::MmuDriver {},
            )) as &'static dyn Driver,
        ),
        #[cfg(feature = "kvm")]
        (
            EngineType::Kvm,
            Box::leak(Box::new(
                crate::function_driver::compute_driver::kvm::KvmDriver {},
            )) as &'static dyn Driver,
        ),
    ]);
}

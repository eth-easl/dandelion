use std::{
    collections::BTreeMap,
    sync::{Arc, OnceLock},
};

use crate::function_driver::{ComputeResourceType, SystemFunction};
#[allow(unused_imports)]
use crate::{
    function_driver::Driver,
    memory_domain::{MemoryDomain, MemoryResource},
};

/// Enum for all engine types that allows use in lookup structures
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum EngineType {
    #[cfg(feature = "reqwest_io")]
    SystemCPU,
    #[cfg(feature = "cheri")]
    Cheri,
    #[cfg(feature = "wasm")]
    RWasm,
    #[cfg(feature = "mmu")]
    Process,
    #[cfg(feature = "kvm")]
    Kvm,
}

pub static ENGINE_TYPES: &'static [EngineType] = &[
    #[cfg(feature = "reqwest_io")]
    EngineType::SystemCPU,
    #[cfg(feature = "cheri")]
    EngineType::Cheri,
    #[cfg(feature = "wasm")]
    EngineType::RWasm,
    #[cfg(feature = "mmu")]
    EngineType::Process,
    #[cfg(feature = "kvm")]
    EngineType::Kvm,
];

pub static ENGINE_RESOURCE_MAP: &'static [ComputeResourceType] = &[
    // Reqwest resource type
    #[cfg(feature = "reqwest_io")]
    ComputeResourceType::CPU,
    // Cheri resource type
    #[cfg(feature = "cheri")]
    ComputeResourceType::CPU,
    // RWasm resource type
    #[cfg(feature = "wasm")]
    ComputeResourceType::CPU,
    // Process resource type
    #[cfg(feature = "mmu")]
    ComputeResourceType::CPU,
    // KVM resource type
    #[cfg(feature = "kvm")]
    ComputeResourceType::CPU,
];

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

/// The entries are ordered in the same order as the enum in engine type
/// This allows the enum to be used as indexes into the array
pub static ENGINE_DOMAIN_TABLE: &'static [DomainType] = &[
    // Engine type Reqwest
    #[cfg(feature = "reqwest_io")]
    DomainType::System,
    // Engine type Cheri
    #[cfg(feature = "cheri")]
    DomainType::Cheri,
    // Engine type RWasm
    #[cfg(feature = "wasm")]
    DomainType::RWasm,
    // Engine type Process
    #[cfg(feature = "mmu")]
    DomainType::Process,
    // Engine type KVM
    #[cfg(feature = "kvm")]
    DomainType::Mmap,
];

#[cfg(any(feature = "reqwest_io"))]
const SYS_FUNC_DEFAULT_CONTEXT_SIZE: usize = 0x200_0000;

pub fn get_system_functions(engine_type: EngineType) -> Vec<(SystemFunction, usize)> {
    return match engine_type {
        #[cfg(feature = "reqwest_io")]
        EngineType::SystemCPU => vec![(SystemFunction::HTTP, SYS_FUNC_DEFAULT_CONTEXT_SIZE)],
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

// TODO this should also be able to be a static table
// The self could probably be removed from the driver, if that simplifies it
pub fn get_available_drivers() -> &'static Vec<&'static dyn Driver> {
    static ENGINE_MAP: OnceLock<Vec<&'static dyn Driver>> = OnceLock::new();
    ENGINE_MAP.get_or_init(|| {
        ENGINE_TYPES
            .iter()
            .map(|engine_type| match engine_type {
                #[cfg(feature = "reqwest_io")]
                EngineType::SystemCPU => Box::leak(Box::new(
                    crate::function_driver::system_driver::reqwest::ReqwestDriver {},
                )) as &'static dyn Driver,
                #[cfg(feature = "cheri")]
                EngineType::Cheri => Box::leak(Box::new(
                    crate::function_driver::compute_driver::cheri::CheriDriver {},
                )) as &'static dyn Driver,
                #[cfg(feature = "wasm")]
                EngineType::RWasm => Box::leak(Box::new(
                    crate::function_driver::compute_driver::wasm::WasmDriver {},
                )) as &'static dyn Driver,
                #[cfg(feature = "mmu")]
                EngineType::Process => Box::leak(Box::new(
                    crate::function_driver::compute_driver::mmu::MmuDriver {},
                )) as &'static dyn Driver,
                #[cfg(feature = "kvm")]
                EngineType::Kvm => Box::leak(Box::new(
                    crate::function_driver::compute_driver::kvm::KvmDriver {},
                )) as &'static dyn Driver,
            })
            .collect()
    })
}

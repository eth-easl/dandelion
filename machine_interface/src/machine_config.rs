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
    #[cfg(feature = "mmu")]
    Process,
    #[cfg(feature = "kvm")]
    Kvm,
}

#[repr(usize)]
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub enum DomainType {
    System,
    #[cfg(feature = "cheri")]
    Cheri,
    #[cfg(feature = "kvm")]
    Kvm,
    #[cfg(feature = "mmu")]
    Process,
}

impl EngineType {
    pub const VARIANTS: &[EngineType] = &[
        #[cfg(feature = "reqwest_io")]
        EngineType::Reqwest,
        #[cfg(feature = "cheri")]
        EngineType::Cheri,
        #[cfg(feature = "mmu")]
        EngineType::Process,
        #[cfg(feature = "kvm")]
        EngineType::Kvm,
    ];

    pub fn get_domain_type(&self) -> DomainType {
        match self {
            #[cfg(feature = "reqwest_io")]
            EngineType::Reqwest => DomainType::System,
            #[cfg(feature = "cheri")]
            EngineType::Cheri => DomainType::Cheri,
            #[cfg(feature = "mmu")]
            EngineType::Process => DomainType::Process,
            #[cfg(feature = "kvm")]
            EngineType::Kvm => DomainType::Kvm,
        }
    }

    pub fn get_driver(&self) -> &dyn Driver {
        match self {
            #[cfg(feature = "reqwest_io")]
            EngineType::Reqwest => {
                &crate::function_driver::system_driver::reqwest::ReqwestDriver {}
            }
            #[cfg(feature = "cheri")]
            EngineType::Cheri => &crate::function_driver::compute_driver::cheri::CheriDriver {},
            #[cfg(feature = "mmu")]
            EngineType::Process => &crate::function_driver::compute_driver::mmu::MmuDriver {},
            #[cfg(feature = "kvm")]
            EngineType::Kvm => &crate::function_driver::compute_driver::kvm::KvmDriver {},
        }
    }
}

#[cfg(any(feature = "reqwest_io"))]
const SYS_FUNC_DEFAULT_CONTEXT_SIZE: usize = 0x200_0000;

pub const SYSTEM_FUNCTIONS: &[(EngineType, SystemFunction, usize)] = &[
    #[cfg(feature = "reqwest_io")]
    (
        EngineType::Reqwest,
        SystemFunction::HTTP,
        SYS_FUNC_DEFAULT_CONTEXT_SIZE,
    ),
];

pub fn get_available_domains(
    resources: BTreeMap<DomainType, MemoryResource>,
) -> Vec<Arc<Box<dyn MemoryDomain>>> {
    let mut default_resources = BTreeMap::from([
        (DomainType::System, MemoryResource::None),
        #[cfg(feature = "cheri")]
        (DomainType::Cheri, MemoryResource::Anonymous { size: 0 }),
        #[cfg(feature = "kvm")]
        (DomainType::Kvm, MemoryResource::Anonymous { size: 0 }),
        #[cfg(feature = "mmu")]
        (
            DomainType::Process,
            MemoryResource::Shared {
                id: u64::MAX,
                size: 0,
            },
        ),
    ]);
    for (dom, resource) in resources {
        default_resources.insert(dom, resource);
    }
    return default_resources
        .into_iter()
        .map(|(dom_type, resource)| match dom_type {
            DomainType::System => Arc::new(
                crate::memory_domain::system_domain::SystemMemoryDomain::init(resource).unwrap(),
            ),
            #[cfg(feature = "cheri")]
            DomainType::Cheri => {
                Arc::new(crate::memory_domain::cheri::CheriMemoryDomain::init(resource).unwrap())
            }
            #[cfg(feature = "kvm")]
            DomainType::Kvm => {
                Arc::new(crate::memory_domain::kvm::KvmMemoryDomain::init(resource).unwrap())
            }
            #[cfg(feature = "mmu")]
            DomainType::Process => {
                Arc::new(crate::memory_domain::mmu::MmuMemoryDomain::init(resource).unwrap())
            }
        })
        .collect();
}

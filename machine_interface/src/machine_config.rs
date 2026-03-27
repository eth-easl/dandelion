use crate::{
    function_driver::Driver,
    memory_domain::{MemoryDomain, MemoryResource},
};
use std::{collections::BTreeMap, sync::Arc};
pub use strum::IntoEnumIterator;
pub use strum::{EnumCount, EnumIter};

/// Enum for all engine types that allows use in lookup structures
#[repr(u8)] // ensure that always safe to cast to usize
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, EnumCount, EnumIter)]
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

pub(crate) const PAGE_SHIFT: usize = 12;
pub(crate) const PAGE_SIZE: usize = 1 << PAGE_SHIFT;
pub(crate) const PAGE_MASK: u64 = (PAGE_SIZE - 1) as u64;

const _: () = assert!(PAGE_SIZE.is_power_of_two());
pub fn round_down_to_page(address: usize) -> usize {
    address & !(PAGE_SIZE - 1)
}

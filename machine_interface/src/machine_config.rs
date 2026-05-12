use crate::{
    function_driver::{functions::Function, ComputeResource, Driver, EngineWorkQueue},
    memory_domain::{MemoryDomain, MemoryResource},
};
use dandelion_commons::DandelionResult;
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

    pub fn start_engine(
        &self,
        resource: ComputeResource,
        queue: impl EngineWorkQueue + Send + 'static,
    ) -> DandelionResult<()> {
        match self {
            #[cfg(feature = "reqwest_io")]
            EngineType::Reqwest => crate::function_driver::system_driver::reqwest::ReqwestDriver {}
                .start_engine(resource, queue),
            #[cfg(feature = "cheri")]
            EngineType::Cheri => crate::function_driver::compute_driver::cheri::CheriDriver {}
                .start_engine(resource, queue),
            #[cfg(feature = "mmu")]
            EngineType::Process => crate::function_driver::compute_driver::mmu::MmuDriver {}
                .start_engine(resource, queue),
            #[cfg(feature = "kvm")]
            EngineType::Kvm => crate::function_driver::compute_driver::kvm::KvmDriver {}
                .start_engine(resource, queue),
        }
    }

    pub fn parse_function(
        &self,
        function_path: String,
        static_domain: &Box<dyn crate::memory_domain::MemoryDomain>,
    ) -> DandelionResult<Function> {
        match self {
            #[cfg(feature = "reqwest_io")]
            EngineType::Reqwest => crate::function_driver::system_driver::reqwest::ReqwestDriver {}
                .parse_function(function_path, static_domain),
            #[cfg(feature = "cheri")]
            EngineType::Cheri => crate::function_driver::compute_driver::cheri::CheriDriver {}
                .parse_function(function_path, static_domain),
            #[cfg(feature = "mmu")]
            EngineType::Process => crate::function_driver::compute_driver::mmu::MmuDriver {}
                .parse_function(function_path, static_domain),
            #[cfg(feature = "kvm")]
            EngineType::Kvm => crate::function_driver::compute_driver::kvm::KvmDriver {}
                .parse_function(function_path, static_domain),
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

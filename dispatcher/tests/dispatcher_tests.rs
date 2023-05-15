use dandelion_commons::{ContextTypeId, EngineTypeId};
use dispatcher::{
    dispatcher::Dispatcher, function_registry::FunctionRegistry, resource_pool::ResourcePool,
};
use futures::lock::Mutex;
use machine_interface::{
    function_lib::{Driver, DriverFunction, Loader, LoaderFunction},
    memory_domain::MemoryDomain,
};
use std::collections::HashMap;

fn setup_dispatcher<Dom: MemoryDomain, Driv: Driver, L: Loader>(
    domain_arg: Vec<u8>,
    path: &str,
    engine_resource: Vec<u8>,
) -> Dispatcher {
    let mut domains = HashMap::new();
    let context_id: ContextTypeId = 0;
    domains.insert(
        context_id,
        Dom::init(domain_arg).expect("Should be able to initialize domain"),
    );
    let engine_id: EngineTypeId = 0;
    let mut drivers = HashMap::<EngineTypeId, DriverFunction>::new();
    let driver_func = Driv::start_engine as DriverFunction;
    drivers.insert(engine_id, driver_func);
    let mut type_map = HashMap::new();
    type_map.insert(engine_id, context_id);
    let mut loader_map = HashMap::new();
    loader_map.insert(engine_id, L::parse_function as LoaderFunction);
    let mut registry = FunctionRegistry::new(loader_map);
    registry.add_local(0, engine_id, path);
    let mut pool_map = HashMap::new();
    pool_map.insert(engine_id, engine_resource);
    let resource_pool = ResourcePool {
        engine_pool: Mutex::new(pool_map),
    };
    return Dispatcher::init(domains, drivers, type_map, registry, resource_pool)
        .expect("Should have initialized dispatcher");
}

macro_rules! dispatcherTests {
    ($domain : ty; $init: expr; $driver : ty; $engine_resource: expr; $loader: ty, $path: expr) => {
        use crate::setup_dispatcher;
        #[test]
        fn test_single_domain_and_engine() {
            let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            path.push($path);
            let dispatcher = setup_dispatcher::<$domain, $driver, $loader>(
                Vec::new(),
                path.to_str().unwrap(),
                $engine_resource,
            );
            let result = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(dispatcher.queue_function(0, Vec::new()));
            match result {
                Ok(_) => (),
                Err(err) => panic!("Failed with: {:?}", err),
            }
        }
    };
}

#[cfg(feature = "cheri")]
mod cheri {
    use machine_interface::{
        function_lib::cheri::{CheriDriver, CheriLoader},
        memory_domain::cheri::CheriMemoryDomain,
    };
    dispatcherTests!(CheriMemoryDomain; Vec::new(); CheriDriver; vec![1]; CheriLoader, "../machine_interface/tests/data/test_elf_aarch64c_basic");
}

#[cfg(feature = "pagetable")]
mod pagetable {
    use machine_interface::{
        function_lib::pagetable::{PagetableDriver, PagetableLoader},
        memory_domain::pagetable::PagetableMemoryDomain,
    };
    dispatcherTests!(PagetableMemoryDomain; Vec::new(); PagetableDriver; vec![1]; PagetableLoader, "../machine_interface/tests/data/test_elf_x86c_basic");
}

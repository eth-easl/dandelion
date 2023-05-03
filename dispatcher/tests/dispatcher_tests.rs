use dispatcher::{
    dispatcher::Dispatcher, function_registry::FunctionRegistry, ContextTypeId, EngineTypeId,
};
use machine_interface::{
    function_lib::{Driver, DriverFunction},
    memory_domain::MemoryDomain,
};
use std::collections::HashMap;

fn setup_dispatcher<Dom: MemoryDomain, Driv: Driver>(
    domain_arg: Vec<u8>,
    path: &str,
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
    let mut registry = FunctionRegistry::new();
    registry.add_local(0, engine_id, path);
    return Dispatcher::init(domains, drivers, type_map, registry)
        .expect("Should have initialized dispatcher");
}

macro_rules! dispatcherTests {
    ($domain : ty; $init: expr; $driver : ty) => {
        mod $name {
            let
            #[test]
            fn test_single_domain_engine() {
                let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
                path.push("tests/data");
                path.push("test_elf_aarch64c_matmul");
                let dispatcher = setup_dispatcher<$domain, $driver>(Vec::new(), path);
            }
        }
    };
}

#[cfg(feature = "cheri")]
mod cheri {
    use machine_interface::function_lib::cheri::CheriDriver as cheriDriver;
    use machine_interface::memory_domain::cheri::CheriMemoryDomain as cheriDomain;
    dispatcherTests!(cheriDomain; Vec::new(); cheriDriver);
}

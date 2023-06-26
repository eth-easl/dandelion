use dandelion_commons::{ContextTypeId, EngineTypeId};
use dispatcher::{
    dispatcher::Dispatcher, function_registry::FunctionRegistry, resource_pool::ResourcePool,
};
use futures::lock::Mutex;
use machine_interface::{
    function_lib::Driver,
    memory_domain::{ContextTrait, MemoryDomain},
    DataItem, Position,
};
use std::collections::HashMap;

fn setup_dispatcher<Dom: MemoryDomain>(
    domain_arg: Vec<u8>,
    path: &str,
    engine_resource: Vec<u8>,
    driver: Box<dyn Driver>,
) -> Dispatcher {
    let mut domains = HashMap::new();
    let context_id: ContextTypeId = 0;
    domains.insert(
        context_id,
        Dom::init(domain_arg).expect("Should be able to initialize domain"),
    );
    let engine_id: EngineTypeId = 0;
    let mut drivers = HashMap::<EngineTypeId, Box<dyn Driver>>::new();
    drivers.insert(engine_id, driver);
    let mut type_map = HashMap::new();
    type_map.insert(engine_id, context_id);
    let mut registry = FunctionRegistry::new(drivers);
    registry.add_local(0, engine_id, path);
    let mut pool_map = HashMap::new();
    pool_map.insert(engine_id, engine_resource);
    let resource_pool = ResourcePool {
        engine_pool: Mutex::new(pool_map),
    };
    return Dispatcher::init(domains, type_map, registry, resource_pool)
        .expect("Should have initialized dispatcher");
}

fn single_domain_and_engine_basic<Domain: MemoryDomain>(
    domain_arg: Vec<u8>,
    relative_path: &str,
    engine_resource: Vec<u8>,
    driver: Box<dyn Driver>,
) {
    let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push(relative_path);
    let dispatcher = setup_dispatcher::<Domain>(
        Vec::new(),
        path.to_str().unwrap(),
        engine_resource,
        driver,
    );
    let result = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(dispatcher.queue_function(0, Vec::new(), false));
    match result {
        Ok(_) => (),
        Err(err) => panic!("Failed with: {:?}", err),
    }
}

fn single_domain_and_engine_matmul<Domain: MemoryDomain>(
    domain_arg: Vec<u8>,
    relative_path: &str,
    engine_resource: Vec<u8>,
    driver: Box<dyn Driver>,
) {
    let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push(relative_path);
    let dispatcher = setup_dispatcher::<Domain>(
        Vec::new(),
        path.to_str().unwrap(),
        engine_resource,
        driver,
    );
    // need space for the input matrix of 2x2 uint64_t as well as a output matrix of the same size
    // and an uint64_t size that gives the column / row size (which is 2)
    const CONTEXT_SIZE: usize = 9 * 8;
    let mut in_context = Domain::init(domain_arg)
        .expect("Should be able to init domain")
        .acquire_context(CONTEXT_SIZE)
        .expect("Should get input matrix context");
    let size_offset = in_context.get_free_space(8, 8).expect("Should have space");
    in_context
        .write(size_offset, u64::to_ne_bytes(2).to_vec())
        .expect("Should be able to write matrix size");
    in_context.dynamic_data.insert(
        0,
        DataItem::Item(Position {
            offset: size_offset,
            size: 8,
        }),
    );
    let mut in_matrix = Vec::new();
    in_matrix.extend_from_slice(&u64::to_ne_bytes(1));
    in_matrix.extend_from_slice(&u64::to_ne_bytes(2));
    in_matrix.extend_from_slice(&u64::to_ne_bytes(3));
    in_matrix.extend_from_slice(&u64::to_ne_bytes(4));
    let in_mat_offset = in_context
        .get_free_space(4 * 8, 8)
        .expect("Should have space");
    in_context
        .write(in_mat_offset, in_matrix)
        .expect("Should be able to write");
    in_context.dynamic_data.insert(
        1,
        DataItem::Item(Position {
            offset: in_mat_offset,
            size: 32,
        }),
    );
    let out_mat_offset = in_context
        .get_free_space(4 * 8, 8)
        .expect("Should have space");
    in_context.dynamic_data.insert(
        2,
        DataItem::Item(Position {
            offset: out_mat_offset,
            size: 32,
        }),
    );
    let input_mapping = vec![(0, None, 0), (1, None, 1), (2, None, 2)];
    let inputs = vec![(&in_context, input_mapping)];
    let result = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(dispatcher.queue_function(0, inputs, false));
    let out_context = match result {
        Ok(context) => context,
        Err(err) => panic!("Failed with: {:?}", err),
    };
    assert_eq!(1, out_context.dynamic_data.len());
    let out_mat_position = match out_context
        .dynamic_data
        .get(&0)
        .expect("Should have item 0")
    {
        DataItem::Item(pos) => pos,
        _ => panic!("Expected item but got set for output matrix"),
    };
    let out_mat = out_context
        .read(out_mat_position.offset, out_mat_position.size)
        .expect("Should read output matrix");
    assert_eq!(32, out_mat.len());
    assert_eq!(u64::to_ne_bytes(5), out_mat[0..8]);
    assert_eq!(u64::to_ne_bytes(11), out_mat[8..16]);
    assert_eq!(u64::to_ne_bytes(11), out_mat[16..24]);
    assert_eq!(u64::to_ne_bytes(25), out_mat[24..32]);
}

macro_rules! dispatcherTests {
    ($domain : ty; $init: expr; $driver : expr; $engine_resource: expr; $loader: ty; $prefix: expr; $basic: expr; $mat: expr) => {
        #[test]
        fn test_single_domain_and_engine_basic() {
            let driver = Box::new($driver);
            crate::single_domain_and_engine_basic::<$domain>(
                $init,
                concat!($prefix, $basic),
                $engine_resource,
                driver,
            )
        }
        #[test]
        fn test_single_domain_and_engine_matmul() {
            let driver = Box::new($driver);
            crate::single_domain_and_engine_matmul::<$domain>(
                $init,
                concat!($prefix, $mat),
                $engine_resource,
                driver,
            )
        }
    };
}

#[cfg(feature = "cheri")]
mod cheri {
    use machine_interface::{
        function_lib::cheri::CheriDriver,
        memory_domain::cheri::CheriMemoryDomain,
    };
    dispatcherTests!(CheriMemoryDomain; Vec::new(); CheriDriver {}; vec![1]; CheriLoader; "../machine_interface/tests/data/test_elf_aarch64c"; "_basic"; "_matmul");
}

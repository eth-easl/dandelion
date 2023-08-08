use dandelion_commons::{ContextTypeId, EngineTypeId};
use dispatcher::{
    dispatcher::Dispatcher, function_registry::FunctionRegistry, resource_pool::ResourcePool,
};
use futures::lock::Mutex;
use machine_interface::{
    function_lib::{Driver, DriverFunction, Loader, LoaderFunction},
    memory_domain::{ContextTrait, MemoryDomain},
    DataItem, DataSet, Position,
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

fn single_domain_and_engine_basic<Domain: MemoryDomain, TestDriver: Driver, TestLoader: Loader>(
    domain_arg: Vec<u8>,
    relative_path: &str,
    engine_resource: Vec<u8>,
) {
    let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push(relative_path);
    let dispatcher = setup_dispatcher::<Domain, TestDriver, TestLoader>(
        domain_arg,
        path.to_str().unwrap(),
        engine_resource,
    );
    let result = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(dispatcher.queue_function(0, Vec::new(), vec![], false));
    match result {
        Ok(_) => (),
        Err(err) => panic!("Failed with: {:?}", err),
    }
}

fn single_domain_and_engine_matmul<Domain: MemoryDomain, TestDriver: Driver, TestLoader: Loader>(
    domain_arg: Vec<u8>,
    relative_path: &str,
    engine_resource: Vec<u8>,
) {
    let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push(relative_path);
    let dispatcher = setup_dispatcher::<Domain, TestDriver, TestLoader>(
        Vec::new(),
        path.to_str().unwrap(),
        engine_resource,
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
        .write(size_offset, &vec![2u64])
        .expect("Should be able to write matrix size");
    in_context.content.push(DataSet {
        ident: "".to_string(),
        buffers: vec![DataItem {
            ident: "".to_string(),
            data: Position {
                offset: size_offset,
                size: 8,
            },
        }],
    });
    let mut in_matrix = vec![1u64, 2u64, 3u64, 4u64];
    let in_mat_offset = in_context
        .get_free_space(4 * 8, 8)
        .expect("Should have space");
    in_context
        .write(in_mat_offset, &mut in_matrix)
        .expect("Should be able to write");
    in_context.content.push(DataSet {
        ident: "".to_string(),
        buffers: vec![DataItem {
            ident: "".to_string(),
            data: Position {
                offset: in_mat_offset,
                size: 32,
            },
        }],
    });

    let input_mapping = vec![(0, None, 0), (1, None, 1)];
    let inputs = vec![(&in_context, input_mapping)];
    let result = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(dispatcher.queue_function(0, inputs, vec![String::from("")], false));
    let out_context = match result {
        Ok(context) => context,
        Err(err) => panic!("Failed with: {:?}", err),
    };
    assert_eq!(1, out_context.content.len());
    let out_mat_set = &out_context.content[0];
    assert_eq!(1, out_mat_set.buffers.len());
    let out_mat_position = out_mat_set.buffers[0].data;
    let mut out_mat = vec![0u64; 4];
    out_context
        .read(out_mat_position.offset, &mut out_mat)
        .expect("Should read output matrix");
    assert_eq!(4, out_mat.len());
    assert_eq!(5, out_mat[0]);
    assert_eq!(11, out_mat[1]);
    assert_eq!(11, out_mat[2]);
    assert_eq!(25, out_mat[3]);
}

macro_rules! dispatcherTests {
    ($domain : ty; $init: expr; $driver : ty; $engine_resource: expr; $loader: ty; $prefix: expr; $basic: expr; $mat: expr) => {
        #[test]
        fn test_single_domain_and_engine_basic() {
            crate::single_domain_and_engine_basic::<$domain, $driver, $loader>(
                $init,
                concat!($prefix, $basic),
                $engine_resource,
            )
        }
        #[test]
        fn test_single_domain_and_engine_matmul() {
            crate::single_domain_and_engine_matmul::<$domain, $driver, $loader>(
                $init,
                concat!($prefix, $mat),
                $engine_resource,
            )
        }
    };
}

#[cfg(feature = "cheri")]
mod cheri {
    use machine_interface::{
        function_lib::cheri::{CheriDriver, CheriLoader},
        memory_domain::cheri::CheriMemoryDomain,
    };
    dispatcherTests!(CheriMemoryDomain; Vec::new(); CheriDriver; vec![1]; CheriLoader; "../machine_interface/tests/data/test_elf_aarch64c"; "_basic"; "_matmul");
}

use dandelion_commons::{ContextTypeId, EngineTypeId};
use dispatcher::{
    composition::{Composition, FunctionDependencies},
    dispatcher::{Dispatcher, TransferIndices},
    function_registry::FunctionRegistry,
    resource_pool::ResourcePool,
};
use futures::lock::Mutex;
use machine_interface::{
    function_lib::{Driver, DriverFunction, Loader, LoaderFunction},
    memory_domain::{ContextTrait, MemoryDomain},
    DataItem, DataSet, Position,
};
use std::{collections::BTreeMap, vec};

fn _setup_dispatcher<Dom: MemoryDomain, Driv: Driver, L: Loader>(
    domain_arg: Vec<u8>,
    path: &str,
    in_set_names: Vec<String>,
    out_set_names: Vec<String>,
    engine_resource: Vec<u8>,
) -> Dispatcher {
    let mut domains = BTreeMap::new();
    let context_id: ContextTypeId = 0;
    domains.insert(
        context_id,
        Dom::init(domain_arg).expect("Should be able to initialize domain"),
    );
    let engine_id: EngineTypeId = 0;
    let mut drivers = BTreeMap::<EngineTypeId, DriverFunction>::new();
    let driver_func = Driv::start_engine as DriverFunction;
    drivers.insert(engine_id, driver_func);
    let mut type_map = BTreeMap::new();
    type_map.insert(engine_id, context_id);
    let mut loader_map = BTreeMap::new();
    loader_map.insert(engine_id, L::parse_function as LoaderFunction);
    let mut registry = FunctionRegistry::new(loader_map);
    registry.add_local(0, engine_id, path, in_set_names, out_set_names);
    let mut pool_map = BTreeMap::new();
    pool_map.insert(engine_id, engine_resource);
    let resource_pool = ResourcePool {
        engine_pool: Mutex::new(pool_map),
    };
    return Dispatcher::init(domains, drivers, type_map, registry, resource_pool)
        .expect("Should have initialized dispatcher");
}

fn _single_domain_and_engine_basic<Domain: MemoryDomain, TestDriver: Driver, TestLoader: Loader>(
    domain_arg: Vec<u8>,
    relative_path: &str,
    engine_resource: Vec<u8>,
) {
    let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push(relative_path);
    let dispatcher = _setup_dispatcher::<Domain, TestDriver, TestLoader>(
        domain_arg,
        path.to_str().unwrap(),
        vec![],
        vec![],
        engine_resource,
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

fn _single_domain_and_engine_matmul<
    Domain: MemoryDomain,
    TestDriver: Driver,
    TestLoader: Loader,
>(
    domain_arg: Vec<u8>,
    relative_path: &str,
    engine_resource: Vec<u8>,
) {
    let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push(relative_path);
    let dispatcher = _setup_dispatcher::<Domain, TestDriver, TestLoader>(
        Vec::new(),
        path.to_str().unwrap(),
        vec![String::from(""), String::from("")],
        vec![String::from("")],
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
        .write(size_offset, u64::to_ne_bytes(2).to_vec())
        .expect("Should be able to write matrix size");
    in_context.content.push(Some(DataSet {
        ident: "".to_string(),
        buffers: vec![DataItem {
            ident: "".to_string(),
            data: Position {
                offset: size_offset,
                size: 8,
            },
        }],
    }));
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
    in_context.content.push(Some(DataSet {
        ident: "".to_string(),
        buffers: vec![DataItem {
            ident: "".to_string(),
            data: Position {
                offset: in_mat_offset,
                size: 32,
            },
        }],
    }));

    let input_mapping = vec![
        TransferIndices {
            input_set_index: 0,
            output_set_index: 0,
            item_indices: None,
        },
        TransferIndices {
            input_set_index: 1,
            output_set_index: 1,
            item_indices: None,
        },
    ];
    let inputs = vec![(&in_context, input_mapping)];
    let result = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(dispatcher.queue_function(0, inputs, false));
    let out_context = match result {
        Ok(context) => context,
        Err(err) => panic!("Failed with: {:?}", err),
    };
    assert_eq!(1, out_context.content.len());
    let out_mat_set = out_context.content[0].as_ref().expect("Should have set");
    assert_eq!(1, out_mat_set.buffers.len());
    let out_mat_position = out_mat_set.buffers[0].data;
    let out_mat = out_context
        .read(out_mat_position.offset, out_mat_position.size)
        .expect("Should read output matrix");
    assert_eq!(32, out_mat.len());
    assert_eq!(u64::to_ne_bytes(5), out_mat[0..8]);
    assert_eq!(u64::to_ne_bytes(11), out_mat[8..16]);
    assert_eq!(u64::to_ne_bytes(11), out_mat[16..24]);
    assert_eq!(u64::to_ne_bytes(25), out_mat[24..32]);
}

fn _composition_single_matmul<Domain: MemoryDomain, TestDriver: Driver, TestLoader: Loader>(
    domain_arg: Vec<u8>,
    relative_path: &str,
    engine_resource: Vec<u8>,
) {
    let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push(relative_path);
    let dispatcher = _setup_dispatcher::<Domain, TestDriver, TestLoader>(
        Vec::new(),
        path.to_str().unwrap(),
        vec![String::from(""), String::from("")],
        vec![String::from("")],
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
        .write(size_offset, u64::to_ne_bytes(2).to_vec())
        .expect("Should be able to write matrix size");
    in_context.content.push(Some(DataSet {
        ident: "".to_string(),
        buffers: vec![DataItem {
            ident: "".to_string(),
            data: Position {
                offset: size_offset,
                size: 8,
            },
        }],
    }));
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
    in_context.content.push(Some(DataSet {
        ident: "".to_string(),
        buffers: vec![DataItem {
            ident: "".to_string(),
            data: Position {
                offset: in_mat_offset,
                size: 32,
            },
        }],
    }));

    let input_mapping = vec![
        TransferIndices {
            input_set_index: 0,
            output_set_index: 0,
            item_indices: None,
        },
        TransferIndices {
            input_set_index: 1,
            output_set_index: 1,
            item_indices: None,
        },
    ];
    let composition = Composition {
        dependencies: vec![FunctionDependencies {
            function: 0,
            input_ids: vec![
                TransferIndices {
                    input_set_index: 0,
                    output_set_index: 0,
                    item_indices: None,
                },
                TransferIndices {
                    input_set_index: 1,
                    output_set_index: 1,
                    item_indices: None,
                },
            ],
            output_ids: vec![TransferIndices {
                input_set_index: 0,
                output_set_index: 2,
                item_indices: None,
            }],
        }],
    };
    let inputs = vec![(&in_context, input_mapping)];
    let result = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(dispatcher.queue_composition(composition, inputs, vec![2], false));
    let out_contexts = match result {
        Ok(context) => context,
        Err(err) => panic!("Failed with: {:?}", err),
    };
    assert_eq!(1, out_contexts.len());
    let (out_context, set_map) = &out_contexts[0];
    assert_eq!(1, set_map.len());
    let transfer_info = set_map[0];
    assert_eq!(0, transfer_info.input_set_index);
    assert_eq!(2, transfer_info.output_set_index);
    assert!(transfer_info.item_indices.is_none());
    assert_eq!(1, out_context.content.len());
    let out_mat_set = out_context.content[0].as_ref().expect("Should have set");
    assert_eq!(1, out_mat_set.buffers.len());
    let out_mat_position = out_mat_set.buffers[0].data;
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
    ($domain : ty; $init: expr; $driver : ty; $engine_resource: expr; $loader: ty; $prefix: expr; $basic: expr; $mat: expr) => {
        use super::*;
        #[test]
        fn test_single_domain_and_engine_basic() {
            _single_domain_and_engine_basic::<$domain, $driver, $loader>(
                $init,
                concat!($prefix, $basic),
                $engine_resource,
            )
        }
        #[test]
        fn test_single_domain_and_engine_matmul() {
            _single_domain_and_engine_matmul::<$domain, $driver, $loader>(
                $init,
                concat!($prefix, $mat),
                $engine_resource,
            )
        }
        #[test]
        fn test_same_engine_matmul() {
            _composition_single_matmul::<$domain, $driver, $loader>(
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
    dispatcherTests!(CheriMemoryDomain; Vec::new(); CheriDriver; vec![1]; CheriLoader; "../machine_interface/tests/data/test_elf_cheri"; "_basic"; "_matmul");
}

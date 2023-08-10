use dandelion_commons::{ContextTypeId, EngineTypeId};
use dispatcher::{
    composition::{Composition, FunctionDependencies},
    dispatcher::{Dispatcher, ItemIndices, TransferIndices},
    function_registry::FunctionRegistry,
    resource_pool::ResourcePool,
};
use futures::lock::Mutex;
use machine_interface::{
    function_driver::{Driver, DriverFunction, Loader, LoaderFunction},
    memory_domain::{Context, ContextTrait, MemoryDomain},
    DataItem, DataSet, Position,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    rc::Rc,
    vec,
};

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

fn _add_matmul_matrix(
    context: &mut Context,
    size_set: usize,
    mat_set: usize,
    matrix_dim: u64,
    matrix: Vec<u64>,
) {
    // check the sets are not already full and ensure they exist
    let max_id = usize::max(size_set, mat_set);
    if context.content.len() <= max_id {
        context.content.resize_with(max_id + 1, || None);
    }
    if context.content[size_set].is_some() || context.content[mat_set].is_some() {
        panic!("trying to add matrix where there is already set");
    }

    let size_offset = context.get_free_space(8, 8).expect("Should have space");

    assert_eq!(matrix_dim * matrix_dim, matrix.len() as u64);

    context
        .write(size_offset, &[matrix_dim])
        .expect("Should be able to write matrix size");
    context.content[size_set] = Some(DataSet {
        ident: "".to_string(),
        buffers: vec![DataItem {
            ident: "".to_string(),
            data: Position {
                offset: size_offset,
                size: 8,
            },
        }],
    });

    let mat_offset = context
        .get_free_space(matrix.len() * 8, 8)
        .expect("Should have space");
    context
        .write(mat_offset, &matrix)
        .expect("Should be able to write");
    context.content[mat_set] = Some(DataSet {
        ident: "".to_string(),
        buffers: vec![DataItem {
            ident: "".to_string(),
            data: Position {
                offset: mat_offset,
                size: matrix.len() * 8,
            },
        }],
    });
}

fn _add_matmac_matrix(
    context: &mut Context,
    size_set: usize,
    rows: u64,
    cols: u64,
    mat_set: usize,
    matrix: Vec<u64>,
) {
    // check the sets are not already full and ensure they exist
    let max_id = usize::max(size_set, mat_set);
    if context.content.len() <= max_id {
        context.content.resize_with(max_id + 1, || None);
    }
    if context.content[size_set].is_some() || context.content[mat_set].is_some() {
        panic!("trying to add matrix where there is already set");
    }
    let size_offset = context.get_free_space(16, 8).expect("Should have space");

    assert_eq!(rows * cols, matrix.len() as u64);

    context
        .write(size_offset, &[rows])
        .expect("Should be able to write matrix size");
    context
        .write(size_offset + 8, &[cols])
        .expect("Should be able to write matrix size");
    context.content[size_set] = Some(DataSet {
        ident: "".to_string(),
        buffers: vec![
            DataItem {
                ident: "".to_string(),
                data: Position {
                    offset: size_offset,
                    size: 8,
                },
            },
            DataItem {
                ident: "".to_string(),
                data: Position {
                    offset: size_offset + 8,
                    size: 8,
                },
            },
        ],
    });
    let in_mat_offset = context
        .get_free_space(matrix.len() * 8, 8)
        .expect("Should have space");
    context
        .write(in_mat_offset, &matrix)
        .expect("Should be able to write");
    context.content[mat_set] = Some(DataSet {
        ident: "".to_string(),
        buffers: vec![DataItem {
            ident: "".to_string(),
            data: Position {
                offset: in_mat_offset,
                size: matrix.len() * 8,
            },
        }],
    });
}

fn _check_matrix(context: &Context, set_id: usize, expected: Vec<u64>) {
    assert!(context.content.len() >= set_id);
    let out_mat_set = context.content[set_id].as_ref().expect("Should have set");
    assert_eq!(1, out_mat_set.buffers.len());
    let out_mat_position = out_mat_set.buffers[0].data;
    let mut out_mat = Vec::<u64>::new();
    assert_eq!(expected.len() * 8, out_mat_position.size);
    out_mat.resize(expected.len(), 0);
    context
        .read(out_mat_position.offset, &mut out_mat)
        .expect("Should read output matrix");
    for i in 0..expected.len() {
        assert_eq!(expected[i], out_mat[i]);
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
    _add_matmul_matrix(&mut in_context, 0, 1, 2, vec![1, 2, 3, 4]);

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
    _check_matrix(&out_context, 0, vec![5, 11, 11, 25])
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
        .write(size_offset, &[2u64])
        .expect("Should be able to write matrix size");
    _add_matmul_matrix(&mut in_context, 0, 1, 2, vec![1, 2, 3, 4]);

    let composition = Composition {
        dependencies: vec![FunctionDependencies {
            function: 0,
            input_ids: vec![(
                0,
                vec![
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
            )],
            output_id: 1,
        }],
    };
    let inputs = vec![(0, Rc::new(in_context))];
    let outputs = BTreeSet::from([1]);
    let result = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(dispatcher.queue_composition(composition, inputs, outputs, false));
    let out_contexts = match result {
        Ok(context) => context,
        Err(err) => panic!("Failed with: {:?}", err),
    };
    assert_eq!(1, out_contexts.len());
    let (out_context_id, out_context) = &out_contexts[0];
    assert_eq!(1, *out_context_id);
    assert_eq!(1, out_context.content.len());
    let out_mat_set = out_context.content[0].as_ref().expect("Should have set");
    assert_eq!(1, out_mat_set.buffers.len());
    _check_matrix(out_context, 0, vec![5, 11, 11, 25])
}

fn _composition_parallel_matmul<Domain: MemoryDomain, TestDriver: Driver, TestLoader: Loader>(
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
    _add_matmul_matrix(&mut in_context, 0, 1, 2, vec![1, 2, 3, 4]);

    let composition = Composition {
        dependencies: vec![
            FunctionDependencies {
                function: 0,
                input_ids: vec![(
                    0,
                    vec![
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
                )],
                output_id: 1,
            },
            FunctionDependencies {
                function: 0,
                input_ids: vec![(
                    0,
                    vec![
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
                )],
                output_id: 2,
            },
        ],
    };
    let inputs = vec![(0, Rc::new(in_context))];
    let outputs = BTreeSet::from([1, 2]);
    let result = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(dispatcher.queue_composition(composition, inputs, outputs, false));
    let out_contexts = match result {
        Ok(context) => context,
        Err(err) => panic!("Failed with: {:?}", err),
    };
    assert_eq!(2, out_contexts.len());
    for (out_context_id, out_context) in out_contexts {
        assert!(1 == out_context_id || 2 == out_context_id);
        assert_eq!(1, out_context.content.len());
        _check_matrix(&out_context, 0, vec![5, 11, 11, 25])
    }
}

fn _composition_chain_matmul<Domain: MemoryDomain, TestDriver: Driver, TestLoader: Loader>(
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
    _add_matmul_matrix(&mut in_context, 0, 1, 2, vec![1, 2, 3, 4]);

    let composition = Composition {
        dependencies: vec![
            FunctionDependencies {
                function: 0,
                input_ids: vec![(
                    0,
                    vec![
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
                )],
                output_id: 1,
            },
            FunctionDependencies {
                function: 0,
                input_ids: vec![
                    (
                        0,
                        vec![TransferIndices {
                            input_set_index: 0,
                            output_set_index: 0,
                            item_indices: None,
                        }],
                    ),
                    (
                        1,
                        vec![TransferIndices {
                            input_set_index: 0,
                            output_set_index: 1,
                            item_indices: None,
                        }],
                    ),
                ],
                output_id: 2,
            },
        ],
    };
    let inputs = vec![(0, Rc::new(in_context))];
    let output_contexts = BTreeSet::from([2]);
    let result = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(dispatcher.queue_composition(composition, inputs, output_contexts, false));
    let out_contexts = match result {
        Ok(context) => context,
        Err(err) => panic!("Failed with: {:?}", err),
    };
    assert_eq!(1, out_contexts.len());
    let (out_context_id, out_context) = &out_contexts[0];
    assert_eq!(2, *out_context_id);
    assert_eq!(1, out_context.content.len());
    _check_matrix(out_context, 0, vec![146, 330, 330, 746])
}

fn _composition_diamond_matmac<Domain: MemoryDomain, TestDriver: Driver, TestLoader: Loader>(
    domain_arg: Vec<u8>,
    relative_path: &str,
    engine_resource: Vec<u8>,
) {
    let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push(relative_path);
    let dispatcher = _setup_dispatcher::<Domain, TestDriver, TestLoader>(
        Vec::new(),
        path.to_str().unwrap(),
        vec![
            String::from(""),
            String::from(""),
            String::from(""),
            String::from(""),
        ],
        vec![String::from(""), String::from("")],
        engine_resource,
    );
    // need space for the input matrix of 2x2 uint64_t as well as a output matrix of the same size
    // and an uint64_t size that gives the column / row size (which is 2)
    const CONTEXT_SIZE: usize = 10 * 8;
    let mut in_context = Domain::init(domain_arg)
        .expect("Should be able to init domain")
        .acquire_context(CONTEXT_SIZE)
        .expect("Should get input matrix context");
    // A = [7]
    _add_matmac_matrix(&mut in_context, 0, 1, 1, 1, vec![7]);
    // B = [1,2,3,5]
    _add_matmac_matrix(&mut in_context, 2, 1, 4, 3, vec![1, 2, 3, 5]);

    let composition = Composition {
        dependencies: vec![
            // C = A*B
            FunctionDependencies {
                function: 0,
                input_ids: vec![(
                    0,
                    vec![
                        TransferIndices {
                            input_set_index: 2,
                            output_set_index: 0,
                            item_indices: None,
                        },
                        TransferIndices {
                            input_set_index: 1,
                            output_set_index: 1,
                            item_indices: None,
                        },
                        TransferIndices {
                            input_set_index: 3,
                            output_set_index: 2,
                            item_indices: None,
                        },
                    ],
                )],
                output_id: 1,
            },
            // D = B*A
            FunctionDependencies {
                function: 0,
                input_ids: vec![(
                    0,
                    vec![
                        TransferIndices {
                            input_set_index: 2,
                            output_set_index: 0,
                            item_indices: Some(ItemIndices {
                                in_index: 0,
                                out_index: 1,
                            }),
                        },
                        TransferIndices {
                            input_set_index: 2,
                            output_set_index: 0,
                            item_indices: Some(ItemIndices {
                                in_index: 1,
                                out_index: 0,
                            }),
                        },
                        TransferIndices {
                            input_set_index: 3,
                            output_set_index: 1,
                            item_indices: None,
                        },
                        TransferIndices {
                            input_set_index: 1,
                            output_set_index: 2,
                            item_indices: None,
                        },
                    ],
                )],
                output_id: 2,
            },
            // E = B + C
            FunctionDependencies {
                function: 0,
                input_ids: vec![
                    (
                        0,
                        vec![
                            TransferIndices {
                                input_set_index: 2,
                                output_set_index: 0,
                                item_indices: None,
                            },
                            TransferIndices {
                                input_set_index: 3,
                                output_set_index: 2,
                                item_indices: None,
                            },
                        ],
                    ),
                    (
                        1,
                        vec![TransferIndices {
                            input_set_index: 1,
                            output_set_index: 3,
                            item_indices: None,
                        }],
                    ),
                ],
                output_id: 3,
            },
            // G = D * C
            FunctionDependencies {
                function: 0,
                input_ids: vec![
                    (
                        1,
                        vec![
                            TransferIndices {
                                input_set_index: 0,
                                output_set_index: 0,
                                item_indices: Some(ItemIndices {
                                    in_index: 1,
                                    out_index: 1,
                                }),
                            },
                            TransferIndices {
                                input_set_index: 1,
                                output_set_index: 2,
                                item_indices: None,
                            },
                        ],
                    ),
                    (
                        2,
                        vec![
                            TransferIndices {
                                input_set_index: 0,
                                output_set_index: 0,
                                item_indices: Some(ItemIndices {
                                    in_index: 0,
                                    out_index: 0,
                                }),
                            },
                            TransferIndices {
                                input_set_index: 1,
                                output_set_index: 1,
                                item_indices: None,
                            },
                        ],
                    ),
                ],
                output_id: 4,
            },
            // Result = D*E + G
            FunctionDependencies {
                function: 0,
                input_ids: vec![
                    (
                        2,
                        vec![TransferIndices {
                            input_set_index: 1,
                            output_set_index: 1,
                            item_indices: None,
                        }],
                    ),
                    (
                        3,
                        vec![TransferIndices {
                            input_set_index: 1,
                            output_set_index: 2,
                            item_indices: None,
                        }],
                    ),
                    (
                        4,
                        vec![
                            TransferIndices {
                                input_set_index: 0,
                                output_set_index: 0,
                                item_indices: None,
                            },
                            TransferIndices {
                                input_set_index: 1,
                                output_set_index: 3,
                                item_indices: None,
                            },
                        ],
                    ),
                ],
                output_id: 5,
            },
        ],
    };
    let inputs = vec![(0, Rc::new(in_context))];
    let output_contexts = BTreeSet::from([5]);
    let result = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(dispatcher.queue_composition(composition, inputs, output_contexts, false));
    let out_contexts = match result {
        Ok(context) => context,
        Err(err) => panic!("Failed with: {:?}", err),
    };
    assert_eq!(1, out_contexts.len());
    let (out_context_id, out_context) = &out_contexts[0];
    assert_eq!(5, *out_context_id);
    assert_eq!(2, out_context.content.len());
    _check_matrix(
        out_context,
        1,
        vec![
            105, 210, 315, 525, 210, 420, 630, 1050, 315, 630, 945, 1575, 525, 1050, 1575, 2625,
        ],
    )
}

macro_rules! dispatcherTests {
    ($domain : ty; $init: expr; $driver : ty; $engine_resource: expr; $loader: ty; $prefix: expr) => {
        use super::*;
        #[test]
        fn test_single_domain_and_engine_basic() {
            _single_domain_and_engine_basic::<$domain, $driver, $loader>(
                $init,
                concat!($prefix, "_basic"),
                $engine_resource,
            )
        }
        #[test]
        fn test_single_domain_and_engine_matmul() {
            _single_domain_and_engine_matmul::<$domain, $driver, $loader>(
                $init,
                concat!($prefix, "_matmul"),
                $engine_resource,
            )
        }
        #[test]
        fn test_composition_single_matmul() {
            _composition_single_matmul::<$domain, $driver, $loader>(
                $init,
                concat!($prefix, "_matmul"),
                $engine_resource,
            )
        }

        #[test]
        fn test_composition_parallel() {
            _composition_parallel_matmul::<$domain, $driver, $loader>(
                $init,
                concat!($prefix, "_matmul"),
                $engine_resource,
            )
        }

        #[test]
        fn test_composition_chain() {
            _composition_chain_matmul::<$domain, $driver, $loader>(
                $init,
                concat!($prefix, "_matmul"),
                $engine_resource,
            )
        }

        #[test]
        fn test_composition_diamond() {
            _composition_diamond_matmac::<$domain, $driver, $loader>(
                $init,
                concat!($prefix, "_matmac"),
                $engine_resource,
            )
        }
    };
}

#[cfg(feature = "cheri")]
mod cheri {
    use machine_interface::{
        function_driver::compute_driver::cheri::{CheriDriver, CheriLoader},
        memory_domain::cheri::CheriMemoryDomain,
    };
    dispatcherTests!(CheriMemoryDomain; Vec::new(); CheriDriver; vec![1]; CheriLoader; "../machine_interface/tests/data/test_elf_cheri");
}

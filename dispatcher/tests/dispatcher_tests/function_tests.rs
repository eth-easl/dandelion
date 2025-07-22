use super::{check_matrix, setup_dispatcher};
use dandelion_commons::records::Recorder;
use dispatcher::{
    composition::{
        Composition, CompositionSet, FunctionDependencies, InputSetDescriptor, ShardingMode,
    },
    dispatcher::Dispatcher,
};
use machine_interface::{
    function_driver::ComputeResource,
    machine_config::{DomainType, EngineType},
    memory_domain::{read_only::ReadOnlyContext, MemoryDomain, MemoryResource},
    DataItem, DataSet, Position,
};
use std::sync::Arc;
use std::{collections::BTreeMap, time::Instant};

pub fn single_domain_and_engine_basic<Domain: MemoryDomain>(
    memory_resource: (DomainType, MemoryResource),
    relative_path: &str,
    engine_type: EngineType,
    engine_resource: Vec<ComputeResource>,
) {
    let (dispatcher, function_id) = setup_dispatcher::<Domain>(
        "single_domain_and_engine_basic",
        relative_path,
        vec![],
        vec![],
        engine_type,
        engine_resource,
        memory_resource,
    );

    let recorder = Recorder::new(0, Instant::now());
    let result = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(dispatcher.queue_function(function_id, Vec::new(), false, recorder));
    match result {
        Ok(_) => (),
        Err(err) => panic!("Failed with: {:?}", err),
    }
}

pub fn single_domain_and_engine_matmul<Domain: MemoryDomain>(
    memory_resource: (DomainType, MemoryResource),
    relative_path: &str,
    engine_type: EngineType,
    engine_resource: Vec<ComputeResource>,
) {
    let (dispatcher, function_id) = setup_dispatcher::<Domain>(
        "single_domain_and_engine_matmul",
        relative_path,
        vec![(String::from(""), None)],
        vec![String::from("")],
        engine_type,
        engine_resource,
        memory_resource,
    );

    // matrix with first eleemnt inidicating number of rows
    let mat_a = vec![2u64, 1, 2, 3, 4];
    let mat_len = mat_a.len();
    let mut in_context =
        ReadOnlyContext::new(mat_a.into()).expect("Should be able to create read only context");
    in_context.content = vec![Some(DataSet {
        ident: String::from(""),
        buffers: vec![DataItem {
            ident: String::from(""),
            data: Position {
                offset: 0,
                size: mat_len * core::mem::size_of::<u64>(),
            },
            key: 0,
        }],
    })];

    let inputs = vec![Some(CompositionSet::from((
        0,
        vec![(Arc::new(in_context))],
    )))];

    let recorder = Recorder::new(0, Instant::now());

    let result = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(dispatcher.queue_function(function_id, inputs, false, recorder));
    let out_sets = match result {
        Ok(context) => context,
        Err(err) => panic!("Failed with: {:?}", err),
    };
    assert_eq!(1, out_sets.len());
    let out_set = out_sets[0].as_ref().expect("Should have set");
    let mut out_set_iter = out_set.into_iter();
    let (_, _, out_context) = out_set_iter.next().unwrap();
    assert!(out_set_iter.next().is_none());
    assert_eq!(1, out_context.content.len());
    check_matrix(&out_context, 0, 0, 2, vec![5, 11, 11, 25])
}

pub fn composition_single_matmul<Domain: MemoryDomain>(
    memory_resource: (DomainType, MemoryResource),
    relative_path: &str,
    engine_type: EngineType,
    engine_resource: Vec<ComputeResource>,
) {
    let (dispatcher, function_id) = setup_dispatcher::<Domain>(
        "composition_single_matmul",
        relative_path,
        vec![(String::from(""), None)],
        vec![String::from("")],
        engine_type,
        engine_resource,
        memory_resource,
    );

    // matrix with first eleemnt inidicating number of rows
    let mat_a = vec![2u64, 1, 2, 3, 4];
    let mat_len = mat_a.len();
    let mut in_context = ReadOnlyContext::new(mat_a.into_boxed_slice())
        .expect("Should be able to create read only context");
    in_context.content = vec![Some(DataSet {
        ident: String::from(""),
        buffers: vec![DataItem {
            ident: String::from(""),
            data: Position {
                offset: 0,
                size: mat_len * core::mem::size_of::<u64>(),
            },
            key: 0,
        }],
    })];

    let composition = Composition {
        dependencies: vec![FunctionDependencies {
            function: function_id,
            join_info: (vec![], vec![]),
            input_set_ids: vec![Some(InputSetDescriptor {
                composition_id: 0,
                sharding: ShardingMode::All,
                optional: false,
            })],
            output_set_ids: vec![Some(1)],
        }],
        output_map: BTreeMap::from([(1, 0)]),
    };
    let inputs = vec![Some(CompositionSet::from((0, vec![Arc::new(in_context)])))];

    let recorder = Recorder::new(0, Instant::now());

    let result = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(dispatcher.queue_composition(composition, inputs, false, recorder));
    let mut out_contexts = match result {
        Ok(context) => context,
        Err(err) => panic!("Failed with: {:?}", err),
    };
    assert_eq!(1, out_contexts.len());
    let out_context_list = out_contexts[0].as_mut().expect("Should have set");

    let mut out_context_iter = out_context_list.into_iter();
    let (_, _, out_context) = out_context_iter.next().unwrap();
    assert!(out_context_iter.next().is_none());
    assert_eq!(1, out_context.content.len());
    let out_mat_set = out_context.content[0].as_ref().expect("Should have set");
    assert_eq!(1, out_mat_set.buffers.len());
    check_matrix(&out_context, 0, 0, 2, vec![5, 11, 11, 25])
}

fn composition_option_helper(
    composition: Composition,
    inputs: Vec<Option<CompositionSet>>,
    dispatcher: &mut Dispatcher,
) -> Vec<Option<CompositionSet>> {
    let recorder = Recorder::new(0, Instant::now());

    let result = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(dispatcher.queue_composition(composition, inputs, false, recorder));
    let out_contexts = match result {
        Ok(context) => context,
        Err(err) => panic!("Failed with: {:?}", err),
    };

    return out_contexts;
}

pub fn composition_optional<Domain: MemoryDomain>(
    memory_resource: (DomainType, MemoryResource),
    relative_path: &str,
    engine_type: EngineType,
    engine_resource: Vec<ComputeResource>,
) {
    let (mut dispatcher, function_id) = setup_dispatcher::<Domain>(
        "composition_optional",
        relative_path,
        vec![(String::from(""), None)],
        vec![String::from(""), String::from("")],
        engine_type,
        engine_resource,
        memory_resource,
    );

    // check case where the set is an input set, not optional and not present
    let composition1 = Composition {
        dependencies: vec![FunctionDependencies {
            function: function_id,
            join_info: (vec![], vec![]),
            input_set_ids: vec![Some(InputSetDescriptor {
                composition_id: 0,
                sharding: ShardingMode::All,
                optional: false,
            })],
            output_set_ids: vec![Some(1)],
        }],
        output_map: BTreeMap::from([(1, 0)]),
    };
    let inputs1 = vec![None];
    let out_contexts = composition_option_helper(composition1, inputs1, &mut dispatcher);
    assert_eq!(1, out_contexts.len());
    assert!(out_contexts[0].is_none());

    // check case where the set is an input set, not optional and empty
    let composition2 = Composition {
        dependencies: vec![FunctionDependencies {
            function: function_id,
            join_info: (vec![], vec![]),
            input_set_ids: vec![Some(InputSetDescriptor {
                composition_id: 0,
                sharding: ShardingMode::All,
                optional: false,
            })],
            output_set_ids: vec![Some(1)],
        }],
        output_map: BTreeMap::from([(1, 0)]),
    };
    let inputs2 = vec![Some(CompositionSet::from((0, vec![])))];
    let out_contexts = composition_option_helper(composition2, inputs2, &mut dispatcher);
    assert_eq!(1, out_contexts.len());
    assert!(out_contexts[0].is_none());

    // check case where the set is an input set, optional and not present
    let composition3 = Composition {
        dependencies: vec![FunctionDependencies {
            function: function_id,
            join_info: (vec![], vec![]),
            input_set_ids: vec![Some(InputSetDescriptor {
                composition_id: 0,
                sharding: ShardingMode::All,
                optional: true,
            })],
            output_set_ids: vec![Some(1)],
        }],
        output_map: BTreeMap::from([(1, 0)]),
    };
    let inputs3 = vec![None];
    let out_contexts = composition_option_helper(composition3, inputs3, &mut dispatcher);
    assert_eq!(1, out_contexts.len());
    assert!(out_contexts[0].is_some());

    // check case where the set is an input set, optional and empty
    let composition4 = Composition {
        dependencies: vec![FunctionDependencies {
            function: function_id,
            join_info: (vec![], vec![]),
            input_set_ids: vec![Some(InputSetDescriptor {
                composition_id: 0,
                sharding: ShardingMode::All,
                optional: true,
            })],
            output_set_ids: vec![Some(1)],
        }],
        output_map: BTreeMap::from([(1, 0)]),
    };
    let inputs4 = vec![Some(CompositionSet::from((0, vec![])))];
    let out_contexts = composition_option_helper(composition4, inputs4, &mut dispatcher);
    assert_eq!(1, out_contexts.len());
    assert!(out_contexts[0].is_some());

    // check case where the set is a composition set, not optional and not present
    let composition5 = Composition {
        dependencies: vec![
            FunctionDependencies {
                function: function_id,
                join_info: (vec![], vec![]),
                input_set_ids: vec![],
                output_set_ids: vec![None, Some(1)],
            },
            FunctionDependencies {
                function: function_id,
                join_info: (vec![], vec![]),
                input_set_ids: vec![Some(InputSetDescriptor {
                    composition_id: 1,
                    sharding: ShardingMode::All,
                    optional: false,
                })],
                output_set_ids: vec![Some(2)],
            },
        ],
        output_map: BTreeMap::from([(2, 0)]),
    };
    let inputs5 = vec![];
    let out_contexts = composition_option_helper(composition5, inputs5, &mut dispatcher);
    assert_eq!(1, out_contexts.len());
    assert!(out_contexts[0].is_none());

    // check case where the set is an input set, optional and not present
    let composition6 = Composition {
        dependencies: vec![
            FunctionDependencies {
                function: function_id,
                join_info: (vec![], vec![]),
                input_set_ids: vec![],
                output_set_ids: vec![None, Some(1)],
            },
            FunctionDependencies {
                function: function_id,
                join_info: (vec![], vec![]),
                input_set_ids: vec![Some(InputSetDescriptor {
                    composition_id: 1,
                    sharding: ShardingMode::All,
                    optional: true,
                })],
                output_set_ids: vec![Some(2)],
            },
        ],
        output_map: BTreeMap::from([(2, 0)]),
    };
    let inputs6 = vec![];
    let out_contexts = composition_option_helper(composition6, inputs6, &mut dispatcher);
    assert_eq!(1, out_contexts.len());
    assert!(out_contexts[0].is_some());
}

pub fn composition_parallel_matmul<Domain: MemoryDomain>(
    memory_resource: (DomainType, MemoryResource),
    relative_path: &str,
    engine_type: EngineType,
    engine_resource: Vec<ComputeResource>,
) {
    let (dispatcher, function_id) = setup_dispatcher::<Domain>(
        "composition_parallel_matmul",
        relative_path,
        vec![(String::from(""), None)],
        vec![String::from("")],
        engine_type,
        engine_resource,
        memory_resource,
    );
    // matrix with first eleemnt inidicating number of rows
    let mat_a = vec![2u64, 1, 2, 3, 4];
    let mat_b = vec![2u64, 1, 2, 3, 4];

    let mut data = vec![];
    data.extend_from_slice(&mat_a);
    data.extend_from_slice(&mat_b);
    let mut in_context = ReadOnlyContext::new(data.into_boxed_slice())
        .expect("Should be able to create read only context");
    in_context.content = vec![Some(DataSet {
        ident: String::from(""),
        buffers: vec![
            DataItem {
                ident: String::from(""),
                data: Position {
                    offset: 0,
                    size: mat_a.len() * core::mem::size_of::<u64>(),
                },
                key: 0,
            },
            DataItem {
                ident: String::from(""),
                data: Position {
                    offset: mat_a.len() * core::mem::size_of::<u64>(),
                    size: mat_b.len() * core::mem::size_of::<u64>(),
                },
                key: 1,
            },
        ],
    })];

    let composition = Composition {
        dependencies: vec![FunctionDependencies {
            function: function_id,
            join_info: (vec![], vec![]),
            input_set_ids: vec![Some(InputSetDescriptor {
                composition_id: 0,
                sharding: ShardingMode::Each,
                optional: false,
            })],
            output_set_ids: vec![Some(1)],
        }],
        output_map: BTreeMap::from([(1, 0)]),
    };
    let inputs = vec![Some(CompositionSet::from((0, vec![Arc::new(in_context)])))];

    let recorder = Recorder::new(0, Instant::now());

    let result = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(dispatcher.queue_composition(composition, inputs, false, recorder));
    let out_vec = match result {
        Ok(v) => v,
        Err(err) => panic!("Failed with: {:?}", err),
    };
    assert_eq!(1, out_vec.len());
    let out_set = out_vec[0].as_ref().expect("Should have set");

    // check for each shard:
    for (index, (_, _, matrix_context)) in out_set.into_iter().enumerate() {
        assert!(index < 2);
        if let Some(matrix_set) = &matrix_context.content[0] {
            assert_eq!(1, matrix_set.buffers.len());
            let matrix_buffer = &matrix_set.buffers[0];
            assert!(matrix_buffer.key == 1 || matrix_buffer.key == 0);
            check_matrix(
                &matrix_context,
                0,
                matrix_buffer.key,
                2,
                vec![5, 11, 11, 25],
            );
        }
    }
}

pub fn composition_chain_matmul<Domain: MemoryDomain>(
    memory_resource: (DomainType, MemoryResource),
    relative_path: &str,
    engine_type: EngineType,
    engine_resource: Vec<ComputeResource>,
) {
    let (dispatcher, function_id) = setup_dispatcher::<Domain>(
        "composition_chain_matmul",
        relative_path,
        vec![(String::from(""), None)],
        vec![String::from("")],
        engine_type,
        engine_resource,
        memory_resource,
    );

    // matrix with the first number indicating the number of rows
    let data = vec![2u64, 1, 2, 3, 4];
    let data_len = data.len();
    let mut in_context = ReadOnlyContext::new(data.into_boxed_slice())
        .expect("Should be able to create read only context");
    in_context.content = vec![Some(DataSet {
        ident: String::from(""),
        buffers: vec![DataItem {
            ident: String::from(""),
            data: Position {
                offset: 0,
                size: data_len * core::mem::size_of::<u64>(),
            },
            key: 1,
        }],
    })];

    let composition = Composition {
        dependencies: vec![
            FunctionDependencies {
                function: function_id,
                join_info: (vec![], vec![]),
                input_set_ids: vec![Some(InputSetDescriptor {
                    composition_id: 0,
                    sharding: ShardingMode::All,
                    optional: false,
                })],
                output_set_ids: vec![Some(1)],
            },
            FunctionDependencies {
                function: function_id,
                join_info: (vec![], vec![]),
                input_set_ids: vec![Some(InputSetDescriptor {
                    composition_id: 1,
                    sharding: ShardingMode::All,
                    optional: false,
                })],
                output_set_ids: vec![Some(2)],
            },
        ],
        output_map: BTreeMap::from([(2, 0)]),
    };

    let recorder = Recorder::new(0, Instant::now());

    let inputs = vec![Some(CompositionSet::from((0, vec![Arc::new(in_context)])))];
    let result = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(dispatcher.queue_composition(composition, inputs, false, recorder));
    let out_contexts = match result {
        Ok(context) => context,
        Err(err) => panic!("Failed with: {:?}", err),
    };
    assert_eq!(1, out_contexts.len());
    let out_composition_set = out_contexts[0].as_ref().expect("Should have set 0");
    let mut out_context_iter = out_composition_set.into_iter();
    let (_, _, out_context) = out_context_iter.next().unwrap();
    assert!(out_context_iter.next().is_none());
    assert_eq!(1, out_context.content.len());
    check_matrix(&out_context, 0, 0, 2, vec![146, 330, 330, 746]);
}

pub fn composition_diamond_matmac<Domain: MemoryDomain>(
    memory_resource: (DomainType, MemoryResource),
    function_name: &str,
    engine_type: EngineType,
    engine_resource: Vec<ComputeResource>,
) {
    let (dispatcher, function_id) = self::setup_dispatcher::<Domain>(
        "composition_diamond_matmac",
        function_name,
        vec![
            (String::from(""), None),
            (String::from(""), None),
            (String::from(""), None),
        ],
        vec![String::from("")],
        engine_type,
        engine_resource,
        memory_resource,
    );
    // A = [7] with row of 1 as first data element
    let mat_a = vec![1u64, 7];
    // B = [1,2,3,5] with row inidcator 1 as first data element
    let mat_b = vec![1u64, 1, 2, 3, 5];
    // B^T with row indicator of 4 as first data element
    let mat_bt = vec![4, 1, 2, 3, 5];

    let mut data = vec![];
    data.extend_from_slice(&mat_a);
    data.extend_from_slice(&mat_b);
    data.extend_from_slice(&mat_bt);
    let mut in_context = ReadOnlyContext::new(data.into_boxed_slice())
        .expect("Should be able to create read only context");
    in_context.content = vec![
        Some(DataSet {
            ident: String::from(""),
            buffers: vec![DataItem {
                ident: String::from(""),
                data: Position {
                    offset: 0,
                    size: mat_a.len() * core::mem::size_of::<u64>(),
                },
                key: 0,
            }],
        }),
        Some(DataSet {
            ident: String::from(""),
            buffers: vec![DataItem {
                ident: String::from(""),
                data: Position {
                    offset: mat_a.len() * core::mem::size_of::<u64>(),
                    size: mat_b.len() * core::mem::size_of::<u64>(),
                },
                key: 0,
            }],
        }),
        Some(DataSet {
            ident: String::from(""),
            buffers: vec![DataItem {
                ident: String::from(""),
                data: Position {
                    offset: (mat_a.len() + mat_b.len()) * core::mem::size_of::<u64>(),
                    size: mat_b.len() * core::mem::size_of::<u64>(),
                },
                key: 0,
            }],
        }),
    ];

    let composition = Composition {
        dependencies: vec![
            // C = A*B
            FunctionDependencies {
                function: function_id,
                join_info: (vec![], vec![]),
                input_set_ids: vec![
                    Some(InputSetDescriptor {
                        composition_id: 0,
                        sharding: ShardingMode::All,
                        optional: false,
                    }),
                    Some(InputSetDescriptor {
                        composition_id: 1,
                        sharding: ShardingMode::All,
                        optional: false,
                    }),
                    None,
                ],
                output_set_ids: vec![Some(3)],
            },
            // D = B^T*A
            FunctionDependencies {
                function: function_id,
                join_info: (vec![], vec![]),
                input_set_ids: vec![
                    Some(InputSetDescriptor {
                        composition_id: 2,
                        sharding: ShardingMode::All,
                        optional: false,
                    }),
                    Some(InputSetDescriptor {
                        composition_id: 0,
                        sharding: ShardingMode::All,
                        optional: false,
                    }),
                    None,
                ],
                output_set_ids: vec![Some(4)],
            },
            // E = B + C
            FunctionDependencies {
                function: function_id,
                join_info: (vec![], vec![]),
                input_set_ids: vec![
                    None,
                    Some(InputSetDescriptor {
                        composition_id: 1,
                        sharding: ShardingMode::All,
                        optional: false,
                    }),
                    Some(InputSetDescriptor {
                        composition_id: 3,
                        sharding: ShardingMode::All,
                        optional: false,
                    }),
                ],
                output_set_ids: vec![Some(5)],
            },
            // G = D * C
            FunctionDependencies {
                function: function_id,
                join_info: (vec![], vec![]),
                input_set_ids: vec![
                    Some(InputSetDescriptor {
                        composition_id: 4,
                        sharding: ShardingMode::All,
                        optional: false,
                    }),
                    Some(InputSetDescriptor {
                        composition_id: 3,
                        sharding: ShardingMode::All,
                        optional: false,
                    }),
                    None,
                ],
                output_set_ids: vec![Some(6)],
            },
            // Result = D*E + G
            FunctionDependencies {
                function: function_id,
                join_info: (vec![], vec![]),
                input_set_ids: vec![
                    Some(InputSetDescriptor {
                        composition_id: 4,
                        sharding: ShardingMode::All,
                        optional: false,
                    }),
                    Some(InputSetDescriptor {
                        composition_id: 5,
                        sharding: ShardingMode::All,
                        optional: false,
                    }),
                    Some(InputSetDescriptor {
                        composition_id: 6,
                        sharding: ShardingMode::All,
                        optional: false,
                    }),
                ],
                output_set_ids: vec![Some(7)],
            },
        ],
        output_map: BTreeMap::from([(7, 0)]),
    };

    let recorder = Recorder::new(0, Instant::now());

    let context_arc = Arc::new(in_context);
    let inputs = vec![
        Some(CompositionSet::from((0, vec![context_arc.clone()]))),
        Some(CompositionSet::from((1, vec![context_arc.clone()]))),
        Some(CompositionSet::from((2, vec![context_arc.clone()]))),
    ];
    let result = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(dispatcher.queue_composition(composition, inputs, false, recorder));
    let out_contexts = match result {
        Ok(context) => context,
        Err(err) => panic!("Failed with: {:?}", err),
    };
    assert_eq!(1, out_contexts.len());
    let out_composition_set = out_contexts[0].as_ref().expect("Should have set 0");
    let mut out_context_iter = out_composition_set.into_iter();
    let (_, _, out_context) = out_context_iter.next().unwrap();
    assert!(out_context_iter.next().is_none());
    assert_eq!(1, out_context.content.len());
    check_matrix(
        &out_context,
        0,
        0,
        4,
        vec![
            105, 210, 315, 525, 210, 420, 630, 1050, 315, 630, 945, 1575, 525, 1050, 1575, 2625,
        ],
    );
}

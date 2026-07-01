use super::{check_matrix, setup_dispatcher, zero_id};
use dandelion_commons::records::Recorder;
use dispatcher::dispatcher::{Dispatcher, RecoveredNodeOutputs};
use log::debug;
use machine_interface::{
    composition::{
        Composition, CompositionSet, FunctionDependencies, InputSetDescriptor, ItemData,
        JoinStrategy, ShardingMode,
    },
    function_driver::ComputeResource,
    machine_config::{DomainType, EngineType},
    memory_domain::{
        read_only::ReadOnlyContext, Context, ContextTrait, MemoryDomain, MemoryResource,
    },
    DataItem, DataSet, Position,
};
use std::{collections::BTreeMap, collections::HashMap, time::Instant};
use std::{iter, sync::Arc};

pub fn single_domain_and_engine_basic<Domain: MemoryDomain>(
    memory_resource: (DomainType, MemoryResource),
    relative_path: &str,
    engine_type: EngineType,
    engine_resource: Vec<ComputeResource>,
) {
    let (dispatcher, function_id) = setup_dispatcher::<Domain>(
        relative_path,
        vec![],
        vec![],
        engine_type,
        engine_resource,
        memory_resource,
    );

    let recorder = Recorder::new(dandelion_commons::InvocationId::nil(), zero_id(), Instant::now());
    let result = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(dispatcher.queue_function(function_id, Vec::new(), false, recorder, None, None, None));
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

    let inputs = CompositionSet::from_context(in_context);

    let recorder = Recorder::new(dandelion_commons::InvocationId::nil(), zero_id(), Instant::now());

    let result = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(dispatcher.queue_function(function_id, inputs, false, recorder, None, None, None));
    let out_sets = match result {
        Ok(context) => context,
        Err(err) => panic!("Failed with: {:?}", err),
    };
    assert_eq!(1, out_sets.len());
    let out_set = out_sets[0].as_ref().expect("Should have set");
    let mut out_set_iter = out_set.into_iter();
    let (item, out_context) = out_set_iter.next().unwrap();
    assert!(out_set_iter.next().is_none());
    assert_eq!(0, item.key);
    check_matrix(out_context, item, 2, vec![5, 11, 11, 25])
}

pub fn composition_single_matmul<Domain: MemoryDomain>(
    memory_resource: (DomainType, MemoryResource),
    relative_path: &str,
    engine_type: EngineType,
    engine_resource: Vec<ComputeResource>,
) {
    let (dispatcher, function_id) = setup_dispatcher::<Domain>(
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
            join_info: (vec![0], vec![]),
            input_set_ids: vec![Some(InputSetDescriptor {
                composition_id: 0,
                sharding: ShardingMode::All,
                optional: false,
            })],
            output_set_ids: vec![Some(1)],
        }],
        output_map: BTreeMap::from([(1, 0)]),
    };
    let inputs = CompositionSet::from_context(in_context);

    let recorder = Recorder::new(dandelion_commons::InvocationId::nil(), zero_id(), Instant::now());

    let result = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(dispatcher.queue_composition(composition, inputs, false, recorder, None));
    let out_contexts = match result {
        Ok(context) => context,
        Err(err) => panic!("Failed with: {:?}", err),
    };
    assert_eq!(1, out_contexts.len());
    let out_context_list = out_contexts[0].as_ref().expect("Should have set");

    let mut out_context_iter = out_context_list.into_iter();
    let (item, out_context) = out_context_iter.next().unwrap();
    assert!(out_context_iter.next().is_none());
    assert_eq!(0, item.key);
    check_matrix(&out_context, item, 2, vec![5, 11, 11, 25])
}

fn composition_option_helper(
    composition: Composition,
    inputs: Vec<Option<CompositionSet>>,
    dispatcher: &mut Dispatcher,
) -> Vec<Option<CompositionSet>> {
    let recorder = Recorder::new(dandelion_commons::InvocationId::nil(), zero_id(), Instant::now());

    let result = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(dispatcher.queue_composition(composition, inputs, false, recorder, None));
    let out_contexts = match result {
        Ok(context) => context,
        Err(err) => panic!("Failed with: {:?}", err),
    };

    return out_contexts;
}

fn read_single_matrix(set: &CompositionSet) -> Vec<u64> {
    let mut iter = set.into_iter();
    let (item, data) = iter.next().expect("Expected a single output item");
    assert!(iter.next().is_none());
    let context = match data {
        ItemData::LocalData(context) => context,
        _ => panic!("Expected local output data"),
    };
    let len = item.data.size / core::mem::size_of::<u64>();
    let mut matrix = vec![0u64; len];
    context
        .context
        .read(item.data.offset, &mut matrix)
        .expect("Should read output matrix");
    matrix
}

fn make_matmul_input_context() -> Context {
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
            key: 0,
        }],
    })];
    in_context
}

fn make_matmac_input_context() -> Context {
    let mat_a = vec![1u64, 7];
    let mat_b = vec![1u64, 1, 2, 3, 5];
    let mat_bt = vec![4, 1, 2, 3, 5];

    let mut data = vec![];
    data.extend_from_slice(&mat_a);
    data.extend_from_slice(&mat_b);
    data.extend_from_slice(&mat_bt);
    let mut in_context = ReadOnlyContext::new(data.into_boxed_slice())
        .expect("Should be able to create read only context");
    in_context.content = vec![
        Some(DataSet {
            ident: String::from("A set"),
            buffers: vec![DataItem {
                ident: String::from("A"),
                data: Position {
                    offset: 0,
                    size: mat_a.len() * core::mem::size_of::<u64>(),
                },
                key: 0,
            }],
        }),
        Some(DataSet {
            ident: String::from("B set"),
            buffers: vec![DataItem {
                ident: String::from("B"),
                data: Position {
                    offset: mat_a.len() * core::mem::size_of::<u64>(),
                    size: mat_b.len() * core::mem::size_of::<u64>(),
                },
                key: 0,
            }],
        }),
        Some(DataSet {
            ident: String::from("BT set"),
            buffers: vec![DataItem {
                ident: String::from("BT"),
                data: Position {
                    offset: (mat_a.len() + mat_b.len()) * core::mem::size_of::<u64>(),
                    size: mat_b.len() * core::mem::size_of::<u64>(),
                },
                key: 0,
            }],
        }),
    ];
    in_context
}

pub fn composition_optional<Domain: MemoryDomain>(
    memory_resource: (DomainType, MemoryResource),
    relative_path: &str,
    engine_type: EngineType,
    engine_resource: Vec<ComputeResource>,
) {
    let (mut dispatcher, function_id) = setup_dispatcher::<Domain>(
        relative_path,
        vec![(String::from(""), None)],
        vec![String::from(""), String::from("")],
        engine_type,
        engine_resource,
        memory_resource,
    );

    debug!("Starting test for case where the set is an input set, not optional and not present");
    let composition1 = Composition {
        dependencies: vec![FunctionDependencies {
            function: function_id.clone(),
            join_info: (vec![0], vec![]),
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

    debug!("Starting test for case where the set is an input set, optional and not present");
    let composition2 = Composition {
        dependencies: vec![FunctionDependencies {
            function: function_id.clone(),
            join_info: (vec![0], vec![]),
            input_set_ids: vec![Some(InputSetDescriptor {
                composition_id: 0,
                sharding: ShardingMode::All,
                optional: true,
            })],
            output_set_ids: vec![Some(1)],
        }],
        output_map: BTreeMap::from([(1, 0)]),
    };
    let inputs2 = vec![None];
    let out_contexts = composition_option_helper(composition2, inputs2, &mut dispatcher);
    assert_eq!(1, out_contexts.len());
    assert!(out_contexts[0].is_some());

    debug!(
        "Starting test for case where the set is a composition set, not optional and not present"
    );
    let composition5 = Composition {
        dependencies: vec![
            FunctionDependencies {
                function: function_id.clone(),
                join_info: (vec![], vec![]),
                input_set_ids: vec![],
                output_set_ids: vec![None, Some(1)],
            },
            FunctionDependencies {
                function: function_id.clone(),
                join_info: (vec![0], vec![]),
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

    debug!("Starting test for case where the set is a composition set, optional and not present");
    let composition6 = Composition {
        dependencies: vec![
            FunctionDependencies {
                function: function_id.clone(),
                join_info: (vec![], vec![]),
                input_set_ids: vec![],
                output_set_ids: vec![None, Some(1)],
            },
            FunctionDependencies {
                function: function_id.clone(),
                join_info: (vec![0], vec![]),
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
            join_info: (vec![0], vec![]),
            input_set_ids: vec![Some(InputSetDescriptor {
                composition_id: 0,
                sharding: ShardingMode::Each,
                optional: false,
            })],
            output_set_ids: vec![Some(1)],
        }],
        output_map: BTreeMap::from([(1, 0)]),
    };
    let inputs = CompositionSet::from_context(in_context);

    let recorder = Recorder::new(dandelion_commons::InvocationId::nil(), zero_id(), Instant::now());

    let result = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(dispatcher.queue_composition(composition, inputs, false, recorder, None));
    let out_vec = match result {
        Ok(v) => v,
        Err(err) => panic!("Failed with: {:?}", err),
    };
    assert_eq!(1, out_vec.len());
    let out_set = out_vec[0].as_ref().expect("Should have set");

    // check for each shard:
    assert_eq!(2, out_set.len());
    for (item, matrix_context) in out_set.into_iter() {
        assert!(item.key == 1 || item.key == 0);
        check_matrix(&matrix_context, item, 2, vec![5, 11, 11, 25]);
    }
}

pub fn composition_recovered_parallel_sibling_still_runs<Domain: MemoryDomain>(
    memory_resource: (DomainType, MemoryResource),
    relative_path: &str,
    engine_type: EngineType,
    engine_resource: Vec<ComputeResource>,
) {
    let (dispatcher, function_id) = setup_dispatcher::<Domain>(
        relative_path,
        vec![(String::from(""), None)],
        vec![String::from("")],
        engine_type,
        engine_resource,
        memory_resource,
    );
    let inputs = CompositionSet::from_context(make_matmul_input_context());
    let recovered_output = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(dispatcher.queue_function(
            function_id.clone(),
            inputs.clone(),
            false,
            Recorder::new(
                dandelion_commons::InvocationId::nil(),
                zero_id(),
                Instant::now(),
            ),
            Some("node-left".to_string()),
            None,
            None,
        ))
        .expect("Should compute recovered output");

    let recovered_nodes: RecoveredNodeOutputs = Arc::new(HashMap::from([(
        1usize,
        recovered_output[0]
            .clone()
            .expect("Recovered function should produce composition output"),
    )]));
    let composition = Composition {
        dependencies: vec![
            FunctionDependencies {
                function: function_id.clone(),
                join_info: (vec![0], vec![]),
                input_set_ids: vec![Some(InputSetDescriptor {
                    composition_id: 0,
                    sharding: ShardingMode::All,
                    optional: false,
                })],
                output_set_ids: vec![Some(1)],
            },
            FunctionDependencies {
                function: function_id,
                join_info: (vec![0], vec![]),
                input_set_ids: vec![Some(InputSetDescriptor {
                    composition_id: 0,
                    sharding: ShardingMode::All,
                    optional: false,
                })],
                output_set_ids: vec![Some(2)],
            },
        ],
        output_map: BTreeMap::from([(1, 0), (2, 1)]),
    };
    let outputs = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(dispatcher.queue_composition(
            composition,
            inputs,
            false,
            Recorder::new(dandelion_commons::InvocationId::nil(), zero_id(), Instant::now()),
            Some(recovered_nodes),
        ))
        .expect("Recovered composition should succeed");

    assert_eq!(2, outputs.len());
    let left = outputs[0].as_ref().expect("Recovered branch should produce output");
    let right = outputs[1]
        .as_ref()
        .expect("Non-recovered sibling should still run");
    assert_eq!(vec![2, 5, 11, 11, 25], read_single_matrix(left));
    assert_eq!(vec![2, 5, 11, 11, 25], read_single_matrix(right));
}

pub fn composition_chain_matmul<Domain: MemoryDomain>(
    memory_resource: (DomainType, MemoryResource),
    relative_path: &str,
    engine_type: EngineType,
    engine_resource: Vec<ComputeResource>,
) {
    let (dispatcher, function_id) = setup_dispatcher::<Domain>(
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
            key: 0,
        }],
    })];

    let composition = Composition {
        dependencies: vec![
            FunctionDependencies {
                function: function_id.clone(),
                join_info: (vec![0], vec![]),
                input_set_ids: vec![Some(InputSetDescriptor {
                    composition_id: 0,
                    sharding: ShardingMode::All,
                    optional: false,
                })],
                output_set_ids: vec![Some(1)],
            },
            FunctionDependencies {
                function: function_id,
                join_info: (vec![0], vec![]),
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

    let recorder = Recorder::new(dandelion_commons::InvocationId::nil(), zero_id(), Instant::now());

    let inputs = CompositionSet::from_context(in_context);
    let result = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(dispatcher.queue_composition(composition, inputs, false, recorder, None));
    let out_contexts = match result {
        Ok(context) => context,
        Err(err) => panic!("Failed with: {:?}", err),
    };
    assert_eq!(1, out_contexts.len());
    let out_composition_set = out_contexts[0].as_ref().expect("Should have set 0");
    let mut out_context_iter = out_composition_set.into_iter();
    let (item, out_context) = out_context_iter.next().unwrap();
    assert!(out_context_iter.next().is_none());
    assert_eq!(0, item.key);
    check_matrix(&out_context, item, 2, vec![146, 330, 330, 746]);
}

pub fn composition_diamond_matmac<Domain: MemoryDomain>(
    memory_resource: (DomainType, MemoryResource),
    relative_path: &str,
    engine_type: EngineType,
    engine_resource: Vec<ComputeResource>,
) {
    let (dispatcher, function_id) = self::setup_dispatcher::<Domain>(
        relative_path,
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
            ident: String::from("A set"),
            buffers: vec![DataItem {
                ident: String::from("A"),
                data: Position {
                    offset: 0,
                    size: mat_a.len() * core::mem::size_of::<u64>(),
                },
                key: 0,
            }],
        }),
        Some(DataSet {
            ident: String::from("B set"),
            buffers: vec![DataItem {
                ident: String::from("B"),
                data: Position {
                    offset: mat_a.len() * core::mem::size_of::<u64>(),
                    size: mat_b.len() * core::mem::size_of::<u64>(),
                },
                key: 0,
            }],
        }),
        Some(DataSet {
            ident: String::from("BT set"),
            buffers: vec![DataItem {
                ident: String::from("BT"),
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
                function: function_id.clone(),
                join_info: (
                    vec![0, 1, 2],
                    vec![JoinStrategy::Cross, JoinStrategy::Cross],
                ),
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
                function: function_id.clone(),
                join_info: (
                    vec![0, 1, 2],
                    vec![JoinStrategy::Cross, JoinStrategy::Cross],
                ),
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
                function: function_id.clone(),
                join_info: (
                    vec![0, 1, 2],
                    vec![JoinStrategy::Cross, JoinStrategy::Cross],
                ),
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
                function: function_id.clone(),
                join_info: (
                    vec![0, 1, 2],
                    vec![JoinStrategy::Cross, JoinStrategy::Cross],
                ),
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
                function: function_id.clone(),
                join_info: (
                    vec![0, 1, 2],
                    vec![JoinStrategy::Cross, JoinStrategy::Cross],
                ),
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

    let recorder = Recorder::new(dandelion_commons::InvocationId::nil(), zero_id(), Instant::now());

    let inputs = CompositionSet::from_context(in_context);
    let result = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(dispatcher.queue_composition(composition, inputs, false, recorder, None));
    let out_contexts = match result {
        Ok(context) => context,
        Err(err) => panic!("Failed with: {:?}", err),
    };
    assert_eq!(1, out_contexts.len());
    let out_composition_set = out_contexts[0].as_ref().expect("Should have set 0");
    let mut out_context_iter = out_composition_set.into_iter();
    let (item, out_context) = out_context_iter.next().unwrap();
    assert!(out_context_iter.next().is_none());
    assert_eq!(0, item.key);
    check_matrix(
        &out_context,
        item,
        4,
        vec![
            105, 210, 315, 525, 210, 420, 630, 1050, 315, 630, 945, 1575, 525, 1050, 1575, 2625,
        ],
    );
}

pub fn composition_recovered_node_unblocks_downstream<Domain: MemoryDomain>(
    memory_resource: (DomainType, MemoryResource),
    relative_path: &str,
    engine_type: EngineType,
    engine_resource: Vec<ComputeResource>,
) {
    let (dispatcher, function_id) = setup_dispatcher::<Domain>(
        relative_path,
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
    let composition = Composition {
        dependencies: vec![
            FunctionDependencies {
                function: function_id.clone(),
                join_info: (
                    vec![0, 1, 2],
                    vec![JoinStrategy::Cross, JoinStrategy::Cross],
                ),
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
            FunctionDependencies {
                function: function_id.clone(),
                join_info: (
                    vec![0, 1, 2],
                    vec![JoinStrategy::Cross, JoinStrategy::Cross],
                ),
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
            FunctionDependencies {
                function: function_id.clone(),
                join_info: (
                    vec![0, 1, 2],
                    vec![JoinStrategy::Cross, JoinStrategy::Cross],
                ),
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
                output_set_ids: vec![Some(5)],
            },
        ],
        output_map: BTreeMap::from([(5, 0)]),
    };

    let baseline_outputs = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(dispatcher.queue_composition(
            composition.clone(),
            CompositionSet::from_context(make_matmac_input_context()),
            false,
            Recorder::new(dandelion_commons::InvocationId::nil(), zero_id(), Instant::now()),
            None,
        ))
        .expect("Baseline composition should succeed");
    let baseline_matrix = read_single_matrix(
        baseline_outputs[0]
            .as_ref()
            .expect("Baseline output should be present"),
    );

    let inputs = CompositionSet::from_context(make_matmac_input_context());
    let recovered_output = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(dispatcher.queue_function(
            function_id,
            vec![inputs[0].clone(), inputs[1].clone(), None],
            false,
            Recorder::new(dandelion_commons::InvocationId::nil(), zero_id(), Instant::now()),
            Some("node-c".to_string()),
            None,
            None,
        ))
        .expect("Should compute recovered node output");
    let recovered_nodes: RecoveredNodeOutputs = Arc::new(HashMap::from([(
        3usize,
        recovered_output[0]
            .clone()
            .expect("Recovered function should produce composition output"),
    )]));

    let recovered_outputs = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(dispatcher.queue_composition(
            composition,
            inputs,
            false,
            Recorder::new(dandelion_commons::InvocationId::nil(), zero_id(), Instant::now()),
            Some(recovered_nodes),
        ))
        .expect("Recovered composition should succeed");
    let recovered_matrix = read_single_matrix(
        recovered_outputs[0]
            .as_ref()
            .expect("Recovered output should be present"),
    );

    assert_eq!(baseline_matrix, recovered_matrix);
}

pub fn composition_chain_large_matmac<Domain: MemoryDomain>(
    memory_resource: (DomainType, MemoryResource),
    relative_path: &str,
    engine_type: EngineType,
    engine_resource: Vec<ComputeResource>,
) {
    let (dispatcher, function_id) = self::setup_dispatcher::<Domain>(
        relative_path,
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
    let matrix_width = 128u64;
    let matrix_size = matrix_width * matrix_width;
    let mut mat_a = vec![matrix_width];
    mat_a.extend(1..matrix_size + 1);
    let mut mat_b = vec![matrix_width];
    mat_b.extend(iter::repeat_n(1, matrix_size as usize));

    let mut data = vec![];
    data.extend_from_slice(&mat_a);
    data.extend_from_slice(&mat_b);
    let mut in_context = ReadOnlyContext::new(data.into_boxed_slice())
        .expect("Should be able to create read only context");
    in_context.content = vec![
        Some(DataSet {
            ident: String::from("a set"),
            buffers: vec![DataItem {
                ident: String::from("a mat"),
                data: Position {
                    offset: 0,
                    size: mat_a.len() * core::mem::size_of::<u64>(),
                },
                key: 0,
            }],
        }),
        Some(DataSet {
            ident: String::from("b set"),
            buffers: vec![DataItem {
                ident: String::from("b mat"),
                data: Position {
                    offset: mat_a.len() * core::mem::size_of::<u64>(),
                    size: mat_b.len() * core::mem::size_of::<u64>(),
                },
                key: 0,
            }],
        }),
    ];

    let mut dependencies = vec![FunctionDependencies {
        // C+C, should be a matrix filled with 2s
        // also test that we can use the same set as mutliple input set for one function
        function: function_id.clone(),
        join_info: (
            vec![0, 1, 2],
            vec![JoinStrategy::Cross, JoinStrategy::Cross],
        ),
        input_set_ids: vec![
            None,
            Some(InputSetDescriptor {
                composition_id: 1,
                sharding: ShardingMode::All,
                optional: false,
            }),
            Some(InputSetDescriptor {
                composition_id: 1,
                sharding: ShardingMode::All,
                optional: false,
            }),
        ],
        output_set_ids: vec![Some(2)],
    }];

    let chain_length = 10;
    for chain_stage in 0..chain_length {
        let (input_set, output_set) = if chain_stage == 0 {
            (0, 3)
        } else {
            (chain_stage + 2, chain_stage + 3)
        };
        dependencies.push(FunctionDependencies {
            function: function_id.clone(),
            join_info: (
                vec![0, 1, 2],
                vec![JoinStrategy::Cross, JoinStrategy::Cross],
            ),
            input_set_ids: vec![
                None,
                Some(InputSetDescriptor {
                    composition_id: 2,
                    sharding: ShardingMode::All,
                    optional: false,
                }),
                Some(InputSetDescriptor {
                    composition_id: input_set,
                    sharding: ShardingMode::All,
                    optional: false,
                }),
            ],
            output_set_ids: vec![Some(output_set)],
        });
    }

    let composition = Composition {
        dependencies,
        output_map: BTreeMap::from([(chain_length + 2, 0)]),
    };

    let recorder = Recorder::new(
        dandelion_commons::InvocationId::nil(),
        Arc::new(0.to_string()),
        Instant::now(),
    );

    let inputs = CompositionSet::from_context(in_context);
    let result = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(dispatcher.queue_composition(composition, inputs, false, recorder, None));
    let out_contexts = match result {
        Ok(context) => context,
        Err(err) => panic!("Failed with: {:?}", err),
    };
    assert_eq!(1, out_contexts.len());
    let out_composition_set = out_contexts[0].as_ref().expect("Should have set 0");
    let mut out_context_iter = out_composition_set.into_iter();
    let (out_item, out_context) = out_context_iter.next().unwrap();
    assert!(out_context_iter.next().is_none());
    let expected =
        (1 + 2 * chain_length as u64..matrix_size + 1 + 2 * chain_length as u64).collect();
    assert_eq!(0, out_item.key);
    check_matrix(&out_context, out_item, matrix_width, expected);
}

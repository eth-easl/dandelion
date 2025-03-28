use super::{check_matrix, setup_dispatcher};
use dandelion_commons::records::{Archive, ArchiveInit, RecordPoint};
use dispatcher::{
    composition::{Composition, CompositionSet, FunctionDependencies, OutputMap, ShardingMode},
    dispatcher::DispatcherInput,
};
use machine_interface::{
    function_driver::ComputeResource,
    machine_config::{DomainType, EngineType},
    memory_domain::{read_only::ReadOnlyContext, MemoryDomain, MemoryResource},
    DataItem, DataSet, Position,
};
use std::collections::BTreeMap;
use std::sync::Arc;

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

    let archive = Box::leak(Box::new(Archive::init(ArchiveInit {
        #[cfg(feature = "timestamp")]
        timestamp_count: 1000,
    })));
    let mut recorder = archive.get_recorder().unwrap();
    let _ = recorder.record(RecordPoint::Arrival);

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

    let archive = Box::leak(Box::new(Archive::init(ArchiveInit {
        #[cfg(feature = "timestamp")]
        timestamp_count: 1000,
    })));
    let mut recorder = archive.get_recorder().unwrap();
    let _ = recorder.record(RecordPoint::Arrival);

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
            input_set_ids: vec![Some((OutputMap::Local(0), ShardingMode::All))],
            output_set_ids: vec![Some(OutputMap::Local(1))],
        }],
        output_map: BTreeMap::from([(OutputMap::Local(1), 0)]),
    };
    let inputs = vec![Some(CompositionSet::from((0, vec![Arc::new(in_context)])))];

    let archive = Box::leak(Box::new(Archive::init(ArchiveInit {
        #[cfg(feature = "timestamp")]
        timestamp_count: 1000,
    })));
    let mut recorder = archive.get_recorder().unwrap();
    let _ = recorder.record(RecordPoint::Arrival);

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
            join_info: (vec![], vec![]),
            input_set_ids: vec![Some((OutputMap::Local(0), ShardingMode::Each))],
            output_set_ids: vec![Some(OutputMap::Local(1))],
        }],
        output_map: BTreeMap::from([(OutputMap::Local(1), 0)]),
    };
    let inputs = vec![Some(CompositionSet::from((0, vec![Arc::new(in_context)])))];

    let archive = Box::leak(Box::new(Archive::init(ArchiveInit {
        #[cfg(feature = "timestamp")]
        timestamp_count: 1000,
    })));
    let mut recorder = archive.get_recorder().unwrap();
    let _ = recorder.record(RecordPoint::Arrival);

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
                input_set_ids: vec![Some((OutputMap::Local(0), ShardingMode::All))],
                output_set_ids: vec![Some(OutputMap::Local(1))],
            },
            FunctionDependencies {
                function: function_id,
                join_info: (vec![], vec![]),
                input_set_ids: vec![Some((OutputMap::Local(1), ShardingMode::All))],
                output_set_ids: vec![Some(OutputMap::Local(2))],
            },
        ],
        output_map: BTreeMap::from([(OutputMap::Local(2), 0)]),
    };

    let archive = Box::leak(Box::new(Archive::init(ArchiveInit {
        #[cfg(feature = "timestamp")]
        timestamp_count: 1000,
    })));
    let mut recorder = archive.get_recorder().unwrap();
    let _ = recorder.record(RecordPoint::Arrival);

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

pub fn composition_chain_matmul_global_sets<Domain: MemoryDomain>(
    memory_resource: (DomainType, MemoryResource),
    relative_path: &str,
    engine_type: EngineType,
    engine_resource: Vec<ComputeResource>,
) {
    let (dispatcher, _) = setup_dispatcher::<Domain>(
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

    let archive = Box::leak(Box::new(Archive::init(ArchiveInit {
        #[cfg(feature = "timestamp")]
        timestamp_count: 1000,
    })));
    let mut recorder = archive.get_recorder().unwrap();
    let _ = recorder.record(RecordPoint::Arrival);

    let inputs = vec![DispatcherInput::Set(CompositionSet::from((
        0,
        vec![Arc::new(in_context)],
    )))];

    let intermediate = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(dispatcher.queue_function_by_name(
            String::from("test_function"),
            inputs,
            vec![(0, 1)],
            false,
            recorder,
        ))
        .unwrap();
    assert_eq!(1, intermediate.len());
    assert!(intermediate[0].is_none());

    let mut recorder = archive.get_recorder().unwrap();
    let _ = recorder.record(RecordPoint::Arrival);
    let result = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(dispatcher.queue_function_by_name(
            String::from("test_function"),
            vec![DispatcherInput::Global(1)],
            vec![],
            false,
            recorder,
        ));
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
                    Some((OutputMap::Local(0), ShardingMode::All)),
                    Some((OutputMap::Local(1), ShardingMode::All)),
                    None,
                ],
                output_set_ids: vec![Some(OutputMap::Local(3))],
            },
            // D = B^T*A
            FunctionDependencies {
                function: function_id,
                join_info: (vec![], vec![]),
                input_set_ids: vec![
                    Some((OutputMap::Local(2), ShardingMode::All)),
                    Some((OutputMap::Local(0), ShardingMode::All)),
                    None,
                ],
                output_set_ids: vec![Some(OutputMap::Local(4))],
            },
            // E = B + C
            FunctionDependencies {
                function: function_id,
                join_info: (vec![], vec![]),
                input_set_ids: vec![
                    None,
                    Some((OutputMap::Local(1), ShardingMode::All)),
                    Some((OutputMap::Local(3), ShardingMode::All)),
                ],
                output_set_ids: vec![Some(OutputMap::Local(5))],
            },
            // G = D * C
            FunctionDependencies {
                function: function_id,
                join_info: (vec![], vec![]),
                input_set_ids: vec![
                    Some((OutputMap::Local(4), ShardingMode::All)),
                    Some((OutputMap::Local(3), ShardingMode::All)),
                    None,
                ],
                output_set_ids: vec![Some(OutputMap::Local(6))],
            },
            // Result = D*E + G
            FunctionDependencies {
                function: function_id,
                join_info: (vec![], vec![]),
                input_set_ids: vec![
                    Some((OutputMap::Local(4), ShardingMode::All)),
                    Some((OutputMap::Local(5), ShardingMode::All)),
                    Some((OutputMap::Local(6), ShardingMode::All)),
                ],
                output_set_ids: vec![Some(OutputMap::Local(7))],
            },
        ],
        output_map: BTreeMap::from([(OutputMap::Local(7), 0)]),
    };

    let archive = Box::leak(Box::new(Archive::init(ArchiveInit {
        #[cfg(feature = "timestamp")]
        timestamp_count: 1000,
    })));
    let mut recorder = archive.get_recorder().unwrap();
    let _ = recorder.record(RecordPoint::Arrival);

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

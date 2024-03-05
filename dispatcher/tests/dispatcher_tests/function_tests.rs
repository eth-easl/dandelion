use super::{check_matrix, setup_dispatcher};
use core::mem::size_of;
use dispatcher::composition::{Composition, CompositionSet, FunctionDependencies, ShardingMode};
use machine_interface::{
    function_driver::ComputeResource,
    machine_config::EngineType,
    memory_domain::{Context, MemoryDomain, MemoryResource},
    DataItem, DataSet, Position,
};
use std::collections::BTreeMap;
use std::sync::Arc;

fn add_matmul_matrix(
    context: &mut Context,
    set: usize,
    key: u32,
    matrix_dim: u64,
    mut matrix: Vec<u64>,
) {
    // check the sets are not already full and ensure they exist
    if context.content.len() <= set {
        context.content.resize_with(set + 1, || None);
    }

    assert_eq!(matrix_dim * matrix_dim, matrix.len() as u64);

    matrix.insert(0, matrix_dim);
    let mat_offset = context
        .get_free_space_and_write_slice(&matrix)
        .expect("Should have space") as usize;
    if let Some(set) = &mut context.content[set] {
        set.buffers.push(DataItem {
            ident: String::from(""),
            data: Position {
                offset: mat_offset,
                size: matrix.len() * size_of::<i64>(),
            },
            key: key,
        });
    } else {
        context.content[set] = Some(DataSet {
            ident: "".to_string(),
            buffers: vec![DataItem {
                ident: "".to_string(),
                data: Position {
                    offset: mat_offset,
                    size: matrix.len() * size_of::<i64>(),
                },
                key: key,
            }],
        });
    }
}

fn add_matmac_matrix(
    context: &mut Context,
    set: usize,
    rows: u64,
    cols: u64,
    mut matrix: Vec<u64>,
) {
    // check the sets are not already full and ensure they exist
    if context.content.len() <= set {
        context.content.resize_with(set + 1, || None);
    }
    if context.content[set].is_some() || context.content[set].is_some() {
        panic!("trying to add matrix where there is already set");
    }

    assert_eq!(rows * cols, matrix.len() as u64);

    matrix.insert(0, rows);

    let in_mat_offset = context
        .get_free_space_and_write_slice(&matrix)
        .expect("Should have space") as usize;
    context.content[set] = Some(DataSet {
        ident: "".to_string(),
        buffers: vec![DataItem {
            ident: "".to_string(),
            data: Position {
                offset: in_mat_offset,
                size: matrix.len() * 8,
            },
            key: 0,
        }],
    });
}

pub fn single_domain_and_engine_basic<Domain: MemoryDomain>(
    relative_path: &str,
    engine_type: EngineType,
    engine_resource: Vec<ComputeResource>,
) {
    let (dispatcher, funcion_id) =
        setup_dispatcher::<Domain>(relative_path, vec![], vec![], engine_type, engine_resource);
    let result = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(dispatcher.queue_function(funcion_id, Vec::new(), Vec::new(), false));
    match result {
        Ok(_) => (),
        Err(err) => panic!("Failed with: {:?}", err),
    }
}

pub fn single_domain_and_engine_matmul<Domain: MemoryDomain>(
    domain_arg: MemoryResource,
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
    );
    const CONTEXT_SIZE: usize = 5 * 8;
    let mut in_context = Domain::init(domain_arg)
        .expect("Should be able to init domain")
        .acquire_context(CONTEXT_SIZE)
        .expect("Should get input matrix context");
    add_matmul_matrix(&mut in_context, 0, 0, 2, vec![1, 2, 3, 4]);

    let inputs = vec![(0, CompositionSet::from((0, vec![(Arc::new(in_context))])))];
    let outputs = vec![Some(0)];
    let result = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(dispatcher.queue_function(function_id, inputs, outputs, false));
    let out_sets = match result {
        Ok(context) => context,
        Err(err) => panic!("Failed with: {:?}", err),
    };
    assert_eq!(1, out_sets.len());
    let out_set = out_sets.get(&0).expect("Should have set 0");
    assert_eq!(1, out_set.context_list.len());
    let out_context = &out_set.context_list[0].0;
    assert_eq!(1, out_context.content.len());
    check_matrix(&out_context, 0, 0, 2, vec![5, 11, 11, 25])
}

pub fn composition_single_matmul<Domain: MemoryDomain>(
    domain_arg: MemoryResource,
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
    );
    const CONTEXT_SIZE: usize = 9 * 8;
    let mut in_context = Domain::init(domain_arg)
        .expect("Should be able to init domain")
        .acquire_context(CONTEXT_SIZE)
        .expect("Should get input matrix context");
    add_matmul_matrix(&mut in_context, 0, 0, 2, vec![1, 2, 3, 4]);

    let composition = Composition {
        dependencies: vec![FunctionDependencies {
            function: function_id,
            input_set_ids: vec![Some((0, ShardingMode::All))],
            output_set_ids: vec![Some(1)],
        }],
        output_map: BTreeMap::from([(1, 0)]),
    };
    let inputs = BTreeMap::from([(0, CompositionSet::from((0, vec![Arc::new(in_context)])))]);
    let result = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(dispatcher.queue_composition(composition, inputs, false));
    let mut out_contexts = match result {
        Ok(context) => context,
        Err(err) => panic!("Failed with: {:?}", err),
    };
    assert_eq!(1, out_contexts.len());
    let mut out_context_list = out_contexts.remove(&0).expect("Should have set 0");

    assert_eq!(1, out_context_list.context_list.len());
    let out_context = out_context_list.context_list.remove(0).0;
    assert_eq!(1, out_context.content.len());
    let out_mat_set = out_context.content[0].as_ref().expect("Should have set");
    assert_eq!(1, out_mat_set.buffers.len());
    check_matrix(&out_context, 0, 0, 2, vec![5, 11, 11, 25])
}

pub fn composition_parallel_matmul<Domain: MemoryDomain>(
    domain_arg: MemoryResource,
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
    );
    // need space for the input matrix of 2x2 uint64_t as well as a output matrix of the same size
    // and an uint64_t size that gives the column / row size (which is 2)
    const CONTEXT_SIZE: usize = 18 * 8;
    let mut in_context = Domain::init(domain_arg)
        .expect("Should be able to init domain")
        .acquire_context(CONTEXT_SIZE)
        .expect("Should get input matrix context");
    add_matmul_matrix(&mut in_context, 0, 0, 2, vec![1, 2, 3, 4]);
    add_matmul_matrix(&mut in_context, 0, 1, 2, vec![1, 2, 3, 4]);

    let composition = Composition {
        dependencies: vec![FunctionDependencies {
            function: function_id,
            input_set_ids: vec![Some((0, ShardingMode::Each))],
            output_set_ids: vec![Some(1)],
        }],
        output_map: BTreeMap::from([(1, 0)]),
    };
    let inputs = BTreeMap::from([(0, CompositionSet::from((0, vec![Arc::new(in_context)])))]);
    let result = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(dispatcher.queue_composition(composition, inputs, false));
    let mut out_vec = match result {
        Ok(v) => v,
        Err(err) => panic!("Failed with: {:?}", err),
    };
    assert_eq!(1, out_vec.len());
    let out_set = out_vec.remove(&0).expect("Should have set 0");
    assert_eq!(2, out_set.context_list.len());
    // check for each shard:
    for (matrix_context, _) in out_set.context_list {
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
    domain_arg: MemoryResource,
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
    );
    const CONTEXT_SIZE: usize = 9 * 8;
    let mut in_context = Domain::init(domain_arg)
        .expect("Should be able to init domain")
        .acquire_context(CONTEXT_SIZE)
        .expect("Should get input matrix context");
    add_matmul_matrix(&mut in_context, 0, 1, 2, vec![1, 2, 3, 4]);

    let composition = Composition {
        dependencies: vec![
            FunctionDependencies {
                function: function_id,
                input_set_ids: vec![Some((0, ShardingMode::All))],
                output_set_ids: vec![Some(1)],
            },
            FunctionDependencies {
                function: function_id,
                input_set_ids: vec![Some((1, ShardingMode::All))],
                output_set_ids: vec![Some(2)],
            },
        ],
        output_map: BTreeMap::from([(2, 0)]),
    };
    let inputs = BTreeMap::from([(0, CompositionSet::from((0, vec![Arc::new(in_context)])))]);
    let result = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(dispatcher.queue_composition(composition, inputs, false));
    let out_contexts = match result {
        Ok(context) => context,
        Err(err) => panic!("Failed with: {:?}", err),
    };
    assert_eq!(1, out_contexts.len());
    let out_composition_set = out_contexts.get(&0).expect("Should have set 0");
    assert_eq!(1, out_composition_set.context_list.len());
    let out_context = &out_composition_set.context_list[0].0;
    assert_eq!(1, out_context.content.len());
    check_matrix(&out_context, 0, 0, 2, vec![146, 330, 330, 746]);
}

pub fn composition_diamond_matmac<Domain: MemoryDomain>(
    domain_arg: MemoryResource,
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
    );
    const CONTEXT_SIZE: usize = 12 * 8;
    let mut in_context = Domain::init(domain_arg)
        .expect("Should be able to init domain")
        .acquire_context(CONTEXT_SIZE)
        .expect("Should get input matrix context");
    // A = [7]
    add_matmac_matrix(&mut in_context, 0, 1, 1, vec![7]);
    // B = [1,2,3,5]
    add_matmac_matrix(&mut in_context, 1, 1, 4, vec![1, 2, 3, 5]);
    // B^T
    add_matmac_matrix(&mut in_context, 2, 4, 1, vec![1, 2, 3, 5]);

    let composition = Composition {
        dependencies: vec![
            // C = A*B
            FunctionDependencies {
                function: function_id,
                input_set_ids: vec![
                    Some((0, ShardingMode::All)),
                    Some((1, ShardingMode::All)),
                    None,
                ],
                output_set_ids: vec![Some(3)],
            },
            // D = B^T*A
            FunctionDependencies {
                function: function_id,
                input_set_ids: vec![
                    Some((2, ShardingMode::All)),
                    Some((0, ShardingMode::All)),
                    None,
                ],
                output_set_ids: vec![Some(4)],
            },
            // E = B + C
            FunctionDependencies {
                function: function_id,
                input_set_ids: vec![
                    None,
                    Some((1, ShardingMode::All)),
                    Some((3, ShardingMode::All)),
                ],
                output_set_ids: vec![Some(5)],
            },
            // G = D * C
            FunctionDependencies {
                function: function_id,
                input_set_ids: vec![
                    Some((4, ShardingMode::All)),
                    Some((3, ShardingMode::All)),
                    None,
                ],
                output_set_ids: vec![Some(6)],
            },
            // Result = D*E + G
            FunctionDependencies {
                function: function_id,
                input_set_ids: vec![
                    Some((4, ShardingMode::All)),
                    Some((5, ShardingMode::All)),
                    Some((6, ShardingMode::All)),
                ],
                output_set_ids: vec![Some(7)],
            },
        ],
        output_map: BTreeMap::from([(7, 0)]),
    };
    let context_arc = Arc::new(in_context);
    let inputs = BTreeMap::from([
        (0, CompositionSet::from((0, vec![context_arc.clone()]))),
        (1, CompositionSet::from((1, vec![context_arc.clone()]))),
        (2, CompositionSet::from((2, vec![context_arc.clone()]))),
    ]);
    let result = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(dispatcher.queue_composition(composition, inputs, false));
    let out_contexts = match result {
        Ok(context) => context,
        Err(err) => panic!("Failed with: {:?}", err),
    };
    assert_eq!(1, out_contexts.len());
    let out_composition_set = out_contexts.get(&0).expect("Should have set 0");
    assert_eq!(1, out_composition_set.context_list.len());
    let out_context = &out_composition_set.context_list[0].0;
    assert_eq!(1, out_context.content.len());
    check_matrix(
        out_context,
        0,
        0,
        4,
        vec![
            105, 210, 315, 525, 210, 420, 630, 1050, 315, 630, 945, 1575, 525, 1050, 1575, 2625,
        ],
    );
}

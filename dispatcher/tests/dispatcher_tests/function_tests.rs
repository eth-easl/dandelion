use super::{check_matrix, run_composition, setup_dispatcher, zero_id};
use dandelion_commons::records::Recorder;
use log::debug;
use machine_interface::{
    function_driver::ComputeResource,
    machine_config::{DomainType, EngineType},
};
use memory::{
    context::{
        read_only::ReadOnlyContext, ContextDataItem, ContextDataSet, MemoryDomain, MemoryResource,
    },
    DataSet, Position,
};
use std::{iter, time::Instant};

/// Function declaration matching the single input/output test function registration.
const DECL_1_TO_1: &str = "function test_function(A) => (Out);";
/// Function declaration matching the three input/single output test function registration.
const DECL_3_TO_1: &str = "function test_function(A, B, C) => (Out);";

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

    let recorder = Recorder::new(zero_id(), Instant::now());
    let result = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(dispatcher.queue_function(0, function_id, Vec::new(), false, recorder));
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
    let mat_size = mat_len * core::mem::size_of::<u64>();
    in_context.content = vec![Some(ContextDataSet {
        total_size: mat_size,
        items: vec![ContextDataItem {
            ident: String::from(""),
            data: Position {
                offset: 0,
                size: mat_size,
            },
            key: 0,
        }],
    })];

    let inputs = DataSet::from_context(in_context);

    let recorder = Recorder::new(zero_id(), Instant::now());

    let result = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(dispatcher.queue_function(0, function_id, inputs, false, recorder));
    let out_sets = match result {
        Ok(context) => context,
        Err(err) => panic!("Failed with: {:?}", err),
    };
    assert_eq!(1, out_sets.len());
    assert_eq!(1, out_sets[0].len());
    let item = &out_sets[0][0];
    assert_eq!(0, item.key);
    check_matrix(item, 2, vec![5, 11, 11, 25])
}

pub fn composition_single_matmul<Domain: MemoryDomain>(
    memory_resource: (DomainType, MemoryResource),
    relative_path: &str,
    engine_type: EngineType,
    engine_resource: Vec<ComputeResource>,
) {
    let (dispatcher, _) = setup_dispatcher::<Domain>(
        relative_path,
        vec![(String::from("A"), None)],
        vec![String::from("Out")],
        engine_type,
        engine_resource,
        memory_resource,
    );

    // matrix with first eleemnt inidicating number of rows
    let mat_a = vec![2u64, 1, 2, 3, 4];
    let mat_len = mat_a.len();
    let mut in_context = ReadOnlyContext::new(mat_a.into_boxed_slice())
        .expect("Should be able to create read only context");
    let mat_size = mat_len * core::mem::size_of::<u64>();
    in_context.content = vec![Some(ContextDataSet {
        total_size: mat_size,
        items: vec![ContextDataItem {
            ident: String::from(""),
            data: Position {
                offset: 0,
                size: mat_size,
            },
            key: 0,
        }],
    })];

    let composition = format!(
        r#"
        {DECL_1_TO_1}
        composition SingleMatmul(InMat) => (OutMat) {{
            test_function(A = all InMat) => (OutMat = Out);
        }}
    "#
    );
    let inputs = DataSet::from_context(in_context);

    let out_sets = run_composition(&dispatcher, &composition, inputs);
    assert_eq!(1, out_sets.len());
    assert_eq!(1, out_sets[0].len());
    let item = &out_sets[0][0];
    assert_eq!(0, item.key);
    check_matrix(item, 2, vec![5, 11, 11, 25])
}

pub fn composition_optional<Domain: MemoryDomain>(
    memory_resource: (DomainType, MemoryResource),
    relative_path: &str,
    engine_type: EngineType,
    engine_resource: Vec<ComputeResource>,
) {
    let (dispatcher, _) = setup_dispatcher::<Domain>(
        relative_path,
        vec![(String::from("A"), None)],
        vec![String::from("Out1"), String::from("Out2")],
        engine_type,
        engine_resource,
        memory_resource,
    );
    let decl = "function test_function(A) => (Out1, Out2);";

    debug!("Starting test for case where the set is an input set, not optional and not present");
    let composition1 = format!(
        r#"
        {decl}
        composition Optional(In) => (Res) {{
            test_function(A = all In) => (Res = Out1);
        }}
    "#
    );
    let out_sets = run_composition(&dispatcher, &composition1, vec![DataSet::default()]);
    assert_eq!(1, out_sets.len());
    assert!(out_sets[0].is_empty());

    debug!("Starting test for case where the set is an input set, optional and not present");
    let composition2 = format!(
        r#"
        {decl}
        composition Optional(In) => (Res) {{
            test_function(A = optional all In) => (Res = Out1);
        }}
    "#
    );
    let out_sets = run_composition(&dispatcher, &composition2, vec![DataSet::default()]);
    assert_eq!(1, out_sets.len());
    // TODO: the basic function produces no output, so this cannot distinguish the function running
    // from it being skipped. Should use a function that produces output even without inputs
    // (probably needs to be implemented first).
    assert!(out_sets[0].is_empty());

    debug!(
        "Starting test for case where the set is a composition set, not optional and not present"
    );
    let composition5 = format!(
        r#"
        {decl}
        composition Optional() => (Res) {{
            test_function() => (Inter = Out2);
            test_function(A = all Inter) => (Res = Out1);
        }}
    "#
    );
    let out_sets = run_composition(&dispatcher, &composition5, vec![]);
    assert_eq!(1, out_sets.len());
    assert!(out_sets[0].is_empty());

    debug!("Starting test for case where the set is a composition set, optional and not present");
    let composition6 = format!(
        r#"
        {decl}
        composition Optional() => (Res) {{
            test_function() => (Inter = Out2);
            test_function(A = optional all Inter) => (Res = Out1);
        }}
    "#
    );
    let out_sets = run_composition(&dispatcher, &composition6, vec![]);
    assert_eq!(1, out_sets.len());
    // TODO: the basic function produces no output, so this cannot distinguish the function running
    // from it being skipped. Should use a function that produces output even without inputs
    // (probably needs to be implemented first).
    assert!(out_sets[0].is_empty());
}

pub fn composition_parallel_matmul<Domain: MemoryDomain>(
    memory_resource: (DomainType, MemoryResource),
    relative_path: &str,
    engine_type: EngineType,
    engine_resource: Vec<ComputeResource>,
) {
    let (dispatcher, _) = setup_dispatcher::<Domain>(
        relative_path,
        vec![(String::from("A"), None)],
        vec![String::from("Out")],
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
    let mat_a_size = mat_a.len() * core::mem::size_of::<u64>();
    let mat_b_size = mat_b.len() * core::mem::size_of::<u64>();
    in_context.content = vec![Some(ContextDataSet {
        items: vec![
            ContextDataItem {
                ident: String::from(""),
                data: Position {
                    offset: 0,
                    size: mat_a_size,
                },
                key: 0,
            },
            ContextDataItem {
                ident: String::from(""),
                data: Position {
                    offset: mat_a_size,
                    size: mat_b_size,
                },
                key: 1,
            },
        ],
        total_size: mat_a_size + mat_b_size,
    })];

    let composition = format!(
        r#"
        {DECL_1_TO_1}
        composition ParallelMatmul(InMats) => (OutMats) {{
            test_function(A = each InMats) => (OutMats = Out);
        }}
    "#
    );
    let inputs = DataSet::from_context(in_context);

    let out_vec = run_composition(&dispatcher, &composition, inputs);
    assert_eq!(1, out_vec.len());
    let out_set = &out_vec[0];

    // check for each shard:
    assert_eq!(2, out_set.len());
    for item in out_set.iter() {
        assert!(item.key == 1 || item.key == 0);
        check_matrix(item, 2, vec![5, 11, 11, 25]);
    }
}

pub fn composition_chain_matmul<Domain: MemoryDomain>(
    memory_resource: (DomainType, MemoryResource),
    relative_path: &str,
    engine_type: EngineType,
    engine_resource: Vec<ComputeResource>,
) {
    let (dispatcher, _) = setup_dispatcher::<Domain>(
        relative_path,
        vec![(String::from("A"), None)],
        vec![String::from("Out")],
        engine_type,
        engine_resource,
        memory_resource,
    );

    // matrix with the first number indicating the number of rows
    let data = vec![2u64, 1, 2, 3, 4];
    let data_len = data.len();
    let mut in_context = ReadOnlyContext::new(data.into_boxed_slice())
        .expect("Should be able to create read only context");
    let data_size = data_len * core::mem::size_of::<u64>();
    in_context.content = vec![Some(ContextDataSet {
        items: vec![ContextDataItem {
            ident: String::from(""),
            data: Position {
                offset: 0,
                size: data_size,
            },
            key: 0,
        }],
        total_size: data_size,
    })];

    let composition = format!(
        r#"
        {DECL_1_TO_1}
        composition ChainMatmul(InMat) => (OutMat) {{
            test_function(A = all InMat) => (Inter = Out);
            test_function(A = all Inter) => (OutMat = Out);
        }}
    "#
    );
    let inputs = DataSet::from_context(in_context);

    let out_sets = run_composition(&dispatcher, &composition, inputs);
    assert_eq!(1, out_sets.len());
    assert_eq!(1, out_sets[0].len());
    let item = &out_sets[0][0];
    assert_eq!(0, item.key);
    check_matrix(item, 2, vec![146, 330, 330, 746]);
}

pub fn composition_diamond_matmac<Domain: MemoryDomain>(
    memory_resource: (DomainType, MemoryResource),
    relative_path: &str,
    engine_type: EngineType,
    engine_resource: Vec<ComputeResource>,
) {
    let (dispatcher, _) = self::setup_dispatcher::<Domain>(
        relative_path,
        vec![
            (String::from("A"), None),
            (String::from("B"), None),
            (String::from("C"), None),
        ],
        vec![String::from("Out")],
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
    let mat_a_size = mat_a.len() * core::mem::size_of::<u64>();
    let mat_b_size = mat_b.len() * core::mem::size_of::<u64>();

    in_context.content = vec![
        Some(ContextDataSet {
            items: vec![ContextDataItem {
                ident: String::from("A"),
                data: Position {
                    offset: 0,
                    size: mat_a_size,
                },
                key: 0,
            }],
            total_size: mat_a_size,
        }),
        Some(ContextDataSet {
            items: vec![ContextDataItem {
                ident: String::from("B"),
                data: Position {
                    offset: mat_a_size,
                    size: mat_b_size,
                },
                key: 0,
            }],
            total_size: mat_b_size,
        }),
        Some(ContextDataSet {
            items: vec![ContextDataItem {
                ident: String::from("BT"),
                data: Position {
                    offset: mat_a_size + mat_b_size,
                    size: mat_b_size,
                },
                key: 0,
            }],
            total_size: mat_b_size,
        }),
    ];

    let composition = format!(
        r#"
        {DECL_3_TO_1}
        composition DiamondMatmac(MatA, MatB, MatBT) => (Result) {{
            // C = A*B
            test_function(A = all MatA, B = all MatB) => (MatC = Out);
            // D = B^T*A
            test_function(A = all MatBT, B = all MatA) => (MatD = Out);
            // E = B + C
            test_function(B = all MatB, C = all MatC) => (MatE = Out);
            // G = D * C
            test_function(A = all MatD, B = all MatC) => (MatG = Out);
            // Result = D*E + G
            test_function(A = all MatD, B = all MatE, C = all MatG) => (Result = Out);
        }}
    "#
    );
    let inputs = DataSet::from_context(in_context);

    let out_sets = run_composition(&dispatcher, &composition, inputs);
    assert_eq!(1, out_sets.len());
    assert_eq!(1, out_sets[0].len());
    let item = &out_sets[0][0];
    assert_eq!(0, item.key);
    check_matrix(
        item,
        4,
        vec![
            105, 210, 315, 525, 210, 420, 630, 1050, 315, 630, 945, 1575, 525, 1050, 1575, 2625,
        ],
    );
}

pub fn composition_chain_large_matmac<Domain: MemoryDomain>(
    memory_resource: (DomainType, MemoryResource),
    relative_path: &str,
    engine_type: EngineType,
    engine_resource: Vec<ComputeResource>,
) {
    let (dispatcher, _) = self::setup_dispatcher::<Domain>(
        relative_path,
        vec![
            (String::from("A"), None),
            (String::from("B"), None),
            (String::from("C"), None),
        ],
        vec![String::from("Out")],
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
    let mat_a_size = mat_a.len() * core::mem::size_of::<u64>();
    let mat_b_size = mat_b.len() * core::mem::size_of::<u64>();

    in_context.content = vec![
        Some(ContextDataSet {
            items: vec![ContextDataItem {
                ident: String::from("a mat"),
                data: Position {
                    offset: 0,
                    size: mat_a_size,
                },
                key: 0,
            }],
            total_size: mat_a_size,
        }),
        Some(ContextDataSet {
            items: vec![ContextDataItem {
                ident: String::from("b mat"),
                data: Position {
                    offset: mat_a_size,
                    size: mat_b_size,
                },
                key: 0,
            }],
            total_size: mat_b_size,
        }),
    ];

    // C+C, should be a matrix filled with 2s
    // also test that we can use the same set as mutliple input set for one function
    let mut applications =
        String::from("test_function(B = all MatB, C = all MatB) => (Twos = Out);\n");
    let chain_length = 10;
    for chain_stage in 0..chain_length {
        let input_set = if chain_stage == 0 {
            String::from("MatA")
        } else {
            format!("Stage{}", chain_stage - 1)
        };
        let output_set = if chain_stage == chain_length - 1 {
            String::from("Result")
        } else {
            format!("Stage{}", chain_stage)
        };
        applications.push_str(&format!(
            "test_function(B = all Twos, C = all {input_set}) => ({output_set} = Out);\n"
        ));
    }
    let composition = format!(
        r#"
        {DECL_3_TO_1}
        composition ChainLargeMatmac(MatA, MatB) => (Result) {{
            {applications}
        }}
    "#
    );

    let inputs = DataSet::from_context(in_context);
    let out_sets = run_composition(&dispatcher, &composition, inputs);
    assert_eq!(1, out_sets.len());
    assert_eq!(1, out_sets[0].len());
    let out_item = &out_sets[0][0];
    let expected =
        (1 + 2 * chain_length as u64..matrix_size + 1 + 2 * chain_length as u64).collect();
    assert_eq!(0, out_item.key);
    check_matrix(out_item, matrix_width, expected);
}

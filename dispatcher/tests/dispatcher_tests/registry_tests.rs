use crate::dispatcher_tests::{check_matrix, setup_dispatcher};
use dandelion_commons::records::Recorder;
use dispatcher::{
    composition::CompositionSet, dispatcher::Dispatcher, function_registry::Metadata,
};
use futures::lock::Mutex;
use machine_interface::{
    function_driver::ComputeResource,
    machine_config::{DomainType, EngineType},
    memory_domain::{read_only::ReadOnlyContext, Context, MemoryDomain, MemoryResource},
    DataItem, DataSet, Position,
};
use std::{collections::BTreeMap, sync::Arc, time::Instant};

// using 0x802_0000 as that is what the WASM test binaries expect
// TODO fix once the update has been merged allowing for 800_0000
const DEFAULT_CONTEXT_SIZE: usize = 0x802_0000; // 128MiB

fn create_context(matrix: Box<[u64]>) -> Context {
    let mat_len = matrix.len();
    let mut fixed =
        ReadOnlyContext::new(matrix).expect("Should be able to make context from boxed array");
    fixed.content.push(Some(DataSet {
        ident: String::from(""),
        buffers: vec![DataItem {
            ident: String::from(""),
            data: Position {
                offset: 0,
                size: mat_len * core::mem::size_of::<u64>(),
            },
            key: 0,
        }],
    }));
    return fixed;
}

/// tests with a single set fixed in the metadata
/// check once for the ouput being correct in absence of an input set for the fixed one,
/// and once for correct behavior if there is a set provided for the fixed one
pub fn single_input_fixed<Domain: MemoryDomain>(
    memory_resource: (DomainType, MemoryResource),
    relative_path: &str,
    engine_type: EngineType,
    engine_resource: Vec<ComputeResource>,
) {
    let matrix_a = Box::new([1u64, 2u64]);
    let matrix_b = Box::new([1u64, 3u64]);
    let matrix_c = Box::new([1u64, 5u64]);
    let fault_matrix = Box::new([1u64, 100u64]);
    let expected = [11, 11, 17];
    // set up input sets
    let mat_con_a = Arc::new(create_context(matrix_a));
    let mat_con_b = Arc::new(create_context(matrix_b));
    let mat_con_c = Arc::new(create_context(matrix_c));
    let mat_fault = Arc::new(create_context(fault_matrix));
    let in_set_names = vec![
        (String::from(""), None),
        (String::from(""), None),
        (String::from(""), None),
    ];
    let out_set_names = vec![String::from("")];
    let (dispatcher, _) = setup_dispatcher::<Domain>(
        relative_path,
        in_set_names.clone(),
        out_set_names.clone(),
        engine_type,
        engine_resource,
        memory_resource,
    );
    let mut absolute_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    absolute_path.pop();
    absolute_path.push("machine_interface/tests/data");
    absolute_path.push(relative_path);
    for i in 0..=2 {
        let mut local_names = in_set_names.clone();
        local_names[i].1 = Some(CompositionSet::from((0, vec![mat_con_a.clone()])));
        // alter metadata for the functions
        let function_id = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(
                dispatcher.insert_func(
                    format!("local_name_{}", i),
                    engine_type,
                    DEFAULT_CONTEXT_SIZE,
                    absolute_path
                        .to_str()
                        .expect("Path should be valid string")
                        .to_string(),
                    Metadata {
                        input_sets: Arc::new(local_names),
                        output_sets: Arc::new(out_set_names.clone()),
                    },
                ),
            )
            .expect("should be able to update function");

        // prepare inputs
        let input_sets = (0..=2)
            .into_iter()
            .filter(|index| *index != i)
            .collect::<Vec<_>>();
        let mut inputs = vec![None; 3];
        inputs[input_sets[0]] = Some(CompositionSet::from((0, vec![mat_con_b.clone()])));
        inputs[input_sets[1]] = Some(CompositionSet::from((0, vec![mat_con_c.clone()])));
        let mut overwrite_inputs = inputs.clone();
        overwrite_inputs[i] = Some(CompositionSet::from((0, vec![mat_fault.clone()])));

        let mut recorder = Recorder::new(0, Instant::now());
        let result = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(dispatcher.queue_function(function_id, inputs, false, recorder));
        recorder = Recorder::new(0, Instant::now());
        let overwrite_result = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(dispatcher.queue_function(function_id, overwrite_inputs, false, recorder));
        let out_sets = match result {
            Ok(composition_sets) => composition_sets,
            Err(err) => panic!("Non overwrite failed with: {:?}", err),
        };
        let overwrite_sets = match overwrite_result {
            Ok(compostion_set) => compostion_set,
            Err(err) => panic!("Overwrite input failed with: {:?}", err),
        };
        assert_eq!(1, out_sets.len());
        assert_eq!(1, overwrite_sets.len());
        let mut result_iter = out_sets[0].as_ref().unwrap().into_iter();
        let (_, _, result_set) = result_iter.next().unwrap();
        assert!(result_iter.next().is_none());
        let mut overwrite_iter = overwrite_sets[0].as_ref().unwrap().into_iter();
        let (_, _, overwrite_set) = overwrite_iter.next().unwrap();
        assert!(overwrite_iter.next().is_none());
        check_matrix(&result_set, 0, 0, 1, vec![expected[i]]);
        check_matrix(&overwrite_set, 0, 0, 1, vec![expected[i]]);
    }
}

/// check functionallity with multiple fixed inputs with and without input provided for the fixed sets
pub fn multiple_input_fixed<Domain: MemoryDomain>(
    memory_resource: (DomainType, MemoryResource),
    relative_path: &str,
    engine_type: EngineType,
    engine_resource: Vec<ComputeResource>,
) {
    let matrix_a = Box::new([1u64, 2u64]);
    let matrix_b = Box::new([1u64, 3u64]);
    let matrix_c = Box::new([1u64, 5u64]);
    let fault_matrix = Box::new([1u64, 100u64]);
    let expected = [11, 11, 17];
    // set up input sets
    let mat_con_a = Arc::new(create_context(matrix_a));
    let mat_con_b = Arc::new(create_context(matrix_b));
    let mat_con_c = Arc::new(create_context(matrix_c));
    let mat_fault = Arc::new(create_context(fault_matrix));
    let in_set_names = vec![
        (String::from(""), None),
        (String::from(""), None),
        (String::from(""), None),
    ];
    let out_set_names = vec![String::from("")];
    let (dispatcher, _) = setup_dispatcher::<Domain>(
        relative_path,
        in_set_names.clone(),
        out_set_names.clone(),
        engine_type,
        engine_resource,
        memory_resource,
    );
    let mut absolute_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    absolute_path.pop();
    absolute_path.push("machine_interface/tests/data");
    absolute_path.push(relative_path);
    for i in 0..=2 {
        let fixed_sets = (0..=2)
            .into_iter()
            .filter(|index| *index != i)
            .collect::<Vec<_>>();
        let mut local_names = in_set_names.clone();
        local_names[fixed_sets[0]].1 = Some(CompositionSet::from((0, vec![mat_con_b.clone()])));
        local_names[fixed_sets[1]].1 = Some(CompositionSet::from((0, vec![mat_con_c.clone()])));
        // alter metadata for the functions
        let function_id = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(
                dispatcher.insert_func(
                    format!("insert_function_{}", i),
                    engine_type,
                    DEFAULT_CONTEXT_SIZE,
                    absolute_path
                        .to_str()
                        .expect("Path should be valid string")
                        .to_string(),
                    Metadata {
                        input_sets: Arc::new(local_names),
                        output_sets: Arc::new(out_set_names.clone()),
                    },
                ),
            )
            .expect("should be able to update function");

        // prepare inputs
        let mut inputs = vec![None; 3];
        inputs[i] = Some(CompositionSet::from((0, vec![mat_con_a.clone()])));
        let mut overwrite_inputs = inputs.clone();
        overwrite_inputs[fixed_sets[0]] = Some(CompositionSet::from((0, vec![mat_fault.clone()])));
        overwrite_inputs[fixed_sets[1]] = Some(CompositionSet::from((0, vec![mat_fault.clone()])));

        let mut recorder = Recorder::new(0, Instant::now());
        let result = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(dispatcher.queue_function(function_id, inputs, false, recorder));
        recorder = Recorder::new(0, Instant::now());
        let overwrite_result = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(dispatcher.queue_function(function_id, overwrite_inputs, false, recorder));
        let out_sets = match result {
            Ok(composition_sets) => composition_sets,
            Err(err) => panic!("Non overwrite failed with: {:?}", err),
        };
        let overwrite_sets = match overwrite_result {
            Ok(compostion_set) => compostion_set,
            Err(err) => panic!("Overwrite input failed with: {:?}", err),
        };
        assert_eq!(1, out_sets.len());
        assert_eq!(1, overwrite_sets.len());
        let mut result_iter = out_sets[0].as_ref().unwrap().into_iter();
        let (_, _, result_set) = result_iter.next().unwrap();
        assert!(result_iter.next().is_none());
        let mut overwrite_iter = overwrite_sets[0].as_ref().unwrap().into_iter();
        let (_, _, overwrite_set) = overwrite_iter.next().unwrap();
        assert!(overwrite_iter.next().is_none());
        check_matrix(&result_set, 0, 0, 1, vec![expected[i]]);
        check_matrix(&overwrite_set, 0, 0, 1, vec![expected[i]]);
    }
}

#[test_log::test]
#[cfg(any(feature = "reqwest_io"))]
fn test_insert_composition_with_http_func() {
    let memory_resources = BTreeMap::from([(
        DomainType::Mmap,
        MemoryResource::Anonymous { size: (1 << 30) },
    )]);
    let dispatcher = Dispatcher::init(
        dispatcher::resource_pool::ResourcePool {
            engine_pool: Mutex::new(BTreeMap::new()),
        },
        memory_resources,
    )
    .unwrap();
    let composition_string = r#"
        function HTTP (request, headers, body) => (status, headers, body);
        composition Composition (comp_request, req_body) => (comp_status, resp_body) {
            HTTP (request = all comp_request, body = all req_body) => (resp_body = body, comp_status = status);
        }
    "#;
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(dispatcher.insert_compositions(String::from(composition_string)))
        .unwrap();
}

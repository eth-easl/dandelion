use crate::dispatcher_tests::{check_matrix, setup_dispatcher};
use dandelion_commons::records::Recorder;
use machine_interface::{
    composition::CompositionSet,
    function_driver::{ComputeResource, Metadata},
    machine_config::{DomainType, EngineType},
    memory_domain::{read_only::ReadOnlyContext, Context, MemoryDomain, MemoryResource},
    DataItem, DataSet, Position,
};
use std::{sync::Arc, time::Instant, vec};

const DEFAULT_CONTEXT_SIZE: usize = 0x800_0000; // 128MiB

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
    // set up input sets
    let mat_a_composition_set =
        CompositionSet::from_context(create_context(Box::new([1u64, 2u64])))[0]
            .take()
            .unwrap();
    let mat_b_composition_set =
        CompositionSet::from_context(create_context(Box::new([1u64, 3u64])))[0]
            .take()
            .unwrap();
    let mat_c_composition_set =
        CompositionSet::from_context(create_context(Box::new([1u64, 5u64])))[0]
            .take()
            .unwrap();
    let mat_fault_composition_set =
        CompositionSet::from_context(create_context(Box::new([1u64, 100u64])))[0]
            .take()
            .unwrap();
    let expected = [11, 11, 17];
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
        local_names[i].1 = Some(mat_a_composition_set.clone());
        // alter metadata for the functions
        let function_id = Arc::new(format!("local_name_{}", i));

        dispatcher
            .insert_function(
                (*function_id).clone(),
                engine_type,
                DEFAULT_CONTEXT_SIZE,
                absolute_path
                    .to_str()
                    .expect("Path should be valid string")
                    .to_string(),
                Metadata {
                    input_sets: local_names
                        .into_iter()
                        .map(|(name, set)| (name, set.map(|s| s.into_local())))
                        .collect(),
                    output_sets: out_set_names.clone(),
                    min_set_bytes: vec![],
                },
            )
            .expect("should be able to update function");

        // prepare inputs
        let input_sets = (0..=2)
            .into_iter()
            .filter(|index| *index != i)
            .collect::<Vec<_>>();
        let mut inputs = vec![None; 3];
        inputs[input_sets[0]] = Some(mat_b_composition_set.clone());
        inputs[input_sets[1]] = Some(mat_c_composition_set.clone());
        let mut overwrite_inputs = inputs.clone();
        overwrite_inputs[i] = Some(mat_fault_composition_set.clone());

        let mut recorder = Recorder::new(function_id.clone(), Instant::now());
        let result = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(dispatcher.queue_function(0, function_id.clone(), inputs, false, recorder));
        recorder = Recorder::new(function_id.clone(), Instant::now());
        let overwrite_result = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(dispatcher.queue_function(
                0,
                function_id.clone(),
                overwrite_inputs,
                false,
                recorder,
            ));
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
        let (result_item, result_set) = result_iter.next().unwrap();
        assert!(result_iter.next().is_none());
        let mut overwrite_iter = overwrite_sets[0].as_ref().unwrap().into_iter();
        let (overwrite_item, overwrite_set) = overwrite_iter.next().unwrap();
        assert!(overwrite_iter.next().is_none());
        assert_eq!(0, result_item.key);
        assert_eq!(0, overwrite_item.key);
        check_matrix(&result_set, result_item, 1, vec![expected[i]]);
        check_matrix(&overwrite_set, overwrite_item, 1, vec![expected[i]]);
    }
}

/// check functionallity with multiple fixed inputs with and without input provided for the fixed sets
pub fn multiple_input_fixed<Domain: MemoryDomain>(
    memory_resource: (DomainType, MemoryResource),
    relative_path: &str,
    engine_type: EngineType,
    engine_resource: Vec<ComputeResource>,
) {
    // set up input sets
    let mat_a_composition_set =
        CompositionSet::from_context(create_context(Box::new([1u64, 2u64])))[0]
            .take()
            .unwrap();
    let mat_b_composition_set =
        CompositionSet::from_context(create_context(Box::new([1u64, 3u64])))[0]
            .take()
            .unwrap();
    let mat_c_composition_set =
        CompositionSet::from_context(create_context(Box::new([1u64, 5u64])))[0]
            .take()
            .unwrap();
    let mat_fault_composition_set =
        CompositionSet::from_context(create_context(Box::new([1u64, 100u64])))[0]
            .take()
            .unwrap();
    let expected = [11, 11, 17];

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
        local_names[fixed_sets[0]].1 = Some(mat_b_composition_set.clone());
        local_names[fixed_sets[1]].1 = Some(mat_c_composition_set.clone());
        // alter metadata for the functions
        let function_id = Arc::new(format!("insert_function_{}", i));

        dispatcher
            .insert_function(
                (*function_id).clone(),
                engine_type,
                DEFAULT_CONTEXT_SIZE,
                absolute_path
                    .to_str()
                    .expect("Path should be valid string")
                    .to_string(),
                Metadata {
                    input_sets: local_names
                        .into_iter()
                        .map(|(name, set_op)| (name, set_op.map(|set| set.into_local())))
                        .collect(),
                    output_sets: out_set_names.clone(),
                    min_set_bytes: vec![],
                },
            )
            .expect("should be able to update function");

        // prepare inputs
        let mut inputs = vec![None; 3];
        inputs[i] = Some(mat_a_composition_set.clone());
        let mut overwrite_inputs = inputs.clone();
        overwrite_inputs[fixed_sets[0]] = Some(mat_fault_composition_set.clone());
        overwrite_inputs[fixed_sets[1]] = Some(mat_fault_composition_set.clone());

        let mut recorder = Recorder::new(function_id.clone(), Instant::now());
        let result = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(dispatcher.queue_function(0, function_id.clone(), inputs, false, recorder));
        recorder = Recorder::new(function_id.clone(), Instant::now());
        let overwrite_result = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(dispatcher.queue_function(
                0,
                function_id.clone(),
                overwrite_inputs,
                false,
                recorder,
            ));
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
        let (result_item, result_set) = result_iter.next().unwrap();
        assert!(result_iter.next().is_none());
        let mut overwrite_iter = overwrite_sets[0].as_ref().unwrap().into_iter();
        let (overwrite_item, overwrite_set) = overwrite_iter.next().unwrap();
        assert!(overwrite_iter.next().is_none());
        assert_eq!(0, result_item.key);
        assert_eq!(0, overwrite_item.key);
        check_matrix(&result_set, result_item, 1, vec![expected[i]]);
        check_matrix(&overwrite_set, overwrite_item, 1, vec![expected[i]]);
    }
}

#[test_log::test]
fn test_insert_composition_with_http_func() {
    use std::collections::BTreeMap;

    use machine_interface::composition::AnyShardingMode;
    let work_queue = dispatcher::queue::WorkQueue::init();
    let dispatcher = dispatcher::dispatcher::Dispatcher::init(
        dispatcher::resource_pool::ResourcePool {
            engine_pool: futures::lock::Mutex::new(BTreeMap::new()),
        },
        BTreeMap::new(),
        work_queue,
        AnyShardingMode::MaxSharding,
    )
    .unwrap();
    let composition_string = r#"
        function HTTP (requests) => (headers, bodies);
        composition Composition (comp_requests) => (comp_headers, comp_bodies) {
            HTTP (requests = all comp_requests) => (comp_headers = headers, comp_bodies = bodies);
        }
    "#;
    dispatcher
        .insert_compositions(String::from(composition_string))
        .unwrap();
}

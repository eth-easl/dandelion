use dandelion_commons::{records::Recorder, FunctionId, Priority};
use dispatcher::dispatcher::Dispatcher;
use machine_interface::{
    composition::CompositionSet,
    function_driver::{ComputeResource, Metadata},
    machine_config::{DomainType, EngineType},
    memory_domain::{read_only::ReadOnlyContext, MemoryResource},
    DataItem, DataSet, Position,
};
use std::{collections::BTreeMap, sync::Arc, time::Instant};

use super::check_matrix;
use dispatcher::resource_pool::ResourcePool;

const DEFAULT_CONTEXT_SIZE: usize = 0x800_0000; // 128MiB

#[inline]
fn zero_id() -> FunctionId {
    Arc::new(0.to_string())
}

/// Build a CompositionSet containing a square matrix of the given size, filled with 1s.
fn make_matrix_input(size: u64) -> Vec<Option<CompositionSet>> {
    let n = size as usize;
    let mut data = Vec::with_capacity(1 + n * n);
    data.push(size);
    for _ in 0..(n * n) {
        data.push(1u64);
    }
    let data_len = data.len();
    let mut ctx = ReadOnlyContext::new(data.into()).expect("Should create context");
    ctx.content = vec![Some(DataSet {
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
    vec![Some(CompositionSet::from((0, vec![Arc::new(ctx)])))]
}

/// Set up a dispatcher with a single KVM engine thread.
fn setup_single_core_dispatcher() -> (Dispatcher, FunctionId) {
    let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.pop();
    path.push("machine_interface/tests/data/test_elf_kvm_x86_64_matmul");
    let path_string = path.to_str().expect("Path should be string").to_string();

    let metadata = Metadata {
        input_sets: vec![(String::from(""), None)],
        output_sets: vec![String::from("")],
    };

    let mut pool_map = BTreeMap::new();
    pool_map.insert(EngineType::Kvm, vec![ComputeResource::CPU(1)]);
    let resource_pool = ResourcePool {
        engine_pool: futures::lock::Mutex::new(pool_map),
    };

    let memory_resources = vec![(
        DomainType::Kvm,
        machine_interface::memory_domain::test_resource::get_resource(
            MemoryResource::Anonymous { size: 1 << 30 },
        ),
    )]
    .into_iter()
    .collect();

    let dispatcher =
        Dispatcher::init(resource_pool, memory_resources).expect("Should init dispatcher");
    let function_id = Arc::new(String::from("matmul"));
    dispatcher
        .insert_function(
            (*function_id).clone(),
            EngineType::Kvm,
            DEFAULT_CONTEXT_SIZE,
            path_string,
            metadata,
        )
        .expect("Should insert function");

    (dispatcher, function_id)
}

/// Test that a high-priority request preempts a running best-effort request,
/// completes first, and the best-effort request also completes after resume.
#[cfg(all(feature = "kvm", target_arch = "x86_64"))]
#[test_log::test]
fn test_preemption_and_resume() {
    let (dispatcher, function_id) = setup_single_core_dispatcher();

    // Spawn best-effort (large matrix) on a separate OS thread with its own runtime.
    // This lets it start running on the engine before we submit the HP request.
    let d_be = Arc::new(dispatcher);
    let d_hp = d_be.clone();
    let f_be = function_id.clone();
    let f_hp = function_id.clone();

    let be_finish_time: Arc<std::sync::Mutex<Option<Instant>>> =
        Arc::new(std::sync::Mutex::new(None));
    let be_finish_clone = be_finish_time.clone();

    let be_thread = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(async {
            let inputs = make_matrix_input(256);
            let recorder = Recorder::new(zero_id(), Instant::now());
            let result = d_be
                .queue_function(f_be, inputs, false, recorder, Priority::BestEffort)
                .await;
            *be_finish_clone.lock().unwrap() = Some(Instant::now());
            result
        })
    });

    // Wait for the BE task to start executing on the engine thread
    std::thread::sleep(std::time::Duration::from_millis(190));

    // Submit high-priority (small matrix) on this thread
    let hp_result = {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(async {
            let inputs = make_matrix_input(2);
            let recorder = Recorder::new(zero_id(), Instant::now());
            d_hp.queue_function(f_hp, inputs, false, recorder, Priority::High)
                .await
        })
    };
    let hp_finish_time = Instant::now();

    // Wait for the best-effort task to finish
    let be_result = be_thread.join().expect("BE thread panicked");

    // Both should succeed
    let hp_sets = hp_result.expect("HP function should succeed");
    let be_sets = be_result.expect("BE function should succeed after resume");

    // Verify HP output: 2x2 ones * ones^T = [[2, 2], [2, 2]]
    assert_eq!(1, hp_sets.len());
    let hp_set = hp_sets[0].as_ref().expect("Should have HP output set");
    let (_, _, hp_ctx) = hp_set.into_iter().next().unwrap();
    check_matrix(&hp_ctx, 0, 0, 2, vec![2, 2, 2, 2]);

    // Verify BE output: 128x128 ones * ones^T = each element is 128
    assert_eq!(1, be_sets.len());
    let be_set = be_sets[0].as_ref().expect("Should have BE output set");
    let (_, _, be_ctx) = be_set.into_iter().next().unwrap();
    let expected: Vec<u64> = vec![256u64; 256 * 256];
    check_matrix(&be_ctx, 0, 0, 256, expected);

    // Check that HP finished before BE
    let be_time = be_finish_time.lock().unwrap().expect("BE should have finished");
    println!("HP finished at: {:?}, BE finished at: {:?}", hp_finish_time, be_time);
    assert!(
        hp_finish_time < be_time,
        "High-priority should finish before best-effort"
    );
}

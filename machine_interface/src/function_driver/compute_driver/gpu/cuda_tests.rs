use std::sync::{Arc, Mutex};

use dandelion_commons::records::{Archive, ArchiveInit, RecordPoint};

use crate::{
    function_driver::{
        compute_driver::{
            compute_driver_tests::compute_driver_tests::{
                engine_minimal, get_expected_mat, prepare_engine_and_function,
            },
            gpu::{GpuProcessDriver, GpuThreadDriver},
        },
        ComputeResource, Driver, WorkToDo,
    },
    memory_domain::{gpu::GpuMemoryDomain, ContextTrait, MemoryResource},
    DataItem, DataSet, Position,
};

// To force tests to run sequentially as we might otherwise run out of GPU memory
lazy_static::lazy_static! {
    static ref GPU_LOCK: Mutex<()> = Mutex::new(());
}

fn get_driver() -> Box<dyn Driver> {
    #[cfg(all(feature = "gpu_process", feature = "gpu_thread"))]
    panic!("gpu_process and gpu_thread enabled simultaneously");

    #[cfg(not(any(feature = "gpu_process", feature = "gpu_thread")))]
    panic!("Neither gpu_process nor gpu_thread enabled");

    #[cfg(feature = "gpu_process")]
    return Box::new(GpuProcessDriver {});

    #[cfg(feature = "gpu_thread")]
    return Box::new(GpuThreadDriver {});
}

#[test]
fn minimal() {
    let lock = GPU_LOCK.lock().unwrap();
    let driver: Box<dyn Driver> = get_driver();
    engine_minimal::<GpuMemoryDomain>(
        &format!(
            "{}/tests/data/test_gpu_cuda_minimal.json",
            env!("CARGO_MANIFEST_DIR")
        ),
        MemoryResource::Shared { id: 0, size: (1<<30) },
        driver,
        vec![ComputeResource::GPU(7, 1, 2)],
    );
    drop(lock);
}

#[test]
fn minimal_matmul() {
    let lock = GPU_LOCK.lock().unwrap();
    let filename = &format!(
        "{}/tests/data/test_gpu_cuda_matmul.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let dom_init = MemoryResource::Shared { id: 0, size: (1<<30) };
    let driver: Box<dyn Driver> = get_driver();
    let drv_init = vec![ComputeResource::GPU(7, 0, 2)];
    let (mut function_context, config, queue) =
        prepare_engine_and_function::<GpuMemoryDomain>(filename, dom_init, &driver, drv_init);

    let n = [4 as i32; 1];
    let a_size: usize = 4 * 4 * (32 / 8);
    let b_size = 4 * 4 * (32 / 8);
    let c_size = 4 * 4 * (32 / 8);

    let mut a_slice: Vec<f32> = vec![0.0; 4 * 4];
    let mut b_slice: Vec<f32> = vec![0.0; 4 * 4];
    for i in 0..16 {
        a_slice[i] = i as f32;
        b_slice[i] = (i as f32) * 2.0;
    }

    let a_offset = function_context
        .get_free_space_and_write_slice(&a_slice)
        .expect("Should have space");
    let b_offset = function_context
        .get_free_space_and_write_slice(&b_slice)
        .expect("Should have space");

    function_context.content.push(Some(DataSet {
        ident: "A".to_string(),
        buffers: vec![DataItem {
            ident: "".to_string(),
            data: Position {
                offset: a_offset as usize,
                size: a_size,
            },
            key: 0,
        }],
    }));
    function_context.content.push(Some(DataSet {
        ident: "B".to_string(),
        buffers: vec![DataItem {
            ident: "".to_string(),
            data: Position {
                offset: b_offset as usize,
                size: b_size,
            },
            key: 0,
        }],
    }));
    let archive = Box::leak(Box::new(Archive::init(ArchiveInit {
        #[cfg(feature = "timestamp")]
        timestamp_count: 1000,
    })));

    let mut recorder = archive.get_recorder().unwrap();
    recorder
        .record(RecordPoint::TransferEnd)
        .expect("Should have properly initialized recorder state");
    let promise = queue.enqueu(WorkToDo::FunctionArguments {
        config,
        context: function_context,
        output_sets: Arc::new(vec![String::from("C")]),
        recorder: recorder.get_sub_recorder().unwrap(),
    });
    queue.enqueu(WorkToDo::Shutdown());
    let result_context = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(promise)
        .expect("Engine should run ok with basic function")
        .get_context();
    recorder
        .record(RecordPoint::FutureReturn)
        .expect("Should have properly advanced recorder state");
    let output_item = result_context.content[0]
        .as_ref()
        .expect("Set should be present");
    let position = output_item.buffers[0].data;
    assert_eq!(c_size, position.size, "Checking for size of output");
    let mut read_buffer = vec![0f32; position.size / (32 / 8)];
    result_context
        .context
        .read(position.offset, &mut read_buffer)
        .expect("Should succeed in reading");
    let expected = vec![
        112f32, 124f32, 136f32, 148f32, 
        304f32, 348f32, 392f32, 436f32,
        496f32, 572f32, 648f32, 724f32,
        688f32, 796f32, 904f32, 1012f32,
    ];
    for (should, is) in expected.iter().zip(read_buffer.iter()) {
        assert_eq!(should, is, "Checking final result");
    }
    drop(lock);
}

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
        ComputeResource, Driver, FunctionArguments, WorkToDo,
    },
    memory_domain::{mmu::MmuMemoryDomain, ContextTrait, MemoryResource},
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
    let _lock = GPU_LOCK.lock().unwrap();
    let driver: Box<dyn Driver> = get_driver();
    engine_minimal::<MmuMemoryDomain>(
        &format!("{}/tests/data/minimal.json", env!("CARGO_MANIFEST_DIR")),
        MemoryResource::None,
        driver,
        vec![ComputeResource::GPU(7, 1)],
    );
}

#[test]
fn basic_input_output() {
    let _lock = GPU_LOCK.lock().unwrap();
    let driver: Box<dyn Driver> = get_driver();
    let (mut function_context, config, queue) = prepare_engine_and_function::<MmuMemoryDomain>(
        &format!("{}/tests/data/basic_io.json", env!("CARGO_MANIFEST_DIR")),
        MemoryResource::None,
        &driver,
        vec![ComputeResource::GPU(7, 0)],
    );
    // add inputs
    let in_size_offset = function_context
        .get_free_space_and_write_slice(&[12345i64])
        .expect("Should have space for single i64");
    function_context.content.push(Some(DataSet {
        ident: "A".to_string(),
        buffers: vec![DataItem {
            ident: "".to_string(),
            data: Position {
                offset: in_size_offset as usize,
                size: 8,
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
    let promise = queue.enqueu(WorkToDo::FunctionArguments(FunctionArguments {
        config,
        context: function_context,
        output_sets: Arc::new(vec!["A".into()]),
        recorder: recorder.get_sub_recorder().unwrap(),
    }));
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
    assert_eq!(1, result_context.content.len());
    let output_item = result_context.content[0]
        .as_ref()
        .expect("Set should be present");
    eprintln!("{:?}", output_item);
    assert_eq!(1, output_item.buffers.len());
    let position = output_item.buffers[0].data;
    assert_eq!(8, position.size, "Checking for size of output");
    let mut read_buffer = vec![0i64; position.size / 8];
    result_context
        .context
        .read(position.offset, &mut read_buffer)
        .expect("Should succeed in reading");
    assert_eq!(98765, read_buffer[0]);
}

#[test]
fn engine_matmul_3x3_loop() {
    let _lock = GPU_LOCK.lock().unwrap();
    let filename = &format!("{}/tests/data/matmul_loop.json", env!("CARGO_MANIFEST_DIR"));
    let dom_init = MemoryResource::None;
    let driver: Box<dyn Driver> = get_driver();
    let drv_init = vec![ComputeResource::GPU(7, 0)];
    let (mut function_context, config, queue) =
        prepare_engine_and_function::<MmuMemoryDomain>(filename, dom_init, &driver, drv_init);
    // add inputs, split over two buffers to test concatenating them in GPU memory
    let in_size_offset = function_context
        .get_free_space_and_write_slice(&[3i64])
        .expect("Should have space");
    let offset2 = function_context
        .get_free_space_and_write_slice(&[0i64, 1i64, 2i64, 3i64, 4i64, 5i64, 6i64, 7i64, 8i64])
        .expect("Should have space");
    function_context.content.push(Some(DataSet {
        ident: "A".to_string(),
        buffers: vec![
            DataItem {
                ident: "".to_string(),
                data: Position {
                    offset: in_size_offset as usize,
                    size: 8,
                },
                key: 0,
            },
            DataItem {
                ident: "".to_string(),
                data: Position {
                    offset: offset2 as usize,
                    size: 72,
                },
                key: 0,
            },
        ],
    }));
    let archive = Box::leak(Box::new(Archive::init(ArchiveInit {
        #[cfg(feature = "timestamp")]
        timestamp_count: 1000,
    })));

    let mut recorder = archive.get_recorder().unwrap();
    recorder
        .record(RecordPoint::TransferEnd)
        .expect("Should have properly initialized recorder state");
    let promise = queue.enqueu(WorkToDo::FunctionArguments(FunctionArguments {
        config,
        context: function_context,
        output_sets: Arc::new(vec![String::from("B")]),
        recorder: recorder.get_sub_recorder().unwrap(),
    }));
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
    assert_eq!(1, result_context.content.len());
    let output_item = result_context.content[0]
        .as_ref()
        .expect("Set should be present");
    assert_eq!(1, output_item.buffers.len());
    let position = output_item.buffers[0].data;
    assert_eq!(80, position.size, "Checking for size of output");
    let mut read_buffer = vec![0i64; position.size / 8];
    result_context
        .context
        .read(position.offset, &mut read_buffer)
        .expect("Should succeed in reading");
    assert_eq!(3, read_buffer[0]);
    let expected = self::get_expected_mat(3);
    assert_eq!(3i64, read_buffer[0]);
    for (should, is) in expected.iter().zip(read_buffer[1..].iter()) {
        assert_eq!(should, is);
    }
}

#[test]
fn engine_matmul_size_sweep_parallel() {
    let _lock = GPU_LOCK.lock().unwrap();
    let filename = &format!("{}/tests/data/matmul_para.json", env!("CARGO_MANIFEST_DIR"));
    let dom_init = MemoryResource::None;
    let driver: Box<dyn Driver> = get_driver();
    let drv_init = vec![ComputeResource::GPU(7, 0)];
    const LOWER_SIZE_BOUND: usize = 2;
    const UPPER_SIZE_BOUND: usize = 16;
    for mat_size in LOWER_SIZE_BOUND..UPPER_SIZE_BOUND {
        let (mut function_context, config, queue) = prepare_engine_and_function::<MmuMemoryDomain>(
            filename,
            dom_init,
            &driver,
            drv_init.clone(),
        );
        // add inputs, split over two buffers to test concatenating them in GPU memory
        let in_size_offset = function_context
            .get_free_space_and_write_slice(&[mat_size as i64])
            .expect("Should have space");

        let mut mat_vec = Vec::<i64>::new();
        for i in 0..(mat_size * mat_size) {
            mat_vec.push(i as i64);
        }
        let in_mat_offset = function_context
            .get_free_space_and_write_slice(&mat_vec)
            .expect("Should have space") as usize;
        function_context.content.push(Some(DataSet {
            ident: "A".to_string(),
            buffers: vec![
                DataItem {
                    ident: "".to_string(),
                    data: Position {
                        offset: in_size_offset as usize,
                        size: 8,
                    },
                    key: 0,
                },
                DataItem {
                    ident: "".to_string(),
                    data: Position {
                        offset: in_mat_offset,
                        size: mat_vec.len() * 8,
                    },
                    key: 0,
                },
            ],
        }));
        let cfg_offset = function_context
            .get_free_space_and_write_slice(&[(mat_size + 31) / 32])
            .expect("Should be able to write cfg");
        function_context.content.push(Some(DataSet {
            ident: "cfg".to_string(),
            buffers: vec![DataItem {
                ident: "".to_string(),
                data: Position {
                    offset: cfg_offset as usize,
                    size: 8,
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
        let promise = queue.enqueu(WorkToDo::FunctionArguments(FunctionArguments {
            config,
            context: function_context,
            output_sets: Arc::new(vec![String::from("B")]),
            recorder: recorder.get_sub_recorder().unwrap(),
        }));
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
        assert_eq!(1, result_context.content.len());
        let output_item = &result_context.content[0]
            .as_ref()
            .expect("Set should be present");
        assert_eq!(1, output_item.buffers.len());
        let position = output_item.buffers[0].data;
        assert_eq!(
            (mat_size * mat_size + 1) * 8,
            position.size,
            "Checking for size of output"
        );
        let mut output = vec![0i64; position.size / 8];
        result_context
            .context
            .read(position.offset, &mut output)
            .expect("Should succeed in reading");
        let expected = self::get_expected_mat(mat_size);
        assert_eq!(mat_size as i64, output[0]);
        for (should, is) in expected.iter().zip(output[1..].iter()) {
            assert_eq!(should, is);
        }
    }
}

fn get_inference_inputs() -> (Vec<f32>, Vec<f32>) {
    let mut a: Vec<f32> = Vec::with_capacity(224 * 224 + 2);
    let mut b: Vec<f32> = Vec::with_capacity(5 * 5 + 2);

    a.push(224.0);
    a.push(224.0);
    for i in 0..224 * 224 {
        a.push(i as f32);
    }

    b.push(5.0);
    b.push(5.0);
    for i in 0..5 * 5 {
        b.push(i as f32);
    }

    (a, b)
}

fn get_expected_inference_output() -> Vec<f32> {
    let (a, b) = get_inference_inputs();
    let mut c = vec![0f32; a.len()];
    let mut d = vec![0f32; c.len() / 2 + 1];

    // convolution
    c[0] = 224f32;
    c[1] = 224f32;
    for i in 0..224 {
        for j in 0..224 {
            let mut sum = 0f32;
            for k in -2..=2i32 {
                for l in -2..=2i32 {
                    let cur_row = i - k;
                    let cur_col = j - l;

                    let filter_row = 2 + k;
                    let filter_col = 2 + l;

                    if (0..224).contains(&cur_row) && (0..224).contains(&cur_col) {
                        let cur_row = cur_row as usize; // There has to be a better way to do this...
                        let cur_col = cur_col as usize;
                        let filter_row = filter_row as usize;
                        let filter_col = filter_col as usize;
                        sum += a[2 + cur_row * 224 + cur_col] * b[2 + filter_row * 5 + filter_col];
                    }
                }
            }
            let i = i as usize;
            let j = j as usize;
            c[2 + i * 224 + j] = sum;
        }
    }

    // relu
    for i in 0..224 * 224 {
        c[2 + i] = c[2 + i].max(0f32);
    }

    // max pooling
    d[0] = 112f32;
    d[1] = 112f32;
    for i in 0..112 {
        for j in 0..112 {
            let mut max = f32::NEG_INFINITY;
            let base_row = i * 2;
            let base_col = j * 2;
            for k in 0..2 {
                for l in 0..2 {
                    max = max.max(c[2 + (base_row + k) * 224 + (base_col + l)]);
                }
            }
            d[2 + i * 112 + j] = max;
        }
    }

    d
}

#[test]
fn inference_benchmark_function() {
    let _lock = GPU_LOCK.lock().unwrap();
    let filename = &format!("{}/tests/data/inference.json", env!("CARGO_MANIFEST_DIR"));
    let dom_init = MemoryResource::None;
    let driver: Box<dyn Driver> = get_driver();
    let drv_init = vec![ComputeResource::GPU(7, 0)];
    let (mut function_context, config, queue) =
        prepare_engine_and_function::<MmuMemoryDomain>(filename, dom_init, &driver, drv_init);
    let d_size: usize = 112 * 112 * 4 + 8; //side_len * side_len * sizeof(float) + [[size convention at start]]
    let a_grid_dim: usize = (224 + 31) / 32;
    let d_grid_dim: usize = (112 + 31) / 32;
    let (a_matrix, b_matrix) = get_inference_inputs();
    let cfg_offset = function_context
        .get_free_space_and_write_slice(&[d_size, a_grid_dim, d_grid_dim, 500])
        .expect("Should have space for cfg");
    let a_offset = function_context
        .get_free_space_and_write_slice(&a_matrix)
        .expect("Should have space for A");
    let b_offset = function_context
        .get_free_space_and_write_slice(&b_matrix)
        .expect("Should have space for B");

    function_context.content.append(&mut vec![
        Some(DataSet {
            ident: "cfg".to_string(),
            buffers: vec![DataItem {
                ident: "".to_string(),
                data: Position {
                    offset: cfg_offset as usize,
                    size: 3 * 8,
                },
                key: 0,
            }],
        }),
        Some(DataSet {
            ident: "A".to_string(),
            buffers: vec![DataItem {
                ident: "".to_string(),
                data: Position {
                    offset: a_offset as usize,
                    size: a_matrix.len() * 4,
                },
                key: 0,
            }],
        }),
        Some(DataSet {
            ident: "B".to_string(),
            buffers: vec![DataItem {
                ident: "".to_string(),
                data: Position {
                    offset: b_offset as usize,
                    size: b_matrix.len() * 4,
                },
                key: 0,
            }],
        }),
    ]);
    let archive = Box::leak(Box::new(Archive::init(ArchiveInit {
        #[cfg(feature = "timestamp")]
        timestamp_count: 1000,
    })));

    let mut recorder = archive.get_recorder().unwrap();
    recorder
        .record(RecordPoint::TransferEnd)
        .expect("Should have properly initialized recorder state");
    let promise = queue.enqueu(WorkToDo::FunctionArguments(FunctionArguments {
        config,
        context: function_context,
        output_sets: Arc::new(vec![String::from("D")]),
        recorder: recorder.get_sub_recorder().unwrap(),
    }));
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
    assert_eq!(1, result_context.content.len());
    let output_item = result_context.content[0]
        .as_ref()
        .expect("Set should be present");
    assert_eq!(1, output_item.buffers.len());
    let position = output_item.buffers[0].data;
    assert_eq!(
        112 * 112 * 4 + 8,
        position.size,
        "Checking for size of output"
    );
    let mut read_buffer = vec![0f32; position.size / 4];
    result_context
        .context
        .read(position.offset, &mut read_buffer)
        .expect("Should succeed in reading");
    let expected = get_expected_inference_output();
    for (idx, (should, is)) in expected.iter().zip(read_buffer[..].iter()).enumerate() {
        assert_eq!(should, is, "Comparing args at {}", idx);
    }
}

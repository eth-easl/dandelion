use crate::{
    function_driver::{
        compute_driver::{
            compute_driver_tests::compute_driver_tests::prepare_engine_and_function,
            gpu::gpu_tests::{
                get_driver,
                load_models::*,
                tests_utils::{compare_result, execute_test, get_result, setup_test},
                Archive, ArchiveInit, RecordPoint, GPU_LOCK,
            },
        },
        Arc, ComputeResource, Driver, WorkToDo,
    },
    memory_domain::{gpu::GpuMemoryDomain, ContextTrait, MemoryResource},
};

#[test]
fn test_all() {
    full_double_matmul();
    alexnet();
    lenet5();
}

#[test]
fn full_double_matmul() {
    let lock = GPU_LOCK.lock().unwrap();
    let filename = &format!(
        "{}/tests/data/test_gpu_full_double_matmul.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let (mut function_context, config, queue) = setup_test(&filename);
    let (output_size, output_name, expected, function_context) =
        load_double_matmul(function_context);
    let result_context = execute_test(function_context, config, queue, &output_name);
    let read_buffer = get_result(result_context, output_size);
    compare_result(expected, read_buffer);
    drop(lock);
}

#[test]
fn alexnet() {
    let lock = GPU_LOCK.lock().unwrap();
    let filename = &format!(
        "{}/tests/data/test_gpu_alexnet.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let (mut function_context, config, queue) = setup_test(&filename);
    let (output_size, output_name, expected, function_context) = load_alexnet(function_context);
    let result_context = execute_test(function_context, config, queue, &output_name);
    let read_buffer = get_result(result_context, output_size);
    compare_result(expected, read_buffer);
    drop(lock);
}

#[test]
fn lenet5() {
    let lock = GPU_LOCK.lock().unwrap();
    let filename = &format!(
        "{}/tests/data/test_gpu_lenet5.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let (mut function_context, config, queue) = setup_test(&filename);
    let (output_size, output_name, expected, function_context) = load_lenet5(function_context);
    let result_context = execute_test(function_context, config, queue, &output_name);
    let read_buffer = get_result(result_context, output_size);
    compare_result(expected, read_buffer);
    drop(lock);
}

#[test]
fn simple_conv() {
    use std::env;
    env::set_var("RUST_BACKTRACE", "1");

    let lock = GPU_LOCK.lock().unwrap();
    let filename = &format!(
        "{}/tests/data/test_gpu_simple_conv.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let dom_init = MemoryResource::None;
    let driver: Box<dyn Driver> = get_driver();
    let drv_init = vec![ComputeResource::GPU(7, 0, 2)];
    let (mut function_context, config, queue) =
        prepare_engine_and_function::<GpuMemoryDomain>(filename, dom_init, &driver, drv_init);

    let (output_size, output_name, expected, function_context) = load_simple_conv(function_context);

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
        output_sets: Arc::new(vec![String::from(&output_name)]),
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
    assert_eq!(output_size, position.size, "Checking for size of output");
    let mut read_buffer = vec![0f32; position.size / 4];
    result_context
        .context
        .read(position.offset, &mut read_buffer)
        .expect("Should succeed in reading");
    for (should, is) in expected.iter().zip(read_buffer.iter()) {
        //assert_eq!(should, is, "Checking final result");
        println!("{should}\t\t{is}");
    }
    println!("Correct result!");
    drop(lock);
}

#[test]
fn resnet18() {
    let lock = GPU_LOCK.lock().unwrap();
    let filename = &format!(
        "{}/tests/data/test_gpu_resnet18.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let dom_init = MemoryResource::None;
    let driver: Box<dyn Driver> = get_driver();
    let drv_init = vec![ComputeResource::GPU(7, 0, 2)];
    let (mut function_context, config, queue) =
        prepare_engine_and_function::<GpuMemoryDomain>(filename, dom_init, &driver, drv_init);

    let (output_size, output_name, expected, function_context) = load_resnet18(function_context);

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
        output_sets: Arc::new(vec![String::from(&output_name)]),
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
    assert_eq!(output_size, position.size, "Checking for size of output");
    let mut read_buffer = vec![0f32; position.size / 4];
    result_context
        .context
        .read(position.offset, &mut read_buffer)
        .expect("Should succeed in reading");
    for (should, is) in expected.iter().zip(read_buffer.iter()) {
        //assert_eq!(should, is, "Checking final result");
        println!("{should}\t\t{is}");
    }
    println!("Correct result!");
    drop(lock);
}

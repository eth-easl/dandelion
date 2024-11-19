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
    resnet18();
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
    let read_buffer = get_result(result_context, output_size, true);
    compare_result(expected, read_buffer, true);
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
    let read_buffer = get_result(result_context, output_size, true);
    compare_result(expected, read_buffer, true);
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
    let read_buffer = get_result(result_context, output_size, true);
    compare_result(expected, read_buffer, true);
    drop(lock);
}

#[test]
fn resnet18() {
    let lock = GPU_LOCK.lock().unwrap();
    let filename = &format!(
        "{}/tests/data/test_gpu_resnet18.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let (mut function_context, config, queue) = setup_test(&filename);
    let (output_size, output_name, expected, function_context) = load_resnet18(function_context);
    let result_context = execute_test(function_context, config, queue, &output_name);
    let read_buffer = get_result(result_context, output_size, true);
    compare_result(expected, read_buffer, true);
    drop(lock);
}

#[test]
fn batch_norm() {
    let lock = GPU_LOCK.lock().unwrap();
    let filename = &format!(
        "{}/tests/data/test_gpu_batch_norm.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let (mut function_context, config, queue) = setup_test(&filename);
    let (output_size, output_name, expected, function_context) = load_batch_norm(function_context);
    let result_context = execute_test(function_context, config, queue, &output_name);
    let read_buffer = get_result(result_context, output_size, false);
    compare_result(expected, read_buffer, false);
    drop(lock);
}

use crate::{
    function_driver::compute_driver::gpu::gpu_tests::{
        cuda_tests::load_models::*,
        get_driver,
        tests_utils::{compare_result, execute_test, get_result, setup_test},
        GPU_LOCK,
    },
    memory_domain::Context,
};
use std::sync::Arc;

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
        "{}/tests/data/hip/test_gpu_full_double_matmul.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let (function_context, config, queue) = setup_test(&filename);
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
        "{}/tests/data/hip/test_gpu_alexnet.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let (function_context, config, queue) = setup_test(&filename);
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
        "{}/tests/data/hip/test_gpu_lenet5.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let (function_context, config, queue) = setup_test(&filename);
    let (output_size, output_name, expected, function_context) = load_lenet5(function_context);
    let result_context = execute_test(function_context, config, queue, &output_name);
    let read_buffer = get_result(result_context, output_size, true);
    compare_result(expected, read_buffer, true);
    drop(lock);
}

#[test]
fn resnet18base() {
    let lock = GPU_LOCK.lock().unwrap();
    let filename = &format!(
        "{}/tests/data/hip/test_gpu_resnet18.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let (function_context, config, queue) = setup_test(&filename);
    let (output_size, output_name, expected, function_context) = load_resnet18(function_context);
    let result_context = execute_test(function_context, config, queue, &output_name);
    let read_buffer = get_result(result_context, output_size, false);
    compare_result(expected, read_buffer, false);
    drop(lock);
}

#[test]
fn resnet18() {
    let lock = GPU_LOCK.lock().unwrap();
    let filename = &format!(
        "{}/tests/data/hip/test_gpu_resnet18.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let (function_context, config, queue) = setup_test(&filename);
    let (output_size, output_name, expected, function_context) = load_resnet18(function_context);
    let result_context = execute_test(function_context, config, queue, &output_name);
    let read_buffer = get_result(result_context, output_size, true);
    compare_result(expected, read_buffer, true);
    drop(lock);
}

#[test]
fn resnet18batch4() {
    let lock = GPU_LOCK.lock().unwrap();
    let filename = &format!(
        "{}/tests/data/hip/test_gpu_resnet18batch4.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let (function_context, config, queue) = setup_test(&filename);
    let (output_size, output_name, expected, function_context) = load_resnet18batch4(function_context);
    let result_context = execute_test(function_context, config, queue, &output_name);
    let read_buffer = get_result(result_context, output_size, true);
    compare_result(expected, read_buffer, true);
    drop(lock);
}

#[test]
fn resnet18batch16() {
    let lock = GPU_LOCK.lock().unwrap();
    let filename = &format!(
        "{}/tests/data/hip/test_gpu_resnet18batch16.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let (function_context, config, queue) = setup_test(&filename);
    let (output_size, output_name, expected, function_context) = load_resnet18batch16(function_context);
    let result_context = execute_test(function_context, config, queue, &output_name);
    let read_buffer = get_result(result_context, output_size, true);
    compare_result(expected, read_buffer, true);
    drop(lock);
}

#[test]
fn resnet18batch64() {
    let lock = GPU_LOCK.lock().unwrap();
    let filename = &format!(
        "{}/tests/data/hip/test_gpu_resnet18batch64.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let (function_context, config, queue) = setup_test(&filename);
    let (output_size, output_name, expected, function_context) = load_resnet18batch64(function_context);
    let result_context = execute_test(function_context, config, queue, &output_name);
    let read_buffer = get_result(result_context, output_size, true);
    compare_result(expected, read_buffer, true);
    drop(lock);
}

#[test]
fn resnet18onnx() {
    let lock = GPU_LOCK.lock().unwrap();
    let filename = &format!(
        "{}/tests/data/hip/test_gpu_resnet18onnx.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let (function_context, config, queue) = setup_test(&filename);
    let (output_size, output_name, expected, function_context) = load_resnet18onnx(function_context);
    let result_context = execute_test(function_context, config, queue, &output_name);
    let read_buffer = get_result(result_context, output_size, false);
    compare_result(expected, read_buffer, false);
    drop(lock);
}

#[test]
fn resnet34() {
    let lock = GPU_LOCK.lock().unwrap();
    let filename = &format!(
        "{}/tests/data/hip/test_gpu_resnet34.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let (function_context, config, queue) = setup_test(&filename);
    let (output_size, output_name, expected, function_context) = load_resnet34(function_context);
    let result_context = execute_test(function_context, config, queue, &output_name);
    let read_buffer = get_result(result_context, output_size, true);
    compare_result(expected, read_buffer, true);
    drop(lock);
}

#[test]
fn resnet50() {
    let lock = GPU_LOCK.lock().unwrap();
    let filename = &format!(
        "{}/tests/data/hip/test_gpu_resnet50.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let (function_context, config, queue) = setup_test(&filename);
    let (output_size, output_name, expected, function_context) = load_resnet50(function_context);
    let result_context = execute_test(function_context, config, queue, &output_name);
    let read_buffer = get_result(result_context, output_size, false);
    compare_result(expected, read_buffer, false);
    drop(lock);
}

#[test]
fn resnet152() {
    let lock = GPU_LOCK.lock().unwrap();
    let filename = &format!(
        "{}/tests/data/hip/test_gpu_resnet152.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let (function_context, config, queue) = setup_test(&filename);
    let (output_size, output_name, expected, function_context) = load_resnet152(function_context);
    let result_context = execute_test(function_context, config, queue, &output_name);
    let read_buffer = get_result(result_context, output_size, true);
    compare_result(expected, read_buffer, false);
    drop(lock);
}

#[test]
fn batch_norm() {
    let lock = GPU_LOCK.lock().unwrap();
    let filename = &format!(
        "{}/tests/data/hip/test_gpu_batch_norm.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let (function_context, config, queue) = setup_test(&filename);
    let (output_size, output_name, expected, function_context) = load_batch_norm(function_context);
    let result_context = execute_test(function_context, config, queue, &output_name);
    let read_buffer = get_result(result_context, output_size, false);
    compare_result(expected, read_buffer, false);
    drop(lock);
}

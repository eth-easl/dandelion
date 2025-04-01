use crate::{
    function_driver::compute_driver::gpu::gpu_tests::{
        cuda_tests::load_models::*,
        get_driver,
        tests_utils::{compare_result, execute_test, get_result, setup_test},
        GPU_LOCK,
    },
    memory_domain::Context,
};
use std::{collections::HashMap, sync::Arc};

#[test]
fn test_all() {
    test_model("simple", true);
    test_model("double_matmul", true);
    test_model("resnet18", true);
    test_model("rnn", true);
    test_model("lstm", true);
    test_model("vit_b_16", true);
    test_model("bert", true);
}

fn get_function(model_name: &str) -> Option<fn(Context) -> (usize, String, Vec<f32>, Context)> {
    let mut methods: HashMap<_, fn(Context) -> (usize, String, Vec<f32>, Context)> = HashMap::new();

    methods.insert("simple", load_simple);
    methods.insert("double_matmul", load_double_matmul);
    methods.insert("resnet18", load_resnet18);
    methods.insert("rnn", load_rnn);
    methods.insert("lstm", load_lstm);
    methods.insert("vit_b_16", load_vit_b_16);
    methods.insert("bert", load_bert);
    methods.insert("llama", load_llama);

    methods.get(model_name).copied()
}

fn test_model(model_name: &str, asserts: bool) {
    let lock = GPU_LOCK.lock().unwrap();
    let filename = &format!(
        "{}/tests/data/cuda/test_gpu_{}.json",
        env!("CARGO_MANIFEST_DIR"),
        model_name
    );
    let (function_context, config, queue) = setup_test(&filename);
    let load_function = get_function(model_name)
        .unwrap_or_else(|| panic!("Model name \"{model_name}\" not recognized. Add it to the methods hash map."));
    let (output_size, output_name, expected, function_context) = load_function(function_context);
    let result_context = execute_test(function_context, config, queue, &output_name);
    let read_buffer = get_result(result_context, output_size, asserts);
    compare_result(expected, read_buffer, asserts);
    drop(lock);
}

#[test]
fn simple() {
    test_model("simple", true);
}

#[test]
fn double_matmul() {
    test_model("double_matmul", true);
}

#[test]
fn resnet18() {
    test_model("resnet18", true);
}

#[test]
fn rnn() {
    test_model("rnn", true);
}

#[test]
fn lstm() {
    test_model("lstm", true);
}

#[test]
fn vit_b_16() {
    test_model("vit_b_16", true);
}

#[test]
fn bert() {
    test_model("bert", true);
}

#[test]
fn llama() {
    test_model("llama", true);
}

use crate::{
    function_driver::compute_driver::gpu::gpu_tests::{
        cuda_tests::load_models::*,
        get_driver,
        tests_utils::{
            compare_result, execute_test, get_result, get_resulti32, get_resulti64, setup_test,
        },
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
    methods.insert("llama_kv", load_llama_kv);

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
    let load_function = get_function(model_name).unwrap_or_else(|| {
        panic!("Model name \"{model_name}\" not recognized. Add it to the methods hash map.")
    });
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
fn one_llama() {
    test_model("llama", false);
}

#[test]
fn full_llama() {
    let lock = GPU_LOCK.lock().unwrap();
    let filename = &format!(
        "{}/tests/data/cuda/test_gpu_llama-full.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let (function_context, config, queue) = setup_test(&filename);
    let (mut output_size, mut output_name, expected, function_context) =
        load_llama(function_context);
    output_size = 1024;
    output_name = "token_ids".to_string();

    let result_context = execute_test(function_context, config, queue, &output_name);

    let read_buffer = get_resulti64(result_context, output_size, false);
    println!("{:?}", read_buffer);

    drop(lock);
}

#[test]
fn one_kv_llama() {
    test_model("llama_kv", true);
}

#[test]
fn hand_kv_llama() {
    let lock = GPU_LOCK.lock().unwrap();
    let filename = &format!(
        "{}/tests/data/cuda/test_gpu_llama_kv.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let (function_context, config, queue) = setup_test(&filename);
    let (mut output_size, mut output_name, expected, function_context) =
        load_llama_kv(function_context);
    output_size = 16777216; // 1024;
    output_name = "b41".to_string(); // "token_ids".to_string();

    let result_context = execute_test(function_context, config, queue, &output_name);

    // let read_buffer = get_resulti64(result_context, output_size, false);
    let read_buffer = get_result(result_context, output_size, false);
    for i in 0..16777216 / 4 {
        if read_buffer[i] != 0.0 {
            println!("{} - {:?}, ", i, read_buffer[i]);
        }
    }

    drop(lock);
}

#[test]
fn full_kv_llama() {
    let lock = GPU_LOCK.lock().unwrap();
    let filename = &format!(
        "{}/tests/data/cuda/test_gpu_llama_kv-full.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let (function_context, config, queue) = setup_test(&filename);
    let (mut output_size, mut output_name, expected, function_context) =
        full_load_llama_kv(function_context);
    output_size = 1024;
    output_name = "token_ids".to_string();

    let result_context = execute_test(function_context, config, queue, &output_name);

    let read_buffer = get_resulti64(result_context, output_size, false);
    println!("{:?}", read_buffer);
    /*let read_buffer = get_result(result_context, output_size, false);
    for i in 5000..5100 { // 16777216 / 4 {
        if read_buffer[i] != 0.0 {
            println!("{} - {:?}, ", i, read_buffer[i]);
        }
    }*/

    drop(lock);
}

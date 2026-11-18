//! Tests running metafunctions end to end, with the echo planner test binary as planner: it returns
//! its only planner param as plan, so the tests choose the plan by passing its source as input.

use super::{
    check_matrix, run_composition, setup_dispatcher_with_meta_depth, test_binary_path,
    try_run_composition, DEFAULT_CONTEXT_SIZE,
};
use dandelion_commons::{DandelionError, DandelionResult, FunctionRegistryError};
use dispatcher::dispatcher::Dispatcher;
use machine_interface::{
    function_driver::ComputeResource,
    machine_config::{DomainType, EngineType},
};
use memory::{context::MemoryResource, data::Metadata, DataItem, DataSet};
use std::sync::Arc;

/// Declaration of the echo planner, registered as metafunction `Plan`.
const PLAN_DECL: &str = "metafunction Plan (plan);";

/// Plan running the matmul test function on its single input.
const MATMUL_PLAN: &str = r#"
    function test_function (A) => (Out);
    composition Matmul (In) => (Result) {
        test_function (A = all In) => (Result = Out);
    }
"#;

/// Plan applying `Plan` once more, with its first input as plan for its second input.
const NESTED_PLAN: &str = r#"
    metafunction Plan (plan);
    composition Nested (Src, In) => (Result) {
        Plan (plan = Src) (In) => (Result);
    }
"#;

/// Applies `Plan` with the plan in `PlanSrc` to the matrix in `InMat`.
fn single_application() -> String {
    format!(
        r#"
        {PLAN_DECL}
        composition Apply (PlanSrc, InMat) => (OutMat) {{
            Plan (plan = PlanSrc) (InMat) => (OutMat);
        }}
    "#
    )
}

/// Applies `Plan` with the plan in `PlanSrc` to the inner plan in `InnerSrc` and the matrix in
/// `InMat`, for plans that themselves apply a metafunction with `InnerSrc` as plan.
fn nested_application() -> String {
    format!(
        r#"
        {PLAN_DECL}
        composition ApplyNested (PlanSrc, InnerSrc, InMat) => (OutMat) {{
            Plan (plan = PlanSrc) (InnerSrc, InMat) => (OutMat);
        }}
    "#
    )
}

/// Sets up a dispatcher with the matmul test function and the echo planner as metafunction `Plan`.
fn setup_meta_dispatcher(
    memory_resource: (DomainType, MemoryResource),
    prefix: &str,
    engine_type: EngineType,
    engine_resource: Vec<ComputeResource>,
    max_meta_depth: Option<usize>,
) -> Dispatcher {
    let (dispatcher, _) = setup_dispatcher_with_meta_depth(
        &format!("test_{prefix}_matmul"),
        vec![(String::from("A"), None)],
        vec![String::from("Out")],
        engine_type,
        engine_resource,
        memory_resource,
        max_meta_depth,
    );
    dispatcher
        .insert_meta_function(
            String::from("Plan"),
            engine_type,
            DEFAULT_CONTEXT_SIZE,
            test_binary_path(&format!("test_{prefix}_planner")),
            Metadata {
                input_sets: vec![(String::from("plan"), None)],
                output_sets: vec![],
                min_set_bytes: vec![],
            },
        )
        .expect("Should be able to insert planner");
    dispatcher
}

fn source_set(source: &str) -> DataSet {
    DataSet::from_bytes([(String::from(""), source.as_bytes().to_vec())]).unwrap()
}

/// The 2x2 input matrix, with the number of rows first.
fn matrix_set() -> DataSet {
    let matrix = [2u64, 1, 2, 3, 4];
    DataSet::from_bytes([(
        String::from(""),
        matrix.iter().flat_map(|v| v.to_le_bytes()).collect(),
    )])
    .unwrap()
}

/// Checks the output is the matmul result of the input matrix.
fn check_matmul_output(out_sets: Vec<Arc<Vec<DataItem>>>) {
    assert_eq!(1, out_sets.len());
    assert_eq!(1, out_sets[0].len());
    check_matrix(&out_sets[0][0], 2, vec![5, 11, 11, 25]);
}

fn assert_registry_error<T: std::fmt::Debug>(
    result: DandelionResult<T>,
    expected: fn(&FunctionRegistryError) -> bool,
) {
    match result {
        Err(err) => assert!(
            matches!(&err.error, DandelionError::FunctionRegistry(e) if expected(e)),
            "unexpected error {:?}",
            err.error
        ),
        Ok(value) => panic!("expected an error, got Ok({:?})", value),
    }
}

fn is_recursion_limit(err: &FunctionRegistryError) -> bool {
    matches!(err, FunctionRegistryError::MetaRecursionLimit(_))
}

// check if metafunction application actually outputs the expected plan (matmul) and runs it.
pub fn meta_function_runs_plan(
    memory_resource: (DomainType, MemoryResource),
    prefix: &str,
    engine_type: EngineType,
    engine_resource: Vec<ComputeResource>,
) {
    let dispatcher =
        setup_meta_dispatcher(memory_resource, prefix, engine_type, engine_resource, None);

    let out_sets = run_composition(
        &dispatcher,
        &single_application(),
        vec![source_set(MATMUL_PLAN), matrix_set()],
    );

    check_matmul_output(out_sets);
}

// setting max_meta_depth to 0 should deactivate metafunctions
pub fn meta_function_disabled_at_depth_zero(
    memory_resource: (DomainType, MemoryResource),
    prefix: &str,
    engine_type: EngineType,
    engine_resource: Vec<ComputeResource>,
) {
    let dispatcher = setup_meta_dispatcher(
        memory_resource,
        prefix,
        engine_type,
        engine_resource,
        Some(0),
    );

    let result = try_run_composition(
        &dispatcher,
        &single_application(),
        vec![source_set(MATMUL_PLAN), matrix_set()],
    );

    assert_registry_error(result, is_recursion_limit);
}

// if max_depth that is sufficiently high, it runs
pub fn nested_meta_function_within_limit_runs(
    memory_resource: (DomainType, MemoryResource),
    prefix: &str,
    engine_type: EngineType,
    engine_resource: Vec<ComputeResource>,
) {
    // the inner application is nested in one plan, so a depth of 2 allows it
    let dispatcher = setup_meta_dispatcher(
        memory_resource,
        prefix,
        engine_type,
        engine_resource,
        Some(2),
    );

    let out_sets = run_composition(
        &dispatcher,
        &nested_application(),
        vec![
            source_set(NESTED_PLAN),
            source_set(MATMUL_PLAN),
            matrix_set(),
        ],
    );

    check_matmul_output(out_sets);
}

// execution which does more recursions than allowed fails
pub fn nested_meta_function_beyond_limit_fails(
    memory_resource: (DomainType, MemoryResource),
    prefix: &str,
    engine_type: EngineType,
    engine_resource: Vec<ComputeResource>,
) {
    let dispatcher = setup_meta_dispatcher(
        memory_resource,
        prefix,
        engine_type,
        engine_resource,
        Some(1),
    );

    // without the limit this would succeed, so a missing check fails instead of recursing
    let result = try_run_composition(
        &dispatcher,
        &nested_application(),
        vec![
            source_set(NESTED_PLAN),
            source_set(MATMUL_PLAN),
            matrix_set(),
        ],
    );

    assert_registry_error(result, is_recursion_limit);
}

pub fn registered_composition_does_not_reset_depth(
    memory_resource: (DomainType, MemoryResource),
    prefix: &str,
    engine_type: EngineType,
    engine_resource: Vec<ComputeResource>,
) {
    let dispatcher = setup_meta_dispatcher(
        memory_resource,
        prefix,
        engine_type,
        engine_resource,
        Some(1),
    );
    dispatcher
        .insert_compositions(String::from(
            r#"
            metafunction Plan (plan);
            composition Wrapper (Src, In) => (Result) {
                Plan (plan = Src) (In) => (Result);
            }
        "#,
        ))
        .expect("Should be able to register the wrapper composition");
    // only applies the metafunction through the registered composition
    let wrapping_plan = r#"
        function Wrapper (Src, In) => (Result);
        composition CallsWrapper (Src, In) => (Result) {
            Wrapper (Src = all Src, In = all In) => (Result = Result);
        }
    "#;

    let result = try_run_composition(
        &dispatcher,
        &nested_application(),
        vec![
            source_set(wrapping_plan),
            source_set(MATMUL_PLAN),
            matrix_set(),
        ],
    );

    assert_registry_error(result, is_recursion_limit);
}

// test with invalid plan
pub fn invalid_plan_fails_application(
    memory_resource: (DomainType, MemoryResource),
    prefix: &str,
    engine_type: EngineType,
    engine_resource: Vec<ComputeResource>,
) {
    let dispatcher =
        setup_meta_dispatcher(memory_resource, prefix, engine_type, engine_resource, None);

    let result = try_run_composition(
        &dispatcher,
        &single_application(),
        vec![source_set("this is not a composition"), matrix_set()],
    );

    assert_registry_error(result, |err| {
        matches!(err, FunctionRegistryError::InvalidPlan(_))
    });
}

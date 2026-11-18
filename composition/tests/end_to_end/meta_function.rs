use crate::common::{data_set, item, items, parse_composition, run_to_completion, TestRegistry};
use composition::{AnyShardingMode, Composition};

const SRC: &str = r#"
    function Prepare(In) => (Out);
    metafunction Plan(Info);

    composition Test(Info, A, B) => (D) {
        Prepare(In = all A) => (C = Out);
        Plan(Info = Info) (A, B, C) => (D);
    }
"#;

fn registry() -> TestRegistry {
    TestRegistry::new()
        .with_function("Prepare", &["In"], &["Out"])
        .with_meta_function("Plan", &["Info"])
}

#[test]
fn meta_application_gets_planner_inputs_followed_by_data_inputs() {
    let registry = registry();
    let template = parse_composition(SRC, "Test", &registry);
    let composition =
        Composition::from_template(&template, AnyShardingMode::MaxSharding, &registry);

    let mut plan_invocations = 0;
    let outputs = run_to_completion(
        composition,
        vec![
            data_set(vec![item("info", 0)]),
            data_set(vec![item("a0", 0), item("a1", 1)]),
            data_set(vec![item("b0", 0)]),
        ],
        |invocation| match invocation.function_id.as_str() {
            "Prepare" => {
                assert!(invocation.meta.is_none());
                vec![items(vec![item("c0", 0)])]
            }
            "Plan" => {
                plan_invocations += 1;
                let application = invocation.meta.as_ref().expect("Plan is a metafunction");
                assert_eq!(application.num_planner_inputs, 1);
                assert_eq!(application.input_names, ["A", "B", "C"]);
                assert_eq!(application.output_names, ["D"]);
                let idents: Vec<Vec<&str>> = invocation
                    .input
                    .iter()
                    .map(|set| set.items.iter().map(|i| i.ident.as_str()).collect())
                    .collect();
                assert_eq!(
                    idents,
                    [vec!["info"], vec!["a0", "a1"], vec!["b0"], vec!["c0"]],
                    "planner input first, then the whole data inputs in application order"
                );
                vec![items(vec![item("d0", 0)])]
            }
            other => panic!("unexpected invocation of {other}"),
        },
    );

    assert_eq!(
        plan_invocations, 1,
        "the planner runs once on the complete sets"
    );
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].items[0].ident, "d0");
}

#[test]
fn meta_application_runs_on_empty_data_inputs() {
    let registry = registry();
    let template = parse_composition(SRC, "Test", &registry);
    let composition =
        Composition::from_template(&template, AnyShardingMode::MaxSharding, &registry);

    let mut plan_invocations = 0;
    let outputs = run_to_completion(
        composition,
        vec![
            data_set(vec![item("info", 0)]),
            data_set(vec![item("a0", 0)]),
            data_set(vec![]),
        ],
        |invocation| match invocation.function_id.as_str() {
            "Prepare" => vec![items(vec![item("c0", 0)])],
            _ => {
                plan_invocations += 1;
                assert!(invocation.input[2].is_empty());
                vec![items(vec![item("d0", 0)])]
            }
        },
    );

    assert_eq!(
        plan_invocations, 1,
        "an empty data input does not prevent planning"
    );
    assert_eq!(outputs[0].items.len(), 1);
}

#[test]
fn meta_application_without_planner_input_does_not_run() {
    let registry = registry();
    let template = parse_composition(SRC, "Test", &registry);
    let composition =
        Composition::from_template(&template, AnyShardingMode::MaxSharding, &registry);

    let outputs = run_to_completion(
        composition,
        vec![
            data_set(vec![]),
            data_set(vec![item("a0", 0)]),
            data_set(vec![item("b0", 0)]),
        ],
        |invocation| match invocation.function_id.as_str() {
            "Prepare" => vec![items(vec![item("c0", 0)])],
            other => panic!("unexpected invocation of {other}"),
        },
    );

    assert!(outputs[0].items.is_empty());
}

#[test]
fn meta_application_output_feeds_downstream_functions() {
    let src = r#"
        function Consume(In) => (Out);
        metafunction Plan(Info);

        composition Downstream(Info, A) => (E) {
            Plan(Info = Info) (A) => (D);
            Consume(In = each D) => (E = Out);
        }
    "#;
    let registry = TestRegistry::new()
        .with_function("Consume", &["In"], &["Out"])
        .with_meta_function("Plan", &["Info"]);
    let template = parse_composition(src, "Downstream", &registry);
    let composition =
        Composition::from_template(&template, AnyShardingMode::MaxSharding, &registry);

    let mut invoked = Vec::new();
    let outputs = run_to_completion(
        composition,
        vec![
            data_set(vec![item("info", 0)]),
            data_set(vec![item("a0", 0)]),
        ],
        |invocation| {
            invoked.push(invocation.function_id.to_string());
            match invocation.function_id.as_str() {
                "Plan" => vec![items(vec![item("d0", 0), item("d1", 1)])],
                "Consume" => {
                    let consumed = &invocation.input[0].items;
                    assert_eq!(consumed.len(), 1, "`each` shards the planned output");
                    let ident = format!("e_{}", consumed[0].ident);
                    vec![items(vec![item(&ident, consumed[0].key)])]
                }
                other => panic!("unexpected invocation of {other}"),
            }
        },
    );

    // "Consume" is executed twice (each sharding), once for each output of the metafunction application of "Plan"
    assert_eq!(
        invoked,
        ["Plan", "Consume", "Consume"],
        "Consume only runs once the metafunction returned its output"
    );
    let mut produced: Vec<&str> = outputs[0].items.iter().map(|i| i.ident.as_str()).collect();
    produced.sort();
    assert_eq!(produced, ["e_d0", "e_d1"]);
}

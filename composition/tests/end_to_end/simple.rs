use crate::common::{data_set, item, parse_composition, run_to_completion, TestRegistry};
use composition::{AnyShardingMode, Composition};

#[test]
fn all_sharded_single_invocation_passes_data_through() {
    let src = r#"
        function Identity(In) => (Out);

        composition PassThrough(CompIn) => (CompOut) {
            Identity(In = all CompIn) => (CompOut = Out);
        }
    "#;
    let registry = TestRegistry::new().with_function("Identity", &["In"], &["Out"]);
    let template = parse_composition(src, "PassThrough", &registry);
    let composition = Composition::from_template(&template, AnyShardingMode::MaxSharding, &registry);

    let input = data_set(vec![item("a", 0), item("b", 1)]);
    let mut invocation_count = 0;
    let outputs = run_to_completion(composition, vec![input], |invocation| {
        invocation_count += 1;
        assert_eq!(invocation.function_id.as_str(), "Identity");
        assert_eq!(invocation.input.len(), 1);
        assert_eq!(
            invocation.input[0].items.len(),
            2,
            "the whole `all` set arrives in a single invocation"
        );
        vec![data_set(vec![item("out", 0)])]
    });

    assert_eq!(invocation_count, 1, "`all` blocks until complete, then runs exactly once");
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].items.len(), 1);
    assert_eq!(outputs[0].items[0].ident, "out");
}

#[test]
fn each_sharded_input_runs_one_invocation_per_item() {
    let src = r#"
        function Double(In) => (Out);

        composition DoubleAll(CompIn) => (CompOut) {
            Double(In = each CompIn) => (CompOut = Out);
        }
    "#;
    let registry = TestRegistry::new().with_function("Double", &["In"], &["Out"]);
    let template = parse_composition(src, "DoubleAll", &registry);
    let composition = Composition::from_template(&template, AnyShardingMode::MaxSharding, &registry);

    let input = data_set(vec![item("a", 0), item("b", 1), item("c", 2)]);
    let mut invocation_count = 0;
    let outputs = run_to_completion(composition, vec![input], |invocation| {
        invocation_count += 1;
        assert_eq!(
            invocation.input[0].items.len(),
            1,
            "each invocation gets exactly one item"
        );
        let doubled = invocation.input[0].items[0].ident.repeat(2);
        let key = invocation.input[0].items[0].key;
        vec![data_set(vec![item(&doubled, key)])]
    });

    assert_eq!(invocation_count, 3);
    assert_eq!(outputs.len(), 1);
    let mut idents: Vec<_> = outputs[0].items.iter().map(|i| i.ident.clone()).collect();
    idents.sort();
    assert_eq!(idents, vec!["aa".to_string(), "bb".to_string(), "cc".to_string()]);
}

/// Regression test: a function whose only input is `each`/`anyEach`-sharded and fed purely by
/// another function's streamed output (never a composition-level input, so it never goes through
/// `set_composition_input`) must still be scheduled correctly by the initial `in_set_complete`
/// sweep in `Composition::start_execution`.
#[test]
fn each_to_each_streaming_chain_runs_without_a_composition_level_intermediate() {
    let src = r#"
        function Increment(In) => (Out);

        composition IncrementTwice(Numbers) => (Result) {
            Increment(In = each Numbers) => (Once = Out);
            Increment(In = each Once) => (Result = Out);
        }
    "#;
    let registry = TestRegistry::new().with_function("Increment", &["In"], &["Out"]);
    let template = parse_composition(src, "IncrementTwice", &registry);
    let composition = Composition::from_template(&template, AnyShardingMode::MaxSharding, &registry);

    let numbers = data_set(vec![item("1", 0), item("2", 0), item("3", 0)]);
    let outputs = run_to_completion(composition, vec![numbers], |invocation| {
        let n: i64 = invocation.input[0].items[0].ident.parse().unwrap();
        let key = invocation.input[0].items[0].key;
        vec![data_set(vec![item(&(n + 1).to_string(), key)])]
    });

    let mut values: Vec<i64> = outputs[0]
        .items
        .iter()
        .map(|i| i.ident.parse().unwrap())
        .collect();
    values.sort();
    assert_eq!(values, vec![3, 4, 5], "each number should have been incremented twice");
}

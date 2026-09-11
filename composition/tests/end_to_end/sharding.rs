use crate::common::{data_set, item, parse_composition, run_to_completion, TestRegistry};
use composition::{AnyShardingMode, Composition};

#[test]
fn keyed_inner_join_pairs_matching_composition_inputs() {
    let src = r#"
        function Combine(A, B) => (C);

        composition Join(InputA, InputB) => (Result) {
            Combine(
                A = keyed InputA,
                B = keyed InputB
            ) => (Result = C)
            by A inner B;
        }
    "#;
    let registry = TestRegistry::new().with_function("Combine", &["A", "B"], &["C"]);
    let template = parse_composition(src, "Join", &registry);
    let composition = Composition::from_template(&template, AnyShardingMode::MaxSharding, &registry);

    let input_a = data_set(vec![item("a1", 1), item("a2", 2), item("a3", 3)]);
    let input_b = data_set(vec![item("b2", 2), item("b3", 3), item("b4", 4)]);

    let mut seen_keys = Vec::new();
    let outputs = run_to_completion(composition, vec![input_a, input_b], |invocation| {
        assert_eq!(invocation.input.len(), 2);
        let a_item = &invocation.input[0].items[0];
        let b_item = &invocation.input[1].items[0];
        assert_eq!(a_item.key, b_item.key, "an inner join must only ever see matching keys");
        seen_keys.push(a_item.key);
        vec![data_set(vec![item("combined", a_item.key)])]
    });

    seen_keys.sort();
    assert_eq!(seen_keys, vec![2, 3], "keys 1 and 4 have no match on the other side");
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].items.len(), 2);
}

#[test]
fn two_stage_pipeline_aggregates_streamed_outputs() {
    let src = r#"
        function Square(In) => (Out);
        function Sum(All) => (Total);

        composition SumOfSquares(Numbers) => (Result) {
            Square(In = each Numbers) => (Squared = Out);
            Sum(All = all Squared) => (Result = Total);
        }
    "#;
    let registry = TestRegistry::new()
        .with_function("Square", &["In"], &["Out"])
        .with_function("Sum", &["All"], &["Total"]);
    let template = parse_composition(src, "SumOfSquares", &registry);
    let composition = Composition::from_template(&template, AnyShardingMode::MaxSharding, &registry);

    let numbers = data_set(vec![item("2", 0), item("3", 0), item("4", 0)]);
    let outputs = run_to_completion(composition, vec![numbers], |invocation| {
        match invocation.function_id.as_str() {
            "Square" => {
                let n: i64 = invocation.input[0].items[0].ident.parse().unwrap();
                vec![data_set(vec![item(&(n * n).to_string(), 0)])]
            }
            "Sum" => {
                let total: i64 = invocation.input[0]
                    .items
                    .iter()
                    .map(|i| i.ident.parse::<i64>().unwrap())
                    .sum();
                vec![data_set(vec![item(&total.to_string(), 0)])]
            }
            other => panic!("unexpected function invoked: {other}"),
        }
    });

    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].items.len(), 1);
    assert_eq!(outputs[0].items[0].ident, "29", "2^2 + 3^2 + 4^2 = 29");
}

/// Two independent `each`-sharded producers feed a function with two `each` inputs. Every arrival
/// on one side must wait for at least one item to already exist on the other side before it can
/// combine; over the whole run every (a, b) pair should still show up exactly once (a full
/// cross-product), regardless of arrival order.
#[test]
fn two_each_inputs_wait_for_both_sides_before_combining() {
    let src = r#"
        function Emit(X) => (Y);
        function Combine(A, B) => (C);

        composition Pipe(InA, InB) => (Out) {
            Emit(X = each InA) => (MidA = Y);
            Emit(X = each InB) => (MidB = Y);
            Combine(A = each MidA, B = each MidB) => (Out = C);
        }
    "#;
    let registry = TestRegistry::new()
        .with_function("Emit", &["X"], &["Y"])
        .with_function("Combine", &["A", "B"], &["C"]);
    let template = parse_composition(src, "Pipe", &registry);
    let composition = Composition::from_template(&template, AnyShardingMode::MaxSharding, &registry);

    let in_a = data_set(vec![item("a1", 1), item("a2", 2)]);
    let in_b = data_set(vec![item("b1", 1), item("b2", 2)]);

    let mut combine_pairs = Vec::new();
    let outputs = run_to_completion(composition, vec![in_a, in_b], |invocation| {
        match invocation.function_id.as_str() {
            // Pass the item through unchanged, preserving its key, so Combine can be checked below.
            "Emit" => vec![data_set(vec![invocation.input[0].items[0].clone()])],
            "Combine" => {
                assert_eq!(invocation.input[0].items.len(), 1, "never combine against an empty side");
                assert_eq!(invocation.input[1].items.len(), 1, "never combine against an empty side");
                combine_pairs.push((
                    invocation.input[0].items[0].key,
                    invocation.input[1].items[0].key,
                ));
                vec![data_set(vec![item("out", 0)])]
            }
            other => panic!("unexpected function invoked: {other}"),
        }
    });

    combine_pairs.sort();
    assert_eq!(
        combine_pairs,
        vec![(1, 1), (1, 2), (2, 1), (2, 2)],
        "every A/B pair should appear exactly once, in whatever order they streamed in"
    );
    assert_eq!(outputs[0].items.len(), 4);
}

/// An optional input that ends up genuinely empty is treated as absent once it's known to be
/// complete: it neither blocks nor appears in the resulting invocations.
#[test]
fn optional_each_input_that_stays_empty_does_not_block_or_appear() {
    let src = r#"
        function Combine(A, B) => (C);

        composition Pipe(InA, InB) => (Out) {
            Combine(A = each InA, B = optional each InB) => (Out = C);
        }
    "#;
    let registry = TestRegistry::new().with_function("Combine", &["A", "B"], &["C"]);
    let template = parse_composition(src, "Pipe", &registry);
    let composition = Composition::from_template(&template, AnyShardingMode::MaxSharding, &registry);

    let in_a = data_set(vec![item("a1", 1), item("a2", 2)]);
    let in_b = data_set(vec![]); // a real composition input, but genuinely empty

    let mut invocation_count = 0;
    let outputs = run_to_completion(composition, vec![in_a, in_b], |invocation| {
        invocation_count += 1;
        assert_eq!(invocation.input[0].items.len(), 1, "one A item per invocation");
        assert!(invocation.input[1].items.is_empty(), "B is optional and stayed empty");
        vec![data_set(vec![item("out", 0)])]
    });

    assert_eq!(invocation_count, 2, "one invocation per InA item, B just came through empty");
    assert_eq!(outputs[0].items.len(), 2);
}

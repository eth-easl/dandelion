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

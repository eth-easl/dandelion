//! The sharding/join syntax: `all`/`each`/`keyed`/`anyKeyed`/`anyEach` input descriptors and the
//! `by ... strategy ...` join-order clause. Most tests only check that valid combinations are
//! accepted. The `join_order_*` tests additionally run the parsed composition to check that the
//! order given in the `by` clause is what determines the joined keys.

use crate::common::{item, parse_composition, run_to_completion, TestRegistry};
use composition::{AnyShardingMode, Composition, CompositionTemplate};
use memory::DataSet;
use std::{collections::BTreeMap, sync::Arc};

#[test]
fn keyed_and_any_sharding_combination_parses() {
    let src = r#"
        function FunA(A, B) => (C);
        function FunB(A, B, C) => (D);
        function FunC(D) => (E);

        composition Test(InputA, InputB, InputC) => (OutputE) {
            FunA(
                A = keyed InputA,
                B = keyed InputB
            ) => (InterC = C);

            FunB(
                A = keyed InputA,
                B = anyKeyed InputB,
                C = anyEach InputC
            ) => (InterD = D);

            FunC(D = all InterD) => (OutputE = E);
        }
    "#;
    let registry = TestRegistry::new()
        .with_function("FunA", &["A", "B"], &["C"])
        .with_function("FunB", &["A", "B", "C"], &["D"])
        .with_function("FunC", &["D"], &["E"]);

    CompositionTemplate::parse(src, &registry)
        .unwrap_or_else(|e| panic!("expected to parse:\n{e}"));
}

#[test]
fn explicit_join_order_with_multiple_strategies_parses() {
    let src = r#"
        function FunA(A, B, C) => (D);

        composition Test(InputA, InputB, InputC) => (OutputD) {
            FunA(
                A = keyed InputA,
                B = keyed InputB,
                C = keyed InputC
            ) => (OutputD = D)
            by A right B full C;
        }
    "#;
    let registry = TestRegistry::new().with_function("FunA", &["A", "B", "C"], &["D"]);

    CompositionTemplate::parse(src, &registry)
        .unwrap_or_else(|e| panic!("expected to parse:\n{e}"));
}

#[test]
fn cross_join_resets_the_join_group_and_parses() {
    let src = r#"
        function FunA(A, B, C, D) => (E);

        composition Test(InputA, InputB, InputC, InputD) => (OutputE) {
            FunA(
                A = keyed InputA,
                B = keyed InputB,
                C = keyed InputC,
                D = keyed InputD
            ) => (OutputE = E)
            by A inner B cross C left D;
        }
    "#;
    let registry = TestRegistry::new().with_function("FunA", &["A", "B", "C", "D"], &["E"]);

    CompositionTemplate::parse(src, &registry)
        .unwrap_or_else(|e| panic!("expected to parse:\n{e}"));
}

/// Parses a composition `Test` around a single three-argument function `Fun(A, B, C)` with all
/// arguments `keyed` and the given `by` clause, runs it on the given input keys and returns, for
/// every key that got joined, which of the arguments `A`, `B`, `C` had items for that key. The
/// order of the booleans is the declaration order of the arguments, independent of the clause.
fn run_keyed_join(by_clause: &str, keys: [&[u32]; 3]) -> BTreeMap<u32, [bool; 3]> {
    let src = format!(
        r#"
        function Fun(A, B, C) => (D);

        composition Test(InputA, InputB, InputC) => (Out) {{
            Fun(
                A = keyed InputA,
                B = keyed InputB,
                C = keyed InputC
            ) => (Out = D)
            by {by_clause};
        }}
    "#
    );
    let registry = TestRegistry::new().with_function("Fun", &["A", "B", "C"], &["D"]);
    let template = parse_composition(&src, "Test", &registry);
    let composition =
        Composition::from_template(&template, AnyShardingMode::MaxSharding, &registry);
    let inputs = keys
        .iter()
        .map(|keys| {
            let items = keys.iter().map(|&key| item(&format!("k{key}"), key));
            DataSet::from_items(Arc::new(items.collect()))
        })
        .collect();

    let mut joined = BTreeMap::new();
    run_to_completion(composition, inputs, |invocation| {
        assert_eq!(invocation.input.len(), 3);
        let present = [0, 1, 2].map(|i| !invocation.input[i].items.is_empty());
        let key = invocation
            .input
            .iter()
            .flat_map(|set| set.items.first())
            .map(|item| item.key)
            .next()
            .expect("an invocation must have at least one non-empty input");
        for set in &invocation.input {
            assert!(
                set.items.iter().all(|item| item.key == key),
                "all items of one invocation must share the key {key}"
            );
        }
        assert!(
            joined.insert(key, present).is_none(),
            "key {key} must only be joined once"
        );
        vec![Arc::new(vec![item("out", key)])]
    });
    joined
}

#[test]
fn join_order_decides_which_side_of_a_left_join_is_kept() {
    // C only adds the key 9, which is full-joined and does not interact with the keys of A and B
    let keys: [&[u32]; 3] = [&[1, 2, 3], &[2, 3], &[9]];
    // A left B keeps every key of A, even the one without a match in B
    let a_left_b = run_keyed_join("A left B full C", keys);
    assert_eq!(
        a_left_b,
        BTreeMap::from([
            (1, [true, false, false]),
            (2, [true, true, false]),
            (3, [true, true, false]),
            (9, [false, false, true]),
        ])
    );
    // B left A keeps every key of B, which are fewer. The sets stay in declaration order.
    let b_left_a = run_keyed_join("B left A full C", keys);
    assert_eq!(
        b_left_a,
        BTreeMap::from([
            (2, [true, true, false]),
            (3, [true, true, false]),
            (9, [false, false, true]),
        ])
    );
}

#[test]
fn join_order_does_not_have_to_follow_the_declaration_order() {
    let keys: [&[u32]; 3] = [&[1, 2, 3, 4], &[2, 3, 4, 5], &[3, 4, 5]];
    // (A inner B) left C: keys 2, 3, 4 of A and B, key 2 has no match in C
    let in_declaration_order = run_keyed_join("A inner B left C", keys);
    assert_eq!(
        in_declaration_order,
        BTreeMap::from([
            (2, [true, true, false]),
            (3, [true, true, true]),
            (4, [true, true, true]),
        ])
    );
    // (C inner A) left B: keys 3, 4 of C and A, which are all present in B
    let permuted = run_keyed_join("C inner A left B", keys);
    assert_eq!(
        permuted,
        BTreeMap::from([(3, [true, true, true]), (4, [true, true, true])])
    );
}

#[test]
fn join_order_is_applied_left_to_right_across_three_sets() {
    let keys: [&[u32]; 3] = [&[1, 2], &[2, 3], &[3, 4]];
    // (A right B) full C: right keeps the keys 2, 3 of B, full adds the key 4 of C
    let right_then_full = run_keyed_join("A right B full C", keys);
    assert_eq!(
        right_then_full,
        BTreeMap::from([
            (2, [true, true, false]),
            (3, [false, true, true]),
            (4, [false, false, true]),
        ])
    );
    // (A full C) right B: full yields 1, 2, 3, 4 and right restricts it to the keys 2, 3 of B
    let full_then_right = run_keyed_join("A full C right B", keys);
    assert_eq!(
        full_then_right,
        BTreeMap::from([(2, [true, true, false]), (3, [false, true, true])])
    );
}

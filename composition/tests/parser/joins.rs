//! Parsing (not execution) of the sharding/join syntax: `all`/`each`/`keyed`/`anyKeyed`/`anyEach`
//! input descriptors and the `by ... strategy ...` join-order clause. Semantic correctness of the
//! resulting joins is covered by the `end_to_end` and `src`-level sharding/iterator tests; these
//! only check that valid combinations are accepted.

use crate::common::TestRegistry;
use composition::CompositionTemplate;

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

    CompositionTemplate::parse(src, &registry).unwrap_or_else(|e| panic!("expected to parse:\n{e}"));
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

    CompositionTemplate::parse(src, &registry).unwrap_or_else(|e| panic!("expected to parse:\n{e}"));
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

    CompositionTemplate::parse(src, &registry).unwrap_or_else(|e| panic!("expected to parse:\n{e}"));
}

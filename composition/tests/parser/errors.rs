use crate::common::TestRegistry;
use composition::CompositionTemplate;
use dandelion_commons::DandelionError;

/// Asserts that `src` fails to parse/build with an error message containing `expected_substring`.
/// (`CompositionTemplate` isn't `Debug`, so this can't just go through `Result::expect_err`.)
fn expect_parse_error(src: &str, registry: &TestRegistry, expected_substring: &str) {
    let Err(err) = CompositionTemplate::parse(src, registry) else {
        panic!("expected a parse error");
    };
    let DandelionError::Parsing(msg) = &err.error else {
        panic!("expected a Parsing error, got {:?}", err.error);
    };
    assert!(
        msg.contains(expected_substring),
        "error message did not contain {expected_substring:?}:\n{msg}"
    );
}

#[test]
fn calling_an_undeclared_function_is_rejected() {
    let src = r#"
        function Foo(A) => (B);
        composition Test(X) => (Y) {
            Bar(A = all X) => (Y = B);
        }
    "#;
    let registry = TestRegistry::new().with_function("Foo", &["A"], &["B"]);
    expect_parse_error(src, &registry, "Unknown function 'Bar'");
}

#[test]
fn declaration_mismatched_with_the_registry_is_rejected() {
    let src = r#"
        function Foo(A) => (B);
    "#;
    // The registry knows a `Foo` with a different signature than the one declared above.
    let registry = TestRegistry::new().with_function("Foo", &["Other"], &["B"]);
    let Err(err) = CompositionTemplate::parse(src, &registry) else {
        panic!("expected a parse error");
    };
    assert!(matches!(err.error, DandelionError::Parsing(_)));
}

#[test]
fn duplicate_composition_identifier_is_rejected() {
    let src = r#"
        composition Test(X) => (X) {}
        composition Test(X) => (X) {}
    "#;
    let registry = TestRegistry::new();
    expect_parse_error(src, &registry, "already taken");
}

#[test]
fn joining_the_same_argument_twice_is_rejected() {
    let src = r#"
        function FunA(A, B) => (C);
        composition Test(InputA, InputB) => (OutputC) {
            FunA(
                A = keyed InputA,
                B = keyed InputB
            ) => (OutputC = C)
            by A cross A;
        }
    "#;
    let registry = TestRegistry::new().with_function("FunA", &["A", "B"], &["C"]);
    expect_parse_error(src, &registry, "twice");
}

#[test]
fn unknown_argument_name_is_rejected() {
    let src = r#"
        function FunA(A, B) => (C);
        composition Test(InputA, InputB) => (OutputC) {
            FunA(Z = all InputA, B = all InputB) => (OutputC = C);
        }
    "#;
    let registry = TestRegistry::new().with_function("FunA", &["A", "B"], &["C"]);
    expect_parse_error(src, &registry, "does not match any of the declared arguments");
}

#[test]
fn undefined_data_set_is_rejected() {
    let src = r#"
        function FunA(A, B) => (C);
        composition Test(InputA, InputB) => (OutputC) {
            FunA(A = all Ghost, B = all InputB) => (OutputC = C);
        }
    "#;
    let registry = TestRegistry::new().with_function("FunA", &["A", "B"], &["C"]);
    expect_parse_error(src, &registry, "Could not find data set Ghost");
}

#[test]
fn mixing_keyed_and_any_keyed_in_a_join_is_rejected() {
    let src = r#"
        function FunA(A, B) => (C);
        composition Test(InputA, InputB) => (OutputC) {
            FunA(
                A = keyed InputA,
                B = anyKeyed InputB
            ) => (OutputC = C)
            by A inner B;
        }
    "#;
    let registry = TestRegistry::new().with_function("FunA", &["A", "B"], &["C"]);
    expect_parse_error(src, &registry, "Mixing keyed and anyKeyed shardings");
}

#[test]
fn joining_a_non_keyed_sharding_is_rejected() {
    let src = r#"
        function FunA(A, B) => (C);
        composition Test(InputA, InputB) => (OutputC) {
            FunA(
                A = all InputA,
                B = all InputB
            ) => (OutputC = C)
            by A cross B;
        }
    "#;
    let registry = TestRegistry::new().with_function("FunA", &["A", "B"], &["C"]);
    expect_parse_error(src, &registry, "Joining set with non-keyed sharding");
}

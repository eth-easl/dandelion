use crate::common::TestRegistry;
use composition::CompositionTemplate;

fn expansion_registry() -> TestRegistry {
    TestRegistry::new()
        .with_function("plan_join", &["stats"], &["result", "__expand"])
        .with_function("split", &["in"], &["parts", "__expand"])
}

fn assert_parses(src: &str) {
    if let Err(err) = CompositionTemplate::parse(src, &expansion_registry()) {
        panic!("expected composition to parse, got {:?}", err.error);
    }
}

#[test]
fn forwarded_sets_can_be_bound_in_the_application() {
    assert_parses(r#"
        function plan_join(stats) => (result) expands (r, s, t);
        composition Query(R, S, T, Stats) => (Out) {
            plan_join(stats = all Stats, r = all R, s = all S, t = all T) => (Out = result);
        }
    "#);
}

#[test]
fn forwarded_sets_can_be_joined() {
    assert_parses(r#"
        function plan_join(stats) => (result) expands (r, s, t);
        composition Query(R, S, T, Stats) => (Out) {
            plan_join(stats = keyed Stats, r = keyed R, s = all S, t = all T) => (Out = result)
                by stats inner r;
        }
    "#);
}

#[test]
fn expansion_target_may_be_defined_after_the_declaration() {
    // recursive expansion: the target uses the expanding function itself
    assert_parses(r#"
        function split(in) => (parts) expands (parts) into (SplitFurther);
        composition SplitFurther(Pieces) => (Parts) {
            split(in = each Pieces) => (Parts = parts);
        }
    "#);
}



use std::{
    sync::{mpsc, Arc, Barrier},
    thread,
    time::Duration,
};

use crate::common::{data_set, item, parse_composition, TestRegistry};
use composition::{AnyShardingMode, Composition};
use dandelion_commons::data::{DataSet, Invocation};

/// How long a round may take before it is considered deadlocked.
const DEADLOCK_TIMEOUT: Duration = Duration::from_secs(10);

/// Runs every group of invocations on its own thread (all starting at the same time), answering
/// each invocation with `respond` and collecting all follow-up invocations per thread.
/// Panics if the threads don't finish within `DEADLOCK_TIMEOUT` or one of them panics.
fn run_groups_concurrently(
    composition: &Arc<Composition>,
    groups: Vec<Vec<Invocation>>,
    respond: fn(&Invocation) -> Vec<DataSet>,
) -> Vec<Invocation> {
    let barrier = Arc::new(Barrier::new(groups.len()));
    let (tx, rx) = mpsc::channel();
    let num_groups = groups.len();
    for group in groups {
        let (composition, barrier, tx) = (composition.clone(), barrier.clone(), tx.clone());
        thread::spawn(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                barrier.wait();
                let mut follow_ups = Vec::new();
                for invocation in group {
                    let output = respond(&invocation);
                    composition.push_invocation_output(output, invocation.composition_idx, &mut follow_ups);
                }
                follow_ups
            }));
            // release the composition before reporting back so the caller can take it back
            drop(composition);
            let _ = tx.send(result);
        });
    }

    let mut follow_ups = Vec::new();
    for _ in 0..num_groups {
        match rx.recv_timeout(DEADLOCK_TIMEOUT) {
            Ok(Ok(invocations)) => follow_ups.extend(invocations),
            Ok(Err(_)) => panic!("a worker thread panicked"),
            Err(_) => panic!("composition deadlocked"),
        }
    }
    follow_ups
}

fn pass_through(invocation: &Invocation) -> Vec<DataSet> {
    vec![data_set(vec![invocation.input[0].items[0].clone()])]
}

/// Two producers streaming into the two `each` inputs of the same function at the same time used
/// to deadlock: each thread held its set's lock while waiting for the other one.
#[test]
fn concurrent_streams_into_two_each_inputs_neither_deadlock_nor_duplicate() {
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
    let n = 100;

    for _ in 0..50 {
        let mut composition = Composition::from_template(&template, AnyShardingMode::MaxSharding, &registry);
        let in_a = data_set((0..n).map(|i| item(&format!("a{i}"), i)).collect());
        let in_b = data_set((0..n).map(|i| item(&format!("b{i}"), i)).collect());
        let mut initial = Vec::new();
        composition.start_execution(vec![in_a, in_b], &mut initial);
        let (side_a, side_b) = initial
            .into_iter()
            .partition(|invocation| invocation.input[0].items[0].ident.starts_with('a'));

        let composition = Arc::new(composition);
        let combines = run_groups_concurrently(&composition, vec![side_a, side_b], pass_through);

        let mut pairs: Vec<_> = combines
            .iter()
            .map(|invocation| {
                assert_eq!(invocation.function_id.as_str(), "Combine");
                (invocation.input[0].items[0].key, invocation.input[1].items[0].key)
            })
            .collect();
        pairs.sort();
        let expected: Vec<_> = (0..n).flat_map(|a| (0..n).map(move |b| (a, b))).collect();
        assert_eq!(pairs, expected, "every A/B pair should appear exactly once");
    }
}

/// When the last two invocations of a function finished at the same time, both could consider
/// themselves the last one, so one of them pushed its output after the function already completed
/// its output set (losing the items).
#[test]
fn concurrent_last_invocations_of_a_function_keep_all_output() {
    let src = r#"
        function Emit(X) => (Y);

        composition Pipe(InA) => (Out) {
            Emit(X = each InA) => (Out = Y);
        }
    "#;
    let registry = TestRegistry::new().with_function("Emit", &["X"], &["Y"]);
    let template = parse_composition(src, "Pipe", &registry);

    for _ in 0..2000 {
        let mut composition = Composition::from_template(&template, AnyShardingMode::MaxSharding, &registry);
        let mut initial = Vec::new();
        composition.start_execution(vec![data_set(vec![item("a0", 0), item("a1", 1)])], &mut initial);
        let groups = initial.into_iter().map(|invocation| vec![invocation]).collect();

        let composition = Arc::new(composition);
        let follow_ups = run_groups_concurrently(&composition, groups, pass_through);
        assert!(follow_ups.is_empty());

        let composition = Arc::into_inner(composition).expect("all threads are done");
        assert_eq!(composition.collect()[0].items.len(), 2, "both outputs must arrive");
    }
}

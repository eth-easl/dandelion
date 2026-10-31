use super::*;
use dandelion_commons::RunId;
use machine_interface::{
    composition::{FunctionDependencies, IoRequestId, ItemData},
    function_driver::{
        functions::SystemFunction,
        system_driver::{
            async_io_policy, recovery_log, IoCompletionData, IoCompletionItem,
            IoCompletionOutputSet, IoCompletionRecord, IoPath, UncoordinatedIo,
        },
        WorkDone,
    },
    memory_domain::{malloc::MallocMemoryDomain, read_only::ReadOnlyContext, ContextTrait},
    DataItem, Position,
};
use std::sync::OnceLock;

fn dispatcher() -> Dispatcher {
    let dispatcher = Dispatcher {
        function_registry: FunctionRegistry::new(),
        work_queue: WorkQueue::init(),
        domains: vec![],
        any_sharding_mode: AnyShardingMode::MaxSharding,
        composition_id: AtomicUsize::new(0),
    };
    for name in ["A", "B", "C"] {
        dispatcher
            .function_registry
            .insert_function(
                Arc::new(name.into()),
                EngineType::System,
                Arc::new(Box::new(MallocMemoryDomain {})),
                0,
                concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml").into(),
                Metadata {
                    input_sets: vec![("input".into(), None)],
                    output_sets: vec!["output".into()],
                    min_set_bytes: vec![0],
                },
            )
            .unwrap();
    }
    dispatcher
}

fn dependency(name: &str, input: usize, outputs: &[usize]) -> FunctionDependencies {
    FunctionDependencies {
        function: Arc::new(name.into()),
        input_set_ids: vec![Some(InputSetDescriptor {
            composition_id: input,
            sharding: ShardingMode::All,
            optional: false,
        })],
        output_set_ids: outputs.iter().copied().map(Some).collect(),
        join_info: (vec![0], vec![]),
    }
}

fn chain() -> Composition {
    Composition {
        dependencies: vec![
            dependency("A", 0, &[1]),
            dependency("HTTP", 1, &[2, 3]),
            dependency("B", 3, &[4]),
        ],
        output_map: BTreeMap::from([(4, 0)]),
    }
}

fn set(values: &[u8]) -> CompositionSet {
    CompositionSet::from_item_list(
        "requests".into(),
        values
            .iter()
            .map(|&value| {
                (
                    DataItem {
                        ident: "duplicate".into(),
                        key: 7,
                        data: Position { offset: 0, size: 1 },
                    },
                    ItemData::LocalData(Arc::new(
                        ReadOnlyContext::from_boxed(vec![value].into_boxed_slice()).unwrap(),
                    )),
                )
            })
            .collect(),
    )
    .unwrap()
}

fn bytes(set: &CompositionSet) -> Vec<u8> {
    set.into_iter()
        .map(|(item, data)| {
            let ItemData::LocalData(context) = data else {
                panic!("expected restored bytes")
            };
            let mut byte = [0];
            context.context.read(item.data.offset, &mut byte).unwrap();
            byte[0]
        })
        .collect()
}

async fn recovery(path: &[usize], n: usize, indices: &[usize]) -> RunId {
    static ROOT: OnceLock<()> = OnceLock::new();
    ROOT.get_or_init(|| {
        recovery_log::set_recovery_log_root(
            std::env::temp_dir().join(format!("dandelion-pruning-{}", RunId::now_v7())),
        )
        .unwrap()
    });
    let run_id = RunId::now_v7();
    let path = IoPath::from(path.to_vec());
    recovery_log::append_run_log_line(
        run_id,
        &format!(
            "event=io_call_size run_id={} io_path={} function=HTTP n={}\n",
            run_id, path, n
        ),
    )
    .await
    .unwrap();
    let records = indices
        .iter()
        .map(|&index| IoCompletionRecord {
            id: IoRequestId {
                run_id,
                io_path: path.clone(),
                request_item_index: index,
            },
            function: SystemFunction::HTTP,
            claim_id: None,
            outputs: ["headers", "bodies"]
                .into_iter()
                .enumerate()
                .map(|(set_index, name)| IoCompletionOutputSet {
                    set_index,
                    set_name: name.into(),
                    items: vec![IoCompletionItem {
                        identifier: "duplicate".into(),
                        key: 7,
                        location: IoCompletionData::Inline(vec![
                            10 * (set_index as u8 + 1) + index as u8,
                        ]),
                    }],
                })
                .collect(),
        })
        .collect::<Vec<_>>();
    recovery_log::install_recovered_io_calls(run_id, &records)
        .await
        .unwrap();
    recovery_log::install_recovered_io_records(run_id, records).unwrap();
    run_id
}

fn cleanup(run_id: RunId) {
    recovery_log::clear_recovered_io(run_id);
    std::fs::remove_file(recovery_log::run_log_path(run_id).unwrap()).unwrap();
}

async fn execute(
    dispatcher: &Dispatcher,
    composition: Composition,
    run_id: RunId,
    expected: &[&str],
    inspect: impl Fn(&str, &[Option<CompositionSet>]),
) -> Vec<Option<CompositionSet>> {
    let recorder = Recorder::new(
        run_id,
        Arc::new("pruning-test".into()),
        std::time::Instant::now(),
    );
    let invocation = dispatcher.queue_composition(
        0,
        composition,
        vec![Some(set(&[1]))],
        false,
        async_io_policy(run_id),
        recorder,
    );
    // A fake compute engine observes actual queue submissions; no executable or HTTP server runs.
    let engine = async {
        let mut seen = Vec::new();
        for _ in expected {
            let (work, debt, _) = loop {
                let notifier = dispatcher.work_queue.queueing_notifier();
                let queued = notifier.notified();
                // A worker can take both local and unresolved inputs. Inspect lazy inputs
                // directly in partial-call tests, without issuing external requests.
                if let Some(work) = dispatcher
                    .work_queue
                    .try_get_work_for_remote(1, 99, 1)
                    .pop()
                {
                    break work;
                }
                queued.await;
            };
            let WorkToDo::FunctionArguments {
                function_id,
                input_sets,
                ..
            } = work
            else {
                panic!("expected compute")
            };
            inspect(&function_id, &input_sets);
            seen.push((*function_id).clone());
            debt.fulfill(Ok(WorkDone::CompositionSet(vec![Some(set(&[30, 31]))])));
        }
        seen.sort();
        let mut expected = expected
            .iter()
            .map(|name| name.to_string())
            .collect::<Vec<_>>();
        expected.sort();
        assert_eq!(seen, expected);
    };
    let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        futures::join!(invocation, engine)
    })
    .await
    .expect("dispatcher stalled");
    result.unwrap()
}

#[tokio::test]
async fn restored_http_skips_upstream_compute_and_preserves_downstream_order() {
    let dispatcher = dispatcher();
    let run_id = recovery(&[1], 2, &[1, 0]).await;
    let outputs = execute(&dispatcher, chain(), run_id, &["B"], |name, inputs| {
        assert_eq!(name, "B");
        assert_eq!(bytes(inputs[0].as_ref().unwrap()), vec![20, 21]);
    })
    .await;
    assert_eq!(bytes(outputs[0].as_ref().unwrap()), vec![30, 31]);
    cleanup(run_id);
}

#[tokio::test]
async fn shared_upstream_compute_is_still_queued_for_a_live_consumer() {
    let dispatcher = dispatcher();
    let run_id = recovery(&[1], 1, &[0]).await;
    let mut composition = chain();
    composition.dependencies.push(dependency("C", 1, &[5]));
    composition.output_map.insert(5, 1);
    execute(
        &dispatcher,
        composition,
        run_id,
        &["A", "B", "C"],
        |name, inputs| {
            if name == "B" {
                assert_eq!(bytes(inputs[0].as_ref().unwrap()), vec![20]);
            }
            if name == "C" {
                assert_eq!(bytes(inputs[0].as_ref().unwrap()), vec![30, 31]);
            }
        },
    )
    .await;
    cleanup(run_id);
}

#[tokio::test]
async fn unreachable_restored_call_is_not_injected_or_resolved() {
    let dispatcher = dispatcher();
    let run_id = recovery(&[1], 1, &[0]).await;
    let mut composition = chain();
    composition.output_map = BTreeMap::from([(0, 0)]);
    let plan = ReplayPlan::discover(&composition, &async_io_policy(run_id)).unwrap();
    assert!(plan.restored.is_empty());
    assert!(plan.needed.iter().all(|needed| !needed));
    let outputs = execute(&dispatcher, composition, run_id, &[], |_, _| unreachable!()).await;
    assert_eq!(bytes(outputs[0].as_ref().unwrap()), vec![1]);
    cleanup(run_id);
}

#[tokio::test]
async fn partial_call_runs_upstream_and_preserves_per_item_reuse() {
    let dispatcher = dispatcher();
    let run_id = recovery(&[1], 2, &[0]).await;
    assert!(ReplayPlan::discover(&chain(), &async_io_policy(run_id)).is_none());
    execute(&dispatcher, chain(), run_id, &["A", "B"], |name, inputs| {
        if name == "B" {
            let items = inputs[0].as_ref().unwrap().into_iter().collect::<Vec<_>>();
            assert_eq!(items.len(), 2);
            #[cfg(not(feature = "exactly-once"))]
            assert!(matches!(&items[0].1, ItemData::LocalData(_)));
            #[cfg(feature = "exactly-once")]
            assert!(matches!(&items[0].1, ItemData::CoordinatedIoData(_)));
            assert!(matches!(&items[1].1, ItemData::CoordinatedIoData(_)));
        }
    })
    .await;
    cleanup(run_id);
}

#[tokio::test]
async fn first_attempt_keeps_all_functions_even_when_outputs_are_unused() {
    let dispatcher = dispatcher();
    let run_id = RunId::now_v7();
    let mut composition = chain();
    composition.dependencies.push(dependency("C", 0, &[5]));
    assert!(ReplayPlan::discover(&composition, &async_io_policy(run_id)).is_none());
    assert!(ReplayPlan::discover(&composition, &UncoordinatedIo).is_none());
    execute(
        &dispatcher,
        composition,
        run_id,
        &["A", "B", "C"],
        |_, _| {},
    )
    .await;
}

#[tokio::test]
async fn empty_restored_call_skips_upstream_and_propagates_empty_outputs() {
    let dispatcher = dispatcher();
    let run_id = recovery(&[1], 0, &[]).await;
    let outputs = execute(&dispatcher, chain(), run_id, &[], |_, _| unreachable!()).await;
    assert!(outputs[0].is_none());
    cleanup(run_id);
}

#[tokio::test]
async fn nested_body_prunes_but_outer_input_producer_still_runs() {
    let dispatcher = dispatcher();
    dispatcher
        .function_registry
        .insert_compositions(
            r#"
        function A (input) => (output);
        function B (input) => (output);
        function HTTP (requests) => (headers, bodies);
        composition Inner (source) => (result) {
            A (input = all source) => (requests = output);
            HTTP (requests = all requests) => (bodies = bodies);
            B (input = all bodies) => (result = output);
        }
    "#,
        )
        .unwrap();
    let inner = match dispatcher
        .function_registry
        .get_function(&Arc::new("Inner".into()))
        .unwrap()
    {
        FunctionType::Composition(info) => info,
        _ => panic!("expected nested composition"),
    };
    let http_index = inner
        .composition
        .dependencies
        .iter()
        .position(|dep| dep.function.as_str() == "HTTP")
        .unwrap();
    let run_id = recovery(&[1, http_index], 1, &[0]).await;
    let outer = Composition {
        dependencies: vec![dependency("C", 0, &[1]), dependency("Inner", 1, &[2])],
        output_map: BTreeMap::from([(2, 0)]),
    };
    assert!(ReplayPlan::discover(&outer, &async_io_policy(run_id)).is_none());
    execute(&dispatcher, outer, run_id, &["C", "B"], |name, inputs| {
        if name == "B" {
            assert_eq!(bytes(inputs[0].as_ref().unwrap()), vec![20]);
        }
    })
    .await;
    cleanup(run_id);
}

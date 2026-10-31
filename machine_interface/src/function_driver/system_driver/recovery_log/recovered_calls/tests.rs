use super::*;
use crate::memory_domain::ContextTrait;

fn path() -> IoPath {
    vec![2, 3].into()
}

fn sizes(n: usize) -> HashMap<IoPath, (SystemFunction, usize)> {
    HashMap::from([(path(), (SystemFunction::HTTP, n))])
}

fn record(run_id: RunId, index: usize) -> IoCompletionRecord {
    IoCompletionRecord {
        id: IoRequestId {
            run_id,
            io_path: path(),
            request_item_index: index,
        },
        function: SystemFunction::HTTP,
        claim_id: None,
        outputs: get_system_function_output_sets(SystemFunction::HTTP)
            .into_iter()
            .enumerate()
            .map(|(set_index, set_name)| IoCompletionOutputSet {
                set_index,
                set_name,
                items: vec![IoCompletionItem {
                    identifier: "duplicate".into(),
                    key: 7,
                    location: if set_index == 0 {
                        IoCompletionData::Inline(vec![index as u8, 42])
                    } else {
                        IoCompletionData::Remote {
                            node_id: 9,
                            data_id: 100 + index as u64,
                            size: index + 1,
                        }
                    },
                }],
            })
            .collect(),
    }
}

#[test]
fn complete_call_rebuilds_inline_and_remote_outputs_in_request_order() {
    let run_id = RunId::now_v7();
    let calls = rebuild_calls(
        run_id,
        &sizes(3),
        &[record(run_id, 2), record(run_id, 0), record(run_id, 1)],
    )
    .unwrap();
    let outputs = &calls[&path()];
    assert_eq!(outputs.len(), 2);
    for (set_index, set) in outputs.iter().enumerate() {
        let set = set.as_ref().unwrap();
        assert_eq!(
            set.get_name(),
            &get_system_function_output_sets(SystemFunction::HTTP)[set_index]
        );
        assert_eq!(set.len(), 3);
        for (index, (item, data)) in set.into_iter().enumerate() {
            assert_eq!(item.ident, "duplicate");
            assert_eq!(item.key, 7);
            assert_eq!(item.data.offset, 0);
            match data {
                ItemData::LocalData(context) => {
                    assert_eq!(set_index, 0);
                    assert_eq!(item.data.size, 2);
                    let mut bytes = [0; 2];
                    context.context.read(0, &mut bytes).unwrap();
                    assert_eq!(bytes, [index as u8, 42]);
                }
                ItemData::RemoteData(data) => {
                    assert_eq!(set_index, 1);
                    assert_eq!(data.node_id, 9);
                    assert_eq!(data.data_id, 100 + index as u64);
                    assert_eq!(data.incarnation, 0);
                    assert_eq!(item.data.size, index + 1);
                }
                _ => panic!("restored outputs must not execute I/O"),
            }
        }
    }
}

#[test]
fn empty_call_needs_size_record_and_restores_none_for_each_output() {
    let run_id = RunId::now_v7();
    let calls = rebuild_calls(run_id, &sizes(0), &[]).unwrap();
    assert_eq!(calls[&path()].len(), 2);
    assert!(calls[&path()].iter().all(Option::is_none));
    assert!(rebuild_calls(run_id, &HashMap::new(), &[])
        .unwrap()
        .is_empty());
    assert!(rebuild_calls(run_id, &HashMap::new(), &[record(run_id, 0)])
        .unwrap()
        .is_empty());
    assert!(rebuild_calls(run_id, &sizes(0), &[record(run_id, 0)])
        .unwrap()
        .is_empty());
}

#[test]
fn missing_and_out_of_range_items_do_not_restore_a_call() {
    let run_id = RunId::now_v7();
    for indices in [vec![0], vec![0, 2], vec![0, 1, 2]] {
        let records = indices
            .into_iter()
            .map(|index| record(run_id, index))
            .collect::<Vec<_>>();
        assert!(rebuild_calls(run_id, &sizes(2), &records)
            .unwrap()
            .is_empty());
    }
}

#[test]
fn identical_redelivery_counts_once_but_conflicting_results_prevent_restoration() {
    let run_id = RunId::now_v7();
    let first = record(run_id, 0);
    let mut duplicate = first.clone();
    duplicate.claim_id = Some(99);
    assert_eq!(
        rebuild_calls(run_id, &sizes(1), &[first.clone(), duplicate.clone()])
            .unwrap()
            .len(),
        1
    );
    // Two copies of item zero cannot stand in for a missing item one.
    assert!(
        rebuild_calls(run_id, &sizes(2), &[first.clone(), duplicate.clone()])
            .unwrap()
            .is_empty()
    );
    duplicate.outputs[0].items[0].location = IoCompletionData::Inline(vec![99]);
    assert!(rebuild_calls(run_id, &sizes(1), &[first, duplicate])
        .unwrap()
        .is_empty());
}

#[test]
fn incomplete_or_inconsistent_output_metadata_prevents_restoration() {
    let run_id = RunId::now_v7();
    let valid = record(run_id, 0);
    let mut missing_set = valid.clone();
    missing_set.outputs.pop();
    let mut duplicate_set = valid.clone();
    duplicate_set.outputs[1] = duplicate_set.outputs[0].clone();
    let mut out_of_range_set = valid.clone();
    out_of_range_set.outputs[1].set_index = 2;
    let mut empty_set = valid.clone();
    empty_set.outputs[0].items.clear();
    let mut extra_item = valid.clone();
    let duplicate_item = extra_item.outputs[0].items[0].clone();
    extra_item.outputs[0].items.push(duplicate_item);
    let mut different_function = valid.clone();
    different_function.function = SystemFunction::MEMCACHED;
    let mut wrong_name = valid.clone();
    wrong_name.outputs[0].set_name = "wrong".into();
    let mut inconsistent_key = valid.clone();
    inconsistent_key.outputs[0].items[0].key = 8;
    let mut overflowing_key = valid.clone();
    for output in &mut overflowing_key.outputs {
        output.items[0].key = u64::MAX;
    }
    for invalid in [
        missing_set,
        duplicate_set,
        out_of_range_set,
        empty_set,
        extra_item,
        different_function,
        wrong_name,
        inconsistent_key,
        overflowing_key,
    ] {
        assert!(rebuild_calls(run_id, &sizes(1), &[invalid])
            .unwrap()
            .is_empty());
    }
}

#[test]
fn calls_and_runs_are_isolated() {
    let run_id = RunId::now_v7();
    let mut other_call = record(run_id, 1);
    other_call.id.io_path = vec![8].into();
    let other_run = record(RunId::now_v7(), 1);
    assert!(rebuild_calls(
        run_id,
        &sizes(2),
        &[record(run_id, 0), other_call, other_run]
    )
    .unwrap()
    .is_empty());
}

#[test]
fn invalidated_checkpoint_makes_call_incomplete_until_replaced() {
    let run_id = RunId::now_v7();
    let first = record(run_id, 0);
    let second = record(run_id, 1);
    let mut log = format!(
        "event=io_call_size run_id={} io_path={} function=HTTP n=2\n",
        run_id,
        path()
    );
    log.push_str(&format_io_completion_line(&first).unwrap());
    log.push_str(&format_io_completion_line(&second).unwrap());
    let state = RunLogState::from_log(&log).unwrap();
    let records = recoverable_io_completion_records_in(&log).unwrap();
    assert_eq!(
        rebuild_calls(run_id, &state.call_sizes, &records)
            .unwrap()
            .len(),
        1
    );
    log.push_str(&io_record_line(
        IO_INVALIDATED_EVENT,
        &second,
        &encode_io_completion_payload(&second.outputs).unwrap(),
    ));
    let records = recoverable_io_completion_records_in(&log).unwrap();
    assert!(rebuild_calls(run_id, &state.call_sizes, &records)
        .unwrap()
        .is_empty());
    let mut replacement = second;
    replacement.outputs[1].items[0].location = IoCompletionData::Remote {
        node_id: 10,
        data_id: 500,
        size: 2,
    };
    log.push_str(&format_io_completion_line(&replacement).unwrap());
    let records = recoverable_io_completion_records_in(&log).unwrap();
    assert_eq!(
        rebuild_calls(run_id, &state.call_sizes, &records)
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn recovery_install_replaces_previous_attempt_and_clear_removes_both_maps() {
    super::super::call_size_tests::init_log_root();
    let run_id = RunId::now_v7();
    append_run_log_line(
        run_id,
        &format!(
            "event=io_call_size run_id={} io_path={} function=HTTP n=1\n",
            run_id,
            path()
        ),
    )
    .await
    .unwrap();
    let records = vec![record(run_id, 0)];
    install_recovered_io_calls(run_id, &records).await.unwrap();
    install_recovered_io_records(run_id, records.clone()).unwrap();
    assert!(recovered_io_call(run_id, &path()).is_some());
    assert!(recovered_io_call(RunId::now_v7(), &path()).is_none());
    install_recovered_io_calls(run_id, &[]).await.unwrap();
    assert!(recovered_io_call(run_id, &path()).is_none());
    install_recovered_io_calls(run_id, &records).await.unwrap();
    clear_recovered_io(run_id);
    assert!(recovered_io_call(run_id, &path()).is_none());
    assert!(recovered_io_item_location(&records[0].id, 0).is_none());
    fs::remove_file(run_log_path(run_id).unwrap()).unwrap();
}

use super::*;
use crate::{
    composition::{CompositionSet, ItemData},
    function_driver::system_driver::{convert_to_references, IoData, IoReferencePolicy},
    DataItem, Position,
};

pub(super) fn init_log_root() {
    static ROOT: OnceLock<()> = OnceLock::new();
    ROOT.get_or_init(|| {
        let root = std::env::temp_dir().join(format!("dandelion-call-size-{}", RunId::now_v7()));
        set_recovery_log_root(root).unwrap();
    });
}

fn size_record(run_id: RunId, n: usize) -> IoCallSizeRecord {
    IoCallSizeRecord {
        run_id,
        io_path: vec![2, 3].into(),
        function: SystemFunction::HTTP,
        n,
    }
}

#[tokio::test]
async fn call_size_is_written_once_and_remains_known_after_reload() {
    init_log_root();
    let run_id = RunId::now_v7();
    activate_async_run_logging(run_id);
    append_io_call_size_record(size_record(run_id, 5))
        .await
        .unwrap();
    append_io_call_size_record(size_record(run_id, 5))
        .await
        .unwrap();
    let content = read_run_log(run_id).await.unwrap();
    assert_eq!(content.lines().count(), 1);
    assert_eq!(
        content,
        format!(
            "event=io_call_size run_id={} io_path=/2/3 function=HTTP n=5\n",
            run_id
        )
    );
    let state = RunLogState::from_log(&content).unwrap();
    assert_eq!(
        state.call_sizes[&vec![2, 3].into()],
        (SystemFunction::HTTP, 5)
    );

    // Simulate the first append after a process restart, which reloads the existing log.
    *run_log_lock(run_id).lock().await = RunLogState::default();
    append_io_call_size_record(size_record(run_id, 5))
        .await
        .unwrap();
    assert_eq!(read_run_log(run_id).await.unwrap(), content);
    assert!(append_io_call_size_record(size_record(run_id, 6))
        .await
        .is_err());
    assert_eq!(read_run_log(run_id).await.unwrap(), content);
    deactivate_async_run_logging(run_id).await;
    fs::remove_file(run_log_path(run_id).unwrap()).unwrap();
}

#[tokio::test]
async fn empty_call_is_logged_and_terminal_state_discards_cached_sizes() {
    init_log_root();
    let run_id = RunId::now_v7();
    activate_async_run_logging(run_id);
    append_io_call_size_record(size_record(run_id, 0))
        .await
        .unwrap();
    let content = read_run_log(run_id).await.unwrap();
    let mut state = RunLogState::from_log(&content).unwrap();
    assert_eq!(state.call_sizes[&vec![2, 3].into()].1, 0);
    state
        .apply(&format!("event=invocation_completed run_id={}\n", run_id))
        .unwrap();
    state.apply(&content).unwrap();
    assert!(state.call_sizes.is_empty());
    deactivate_async_run_logging(run_id).await;
    fs::remove_file(run_log_path(run_id).unwrap()).unwrap();
}

#[test]
fn incomplete_size_record_is_ignored_and_malformed_records_are_rejected() {
    let line = format!(
        "event=io_call_size run_id={} io_path=/2 function=HTTP n=4\n",
        RunId::now_v7()
    );
    assert!(RunLogState::from_log(line.trim_end())
        .unwrap()
        .call_sizes
        .is_empty());
    for invalid in [
        line.replace("n=4", "n=bad"),
        line.replace("function=HTTP", "function=unknown"),
        line.replace(" n=4", ""),
    ] {
        assert!(RunLogState::from_log(&invalid).is_err());
    }
}

#[tokio::test]
async fn queued_appends_recheck_active_state_after_deactivation() {
    init_log_root();
    let run_id = RunId::now_v7();
    activate_async_run_logging(run_id);
    let run_lock = run_log_lock(run_id);
    let guard = run_lock.lock().await;

    // Poll in order to put deactivation ahead of both appends in Tokio's FIFO lock queue.
    // No timing assumptions: every operation is confirmed to be waiting for the lock.
    let mut deactivate = Box::pin(deactivate_async_run_logging(run_id));
    assert!(futures::poll!(&mut deactivate).is_pending());
    assert!(async_run_logging_active(run_id));
    let mut size_append = Box::pin(append_io_call_size_record(size_record(run_id, 1)));
    assert!(futures::poll!(&mut size_append).is_pending());
    let completion = IoCompletionRecord {
        id: IoRequestId {
            run_id,
            io_path: vec![2, 3].into(),
            request_item_index: 0,
        },
        function: SystemFunction::HTTP,
        outputs: vec![],
        claim_id: None,
    };
    let mut completion_append = Box::pin(append_io_completion_record(&completion));
    assert!(futures::poll!(&mut completion_append).is_pending());
    drop(guard);
    deactivate.await;
    size_append.await.unwrap();
    completion_append.await.unwrap();

    append_io_call_size_record(size_record(run_id, 1))
        .await
        .unwrap();
    append_io_completion_record(&completion).await.unwrap();
    assert!(!run_log_path(run_id).unwrap().exists());
}

#[test]
fn reference_conversion_records_the_whole_input_size_including_empty_calls() {
    #[derive(Clone)]
    struct RecordingPolicy(Arc<Mutex<Vec<(SystemFunction, usize)>>>);
    impl IoReferencePolicy for RecordingPolicy {
        fn record_call_size(&self, function: SystemFunction, n: usize) {
            self.0.lock().unwrap().push((function, n));
        }
        fn wrap(
            &self,
            request: IoData,
            _item: &DataItem,
            _index: usize,
        ) -> (ItemData, Option<usize>) {
            (ItemData::IoData(request), None)
        }
    }
    let policy = RecordingPolicy(Arc::new(Mutex::new(Vec::new())));
    let recorder = dandelion_commons::records::Recorder::new(
        RunId::now_v7(),
        Arc::new("test".into()),
        std::time::Instant::now(),
    );
    let context = Arc::new(ReadOnlyContext::from_boxed(vec![0].into_boxed_slice()).unwrap());
    let input = CompositionSet::from_item_list(
        "requests".into(),
        (0..3)
            .map(|key| {
                (
                    DataItem {
                        ident: "request".into(),
                        key,
                        data: Position { offset: 0, size: 1 },
                    },
                    ItemData::LocalData(context.clone()),
                )
            })
            .collect(),
    )
    .unwrap();
    convert_to_references(
        SystemFunction::HTTP,
        vec![Some(input)],
        policy.clone(),
        recorder.clone(),
    )
    .unwrap();
    let empty = convert_to_references(
        SystemFunction::HTTP,
        vec![None],
        policy.clone(),
        recorder.clone(),
    )
    .unwrap();
    assert!(empty.iter().all(Option::is_none));
    assert!(convert_to_references(SystemFunction::HTTP, vec![], policy.clone(), recorder).is_err());
    assert_eq!(
        *policy.0.lock().unwrap(),
        vec![(SystemFunction::HTTP, 3), (SystemFunction::HTTP, 0)]
    );
}

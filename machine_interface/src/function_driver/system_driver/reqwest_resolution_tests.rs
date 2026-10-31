use super::*;
use crate::{
    composition::{RemoteData, RemoteDataClient},
    DataItem,
};
use std::{collections::HashMap, future::Future, pin::Pin, sync::Mutex};
use tokio::sync::{mpsc, oneshot, OnceCell};

fn context(bytes: &[u8]) -> Arc<Context> {
    Arc::new(ReadOnlyContext::from_boxed(bytes.to_vec().into_boxed_slice()).unwrap())
}

fn item(key: u32, data: ItemData) -> (DataItem, ItemData) {
    (
        DataItem {
            ident: "duplicate".into(),
            key,
            data: Position { offset: 0, size: 1 },
        },
        data,
    )
}

fn local(value: u8) -> ItemData {
    ItemData::LocalData(context(&[value]))
}

fn cached_io(original_data: ItemData, result: DandelionResult<Vec<Arc<Context>>>) -> ItemData {
    ItemData::IoData(IoData {
        original_position: Position { offset: 0, size: 1 },
        original_data: Box::new(original_data),
        resolved: Arc::new(OnceCell::new_with(Some(result))),
        function: SystemFunction::HTTP,
        set_index: 0,
    })
}

async fn resolve(
    inputs: Vec<Option<CompositionSet>>,
) -> DandelionResult<Vec<Option<CompositionSet>>> {
    let semaphore = Arc::new(Semaphore::new(16));
    let ticket = semaphore.clone().acquire_owned().await.unwrap();
    let (sender, receiver) = oneshot::channel();
    resolve_all_sets(
        HttpClient::new(),
        inputs,
        semaphore,
        ticket,
        move |result| {
            let _ = sender.send(result);
        },
    )
    .await;
    receiver.await.unwrap()
}

fn recoverable_when_enabled(data: ItemData) -> ItemData {
    #[cfg(feature = "at-least-once")]
    {
        let ItemData::IoData(request) = data else {
            unreachable!()
        };
        ItemData::CoordinatedIoData(CoordinatedIoData {
            coordination: IoCoordination {
                id: crate::composition::IoRequestId {
                    run_id: dandelion_commons::RunId::nil(),
                    io_path: Default::default(),
                    request_item_index: 0,
                },
                item_identifier: "duplicate".into(),
                item_key: 7,
                owner_node_id: Some(0),
            },
            request,
        })
    }
    #[cfg(not(feature = "at-least-once"))]
    data
}

// Gates control completion order without network requests or timing-based sleeps.
struct GatedRemoteClient {
    gates: Mutex<HashMap<u64, oneshot::Receiver<()>>>,
    started: mpsc::UnboundedSender<u64>,
    finished: mpsc::UnboundedSender<u64>,
}

impl GatedRemoteClient {
    async fn wait(&self, id: u64) {
        let gate = self.gates.lock().unwrap().remove(&id).unwrap();
        self.started.send(id).unwrap();
        gate.await.unwrap();
        self.finished.send(id).unwrap();
    }
}

impl RemoteDataClient for GatedRemoteClient {
    fn local_node_id(&self) -> u64 {
        0
    }

    fn resolve_remote_data(
        &self,
        data: RemoteData,
    ) -> Pin<Box<dyn Future<Output = DandelionResult<(Arc<Context>, Position)>> + Send + '_>> {
        Box::pin(async move {
            self.wait(data.data_id).await;
            Ok((
                context(&[data.data_id as u8]),
                Position { offset: 0, size: 1 },
            ))
        })
    }

    fn resolve_multiple_data<'meta>(
        &'meta self,
        metadata: &'meta mut Vec<(usize, DataItem)>,
        data: Vec<RemoteData>,
    ) -> Pin<Box<dyn Future<Output = DandelionResult<Arc<Context>>> + Send + 'meta>> {
        Box::pin(async move {
            self.wait(data[0].data_id).await;
            assert_eq!(metadata.len(), data.len());
            for (offset, (_, item)) in metadata.iter_mut().enumerate() {
                item.data = Position { offset, size: 1 };
            }
            Ok(context(
                &data
                    .iter()
                    .map(|item| item.data_id as u8)
                    .collect::<Vec<_>>(),
            ))
        })
    }

    fn delete_remote_data(
        &self,
        _data: RemoteData,
    ) -> Pin<Box<dyn Future<Output = DandelionResult<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }

    #[cfg(feature = "at-least-once")]
    fn durable_data_size(
        &self,
        _data: RemoteData,
    ) -> Pin<Box<dyn Future<Output = DandelionResult<Option<usize>>> + Send + '_>> {
        Box::pin(async { unreachable!("not used by ordinary resolution") })
    }

    #[cfg(feature = "at-least-once")]
    fn publish_io_completion(
        &self,
        _completion: IoCoordinationCompletion,
    ) -> Pin<Box<dyn Future<Output = DandelionResult<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }

    #[cfg(feature = "exactly-once")]
    fn resolve_io(
        &self,
        _request: IoResolveRequest,
    ) -> Pin<Box<dyn Future<Output = DandelionResult<IoResolveOutcome>> + Send + '_>> {
        Box::pin(async {
            Ok(IoResolveOutcome::Execute {
                input: None,
                claim_id: 1,
            })
        })
    }

    #[cfg(feature = "exactly-once")]
    fn publish_io_resolution(
        &self,
        _completion: IoCoordinationCompletion,
    ) -> Pin<Box<dyn Future<Output = DandelionResult<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }

    #[cfg(feature = "exactly-once")]
    fn clear_io_coordination(
        &self,
        _run_id: dandelion_commons::RunId,
    ) -> Pin<Box<dyn Future<Output = DandelionResult<()>> + Send + '_>> {
        Box::pin(async { unreachable!("not used by ordinary resolution") })
    }
}

#[tokio::test]
async fn mixed_results_follow_build_ordering_policy() {
    let (started_tx, mut started) = mpsc::unbounded_channel();
    let (finished_tx, mut finished) = mpsc::unbounded_channel();
    let mut gates = HashMap::new();
    let mut releases = HashMap::new();
    for id in [10, 20, 30, 40] {
        let (sender, receiver) = oneshot::channel();
        gates.insert(id, receiver);
        releases.insert(id, sender);
    }
    crate::composition::set_remote_data_client(Arc::new(GatedRemoteClient {
        gates: Mutex::new(gates),
        started: started_tx,
        finished: finished_tx,
    }));

    let remote = |node, id| ItemData::RemoteData(RemoteData::new(node, 1, id));
    let inputs = vec![
        CompositionSet::from_item_list(
            "mixed".into(),
            vec![
                item(9, remote(1, 10)),
                item(8, local(11)),
                item(
                    7,
                    recoverable_when_enabled(cached_io(remote(3, 30), Ok(vec![context(&[31])]))),
                ),
                item(9, remote(2, 20)),
                item(7, cached_io(remote(3, 40), Ok(vec![context(&[41])]))),
                item(8, remote(1, 12)),
            ],
        ),
        None,
        CompositionSet::from_item_list(
            "local".into(),
            vec![item(5, local(51)), item(4, local(50))],
        ),
        CompositionSet::from_item_list(
            "other".into(),
            vec![
                item(6, local(61)),
                item(6, remote(2, 21)),
                item(6, remote(1, 13)),
            ],
        ),
    ];
    let resolving = tokio::spawn(resolve(inputs));
    let mut started_ids = Vec::new();
    for _ in 0..4 {
        started_ids.push(started.recv().await.unwrap());
    }
    started_ids.sort_unstable();
    assert_eq!(started_ids, vec![10, 20, 30, 40]);

    // Reverse the IO completion order, then the node-fetch completion order.
    for id in [40, 30, 20, 10] {
        releases.remove(&id).unwrap().send(()).unwrap();
        assert_eq!(finished.recv().await.unwrap(), id);
        tokio::task::yield_now().await;
    }
    let outputs = resolving.await.unwrap().unwrap();
    assert_eq!(outputs.len(), 4);
    assert!(outputs[1].is_none());
    for (index, name, keys, bytes) in [
        (
            0,
            "mixed",
            vec![9, 8, 7, 9, 7, 8],
            vec![10, 11, 31, 20, 41, 12],
        ),
        (2, "local", vec![5, 4], vec![51, 50]),
        (3, "other", vec![6, 6, 6], vec![61, 21, 13]),
    ] {
        let set = outputs[index].as_ref().unwrap();
        assert_eq!(set.get_name(), name);
        assert!(set.is_local());
        assert_eq!(set.size(), bytes.len());
        let mut actual_keys = Vec::new();
        let mut actual_bytes = Vec::new();
        for (item, data) in set {
            assert_eq!(item.ident, "duplicate");
            actual_keys.push(item.key);
            let ItemData::LocalData(context) = data else {
                panic!("unresolved item")
            };
            actual_bytes.extend_from_slice(
                context
                    .get_chunk_ref(item.data.offset, item.data.size)
                    .unwrap(),
            );
        }
        #[cfg(any(feature = "checkpointed-at-least-once", feature = "exactly-once"))]
        {
            assert_eq!(actual_keys, keys);
            assert_eq!(actual_bytes, bytes);
        }
        #[cfg(not(any(feature = "checkpointed-at-least-once", feature = "exactly-once")))]
        {
            // Mixed sets append their already-local items before any fetched results.
            if index == 0 {
                assert_eq!(actual_bytes[0], 11);
            }
            let mut actual = actual_keys
                .into_iter()
                .zip(actual_bytes)
                .collect::<Vec<_>>();
            let mut expected = keys.into_iter().zip(bytes).collect::<Vec<_>>();
            actual.sort_unstable();
            expected.sort_unstable();
            assert_eq!(actual, expected);
        }
    }
}

#[tokio::test]
async fn empty_and_local_only_inputs_preserve_set_slots() {
    assert!(resolve(vec![]).await.unwrap().is_empty());
    let result = resolve(vec![
        None,
        CompositionSet::from_item_list("local".into(), vec![item(2, local(2)), item(1, local(1))]),
        None,
    ])
    .await
    .unwrap();
    assert!(result[0].is_none());
    assert!(result[2].is_none());
    assert_eq!(
        result[1]
            .as_ref()
            .unwrap()
            .into_iter()
            .map(|(item, _)| item.key)
            .collect::<Vec<_>>(),
        vec![2, 1]
    );
}

#[tokio::test]
async fn io_error_is_returned_instead_of_partial_output() {
    let error = dandelion_err!(DandelionError::SystemFuncResponseError);
    let result = resolve(vec![CompositionSet::from_item_list(
        "mixed".into(),
        vec![item(1, local(1)), item(1, cached_io(local(0), Err(error)))],
    )])
    .await;
    assert!(matches!(
        result.unwrap_err().error,
        DandelionError::SystemFuncResponseError
    ));
}

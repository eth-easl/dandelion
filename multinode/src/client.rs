#[cfg(feature = "at-least-once")]
use crate::proto::{IoCompletionAcknowledgement, IoCompletionDelivery};
use crate::{
    data::ExportRegistry,
    deserialize_node_info, deserialize_queue_message, deserialize_remote_message,
    proto::{
        self, queue_message, remote_message, Invocation, NodeInfo, NodeUpdate, QueueMessage,
        RemoteMessage, RepeatedEngines, RepeatedInvocations, Response,
    },
    serialize_node_info, serialize_queue_message, serialize_remote_message,
    util::{
        composition_sets_to_proto_and_refs, engine_type_dtop, engine_type_ptod,
        pack_metadata_size_and_flags, proto_data_sets_to_composition_sets,
        proto_data_sets_to_composition_sets_with_delete_on_drop, recorder_add_timestamps,
        recorder_dtop, try_composition_sets_to_proto, unpack_metadata_size_and_flags,
        ADDITIONAL_DATA_BUFFER, NO_FLAGS,
    },
};
use dandelion_commons::{
    err_dandelion, records::Recorder, DandelionError, DandelionResult, FunctionRegistryError,
    InvocationId, MultinodeError,
};
use dispatcher::{
    dispatcher::Dispatcher,
    function_registry::ExportedFunctionRegistration,
    queue::{get_engine_flag, WorkQueue},
};
use log::{error, info, trace, warn};
#[cfg(feature = "at-least-once")]
use machine_interface::function_driver::{
    functions::SystemFunction,
    system_driver::recovery_log::{
        format_io_completion_line, parse_io_completion_line, IoCompletionDisposition,
        IoCompletionKey,
    },
};
use machine_interface::{
    composition::{CompositionSet, RemoteData},
    function_driver::{system_driver::UncoordinatedIo, WorkDone, WorkToDo},
    machine_config::{EngineType, IntoEnumIterator},
    promise::Debt,
};
use prost::bytes::{Bytes, BytesMut};
#[cfg(feature = "at-least-once")]
use std::collections::HashSet;
use std::{
    collections::{BTreeMap, BTreeSet, BinaryHeap},
    fs,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
    spawn,
    sync::{
        mpsc::{self, error::SendError},
        watch, Notify,
    },
};

#[cfg(test)]
mod test;

const _: () = assert!(size_of::<u64>() == size_of::<usize>());

// TODO ADDITIONAL_DATA_BUFFER and data_buffer are currently used only to carry IoData
// We should consider removing this when recursive resolution of IoData is implemented,
// as then all sets will be exchanged via the remote data server.
fn invocation_from_work(
    work: &WorkToDo,
    remote_invocation_id: u32,
    metadata_sets: Vec<proto::MetadataSet>,
    owner_invocation_id: InvocationId,
) -> DandelionResult<Invocation> {
    match work {
        WorkToDo::FunctionArguments {
            function_id,
            caching,
            ..
        } => Ok(Invocation {
            remote_invocation_id,
            function_id: (**function_id).clone(),
            metadata_sets,
            caching: *caching,
            owner_invocation_id: owner_invocation_id.to_string(),
        }),
        WorkToDo::SetsToResolve { .. }
        | WorkToDo::RemoteToDelete { .. }
        | WorkToDo::Shutdown(_) => {
            err_dandelion!(DandelionError::Multinode(MultinodeError::ConfigError(
                "Only executable function work can be offloaded".to_string(),
            )))
        }
    }
}

fn queue_function_args_from_invocation(
    invocation: &Invocation,
) -> DandelionResult<(Arc<String>, bool)> {
    if invocation.function_id.is_empty() {
        return err_dandelion!(DandelionError::Multinode(
            MultinodeError::DeserializationError("Invocation missing function id".to_string())
        ));
    }
    Ok((Arc::new(invocation.function_id.clone()), invocation.caching))
}

/// To send a message between nodes, always first send the length of the message,
/// then the message, so the other side knows when one message ends.
/// Returns an error if the underlying connection failed, so the caller can tear down
/// the connection instead of panicking.
async fn send_message(
    metadata_buffer: &Bytes,
    mut sender: impl AsyncWriteExt + Unpin,
    data_buffer: Option<(Vec<Option<CompositionSet>>, u64)>,
) -> std::io::Result<()> {
    let metadata_size: u32 = metadata_buffer.len().try_into().unwrap();
    let flags = match data_buffer {
        Some((_, total_size)) => {
            debug_assert!(total_size > 0);
            ADDITIONAL_DATA_BUFFER
        }
        _ => NO_FLAGS,
    };

    let packed_metadata = pack_metadata_size_and_flags(metadata_size, flags);

    sender.write_u64(packed_metadata).await?;
    sender.write_all(metadata_buffer).await?;

    // Code for sending data along with the request if needed
    // Keeping for later when we want to send small items along with requests / responses.
    // if let Some((data_sets, total_size)) = data_buffer {
    //     sender.write_u64(total_size).await.unwrap();
    //     for data_set in data_sets.into_iter().filter_map(|item| item) {
    //         for (_item, item_data) in data_set.into_iter() {
    //             let (offset, size, context) = match item_data {
    //                 ItemData::LocalData(_) => continue,
    //                 ItemData::IoData(io_data) => {
    //                     let IoData {
    //                         original_position,
    //                         original_data,
    //                         function: _,
    //                         set_index: _,
    //                         resolved: _,
    //                     } = io_data;
    //                     // TODO: could think about sending the data if it is already resolved
    //                     let Position { offset, size } = original_position;
    //                     (offset, size, original_data)
    //                 }
    //                 // if there is nothing to write to the buffer do a continue to skip writing
    //                 ItemData::RemoteData(_) => continue,
    //             };
    //             debug_assert_ne!(0, size);
    //             let mut bytes_written = 0;
    //             while bytes_written < size {
    //                 let next_chunk = context
    //                     .get_chunk_ref(offset + bytes_written, size - bytes_written)
    //                     .unwrap();
    //                 sender.write_all(next_chunk).await.unwrap();
    //                 bytes_written += next_chunk.len();
    //             }
    //         }
    //     }
    // }
    sender.flush().await
}

/// For small messages we are expecting repeteatly, could have spezial read function with permanent preallocated buffers
/// Issue: serialization does not give constant sizes, so would need to find an upper bound first
/// Returns an error if the underlying connection failed (or was closed mid-message),
/// so the caller can tear down the connection instead of panicking.
async fn receive_message(
    mut receiver: impl AsyncReadExt + Unpin,
) -> std::io::Result<(Bytes, Option<Bytes>)> {
    let packed_metadata = receiver.read_u64().await?;
    let (metadata_size, _) = unpack_metadata_size_and_flags(packed_metadata);
    // new buffer with size of message
    let mut metadata_buffer = BytesMut::with_capacity(metadata_size as usize);
    while metadata_buffer.len() < metadata_size as usize {
        // A read of 0 bytes means the peer closed the connection before sending the
        // full message, treat it as an unexpected end of file.
        if receiver.read_buf(&mut metadata_buffer).await? == 0 {
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
        }
    }
    // Keep for when we want to send additional data long with requests
    // if (flags & ADDITIONAL_DATA_BUFFER) != 0 {
    //     let total_data_size = receiver.read_u64().await.unwrap();
    //     let mut data_buffer = BytesMut::with_capacity(total_data_size as usize);
    //     let mut bytes_read = 0;
    //     while bytes_read < total_data_size as usize {
    //         bytes_read += receiver.read_buf(&mut data_buffer).await.unwrap();
    //     }
    //     return (metadata_buffer.freeze(), Some(data_buffer.freeze()));
    // } else {
    //     return (metadata_buffer.freeze(), None);
    // }
    Ok((metadata_buffer.freeze(), None))
}

enum QueueOption {
    Message(remote_message::RemoteMessage, Option<Bytes>),
    WorkAvailable,
    TryOffload(WorkToDo, machine_interface::promise::Debt, usize),
    CancelRemote(u32),
    /// The connection to the remote node was lost, so the server logic should tear down.
    Disconnected,
}

async fn remote_queue_sever_notification(receiver: Arc<Notify>, sender: mpsc::Sender<QueueOption>) {
    loop {
        receiver.notified().await;
        if sender.send(QueueOption::WorkAvailable).await.is_err() {
            // logic loop has shut down, nothing left to notify
            break;
        }
    }
}

/// The reciever socket handling for the remote queue server
async fn remote_queue_server_receiver(
    mut socket: OwnedReadHalf,
    sender: mpsc::Sender<QueueOption>,
) {
    loop {
        let message_buffer = match receive_message(&mut socket).await {
            Ok((message_buffer, _)) => message_buffer,
            Err(_) => {
                // The connection was lost, inform the logic loop so it can clean up.
                let _ = sender.send(QueueOption::Disconnected).await;
                break;
            }
        };
        let message = deserialize_remote_message(message_buffer)
            .unwrap()
            .remote_message
            .unwrap();
        if sender
            .send(QueueOption::Message(message, None))
            .await
            .is_err()
        {
            break;
        }
    }
}

/// The sender socket handling for the remote queue server.
/// TODO: check if we can unite this and the receiver with the other client, by using traits.
async fn remote_queue_server_sender(
    mut socket: OwnedWriteHalf,
    mut receiver: mpsc::Receiver<queue_message::QueueMessage>,
) {
    while let Some(queue_message) = receiver.recv().await {
        let message_buffer = serialize_queue_message(QueueMessage {
            queue_message: Some(queue_message),
        });
        if send_message(&message_buffer, &mut socket, None)
            .await
            .is_err()
        {
            // connection lost, the receiver side will trigger the teardown
            break;
        }
    }
}

/// Translating the messages from the queue for offlaoding into something the server logic understands
async fn remote_queue_server_try_offload(
    mut queue_receiver: mpsc::UnboundedReceiver<(WorkToDo, Debt, usize)>,
    sender: mpsc::Sender<QueueOption>,
    queue: WorkQueue,
) {
    while let Some((work, debt, composition_id)) = queue_receiver.recv().await {
        if let Err(send_err) = sender
            .send(QueueOption::TryOffload(work, debt, composition_id))
            .await
        {
            if let SendError(QueueOption::TryOffload(w, d, id)) = send_err {
                queue.reenqueue(w, d, id);
            }
            break;
        }
    }
    // can't send anymore so make sure the channel does not have things added to it.
    queue_receiver.close();
    // drain the channel, reqenque all the work
    while let Some((work, debt, composition_id)) = queue_receiver.recv().await {
        queue.reenqueue(work, debt, composition_id);
    }
}

/// The protocol logic handling for the remote queue server
async fn remote_queue_server_logic(
    mut message_receiver: mpsc::Receiver<QueueOption>,
    local_sender: mpsc::Sender<QueueOption>,
    message_sender: mpsc::Sender<queue_message::QueueMessage>,
    queue: WorkQueue,
    export_registry: ExportRegistry,
    remote_data_deletion_sender: mpsc::UnboundedSender<RemoteData>,
    node_id: u64,
    mut remote_num_cores: u64,
) {
    let mut waiting_for_work = false;
    let mut invocations_running = 0;

    let mut debt_map = BTreeMap::new();
    let mut cancelled_debt_ids = BTreeSet::new();
    let mut free_debt_ids = BinaryHeap::new();
    let mut max_debt_id = 0;

    // are ready, wait for the remote to ask for work or return completed tasks
    while let Some(queue_option) = message_receiver.recv().await {
        match queue_option {
            QueueOption::Message(message, data_option) => {
                match message {
                    remote_message::RemoteMessage::WorkRequest(work_request) => {
                        debug_assert!(data_option.is_none());
                        // For now just send one matching function
                        // for each engine try to get as much work as possible up to the amount asked for

                        trace!(
                            "Queue Server received work request for engines: {:?}",
                            work_request.engines
                        );
                        let mut invocations = Vec::new();
                        for engine in work_request.engines {
                            let engine_flags =
                                get_engine_flag(engine_type_ptod(engine.engine_type).unwrap());
                            let target_invocations = engine.engine_capacity as usize;
                            while invocations.len() < target_invocations {
                                let remaining_capacity = target_invocations - invocations.len();
                                let work_found = queue.try_get_work_for_remote(
                                    engine_flags,
                                    node_id,
                                    remaining_capacity,
                                );
                                if work_found.is_empty() {
                                    break;
                                }
                                trace!(
                                    "Found {} work item(s) while filling {} remaining remote slot(s)",
                                    work_found.len(),
                                    remaining_capacity
                                );
                                // TODO: consider limiting the work we give to a node based on the know max capacity,
                                // to limit potential stragglers if we know the node asked for more than it can handle (possibly because of race conditions)
                                // do not give even more.
                                invocations.extend(work_found.into_iter().filter_map(|(work, debt, composition_id)|
                                {
                                    if !debt.is_alive() {
                                        return None;
                                    }
                                    // there is some work so send it out
                                    // find the local function id to use
                                    let promise_id = if let Some(free_id) = free_debt_ids.pop() {
                                        free_id
                                    } else {
                                        let promise_id = max_debt_id;
                                        max_debt_id += 1;
                                        promise_id
                                    };
                                    // Todo send along relevant information, like caching bool and recorder start time
                                    let (data_sets, recorder) = match &work {
                                        WorkToDo::FunctionArguments {
                                            input_sets,
                                            recorder,
                                            ..
                                        } => {
                                            (input_sets, recorder)
                                        }
                                        WorkToDo::SetsToResolve { .. }
                                        | WorkToDo::RemoteToDelete { .. }
                                        | WorkToDo::Shutdown(_) => {
                                            panic!("Should only get function arguments when polling for remote queue")
                                        }
                                    };
                                    let mut new_recorder = recorder.clone();
                                    new_recorder
                                        .record(dandelion_commons::records::RecordPoint::RemoteTake);
                                    let start_reference = SystemTime::elapsed(&std::time::UNIX_EPOCH)
                                        .unwrap()
                                        .as_micros();
                                    let (metadata_sets, remote_data_references) =
                                        composition_sets_to_proto_and_refs(
                                            data_sets,
                                            export_registry.get_node_id(),
                                            |item, context| {
                                                export_registry.insert_function(
                                                    item,
                                                    context,
                                                    Some(remote_data_deletion_sender.clone()),
                                                )
                                            },
                                        );
                                    let owner_invocation_id = recorder.invocation_id();
                                    let cancel_sender = local_sender.clone();
                                    debt.install_abort_handle(move || {
                                        trace!(
                                            "Abort callback fired for remote invocation {}",
                                            promise_id
                                        );
                                        if cancel_sender
                                            .try_send(QueueOption::CancelRemote(promise_id))
                                            .is_err()
                                        {
                                            warn!(
                                                "Failed to enqueue cancellation for remote invocation {}",
                                                promise_id
                                            );
                                        }
                                    });
                                    let invocation = invocation_from_work(
                                        &work,
                                        promise_id,
                                        metadata_sets,
                                        owner_invocation_id,
                                    )
                                    .expect("Work already validated before remote packaging");
                                    debt_map.insert(
                                        promise_id,
                                        (
                                            composition_id,
                                            debt,
                                            new_recorder,
                                            start_reference,
                                            remote_data_references,
                                            work,
                                        ),
                                    );
                                    Some(invocation)
                                }));
                            }
                        }
                        if invocations.is_empty() {
                            waiting_for_work = true;
                            trace!("No work available");
                            // there is no work, so send message accordingly
                            if message_sender
                                .send(queue_message::QueueMessage::NoWork(true))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        } else {
                            invocations_running += invocations.len();
                            if message_sender
                                .send(queue_message::QueueMessage::Invocations(
                                    RepeatedInvocations { invocations },
                                ))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                    }
                    remote_message::RemoteMessage::Response(response) => {
                        debug_assert!(data_option.is_none());
                        trace!("Queue Server received response");
                        let Response {
                            remote_invocation_id,
                            response,
                        } = response;
                        // TODO: handle failure
                        let Some((
                            composition_id,
                            debt,
                            mut recorder,
                            start_epoch,
                            remote_data_references,
                            work,
                        )) = debt_map.remove(&remote_invocation_id)
                        else {
                            if cancelled_debt_ids.remove(&remote_invocation_id) {
                                invocations_running = invocations_running.saturating_sub(1);
                                trace!(
                                    "Ignoring response for canceled remote invocation {}",
                                    remote_invocation_id
                                );
                                free_debt_ids.push(remote_invocation_id);
                            } else {
                                warn!(
                                    "Received response for unknown remote invocation {}",
                                    remote_invocation_id
                                );
                            }
                            continue;
                        };
                        invocations_running = invocations_running.saturating_sub(1);
                        free_debt_ids.push(remote_invocation_id);
                        drop(remote_data_references);
                        // remote did not do work, was a try offload request, reenqueu the work
                        if let Some(response) = response {
                            let result = match response {
                                proto::response::Response::MetadataSets(metadata_sets) => {
                                    recorder_add_timestamps(
                                        &mut recorder,
                                        metadata_sets.timestamps,
                                        start_epoch,
                                        node_id,
                                    );
                                    Ok(WorkDone::CompositionSet(
                                        proto_data_sets_to_composition_sets_with_delete_on_drop(
                                            metadata_sets.metadata_sets,
                                            data_option,
                                            remote_data_deletion_sender.clone(),
                                        ),
                                    ))
                                }
                                proto::response::Response::ErrorMsg(error_message) => {
                                    err_dandelion!(DandelionError::Multinode(
                                        MultinodeError::RequestFailed(error_message)
                                    ))
                                }
                            };
                            debt.fulfill(result)
                        } else {
                            // did not get response so need to reenqueue the work
                            queue.reenqueue(work, debt, composition_id);
                        }
                    }
                    remote_message::RemoteMessage::NodeUpdate(node_update) => {
                        trace!(
                            "Queue Server received node update with new local count: {}",
                            node_update.num_local_cores
                        );
                        let mut success = true;
                        if node_update.num_local_cores < remote_num_cores {
                            success = queue
                                .remove_remote_cores(
                                    (remote_num_cores - node_update.num_local_cores) as usize,
                                )
                                .is_ok();
                        } else {
                            queue.add_remote_cores(
                                (node_update.num_local_cores - remote_num_cores) as usize,
                            );
                        }
                        if success {
                            remote_num_cores = node_update.num_local_cores;
                        } else {
                            error!("Failed to update remote core count: Total number of remote cores underflows.");
                        }
                    }
                    remote_message::RemoteMessage::IoCompletion(delivery) => {
                        #[cfg(not(feature = "at-least-once"))]
                        {
                            let _ = delivery;
                            warn!("Ignoring exactly-once I/O completion in this build");
                            continue;
                        }
                        #[cfg(feature = "at-least-once")]
                        {
                            debug_assert!(data_option.is_none());
                            let record = match parse_io_completion_line(&delivery.journal_line) {
                                Ok(Some(record)) => record,
                                Ok(None) => {
                                    warn!("Worker sent a non-completion journal record");
                                    continue;
                                }
                                Err(err) => {
                                    warn!("Worker sent an invalid IO completion: {}", err);
                                    continue;
                                }
                            };
                            let disposition = match machine_interface::function_driver::system_driver::recovery_log::accept_delivered_io_completion_record(&record) {
                                Ok(disposition) => disposition,
                                Err(err) => {
                                    error!("Failed to persist worker IO completion: {}", err);
                                    continue;
                                }
                            };
                            #[cfg(feature = "exactly-once")]
                            if disposition == IoCompletionDisposition::Retain {
                                if let Err(err) =
                                    export_registry.publish_io_resolution_from_record(&record)
                                {
                                    error!("Failed to complete coordinated worker IO: {}", err);
                                    continue;
                                }
                            }
                            let completion_key = match record.completion_key() {
                                Ok(completion_key) => completion_key,
                                Err(err) => {
                                    warn!("Worker sent an invalid IO completion identity: {}", err);
                                    continue;
                                }
                            };
                            if message_sender
                                .send(queue_message::QueueMessage::IoCompletionAck(
                                    IoCompletionAcknowledgement {
                                        invocation_id: completion_key.invocation_id.to_string(),
                                        composition_set_id: completion_key.composition_set_id
                                            as u64,
                                        function: completion_key.function.to_string(),
                                        identifier: completion_key.identifier,
                                        item_key: completion_key.item_key,
                                        delete_outputs: disposition
                                            == IoCompletionDisposition::Delete,
                                    },
                                ))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                    }
                }
            }
            QueueOption::TryOffload(work, debt, composition_id) => {
                if !debt.is_alive() {
                    continue;
                }
                // if this node already sent enough work for the remote to be at capacity don't send more
                if invocations_running >= remote_num_cores as usize {
                    queue.reenqueue(work, debt, composition_id);
                    continue;
                }
                invocations_running += 1;
                // Ask remote if it can take the invocation, otherwise requeue it locally
                let promise_id = if let Some(free_id) = free_debt_ids.pop() {
                    free_id
                } else {
                    let promise_id = max_debt_id;
                    max_debt_id += 1;
                    promise_id
                };
                // Todo send along relevant information, like caching bool and recorder start time
                let (data_sets, recorder) = match &work {
                    WorkToDo::FunctionArguments {
                        input_sets,
                        recorder,
                        ..
                    } => (input_sets, recorder),
                    WorkToDo::SetsToResolve { .. }
                    | WorkToDo::RemoteToDelete { .. }
                    | WorkToDo::Shutdown(_) => {
                        panic!("Should only get function arguments when polling for remote queue")
                    }
                };
                let mut new_recorder = recorder.clone();
                new_recorder.record(dandelion_commons::records::RecordPoint::RemoteTake);
                let start_reference = SystemTime::elapsed(&std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_micros();
                let (metadata_sets, remote_data_references) = composition_sets_to_proto_and_refs(
                    data_sets,
                    export_registry.get_node_id(),
                    |item, context| {
                        export_registry.insert_function(
                            item,
                            context,
                            Some(remote_data_deletion_sender.clone()),
                        )
                    },
                );
                let owner_invocation_id = recorder.invocation_id();
                let cancel_sender = local_sender.clone();
                debt.install_abort_handle(move || {
                    trace!("Abort callback fired for remote invocation {}", promise_id);
                    if cancel_sender
                        .try_send(QueueOption::CancelRemote(promise_id))
                        .is_err()
                    {
                        warn!(
                            "Failed to enqueue cancellation for remote invocation {}",
                            promise_id
                        );
                    }
                });
                let invocation =
                    invocation_from_work(&work, promise_id, metadata_sets, owner_invocation_id)
                        .expect("Work already validated before remote packaging");
                debt_map.insert(
                    promise_id,
                    (
                        composition_id,
                        debt,
                        new_recorder,
                        start_reference,
                        remote_data_references,
                        work,
                    ),
                );
                trace!("Prepared work, sending out now");
                let try_offload_message = queue_message::QueueMessage::TryOffload(invocation);
                if message_sender.send(try_offload_message).await.is_err() {
                    break;
                }
            }
            QueueOption::CancelRemote(remote_invocation_id) => {
                if let Some((
                    _composition_id,
                    _debt,
                    _recorder,
                    _start_epoch,
                    remote_data_references,
                    _work,
                )) = debt_map.remove(&remote_invocation_id)
                {
                    trace!(
                        "Locally canceled remote invocation {}, removing from in-flight map",
                        remote_invocation_id
                    );
                    drop(remote_data_references);
                    cancelled_debt_ids.insert(remote_invocation_id);
                    // ask the remote to cancel the invocation
                    if message_sender
                        .send(queue_message::QueueMessage::CancelInvocation(
                            remote_invocation_id,
                        ))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            }
            QueueOption::WorkAvailable => {
                trace!("Queue Server received work available notification");
                if waiting_for_work {
                    waiting_for_work = false;
                    if message_sender
                        .send(queue_message::QueueMessage::NoWork(false))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            }
            QueueOption::Disconnected => {
                // The remote node disconnected, stop the loop and run the cleanup below.
                break;
            }
        }
    }

    // The connection to the remote node was lost (or the channel was closed).
    // Close the message receiver so all senders will also shut down and no more messages can be enqueued.
    info!("Lost connection to worker node {}", node_id);
    message_receiver.close();

    // Undo the bookkeeping for this node
    let _ = queue.remove_remote_cores(remote_num_cores as usize);
    queue.remove_remote_channel(node_id);

    // Drain any remaining messages.
    while let Some(message) = message_receiver.recv().await {
        match message {
            // Ignore messages that have no effect on the clean up
            QueueOption::WorkAvailable
            | QueueOption::Disconnected
            | QueueOption::Message(_, _)
            | QueueOption::CancelRemote(_) => (),
            // Reenqueue work that was tried to offload
            QueueOption::TryOffload(work, debt, composition_id) => {
                queue.reenqueue(work, debt, composition_id);
            }
        }
    }

    // Recover any work that was offloaded but never completed, so it can be re-scheduled locally or on another node.
    for (
        _promise_id,
        (composition_id, debt, _recorder, _start_epoch, _remote_data_references, work),
    ) in debt_map
    {
        queue.reenqueue(work, debt, composition_id);
    }
}

/// Handler for one remote node, polling the local queue for them.
/// The first message from the remote should contain the possible engines it will poll for,
/// and the maximum number of requests for those engines it will poll.
/// Protocol for polling is sending a poll message, saying which engines are polled for.
/// Response is either a single available task or if none are available immediately,
/// a message that notifies the remote of that. If the response is yes,
/// Each task sent out is associated with an id.
/// When a task if finished, the response is to carry that same id, so the promise can be fulfilled.
pub async fn remote_queue_server(
    socket: TcpStream,
    queue: WorkQueue,
    export_registry: ExportRegistry,
    remote_data_deletion_sender: mpsc::UnboundedSender<RemoteData>,
) {
    let (mut read_socket, write_socket) = socket.into_split();

    // First ask for the information about the other node
    // Currently not using engine information
    trace!("Queue Server wait for initial message");
    let node_info_buffer = match receive_message(&mut read_socket).await {
        Ok((node_info_buffer, node_info_data)) => {
            debug_assert!(node_info_data.is_none());
            node_info_buffer
        }
        Err(err) => {
            // The connection dropped before we could identify the node, nothing to clean up.
            warn!(
                "Failed to receive initial message from worker node: {}",
                err
            );
            return;
        }
    };
    let NodeInfo {
        version,
        id: node_id,
        num_local_cores,
    } = deserialize_node_info(node_info_buffer).unwrap();
    assert_eq!(version, 1);
    info!(
        "Established connection to worker node {} ({} cores)",
        node_id, num_local_cores
    );

    // tell the queue about the remote cores
    queue.add_remote_cores(num_local_cores as usize);

    // start sender loop
    let (queue_message_sender, queue_message_reciever) = mpsc::channel(64);
    let sender_handle = spawn(remote_queue_server_sender(
        write_socket,
        queue_message_reciever,
    ));
    // start receiver loop
    let (queue_option_sender, queue_option_receiver) = mpsc::channel(64);
    let receiver_handle = spawn(remote_queue_server_receiver(
        read_socket,
        queue_option_sender.clone(),
    ));
    // spawn notificaiton loop
    let notification_handle = spawn(remote_queue_sever_notification(
        queue.queueing_notifier(),
        queue_option_sender.clone(),
    ));
    // spawn loop to check for queue trying to offload
    let (offload_sender, offload_receiver) = mpsc::unbounded_channel();
    queue.add_remote_channel(node_id, offload_sender);
    let offload_handle = spawn(remote_queue_server_try_offload(
        offload_receiver,
        queue_option_sender.clone(),
        queue.clone(),
    ));

    remote_queue_server_logic(
        queue_option_receiver,
        queue_option_sender.clone(),
        queue_message_sender,
        queue,
        export_registry.clone(),
        remote_data_deletion_sender,
        node_id,
        num_local_cores,
    )
    .await;

    // Work that completed durably remains reusable after the worker restarts. Only unfinished
    // claims need to be released so requeued attempts can elect a replacement winner.
    #[cfg(feature = "exactly-once")]
    export_registry.invalidate_running_io_for_node(node_id);

    // The logic loop returned because the connection was lost, stop the helper tasks so they do not
    // linger waiting on a dead socket or closed channels.
    sender_handle.abort();
    receiver_handle.abort();
    notification_handle.abort();
    offload_handle.abort();
}

pub enum PollingOption {
    Message(DandelionResult<queue_message::QueueMessage>, Option<Bytes>),
    IdleChanged,
    LocalCoreCountChanged(usize),
    // Results(RemoteMessage, Option<(Vec<Option<CompositionSet>>, u64)>),
    Results(u32, remote_message::RemoteMessage),
    /// The connection to the remote node was lost, so the client logic should tear down.
    Disconnected,
}

async fn remote_queue_client_receiver(
    mut socket: OwnedReadHalf,
    sender: mpsc::Sender<PollingOption>,
) {
    loop {
        let message_buffer = match receive_message(&mut socket).await {
            Ok((message_buffer, _)) => message_buffer,
            Err(_) => {
                // The connection was lost, inform the logic loop so it can clean up.
                let _ = sender.send(PollingOption::Disconnected).await;
                break;
            }
        };
        let message = deserialize_queue_message(message_buffer)
            .and_then(|message| Ok(message.queue_message.unwrap()));
        if sender
            .send(PollingOption::Message(message, None))
            .await
            .is_err()
        {
            break;
        }
    }
}

async fn remote_queue_client_sender(
    mut socket: OwnedWriteHalf,
    mut receiver: mpsc::Receiver<remote_message::RemoteMessage>,
) {
    while let Some(remote_message) = receiver.recv().await {
        let message_buffer = serialize_remote_message(RemoteMessage {
            remote_message: Some(remote_message),
        });
        if send_message(&message_buffer, &mut socket, None)
            .await
            .is_err()
        {
            // connection lost, the receiver side will trigger the teardown
            break;
        }
    }
}

#[cfg(feature = "at-least-once")]
async fn io_completion_delivery_loop(
    export_registry: ExportRegistry,
    sender: mpsc::Sender<remote_message::RemoteMessage>,
) {
    let mut sent_on_this_connection = HashSet::new();
    loop {
        for record in export_registry.pending_io_completion_records() {
            let key = match record.completion_key() {
                Ok(key) => key,
                Err(err) => {
                    error!("Invalid pending IO completion identity: {}", err);
                    return;
                }
            };
            if sent_on_this_connection.contains(&key) {
                continue;
            }
            let journal_line = match format_io_completion_line(&record) {
                Ok(journal_line) => journal_line,
                Err(err) => {
                    error!("Failed to encode pending IO completion: {}", err);
                    return;
                }
            };
            if sender
                .send(remote_message::RemoteMessage::IoCompletion(
                    IoCompletionDelivery { journal_line },
                ))
                .await
                .is_err()
            {
                return;
            }
            sent_on_this_connection.insert(key);
        }
        export_registry.wait_for_pending_io_completions().await;
    }
}

// TODO: think about limiting number of notification, to make sure we are not adding
// additional load when the queue is filled / emptied in big strides
async fn remote_queue_client_queue_state(
    receiver: Arc<Notify>,
    sender: mpsc::Sender<PollingOption>,
) {
    // Send a initial idle changed message, to make sure it always asks once.
    // An idle machine otherwise would never start asking for work
    if sender.send(PollingOption::IdleChanged).await.is_err() {
        return;
    }
    loop {
        receiver.notified().await;
        trace!("received local idle-core change");
        if sender.send(PollingOption::IdleChanged).await.is_err() {
            break;
        }
    }
}

async fn remote_queue_client_core_count(
    mut receiver: watch::Receiver<usize>,
    sender: mpsc::Sender<PollingOption>,
) {
    loop {
        if receiver.changed().await.is_err() {
            break;
        }
        let num_local_cores = *receiver.borrow_and_update();
        if sender
            .send(PollingOption::LocalCoreCountChanged(num_local_cores))
            .await
            .is_err()
        {
            break;
        }
    }
}

async fn dispatcher_call(
    dispatcher: &'static Dispatcher,
    sender: mpsc::Sender<PollingOption>,
    export_registry: ExportRegistry,
    owner_frontend_url: Option<Arc<String>>,
    function_cache_path: Arc<String>,
    start_time: Duration,
    remote_invocation_id: u32,
    function_id: Arc<String>,
    caching: bool,
    input_sets: Vec<Option<CompositionSet>>,
    recorder: Recorder,
) {
    let composition_id = dispatcher.get_composition_id();
    let retry_input_sets = input_sets.clone();
    let retry_function_id = function_id.clone();
    let retry_caching = caching;
    let queue_remote_invocation = |function_id: Arc<String>,
                                   caching: bool,
                                   input_sets: Vec<Option<CompositionSet>>,
                                   recorder: Recorder| async move {
        dispatcher
            .queue_function(
                composition_id,
                function_id,
                input_sets,
                caching,
                UncoordinatedIo,
                recorder,
                None,
            )
            .await
    };
    let mut function_result =
        queue_remote_invocation(function_id.clone(), caching, input_sets, recorder.clone()).await;
    if matches!(
        &function_result,
        Err(err) if matches!(
            err.error,
            DandelionError::FunctionRegistry(FunctionRegistryError::UnknownFunction(_))
        )
    ) {
        if let Some(owner_frontend_url) = owner_frontend_url {
            warn!(
                "Lazy-loading missing function {} from owner {}",
                retry_function_id, owner_frontend_url
            );
            match lazy_load_function(
                dispatcher,
                owner_frontend_url.as_ref(),
                function_cache_path.as_ref(),
                retry_function_id.as_ref(),
            )
            .await
            {
                Ok(true) => {
                    warn!(
                        "Lazy-loaded function {} successfully, retrying invocation",
                        retry_function_id
                    );
                    function_result = queue_remote_invocation(
                        retry_function_id,
                        retry_caching,
                        retry_input_sets,
                        recorder.clone(),
                    )
                    .await;
                }
                Ok(false) => {}
                Err(err) => {
                    warn!(
                        "Lazy-loading function {} failed: {}",
                        retry_function_id, err
                    );
                    function_result = Err(err);
                }
            }
        }
    }
    let response_message = match function_result {
        Ok(sets) => {
            let exported = try_composition_sets_to_proto(
                sets,
                export_registry.get_node_id(),
                |item, context| Ok(export_registry.insert_function(item, context, None)),
            );
            match exported {
                Ok(metadata_sets) => {
                    proto::response::Response::MetadataSets(proto::RepeatedMetadataSet {
                        metadata_sets,
                        timestamps: recorder_dtop(recorder, start_time),
                    })
                }
                Err(err) => proto::response::Response::ErrorMsg(err.error.to_string()),
            }
        }
        Err(err) => proto::response::Response::ErrorMsg(err.error.to_string()),
    };
    // If the connection was lost in the meantime the logic loop is gone; dropping the result is
    // fine, the master will reenqueue the work after detecting the disconnect.
    let _ = sender
        .send(PollingOption::Results(
            remote_invocation_id,
            remote_message::RemoteMessage::Response(Response {
                remote_invocation_id,
                response: Some(response_message),
            }),
        ))
        .await;
}

fn sanitize_identifier(value: &str) -> String {
    value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn parse_engine_type(engine_type: &str) -> DandelionResult<EngineType> {
    match engine_type {
        #[cfg(feature = "mmu")]
        "Process" => Ok(EngineType::Process),
        #[cfg(feature = "kvm")]
        "Kvm" => Ok(EngineType::Kvm),
        #[cfg(feature = "cheri")]
        "Cheri" => Ok(EngineType::Cheri),
        _ => err_dandelion!(DandelionError::Multinode(MultinodeError::ConfigError(
            format!("Unknown engine type {engine_type} in lazy-loaded function"),
        ))),
    }
}

fn register_exported_function(
    dispatcher: &'static Dispatcher,
    function_cache_path: &str,
    exported: ExportedFunctionRegistration,
) -> DandelionResult<()> {
    let remote_function_dir = PathBuf::from(function_cache_path).join("remote_functions");
    fs::create_dir_all(&remote_function_dir).map_err(|err| {
        dandelion_commons::dandelion_err!(DandelionError::Multinode(MultinodeError::RequestFailed(
            format!("Failed to create lazy-load cache directory: {}", err)
        ),))
    })?;
    let function_name = exported.name.clone();
    for (index, alternative) in exported.alternatives.into_iter().enumerate() {
        let engine_type = parse_engine_type(&alternative.engine_type)?;
        let file_name = format!(
            "{}_{}_{}.bin",
            sanitize_identifier(&function_name),
            alternative.engine_type.to_lowercase(),
            index,
        );
        let path = remote_function_dir.join(file_name);
        fs::write(&path, &alternative.binary).map_err(|err| {
            dandelion_commons::dandelion_err!(DandelionError::Multinode(
                MultinodeError::RequestFailed(format!(
                    "Failed to write lazy-loaded function binary: {}",
                    err
                )),
            ))
        })?;
        let metadata = machine_interface::function_driver::Metadata {
            input_sets: exported
                .metadata
                .input_sets
                .iter()
                .cloned()
                .map(|name| (name, None))
                .collect(),
            output_sets: exported.metadata.output_sets.clone(),
            min_set_bytes: exported.metadata.min_set_bytes.clone(),
        };
        match dispatcher.insert_function(
            function_name.clone(),
            engine_type,
            alternative.context_size,
            path.to_string_lossy().to_string(),
            metadata,
        ) {
            Ok(()) => {}
            Err(err)
                if matches!(
                    err.error,
                    DandelionError::FunctionRegistry(FunctionRegistryError::DuplicateInsert(_))
                ) => {}
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

async fn lazy_load_function(
    dispatcher: &'static Dispatcher,
    owner_frontend_url: &str,
    function_cache_path: &str,
    function_name: &str,
) -> DandelionResult<bool> {
    let base_url = reqwest::Url::parse(owner_frontend_url).map_err(|err| {
        dandelion_commons::dandelion_err!(DandelionError::Multinode(MultinodeError::ConfigError(
            format!("Invalid owner frontend url {}: {}", owner_frontend_url, err)
        ),))
    })?;
    let mut url = base_url.clone();
    url.path_segments_mut()
        .map_err(|_| {
            dandelion_commons::dandelion_err!(DandelionError::Multinode(
                MultinodeError::ConfigError(format!(
                    "Owner frontend url {} cannot accept path segments",
                    owner_frontend_url
                )),
            ))
        })?
        .extend(["internal", "function", function_name]);

    let response = reqwest::get(url).await.map_err(|err| {
        dandelion_commons::dandelion_err!(DandelionError::Multinode(
            MultinodeError::ConnectionFailed(format!(
                "Failed to fetch function {} from owner: {}",
                function_name, err
            )),
        ))
    })?;
    if !response.status().is_success() {
        return err_dandelion!(DandelionError::Multinode(MultinodeError::RequestFailed(
            format!(
                "Owner returned {} when fetching function {}",
                response.status(),
                function_name
            ),
        )));
    }
    let response_bytes = response.bytes().await.map_err(|err| {
        dandelion_commons::dandelion_err!(DandelionError::Multinode(MultinodeError::RequestFailed(
            format!(
                "Failed to read lazy-loaded function {} response: {}",
                function_name, err
            )
        ),))
    })?;
    let exported = serde_json::from_slice::<ExportedFunctionRegistration>(&response_bytes)
        .map_err(|err| {
            dandelion_commons::dandelion_err!(DandelionError::Multinode(
                MultinodeError::DeserializationError(format!(
                    "Failed to deserialize lazy-loaded function {}: {}",
                    function_name, err
                )),
            ))
        })?;
    register_exported_function(dispatcher, function_cache_path, exported)?;
    Ok(true)
}

async fn remote_queue_client_logic(
    mut receiver: mpsc::Receiver<PollingOption>,
    message_sender: mpsc::Sender<remote_message::RemoteMessage>,
    dispatcher_sender: impl Fn(
        ExportRegistry,
        Duration,
        u32,
        Arc<String>,
        bool,
        Vec<Option<CompositionSet>>,
        Recorder,
    ),
    idle_cores: impl Fn() -> usize,
    prefetch_multiplier: usize,
    export_registry: ExportRegistry,
    mut num_local_cores: usize,
) {
    let mut remote_had_work = true;
    let mut invocation_request_in_flight = false;
    // This makes sure we don't overfetch from this node.
    // TODO: think about the general issue of state synchronization between the dispatcher and multinode client adding things,
    // and the engines and server taking things.
    let mut work_from_remote: usize = 0;
    let mut cancelled_remote_invocations = BTreeSet::new();

    while let Some(current_future) = receiver.recv().await {
        match current_future {
            PollingOption::Message(Ok(message), data_option) => {
                match message {
                    // remote did not have work, can ignore local capacity to work until we get a message the more is available
                    queue_message::QueueMessage::NoWork(true) => {
                        trace!("Queue Client recieved NoWork(true)");
                        debug_assert!(data_option.is_none());

                        invocation_request_in_flight = false;
                        remote_had_work = false
                    }
                    // remote signals it may have work so can ask for it, if we have capacity
                    queue_message::QueueMessage::NoWork(false) => {
                        trace!("Queue Client recieved NoWork(false)");
                        debug_assert!(data_option.is_none());

                        remote_had_work = true;
                    }
                    queue_message::QueueMessage::CancelInvocation(remote_invocation_id) => {
                        trace!(
                            "Queue Client received cancel for remote invocation {}",
                            remote_invocation_id
                        );
                        cancelled_remote_invocations.insert(remote_invocation_id);
                    }
                    queue_message::QueueMessage::IoCompletionAck(acknowledgement) => {
                        #[cfg(not(feature = "at-least-once"))]
                        {
                            let _ = acknowledgement;
                            warn!("Ignoring exactly-once I/O acknowledgement in this build");
                            continue;
                        }
                        #[cfg(feature = "at-least-once")]
                        {
                            let invocation_id = match acknowledgement.invocation_id.parse() {
                                Ok(invocation_id) => invocation_id,
                                Err(_) => {
                                    error!("Owner sent an invalid IO completion acknowledgement");
                                    break;
                                }
                            };
                            let composition_set_id = match usize::try_from(
                                acknowledgement.composition_set_id,
                            ) {
                                Ok(composition_set_id) => composition_set_id,
                                Err(_) => {
                                    error!(
                                        "Owner sent an IO completion acknowledgement with an invalid composition set id"
                                    );
                                    break;
                                }
                            };
                            let function = match acknowledgement.function.as_str() {
                                "HTTP" => SystemFunction::HTTP,
                                "MEMCACHED" => SystemFunction::MEMCACHED,
                                _ => {
                                    error!(
                                    "Owner sent an IO completion acknowledgement with an invalid function"
                                );
                                    break;
                                }
                            };
                            let completion_key = IoCompletionKey {
                                invocation_id,
                                composition_set_id,
                                function,
                                identifier: acknowledgement.identifier,
                                item_key: acknowledgement.item_key,
                            };
                            let disposition = if acknowledgement.delete_outputs {
                                IoCompletionDisposition::Delete
                            } else {
                                IoCompletionDisposition::Retain
                            };
                            if let Err(err) = export_registry
                                .apply_io_completion_ack(&completion_key, disposition)
                            {
                                // Reconnect so the delivery loop retries the record instead of treating
                                // an acknowledgement that was not persisted locally as complete.
                                error!("Failed to acknowledge delivered IO completion: {}", err);
                                break;
                            }
                        }
                    }
                    // TODO for try offload decide when to refuse work
                    queue_message::QueueMessage::TryOffload(invocation) => {
                        trace!("Queue Client recieved try offload");
                        let Invocation {
                            remote_invocation_id,
                            metadata_sets,
                            owner_invocation_id,
                            ..
                        } = &invocation;
                        let remote_invocation_id = *remote_invocation_id;
                        if cancelled_remote_invocations.remove(&remote_invocation_id) {
                            trace!(
                                "Discarding stale cancellation before reused try-offload invocation {}",
                                remote_invocation_id
                            );
                        }
                        work_from_remote += 1;

                        // mark remote as having work, so we ask for more as idle cores change
                        remote_had_work = true;
                        let start_instance = Instant::now();
                        let start_time =
                            std::time::SystemTime::elapsed(&std::time::SystemTime::UNIX_EPOCH)
                                .unwrap();
                        let (function_id, caching) = queue_function_args_from_invocation(
                            &invocation,
                        )
                        .expect(
                            "Remote try-offload message should always contain invocation details",
                        );
                        let owner_invocation_id = owner_invocation_id
                            .parse::<InvocationId>()
                            .expect("Owner invocation id should be a valid UUID");
                        let recorder =
                            Recorder::new(owner_invocation_id, function_id.clone(), start_instance);
                        let inputs =
                            proto_data_sets_to_composition_sets(metadata_sets.clone(), data_option);
                        dispatcher_sender(
                            export_registry.clone(),
                            start_time,
                            remote_invocation_id,
                            function_id,
                            caching,
                            inputs,
                            recorder,
                        );
                    }
                    queue_message::QueueMessage::Invocations(invocations) => {
                        trace!("Queue Client recieved invocation");
                        invocation_request_in_flight = false;

                        // mark remote as having work, so we ask for more as idle cores change
                        remote_had_work = true;
                        let start_instance = Instant::now();
                        let start_time =
                            std::time::SystemTime::elapsed(&std::time::SystemTime::UNIX_EPOCH)
                                .unwrap();
                        for invocation in invocations.invocations {
                            let Invocation {
                                remote_invocation_id,
                                metadata_sets,
                                owner_invocation_id,
                                ..
                            } = &invocation;
                            let remote_invocation_id = *remote_invocation_id;
                            if cancelled_remote_invocations.remove(&remote_invocation_id) {
                                trace!(
                                    "Discarding stale cancellation before reused invocation {}",
                                    remote_invocation_id
                                );
                            }
                            work_from_remote += 1;
                            let (function_id, caching) =
                                queue_function_args_from_invocation(&invocation).expect(
                                "Remote invocation message should always contain invocation details",
                            );
                            let owner_invocation_id = owner_invocation_id
                                .parse::<InvocationId>()
                                .expect("Owner invocation id should be a valid UUID");
                            let recorder = Recorder::new(
                                owner_invocation_id,
                                function_id.clone(),
                                start_instance,
                            );
                            let inputs = proto_data_sets_to_composition_sets(
                                metadata_sets.clone(),
                                data_option.clone(),
                            );
                            dispatcher_sender(
                                export_registry.clone(),
                                start_time,
                                remote_invocation_id,
                                function_id,
                                caching,
                                inputs,
                                recorder,
                            )
                        }
                    }
                }
            }
            PollingOption::Message(Err(error), _) => {
                // A malformed message means the connection is unusable, tear it down so
                // the caller can re-establish it instead of crashing the whole node.
                error!("Receiving remote queue message failed with: {}", error);
                break;
            }
            PollingOption::Disconnected => {
                // The remote node disconnected, stop the loop so the caller can reconnect.
                break;
            }
            PollingOption::Results(remote_invocation_id, results) => {
                work_from_remote -= 1;
                let was_cancelled = cancelled_remote_invocations.remove(&remote_invocation_id);

                if was_cancelled {
                    trace!(
                        "Returning completion acknowledgement for canceled remote invocation {}",
                        remote_invocation_id
                    );
                }
                trace!("Queue Client sending out result");
                if message_sender.send(results).await.is_err() {
                    break;
                }
            }
            // getting a notification so should poll the queue
            PollingOption::IdleChanged => {
                trace!(
                    "Queue Client checking updated idle-core state, remote_had_work {}",
                    remote_had_work,
                );
            }
            PollingOption::LocalCoreCountChanged(new_core_number) => {
                trace!("Sending new local core count: {}", new_core_number);
                num_local_cores = new_core_number;
                if message_sender
                    .send(remote_message::RemoteMessage::NodeUpdate(NodeUpdate {
                        num_local_cores: new_core_number as u64,
                    }))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }
        trace!(
            "remote had work: {}, invocation in flight: {}",
            remote_had_work,
            invocation_request_in_flight
        );
        if remote_had_work && !invocation_request_in_flight {
            // ! This currently assumes all works in the queue comes from a single remote master.
            // ! The assumption starts to break if there are things directly enqueued or come from multiple master.
            // ! The reason we need this assumption is, because otherwise, the client cannot get a good bound
            // ! of how full the the queue is, since the dispatcher may not have processed new invocations added by
            // ! a message received to the work queue yet when we perform this check.
            let idle = idle_cores();
            // Want to make sure we fill the prefetching buffer.
            let max_prefetch = num_local_cores * prefetch_multiplier;
            // The amount of work that is in the system from the remote, need to subtract the amount actually running on a compute core
            // everything else is still in prefetching
            let current_prefetch = work_from_remote.saturating_sub(num_local_cores - idle);
            // If we have 0 prefetching, ask for as much as we have idle cores
            let engine_number = if prefetch_multiplier == 0 {
                if work_from_remote < num_local_cores {
                    num_local_cores - work_from_remote
                } else {
                    continue;
                }
            // otherwise check how much prefetch capacity we have and check if we have already reached it
            } else if max_prefetch > current_prefetch {
                max_prefetch - current_prefetch
            // if neither do not ask for more work
            } else {
                continue;
            };
            let engines = EngineType::iter()
                .map(|engine_type| proto::Engine {
                    engine_type: engine_type_dtop(engine_type) as i32,
                    engine_capacity: engine_number as u32,
                })
                .collect();
            trace!("Asking for more work for {} engines", engine_number);
            if message_sender
                .send(remote_message::RemoteMessage::WorkRequest(
                    RepeatedEngines { engines },
                ))
                .await
                .is_err()
            {
                break;
            }
            trace!("Finished sending message asking for more work");
            // set false, to avoid double sending if multiple cores become idle, but did not have a response in between
            invocation_request_in_flight = true;
        }
    }
    // Reaching here means the connection was lost; the caller will attempt to reconnect.
    trace!("remote_queue_client_logic exited, connection to remote was lost");
}

/// Client to ask for work from a remote queue.
/// Whenever the local idle engine number changes, if there are idle engines send out request for work.
/// There is no request for work when the remote has not replied with work (or not replied yet).
/// Expect a single invocation. The change in idle cores going down 1 (from the work that was enqueued),
/// should retrigger asking for more work if there are more idle cores.
pub async fn remote_queue_client(
    socket: TcpStream,
    dispatcher: &'static Dispatcher,
    // TODO: might want a differnet mechanism, to wake them one after each other to go check
    // But each poller also needs to be able to check if there are still cores available,
    // in case the remote sends a message that there is work available
    export_registry: ExportRegistry,
    queue: WorkQueue,
    owner_frontend_url: Option<String>,
    function_cache_path: String,
) {
    // set up the connection by sending a single node info
    let mut local_core_watcher = queue.system_info.num_local_cores_watcher.clone();
    let local_core_count = *local_core_watcher.borrow_and_update();
    let node_info_buffer = serialize_node_info(NodeInfo {
        version: 1,
        id: export_registry.get_node_id(),
        num_local_cores: local_core_count as u64,
    });

    let (read_socket, mut write_socket) = socket.into_split();

    if send_message(&node_info_buffer, &mut write_socket, None)
        .await
        .is_err()
    {
        // Could not even send the initial message, let the caller retry the connection.
        warn!("Failed to send initial message to remote queue, connection lost");
        return;
    }
    trace!("Queue Client sent out initial message");

    // start sender loop
    let (remote_message_sender, remote_message_reciever) = mpsc::channel(64);
    let sender_handle = spawn(remote_queue_client_sender(
        write_socket,
        remote_message_reciever,
    ));
    #[cfg(feature = "at-least-once")]
    let io_completion_delivery_handle = spawn(io_completion_delivery_loop(
        export_registry.clone(),
        remote_message_sender.clone(),
    ));
    // start receiver loop
    let (poll_option_sender, poll_option_receiver) = mpsc::channel(64);
    let receiver_handle = spawn(remote_queue_client_receiver(
        read_socket,
        poll_option_sender.clone(),
    ));
    // start core count loop
    let core_count_handle = spawn(remote_queue_client_core_count(
        local_core_watcher,
        poll_option_sender.clone(),
    ));
    // spawn queue state loop
    let queue_state_handle = spawn(remote_queue_client_queue_state(
        queue.idle_notifier(),
        poll_option_sender.clone(),
    ));

    let owner_frontend_url = owner_frontend_url.map(Arc::new);
    let function_cache_path = Arc::new(function_cache_path);
    remote_queue_client_logic(
        poll_option_receiver,
        remote_message_sender,
        move |registry,
              start_time,
              remote_invocation_id,
              function_id,
              caching,
              input_sets,
              recorder| {
            let sender_clone = poll_option_sender.clone();
            let owner_frontend_url = owner_frontend_url.clone();
            let function_cache_path = function_cache_path.clone();
            spawn(dispatcher_call(
                dispatcher,
                sender_clone,
                registry,
                owner_frontend_url,
                function_cache_path,
                start_time,
                remote_invocation_id,
                function_id,
                caching,
                input_sets,
                recorder,
            ));
        },
        || queue.idle_cores(),
        dispatcher::queue::PREFETCH_PER_CORE,
        export_registry,
        local_core_count,
    )
    .await;

    // The logic loop returned because the connection was lost, stop the helper tasks so they do not
    // linger waiting on a dead socket or closed channels.
    sender_handle.abort();
    #[cfg(feature = "at-least-once")]
    io_completion_delivery_handle.abort();
    receiver_handle.abort();
    core_count_handle.abort();
    queue_state_handle.abort();
}

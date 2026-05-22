use crate::{
    data::ExportRegistry,
    deserialize_node_info, deserialize_queue_message, deserialize_remote_message,
    proto::{
        self, queue_message, remote_message, Invocation, NodeInfo, NodeUpdate, QueueMessage,
        RemoteMessage, RepeatedEngines, Response,
    },
    serialize_node_info, serialize_queue_message, serialize_remote_message,
    util::{
        collect_remote_data_references, engine_type_dtop, engine_type_ptod,
        pack_metadata_size_and_flags, proto_data_sets_to_composition_sets,
        proto_data_sets_to_composition_sets_with_delete_on_drop, recorder_add_timestamps,
        recorder_dtop, unpack_metadata_size_and_flags, ADDITIONAL_DATA_BUFFER, NO_FLAGS,
    },
    DispatcherCommand,
};
use dandelion_commons::{
    err_dandelion, records::Recorder, DandelionError, DandelionResult, MultinodeError,
};
use dispatcher::queue::{get_engine_flag, WorkQueue};
use futures::{future::Either, stream::FuturesUnordered, FutureExt, StreamExt};
use log::{error, trace, warn};
use machine_interface::{
    composition::{CompositionSet, RemoteData},
    function_driver::{WorkDone, WorkToDo},
    machine_config::{EngineType, IntoEnumIterator},
};
use prost::bytes::{Bytes, BytesMut};
use std::{
    collections::{BTreeMap, BinaryHeap},
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};
use tokio::{
    io::{split, AsyncReadExt, AsyncWriteExt},
    sync::{mpsc, oneshot, watch, Notify},
};

#[cfg(test)]
mod test;

const _: () = assert!(size_of::<u64>() == size_of::<usize>());

// TODO ADDITIONAL_DATA_BUFFER and data_buffer are currently used only to carry IoData
// We should consider removing this when recursive resolution of IoData is implemented,
// as then all sets will be exchanged via the remote data server.
// TODO handle connection failure
/// To send a message between nodes, always first send the length of the message,
/// then the message, so the other side knows when one message ends.
async fn send_message(
    metadata_buffer: &Bytes,
    mut sender: impl AsyncWriteExt + std::marker::Unpin,
    data_buffer: Option<(Vec<Option<CompositionSet>>, u64)>,
) {
    let metadata_size: u32 = metadata_buffer.len().try_into().unwrap();
    let flags = match data_buffer {
        Some((_, total_size)) => {
            debug_assert!(total_size > 0);
            ADDITIONAL_DATA_BUFFER
        }
        _ => NO_FLAGS,
    };

    let packed_metadata = pack_metadata_size_and_flags(metadata_size, flags);

    sender.write_u64(packed_metadata).await.unwrap();
    sender.write_all(metadata_buffer).await.unwrap();

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
    sender.flush().await.unwrap();
}

// TODO handle connection failure
// For small messages we are expecting repeteatly, could have spezial read function with permanent preallocated buffers
// Issue: serialization does not give constant sizes, so would need to find an upper bound first
async fn receive_message(
    mut receiver: impl AsyncReadExt + std::marker::Unpin,
) -> (Bytes, Option<Bytes>) {
    let packed_metadata = receiver.read_u64().await.unwrap();
    let (metadata_size, _) = unpack_metadata_size_and_flags(packed_metadata);

    // new buffer with size of message
    let mut metadata_buffer = BytesMut::with_capacity(metadata_size as usize);
    while metadata_buffer.len() < metadata_size as usize {
        receiver.read_buf(&mut metadata_buffer).await.unwrap();
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
    (metadata_buffer.freeze(), None)
}

enum QueueOption<Stream: AsyncReadExt + std::marker::Unpin> {
    Message(Stream, remote_message::RemoteMessage, Option<Bytes>),
    WorkAvailable(Arc<Notify>),
}

async fn receive_client_message<Stream: AsyncReadExt + std::marker::Unpin>(
    mut socket: Stream,
) -> QueueOption<Stream> {
    let (message_bytes, data_bytes) = receive_message(&mut socket).await;
    let message = deserialize_remote_message(message_bytes)
        .unwrap()
        .remote_message
        .unwrap();
    QueueOption::Message(socket, message, data_bytes)
}

async fn receive_work_available_notification<Stream: AsyncReadExt + std::marker::Unpin>(
    receiver: Arc<Notify>,
) -> QueueOption<Stream> {
    receiver.notified().await;
    QueueOption::WorkAvailable(receiver)
}

/// Handler for one remote node, polling the local queue for them.
/// The first message from the remote should contain the possible engines it will poll for,
/// and the maximum number of requests for those engines it will poll.
/// Protocol for polling is sending a poll message, saying which engines are polled for.
/// Response is either a single available task or if none are available immediately,
/// a message that notifies the remote of that. If the response is yes,
/// Each task sent out is associated with an id.
/// When a task if finished, the response is to carry that same id, so the promise can be fulfilled.
pub async fn remote_queue_server<Stream: AsyncReadExt + AsyncWriteExt + std::marker::Unpin>(
    socket: Stream,
    queue: WorkQueue,
    export_registry: ExportRegistry,
    remote_data_deletion_sender: mpsc::UnboundedSender<RemoteData>,
) {
    let mut waiting_for_work = false;
    let (mut read_socket, mut write_socket) = split(socket);

    // First ask for the information about the other node
    // Currently not using engine information
    trace!("Queue Server wait for initial message");
    let (node_info_buffer, node_info_data) = receive_message(&mut read_socket).await;
    debug_assert!(node_info_data.is_none());
    let NodeInfo {
        version,
        id: node_id,
        num_local_cores,
    } = deserialize_node_info(node_info_buffer).unwrap();
    assert_eq!(version, 1);
    trace!("Queue Server received initial message");

    // track current number remote cores
    let mut remote_num_cores = num_local_cores;
    queue.add_remote_cores(num_local_cores as usize);

    let mut debt_map = BTreeMap::new();
    let mut free_debt_ids = BinaryHeap::new();
    let mut max_debt_id = 0;

    let mut merged_stream = FuturesUnordered::new();
    merged_stream.push(Either::Right(receive_client_message(read_socket)));
    merged_stream.push(Either::Left(receive_work_available_notification(
        queue.queueing_notifier(),
    )));

    // are ready, wait for the remote to ask for work or return completed tasks
    while let Some(current_future) = merged_stream.next().await {
        match current_future {
            QueueOption::Message(read_socket, message, data_option) => {
                match message {
                    remote_message::RemoteMessage::WorkRequest(work_request) => {
                        debug_assert!(data_option.is_none());
                        // For now just send one matching function
                        let engines = work_request.engines;
                        let mut engine_flags = 0;
                        for engine in engines {
                            engine_flags |=
                                get_engine_flag(engine_type_ptod(engine.engine_type).unwrap());
                        }
                        trace!(
                            "Queue Server received work request for engine flags: {}",
                            engine_flags
                        );
                        // poll work
                        let (queue_message, data_buffer) = if let Some((work, debt)) =
                            queue.try_get_work_for_remote(engine_flags, node_id)
                        {
                            // there is some work so send it out
                            // find the local function id to use
                            let promise_id = if let Some(free_id) = free_debt_ids.pop() {
                                free_id
                            } else {
                                let promise_id = max_debt_id;
                                max_debt_id += 1;
                                promise_id
                            };
                            let (function_id, data_sets, mut recorder, caching) = match work {
                                // Todo send along relevant information, like caching bool and recorder start time
                                WorkToDo::FunctionArguments {
                                    function_id,
                                    function_alternatives: _,
                                    input_sets,
                                    metadata: _,
                                    caching,
                                    recorder,
                                } => (function_id, input_sets, recorder, caching),
                                WorkToDo::SetsToResolve { input_sets: _ }
                                | WorkToDo::RemoteToDelete { remote_data: _ }
                                | WorkToDo::Shutdown(_) => {
                                    panic!("Should only get function arguments when polling for remote queue")
                                }
                            };
                            recorder.record(dandelion_commons::records::RecordPoint::RemoteTake);
                            let start_reference = SystemTime::elapsed(&std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_micros();
                            let remote_data_references = collect_remote_data_references(&data_sets);
                            debt_map.insert(
                                promise_id,
                                (debt, recorder, start_reference, remote_data_references),
                            );
                            let metadata_sets =
                                export_registry.composition_sets_to_proto(data_sets).await;
                            (
                                QueueMessage {
                                    queue_message: Some(queue_message::QueueMessage::Invocation(
                                        Invocation {
                                            metadata_sets,
                                            function_id: function_id.to_string(),
                                            invocation_id: promise_id,
                                            caching,
                                        },
                                    )),
                                },
                                None, // data_sets,
                            )
                        } else {
                            waiting_for_work = true;
                            // there is no work, so send message accordingly
                            (
                                QueueMessage {
                                    queue_message: Some(queue_message::QueueMessage::NoWork(true)),
                                },
                                None,
                            )
                        };
                        let message_bytes = serialize_queue_message(queue_message);
                        send_message(&message_bytes, &mut write_socket, data_buffer).await;
                    }
                    remote_message::RemoteMessage::Response(response) => {
                        debug_assert!(data_option.is_none());
                        trace!("Queue Server received response");
                        let Response {
                            invocation_id,
                            response,
                        } = response;
                        // TODO: handle failure
                        let (debt, recorder, start_epoch, remote_data_references) = debt_map
                            .remove(&invocation_id)
                            .expect("Should always get back function response for a present debt");
                        free_debt_ids.push(invocation_id);
                        let result = match response.unwrap() {
                            proto::response::Response::MetadataSets(metadata_sets) => {
                                recorder_add_timestamps(
                                    recorder,
                                    metadata_sets.timestamps,
                                    start_epoch,
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
                        drop(remote_data_references);
                        debt.fulfill(result)
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
                }
                merged_stream.push(Either::Right(receive_client_message(read_socket)));
            }
            QueueOption::WorkAvailable(receiver) => {
                trace!("Queue Server received work available notification");
                if waiting_for_work {
                    waiting_for_work = false;
                    let message = serialize_queue_message(QueueMessage {
                        queue_message: Some(queue_message::QueueMessage::NoWork(false)),
                    });
                    send_message(&message, &mut write_socket, None).await;
                }
                merged_stream.push(Either::Left(receive_work_available_notification(receiver)));
            }
        }
    }
}

enum PollingOption<Stream: AsyncReadExt + std::marker::Unpin> {
    Message(
        Stream,
        DandelionResult<queue_message::QueueMessage>,
        Option<Bytes>,
    ),
    IdleCoresChanged(watch::Receiver<u32>, u32),
    LocalCoreCountChanged(watch::Receiver<usize>, usize),
    // Results(RemoteMessage, Option<(Vec<Option<CompositionSet>>, u64)>),
    Results(RemoteMessage),
}

async fn receive_queue_message<Stream: AsyncReadExt + std::marker::Unpin>(
    mut receiver: Stream,
) -> PollingOption<Stream> {
    let (message_buffer, data_buf) = receive_message(&mut receiver).await;
    let new_message = deserialize_queue_message(message_buffer)
        .and_then(|message| Ok(message.queue_message.unwrap()));
    PollingOption::Message(receiver, new_message, data_buf)
}

async fn poll_idle_core<Stream: AsyncReadExt + std::marker::Unpin>(
    mut receiver: watch::Receiver<u32>,
) -> PollingOption<Stream> {
    receiver.changed().await.unwrap();
    let engine_state = *receiver.borrow_and_update();
    PollingOption::IdleCoresChanged(receiver, engine_state)
}

async fn poll_local_core_count<Stream: AsyncReadExt + std::marker::Unpin>(
    mut receiver: watch::Receiver<usize>,
) -> PollingOption<Stream> {
    receiver.changed().await.unwrap();
    let num_local_cores = *receiver.borrow_and_update();
    PollingOption::LocalCoreCountChanged(receiver, num_local_cores)
}

async fn poll_results<Stream: AsyncReadExt + std::marker::Unpin>(
    result_receiver: oneshot::Receiver<DandelionResult<(Vec<Option<CompositionSet>>, Recorder)>>,
    invocation_id: u32,
    export_registry: ExportRegistry,
    start_time: Duration,
) -> PollingOption<Stream> {
    let response_message = match result_receiver.await.unwrap() {
        Ok((sets, recorder)) => {
            let metadata_sets = export_registry.composition_sets_to_proto(sets).await;
            proto::response::Response::MetadataSets(proto::RepeatedMetadataSet {
                metadata_sets,
                timestamps: recorder_dtop(recorder, start_time),
            })
        }
        Err(err) => proto::response::Response::ErrorMsg(err.error.to_string()),
    };
    PollingOption::Results(
        RemoteMessage {
            remote_message: Some(proto::remote_message::RemoteMessage::Response(Response {
                invocation_id,
                response: Some(response_message),
            })),
        },
        // response_set,
    )
}

/// Client to ask for work from a remote queue.
/// Whenever the local idle engine number changes, if there are idle engines send out request for work.
/// There is no request for work when the remote has not replied with work (or not replied yet).
/// Expect a single invocation. The change in idle cores going down 1 (from the work that was enqueued),
/// should retrigger asking for more work if there are more idle cores.
pub async fn remote_queue_client<Stream: AsyncReadExt + AsyncWriteExt + std::marker::Unpin>(
    socket: Stream,
    sender: mpsc::Sender<DispatcherCommand>,
    // TODO: might want a differnet mechanism, to wake them one after each other to go check
    // But each poller also needs to be able to check if there are still cores available,
    // in case the remote sends a message that there is work available
    export_registry: ExportRegistry,
    queue: WorkQueue,
) {
    // local state keeping variables
    let mut remote_had_work = true;

    // set up the connection by sending a single node info
    let node_info_buffer = serialize_node_info(NodeInfo {
        version: 1,
        id: export_registry.get_node_id(),
        num_local_cores: { *queue.system_info.num_local_cores_watcher.borrow() } as u64,
    });

    let (read_socket, mut write_socket) = split(socket);

    send_message(&node_info_buffer, &mut write_socket, None).await;
    trace!("Queue Client sent out initial message");

    // Create a second copy of the watcher to wait on asynchronously, marke changed to check once in the beginning,
    // in case there are already idle cores
    let local_available = queue.idle_watcher();
    let mut new_local_available = local_available.clone();
    new_local_available.mark_changed();

    // stream for the promises of the local requests wait for them to resolve
    // Once stream interfaces stabilize would be nice to move this to merged streams, but for now this seems
    let mut merged_stream = FuturesUnordered::new();
    merged_stream.push(Either::Right(Either::Right(poll_idle_core(
        new_local_available,
    ))));
    merged_stream.push(Either::Right(
        receive_queue_message(read_socket).left_future(),
    ));
    merged_stream.push(Either::Left(Either::Right(poll_local_core_count(
        queue.system_info.num_local_cores_watcher.clone(),
    ))));

    while let Some(current_future) = merged_stream.next().await {
        match current_future {
            PollingOption::Message(read_socket, Ok(message), data_option) => {
                match message {
                    // remote did not have work, can ignore local capacity to work until we get a message the more is available
                    queue_message::QueueMessage::NoWork(true) => {
                        trace!("Queue Client recieved NoWork(true)");
                        debug_assert!(data_option.is_none());
                        remote_had_work = false
                    }
                    // remote signals it may have work so can ask for it, if we have capacity
                    queue_message::QueueMessage::NoWork(false) => {
                        trace!("Queue Client recieved NoWork(false)");
                        debug_assert!(data_option.is_none());
                        let engine_capacity = *local_available.borrow();
                        if engine_capacity > 0 {
                            let engines = EngineType::iter()
                                .map(|engine_type| proto::Engine {
                                    engine_type: engine_type_dtop(engine_type) as i32,
                                    engine_capacity,
                                })
                                .collect();
                            let request_buffer = serialize_remote_message(RemoteMessage {
                                remote_message: Some(remote_message::RemoteMessage::WorkRequest(
                                    RepeatedEngines { engines },
                                )),
                            });
                            send_message(&request_buffer, &mut write_socket, None).await;
                        } else {
                            // Only set to true, if we did not send out a message already,
                            // otherwise avoid retriggering sending a message on state change, before we get answer.
                            remote_had_work = true;
                        }
                    }
                    queue_message::QueueMessage::Invocation(invocation) => {
                        trace!("Queue Client recieved invocation");
                        // mark remote as having work, so we ask for more as idle cores change
                        remote_had_work = true;
                        let start_instance = Instant::now();
                        let start_time =
                            std::time::SystemTime::elapsed(&std::time::SystemTime::UNIX_EPOCH)
                                .unwrap();
                        let Invocation {
                            invocation_id,
                            function_id,
                            metadata_sets,
                            caching,
                        } = invocation;
                        let function_arc = Arc::new(function_id);
                        let recorder = Recorder::new(function_arc.clone(), start_instance);
                        let inputs =
                            proto_data_sets_to_composition_sets(metadata_sets, data_option);
                        let (callback_sender, callback_receriver) = oneshot::channel();
                        sender
                            .send(DispatcherCommand::RemoteFunctionRequest {
                                function_id: function_arc,
                                inputs,
                                is_cold: !caching,
                                recorder,
                                callback: callback_sender,
                            })
                            .await
                            .unwrap();
                        merged_stream.push(Either::Left(Either::Left(poll_results(
                            callback_receriver,
                            invocation_id,
                            export_registry.clone(),
                            start_time,
                        ))));
                    }
                }
                merged_stream.push(Either::Right(Either::Left(receive_queue_message(
                    read_socket,
                ))));
            }
            PollingOption::Message(_, Err(error), _) => {
                // TODO: recover from message reception failure
                panic!("Receiving remote queue message faied with: {}", error);
            }
            PollingOption::Results(results) => {
                trace!("Queue Client sending out result");
                send_message(&serialize_remote_message(results), &mut write_socket, None).await;
            }
            // getting a notification so should poll the queue
            PollingOption::IdleCoresChanged(receiver, available_engines) => {
                trace!(
                    "Queue Client checking updated idle state: {:?}",
                    available_engines
                );
                // send message to get work if we have not asked already
                if remote_had_work && available_engines > 0 {
                    let engines: Vec<_> = EngineType::iter()
                        .map(|engine_type| proto::Engine {
                            engine_type: engine_type_dtop(engine_type) as i32,
                            engine_capacity: available_engines,
                        })
                        .collect();

                    let request_buffer = serialize_remote_message(RemoteMessage {
                        remote_message: Some(remote_message::RemoteMessage::WorkRequest(
                            RepeatedEngines { engines },
                        )),
                    });
                    trace!("Asking for more work, after seeing more idle engines");
                    send_message(&request_buffer, &mut write_socket, None).await;
                    // set false, to avoid double sending if multiple cores become idle, but did not have a response in between
                    remote_had_work = false;
                }
                merged_stream.push(Either::Right(Either::Right(poll_idle_core(receiver))));
            }
            PollingOption::LocalCoreCountChanged(receiver, num_local_cores) => {
                let request_buffer = serialize_remote_message(RemoteMessage {
                    remote_message: Some(remote_message::RemoteMessage::NodeUpdate(NodeUpdate {
                        num_local_cores: num_local_cores as u64,
                    })),
                });
                trace!("Sending new local core count: {}", num_local_cores);
                send_message(&request_buffer, &mut write_socket, None).await;
                merged_stream.push(Either::Left(Either::Right(poll_local_core_count(receiver))));
            }
        }
    }
    warn!("Exited queue client loop");
}

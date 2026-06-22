use crate::{
    data::ExportRegistry,
    deserialize_node_info, deserialize_queue_message, deserialize_remote_message,
    proto::{
        self, queue_message, remote_message, Invocation, NodeInfo, NodeUpdate, QueueMessage,
        RemoteMessage, RepeatedEngines, RepeatedInvocations, Response,
    },
    serialize_node_info, serialize_queue_message, serialize_remote_message,
    util::{
        composition_sets_to_proto, composition_sets_to_proto_and_refs, engine_type_dtop,
        engine_type_ptod, pack_metadata_size_and_flags, proto_data_sets_to_composition_sets,
        proto_data_sets_to_composition_sets_with_delete_on_drop, recorder_add_timestamps,
        recorder_dtop, unpack_metadata_size_and_flags, ADDITIONAL_DATA_BUFFER, NO_FLAGS,
    },
};
use dandelion_commons::{
    err_dandelion, records::Recorder, DandelionError, DandelionResult, MultinodeError,
};
use dispatcher::{
    dispatcher::Dispatcher,
    queue::{get_engine_flag, WorkQueue},
};
use log::{error, trace, warn};
use machine_interface::{
    composition::{CompositionSet, RemoteData},
    function_driver::{WorkDone, WorkToDo},
    machine_config::{EngineType, IntoEnumIterator},
    promise::Debt,
};
use prost::bytes::{Bytes, BytesMut};
use std::{
    collections::{BTreeMap, BinaryHeap},
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
    sync::{mpsc, watch, Notify},
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
    mut sender: impl AsyncWriteExt + Unpin,
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
async fn receive_message(mut receiver: impl AsyncReadExt + Unpin) -> (Bytes, Option<Bytes>) {
    let packed_metadata = receiver.read_u64().await.unwrap();
    let (metadata_size, _) = unpack_metadata_size_and_flags(packed_metadata);
    trace!("strart receiving: {}", metadata_size);

    // new buffer with size of message
    let mut metadata_buffer = BytesMut::with_capacity(metadata_size as usize);
    while metadata_buffer.len() < metadata_size as usize {
        receiver.read_buf(&mut metadata_buffer).await.unwrap();
    }
    trace!("finish receiving");

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

enum QueueOption {
    Message(remote_message::RemoteMessage, Option<Bytes>),
    WorkAvailable,
    TryOffload(WorkToDo, machine_interface::promise::Debt),
}

async fn remote_queue_sever_notification(receiver: Arc<Notify>, sender: mpsc::Sender<QueueOption>) {
    loop {
        receiver.notified().await;
        sender.send(QueueOption::WorkAvailable).await.unwrap();
    }
}

/// The reciever socket handling for the remote queue server
async fn remote_queue_server_receiver(
    mut socket: OwnedReadHalf,
    sender: mpsc::Sender<QueueOption>,
) {
    loop {
        let (message_buffer, _) = receive_message(&mut socket).await;
        let message = deserialize_remote_message(message_buffer)
            .unwrap()
            .remote_message
            .unwrap();
        sender
            .send(QueueOption::Message(message, None))
            .await
            .unwrap();
    }
}

/// The sender docket handling for the remote queue server
/// check if we can unite this and the reciever with the other one, by using traits
async fn remote_queue_server_sender(
    mut socket: OwnedWriteHalf,
    mut receiver: mpsc::Receiver<queue_message::QueueMessage>,
) {
    while let Some(queue_message) = receiver.recv().await {
        let message_buffer = serialize_queue_message(QueueMessage {
            queue_message: Some(queue_message),
        });
        send_message(&message_buffer, &mut socket, None).await;
    }
}

/// Translating the messages from the queue for offlaoding into something the server logic understands
async fn remote_queue_server_try_offload(
    mut queue_receiver: mpsc::UnboundedReceiver<(WorkToDo, Debt)>,
    sender: mpsc::Sender<QueueOption>,
) {
    while let Some((work, debt)) = queue_receiver.recv().await {
        sender
            .send(QueueOption::TryOffload(work, debt))
            .await
            .unwrap();
    }
}

/// The protocol logic handling for the remote queue server
async fn remote_queue_server_logic(
    mut message_receiver: mpsc::Receiver<QueueOption>,
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
                            let work_found = queue.try_get_work_for_remote(
                                engine_flags,
                                node_id,
                                engine.engine_capacity as usize,
                            );
                            trace!(
                                "Found work, adding {} invocations to response",
                                work_found.len()
                            );
                            // TODO: consider limiting the work we give to a node based on the know max capacity,
                            // to limit potential stragglers if we know the node asked for more than it can handle (possibly because of race conditions)
                            // do not give even more.
                            invocations.extend(work_found.into_iter().map(|(work, debt)|
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
                                let (function_id, data_sets, recorder, &caching) = match &work {
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
                                let mut new_recorder = recorder.clone();
                                new_recorder
                                    .record(dandelion_commons::records::RecordPoint::RemoteTake);
                                let start_reference = SystemTime::elapsed(&std::time::UNIX_EPOCH)
                                    .unwrap()
                                    .as_micros();
                                let (metadata_sets, remote_data_references) =
                                    composition_sets_to_proto_and_refs(
                                        data_sets,
                                        |item, context| {
                                            export_registry.insert_function(
                                                item,
                                                context,
                                                Some(remote_data_deletion_sender.clone()),
                                            )
                                        },
                                    );
                                new_recorder.record(dandelion_commons::records::RecordPoint::RegistryInserted);
                                let caching = caching;
                                let function_id = function_id.to_string();
                                debt_map.insert(
                                    promise_id,
                                    (
                                        debt,
                                        new_recorder,
                                        start_reference,
                                        remote_data_references,
                                        work,
                                    ),
                                );
                                Invocation {
                                    metadata_sets,
                                    function_id,
                                    invocation_id: promise_id,
                                    caching,
                                }
                            }));
                        }
                        if invocations.is_empty() {
                            waiting_for_work = true;
                            trace!("No work available");
                            // there is no work, so send message accordingly
                            message_sender
                                .send(queue_message::QueueMessage::NoWork(true))
                                .await
                                .unwrap();
                        } else {
                            invocations_running += invocations.len();
                            message_sender
                                .send(queue_message::QueueMessage::Invocations(
                                    RepeatedInvocations { invocations },
                                ))
                                .await
                                .unwrap();
                        }
                    }
                    remote_message::RemoteMessage::Response(response) => {
                        invocations_running -= 1;
                        debug_assert!(data_option.is_none());
                        trace!("Queue Server received response");
                        let Response {
                            invocation_id,
                            response,
                        } = response;
                        // TODO: handle failure
                        let (debt, mut recorder, start_epoch, remote_data_references, work) =
                            debt_map.remove(&invocation_id).expect(
                                "Should always get back function response for a present debt",
                            );
                        free_debt_ids.push(invocation_id);
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
                            queue.reenqueue(work, debt).await;
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
                }
            }
            QueueOption::TryOffload(work, debt) => {
                // if this node already sent enough work for the remote to be at capacity don't send more
                if invocations_running >= remote_num_cores as usize {
                    queue.reenqueue(work, debt).await;
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
                let (function_id, data_sets, recorder, &caching) = match &work {
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
                let mut new_recorder = recorder.clone();
                new_recorder.record(dandelion_commons::records::RecordPoint::RemoteTake);
                let start_reference = SystemTime::elapsed(&std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_micros();
                let (metadata_sets, remote_data_references) =
                    composition_sets_to_proto_and_refs(data_sets, |item, context| {
                        export_registry.insert_function(
                            item,
                            context,
                            Some(remote_data_deletion_sender.clone()),
                        )
                    });
                let caching = caching;
                let function_id = function_id.to_string();
                debt_map.insert(
                    promise_id,
                    (
                        debt,
                        new_recorder,
                        start_reference,
                        remote_data_references,
                        work,
                    ),
                );
                trace!("Prepared work, sending out now");
                let try_offload_message = queue_message::QueueMessage::TryOffload(Invocation {
                    invocation_id: promise_id,
                    function_id,
                    metadata_sets,
                    caching,
                });
                message_sender.send(try_offload_message).await.unwrap();
            }
            QueueOption::WorkAvailable => {
                trace!("Queue Server received work available notification");
                if waiting_for_work {
                    waiting_for_work = false;
                    message_sender
                        .send(queue_message::QueueMessage::NoWork(false))
                        .await
                        .unwrap();
                }
            }
        }
    }
    warn!("Arrived at end of remtote_queue_server_logic, which should stay in the loop forever");
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
    let (node_info_buffer, node_info_data) = receive_message(&mut read_socket).await;
    debug_assert!(node_info_data.is_none());
    let NodeInfo {
        version,
        id: node_id,
        num_local_cores,
    } = deserialize_node_info(node_info_buffer).unwrap();
    assert_eq!(version, 1);
    trace!("Queue Server received initial message");

    // tell the queue about the remote cores
    queue.add_remote_cores(num_local_cores as usize);

    // start sender loop
    let (queue_message_sender, queue_message_reciever) = mpsc::channel(64);
    spawn(remote_queue_server_sender(
        write_socket,
        queue_message_reciever,
    ));
    // start receiver loop
    let (queue_option_sender, queue_option_receiver) = mpsc::channel(64);
    spawn(remote_queue_server_receiver(
        read_socket,
        queue_option_sender.clone(),
    ));
    // spawn notificaiton loop
    spawn(remote_queue_sever_notification(
        queue.queueing_notifier(),
        queue_option_sender.clone(),
    ));
    // spawn loop to check for queue trying to offload
    let (offload_sender, offload_receiver) = mpsc::unbounded_channel();
    queue.add_remote_channel(node_id, offload_sender);
    spawn(remote_queue_server_try_offload(
        offload_receiver,
        queue_option_sender,
    ));

    remote_queue_server_logic(
        queue_option_receiver,
        queue_message_sender,
        queue,
        export_registry,
        remote_data_deletion_sender,
        node_id,
        num_local_cores,
    )
    .await;
}

pub enum PollingOption {
    Message(DandelionResult<queue_message::QueueMessage>, Option<Bytes>),
    QueueStateChanged(usize),
    LocalCoreCountChanged(usize),
    // Results(RemoteMessage, Option<(Vec<Option<CompositionSet>>, u64)>),
    Results(remote_message::RemoteMessage),
}

async fn remote_queue_client_receiver(
    mut socket: OwnedReadHalf,
    sender: mpsc::Sender<PollingOption>,
) {
    loop {
        let (message_buffer, _) = receive_message(&mut socket).await;
        let message = deserialize_queue_message(message_buffer)
            .and_then(|message| Ok(message.queue_message.unwrap()));
        sender
            .send(PollingOption::Message(message, None))
            .await
            .unwrap();
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
        send_message(&message_buffer, &mut socket, None).await;
    }
}

// TODO: think about limiting number of notification, to make sure we are not adding
// additional load when the queue is filled / emptied in big strides
async fn remote_queue_client_queue_state(
    mut receiver: watch::Receiver<usize>,
    sender: mpsc::Sender<PollingOption>,
) {
    loop {
        receiver.changed().await.unwrap();
        trace!("received local queue state");
        let queue_state = *receiver.borrow_and_update();
        sender
            .send(PollingOption::QueueStateChanged(queue_state))
            .await
            .unwrap();
    }
}

async fn remote_queue_client_core_count(
    mut receiver: watch::Receiver<usize>,
    sender: mpsc::Sender<PollingOption>,
) {
    loop {
        receiver.changed().await.unwrap();
        let num_local_cores = *receiver.borrow_and_update();
        sender
            .send(PollingOption::LocalCoreCountChanged(num_local_cores))
            .await
            .unwrap();
    }
}

async fn dispatcher_call(
    dispatcher: &'static Dispatcher,
    sender: mpsc::Sender<PollingOption>,
    export_registry: ExportRegistry,
    start_time: Duration,
    invocation_id: u32,
    function_id: Arc<String>,
    input_sets: Vec<Option<CompositionSet>>,
    caching: bool,
    recorder: Recorder,
) {
    let function_result = dispatcher
        .queue_function(function_id, input_sets, caching, recorder.clone())
        .await;
    let response_message = match function_result {
        Ok(sets) => {
            let metadata_sets = composition_sets_to_proto(sets, |item, context| {
                export_registry.insert_function(item, context, None)
            });
            proto::response::Response::MetadataSets(proto::RepeatedMetadataSet {
                metadata_sets,
                timestamps: recorder_dtop(recorder, start_time),
            })
        }
        Err(err) => proto::response::Response::ErrorMsg(err.error.to_string()),
    };
    sender
        .send(PollingOption::Results(
            remote_message::RemoteMessage::Response(Response {
                invocation_id,
                response: Some(response_message),
            }),
        ))
        .await
        .unwrap();
}

async fn remote_queue_client_logic(
    mut receiver: mpsc::Receiver<PollingOption>,
    message_sender: mpsc::Sender<remote_message::RemoteMessage>,
    dispatcher_sender: impl Fn(
        ExportRegistry,
        Duration,
        u32,
        Arc<String>,
        Vec<Option<CompositionSet>>,
        bool,
        Recorder,
    ),
    export_registry: ExportRegistry,
    mut num_local_cores: usize,
) {
    let mut remote_had_work = true;
    let mut queue_state = 0;
    // This makes sure we don't overfetch from this node.
    // TODO: think about the general issue of state synchronization between the dispatcher and multinode client adding things,
    // and the engines and server taking things.
    let mut work_from_remote = 0;

    while let Some(current_future) = receiver.recv().await {
        match current_future {
            PollingOption::Message(Ok(message), data_option) => {
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

                        // let current_jobs_queued = *local_queue_state.borrow();
                        let occupancy = std::cmp::max(queue_state, work_from_remote);
                        if occupancy < num_local_cores {
                            let engines = EngineType::iter()
                                .map(|engine_type| proto::Engine {
                                    engine_type: engine_type_dtop(engine_type) as i32,
                                    engine_capacity: (num_local_cores - occupancy) as u32,
                                })
                                .collect();
                            message_sender
                                .send(remote_message::RemoteMessage::WorkRequest(
                                    RepeatedEngines { engines },
                                ))
                                .await
                                .unwrap();
                            remote_had_work = false;
                        } else {
                            // Only set to true, if we did not send out a message already,
                            // otherwise avoid retriggering sending a message on state change, before we get answer.
                            remote_had_work = true;
                        }
                    }
                    // TODO for try offload decide when to refuse work
                    queue_message::QueueMessage::TryOffload(invocation) => {
                        trace!("Queue Client recieved try offload");

                        work_from_remote += 1;

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
                        dispatcher_sender(
                            export_registry.clone(),
                            start_time,
                            invocation_id,
                            function_arc,
                            inputs,
                            !caching,
                            recorder,
                        );
                    }
                    queue_message::QueueMessage::Invocations(invocations) => {
                        trace!("Queue Client recieved invocation");

                        // mark remote as having work, so we ask for more as idle cores change
                        remote_had_work = true;
                        work_from_remote += invocations.invocations.len();
                        let start_instance = Instant::now();
                        let start_time =
                            std::time::SystemTime::elapsed(&std::time::SystemTime::UNIX_EPOCH)
                                .unwrap();
                        for invocation in invocations.invocations {
                            let Invocation {
                                invocation_id,
                                function_id,
                                metadata_sets,
                                caching,
                            } = invocation;
                            let function_arc = Arc::new(function_id);
                            let recorder = Recorder::new(function_arc.clone(), start_instance);
                            let inputs = proto_data_sets_to_composition_sets(
                                metadata_sets,
                                data_option.clone(),
                            );
                            dispatcher_sender(
                                export_registry.clone(),
                                start_time,
                                invocation_id,
                                function_arc,
                                inputs,
                                !caching,
                                recorder,
                            )
                        }
                    }
                }
            }
            PollingOption::Message(Err(error), _) => {
                // TODO: recover from message reception failure
                panic!("Receiving remote queue message faied with: {}", error);
            }
            PollingOption::Results(results) => {
                trace!("Queue Client sending out result");
                work_from_remote -= 1;
                message_sender.send(results).await.unwrap();
                let occupancy = std::cmp::max(queue_state, work_from_remote);
                if remote_had_work && occupancy < num_local_cores {
                    let engines: Vec<_> = EngineType::iter()
                        .map(|engine_type| proto::Engine {
                            engine_type: engine_type_dtop(engine_type) as i32,
                            engine_capacity: (num_local_cores - occupancy) as u32,
                        })
                        .collect();

                    trace!("Asking for more work");
                    message_sender
                        .send(remote_message::RemoteMessage::WorkRequest(
                            RepeatedEngines { engines },
                        ))
                        .await
                        .unwrap();
                    trace!("Finished sending the message asking for more work");
                    // set false, to avoid double sending if multiple cores become idle, but did not have a response in between
                    remote_had_work = false;
                }
            }
            // getting a notification so should poll the queue
            PollingOption::QueueStateChanged(current_queue_state) => {
                trace!(
                    "Queue Client checking updated queue state: {:?}, remote_had_work {}",
                    current_queue_state,
                    remote_had_work,
                );
                queue_state = current_queue_state;
                // send message to get work if we have not asked already
                let occupancy = std::cmp::max(queue_state, work_from_remote);
                if remote_had_work && occupancy < num_local_cores {
                    let engines: Vec<_> = EngineType::iter()
                        .map(|engine_type| proto::Engine {
                            engine_type: engine_type_dtop(engine_type) as i32,
                            engine_capacity: (num_local_cores - occupancy) as u32,
                        })
                        .collect();

                    trace!("Asking for more work");
                    message_sender
                        .send(remote_message::RemoteMessage::WorkRequest(
                            RepeatedEngines { engines },
                        ))
                        .await
                        .unwrap();
                    trace!("Finished sending the message asking for more work");
                    // set false, to avoid double sending if multiple cores become idle, but did not have a response in between
                    remote_had_work = false;
                }
            }
            PollingOption::LocalCoreCountChanged(new_core_number) => {
                trace!("Sending new local core count: {}", new_core_number);
                num_local_cores = new_core_number;
                message_sender
                    .send(remote_message::RemoteMessage::NodeUpdate(NodeUpdate {
                        num_local_cores: new_core_number as u64,
                    }))
                    .await
                    .unwrap();
                // check if we now want to get more work
                let occupancy = std::cmp::max(queue_state, work_from_remote);
                if remote_had_work && occupancy < num_local_cores {
                    let engines: Vec<_> = EngineType::iter()
                        .map(|engine_type| proto::Engine {
                            engine_type: engine_type_dtop(engine_type) as i32,
                            engine_capacity: (num_local_cores - occupancy) as u32,
                        })
                        .collect();

                    trace!("Asking for more work");
                    message_sender
                        .send(remote_message::RemoteMessage::WorkRequest(
                            RepeatedEngines { engines },
                        ))
                        .await
                        .unwrap();
                    trace!("Finished sending the message asking for more work");
                    // set false, to avoid double sending if multiple cores become idle, but did not have a response in between
                    remote_had_work = false;
                }
            }
        }
    }
    warn!("Arrived at end of remote_qeueu_client_logic, which should stay in the loop forever");
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

    send_message(&node_info_buffer, &mut write_socket, None).await;
    trace!("Queue Client sent out initial message");

    // Create a second copy of the watcher to wait on asynchronously, marke changed to check once in the beginning,
    // in case there are already idle cores
    let mut local_queue_state = queue.queue_state_watcher();
    // needs to be marked changed, so a server with an empty queue and already stead number of local cores starts to poll
    local_queue_state.mark_changed();

    // start sender loop
    let (remote_message_sender, remote_message_reciever) = mpsc::channel(64);
    spawn(remote_queue_client_sender(
        write_socket,
        remote_message_reciever,
    ));
    // start receiver loop
    let (poll_option_sender, poll_option_receiver) = mpsc::channel(64);
    spawn(remote_queue_client_receiver(
        read_socket,
        poll_option_sender.clone(),
    ));
    // start core count loop
    spawn(remote_queue_client_core_count(
        local_core_watcher,
        poll_option_sender.clone(),
    ));
    // spawn queue state loop
    spawn(remote_queue_client_queue_state(
        local_queue_state,
        poll_option_sender.clone(),
    ));

    remote_queue_client_logic(
        poll_option_receiver,
        remote_message_sender,
        |registry, start_time, invocation_id, function_id, input_sets, caching, recorder| {
            let sender_clone = poll_option_sender.clone();
            spawn(dispatcher_call(
                dispatcher,
                sender_clone,
                registry,
                start_time,
                invocation_id,
                function_id,
                input_sets,
                caching,
                recorder,
            ));
        },
        export_registry,
        local_core_count,
    )
    .await;
}

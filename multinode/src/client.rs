use crate::{
    deserialize_node_info, deserialize_queue_message, deserialize_remote_message,
    proto::{
        self, queue_message, remote_message, Engine, Invocation, NodeInfo, QueueMessage,
        RemoteMessage, Response,
    },
    serialize_node_info, serialize_queue_message, serialize_remote_message,
    util::{
        composition_sets_to_proto, engine_type_dtop, engine_type_ptod,
        proto_data_sets_to_composition_sets, proto_data_sets_to_context,
    },
    DispatcherCommand,
};
use dandelion_commons::{
    err_dandelion, records::Recorder, DandelionError, DandelionResult, MultinodeError,
};
use dispatcher::queue::{get_engine_flag, WorkQueue};
use futures::{future::Either, stream::FuturesUnordered, FutureExt, StreamExt};
use machine_interface::{
    composition::CompositionSet,
    function_driver::{WorkDone, WorkToDo},
    machine_config::EngineType,
};
use prost::bytes::{Bytes, BytesMut};
use std::{
    collections::{BTreeMap, BinaryHeap},
    sync::Arc,
    time::Instant,
};
use tokio::{
    io::{split, AsyncReadExt, AsyncWriteExt},
    sync::{mpsc, oneshot, watch},
};

#[cfg(test)]
mod test;

const _: () = assert!(size_of::<u64>() == size_of::<usize>());

// TODO handle connection failure
/// To send a message between nodes, always first send the length of the message,
/// then the message, so the other side knows when one message ends.
async fn send_message(buffer: &Bytes, mut sender: impl AsyncWriteExt + std::marker::Unpin) {
    let message_size = buffer.len() as u64;
    sender.write_u64(message_size).await.unwrap();
    sender.write_all(buffer).await.unwrap();
    sender.flush().await.unwrap();
}

// TODO handle connection failure
// For small messages we are expecting repeteatly, could have spezial read function with permanent preallocated buffers
// Issue: serialization does not give constant sizes, so would need to find an upper bound first
async fn receive_message(mut receiver: impl AsyncReadExt + std::marker::Unpin) -> Bytes {
    let message_size = receiver.read_u64().await.unwrap();
    // new buffer with size of message
    let mut new_buffer = BytesMut::with_capacity(message_size as usize);
    let mut bytes_read = 0;
    while bytes_read < message_size as usize {
        bytes_read += receiver.read_buf(&mut new_buffer).await.unwrap();
    }
    new_buffer.freeze()
}

enum QueueOption<Stream: AsyncReadExt + std::marker::Unpin> {
    Message(Stream, remote_message::RemoteMessage),
    WorkAvailable(mpsc::Receiver<()>),
}

async fn receive_client_message<Stream: AsyncReadExt + std::marker::Unpin>(
    mut socket: Stream,
) -> QueueOption<Stream> {
    let message = deserialize_remote_message(receive_message(&mut socket).await)
        .unwrap()
        .remote_message
        .unwrap();
    QueueOption::Message(socket, message)
}

async fn receive_work_available_notification<Stream: AsyncReadExt + std::marker::Unpin>(
    mut receiver: mpsc::Receiver<()>,
) -> QueueOption<Stream> {
    receiver.recv().await.unwrap();
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
    work_available: mpsc::Receiver<()>,
) {
    let mut waiting_for_work = false;
    let (mut read_socket, mut write_socket) = split(socket);

    // First ask for the information about the other node
    // Currently not using engine information
    let NodeInfo {
        version,
        engines: _,
    } = deserialize_node_info(receive_message(&mut read_socket).await).unwrap();
    assert_eq!(version, 1);

    let mut debt_map = BTreeMap::new();
    let mut free_debt_ids = BinaryHeap::new();
    let mut max_debt_id = 0;

    let mut merged_stream = FuturesUnordered::new();
    merged_stream.push(Either::Right(receive_client_message(read_socket)));
    merged_stream.push(Either::Left(receive_work_available_notification(
        work_available,
    )));

    // are ready, wait for the remote to ask for work or return completed tasks
    while let Some(current_future) = merged_stream.next().await {
        match current_future {
            QueueOption::Message(read_socket, message) => {
                match message {
                    remote_message::RemoteMessage::WorkRequest(work_request) => {
                        // For now just send one matching function
                        let NodeInfo {
                            version: _,
                            engines,
                        } = work_request;
                        let mut engine_flags = 0;
                        for engine in engines {
                            engine_flags |=
                                get_engine_flag(engine_type_ptod(engine.engine_type).unwrap());
                        }
                        // poll work
                        let queue_message = if let Some((work, debt)) =
                            queue.try_get_work_no_shutdown(engine_flags)
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
                            debt_map.insert(promise_id, debt);
                            let (function_id, data_sets, caching) = match work {
                                // Todo send along relevant information, like caching bool and recorder start time
                                WorkToDo::FunctionArguments {
                                    function_id,
                                    function_alternatives: _,
                                    input_sets,
                                    metadata: _,
                                    caching,
                                    recorder: _,
                                } => (function_id, input_sets, caching),
                                WorkToDo::Shutdown(_) => {
                                    panic!("Should never get shutdown in remote engine queue")
                                }
                            };
                            QueueMessage {
                                queue_message: Some(queue_message::QueueMessage::Invocation(
                                    Invocation {
                                        data_sets: composition_sets_to_proto(&data_sets),
                                        function_id: function_id.to_string(),
                                        invocation_id: promise_id,
                                        caching,
                                    },
                                )),
                            }
                        } else {
                            waiting_for_work = true;
                            // there is no work, so send message accordingly
                            QueueMessage {
                                queue_message: Some(queue_message::QueueMessage::NoWork(true)),
                            }
                        };
                        let message_bytes = serialize_queue_message(queue_message);
                        send_message(&message_bytes, &mut write_socket).await;
                    }
                    remote_message::RemoteMessage::Response(response) => {
                        let Response {
                            invocation_id,
                            response,
                        } = response;
                        // TODO: handle failure
                        let debt = debt_map
                            .remove(&invocation_id)
                            .expect("Should always get back function response for a present debt");
                        let result = match response.unwrap() {
                            proto::response::Response::DataSets(datasets) => Ok(WorkDone::Context(
                                proto_data_sets_to_context(datasets.data_sets),
                            )),
                            proto::response::Response::ErrorMsg(error_message) => {
                                err_dandelion!(DandelionError::Multinode(
                                    MultinodeError::RequestFailed(error_message)
                                ))
                            }
                        };
                        debt.fulfill(result)
                    }
                }
                merged_stream.push(Either::Right(receive_client_message(read_socket)));
            }
            QueueOption::WorkAvailable(receiver) => {
                if waiting_for_work {
                    waiting_for_work = false;
                    let message = serialize_queue_message(QueueMessage {
                        queue_message: Some(queue_message::QueueMessage::NoWork(false)),
                    });
                    send_message(&message, &mut write_socket).await;
                }
                merged_stream.push(Either::Left(receive_work_available_notification(receiver)));
            }
        }
    }
}

enum PollingOption<Stream: AsyncReadExt + std::marker::Unpin> {
    Message(Stream, DandelionResult<queue_message::QueueMessage>),
    PollFor(
        watch::Receiver<Vec<(EngineType, u32)>>,
        Vec<(EngineType, u32)>,
    ),
    Results(RemoteMessage),
}

async fn receive_queue_message<Stream: AsyncReadExt + std::marker::Unpin>(
    mut receiver: Stream,
) -> PollingOption<Stream> {
    let new_message = deserialize_queue_message(receive_message(&mut receiver).await)
        .and_then(|message| Ok(message.queue_message.unwrap()));
    PollingOption::Message(receiver, new_message)
}

async fn poll_idle_core<Stream: AsyncReadExt + std::marker::Unpin>(
    mut engines: Vec<(EngineType, u32)>,
    mut receiver: watch::Receiver<Vec<(EngineType, u32)>>,
) -> PollingOption<Stream> {
    receiver.changed().await.unwrap();
    {
        let engine_state = receiver.borrow_and_update();
        for engine_index in 0..engine_state.len() {
            engines[engine_index] = engine_state[engine_index];
        }
    }
    PollingOption::PollFor(receiver, engines)
}

async fn poll_results<Stream: AsyncReadExt + std::marker::Unpin>(
    result_receiver: oneshot::Receiver<DandelionResult<(Vec<Option<CompositionSet>>, Recorder)>>,
    invocation_id: u32,
) -> PollingOption<Stream> {
    let response = match result_receiver.await.unwrap() {
        Ok((sets, _)) => proto::response::Response::DataSets(proto::RepeatedDataSets {
            data_sets: composition_sets_to_proto(&sets),
        }),
        Err(err) => proto::response::Response::ErrorMsg(err.error.to_string()),
    };
    PollingOption::Results(RemoteMessage {
        remote_message: Some(proto::remote_message::RemoteMessage::Response(Response {
            invocation_id,
            response: Some(response),
        })),
    })
}

pub async fn remote_queue_client<Stream: AsyncReadExt + AsyncWriteExt + std::marker::Unpin>(
    socket: Stream,
    sender: mpsc::Sender<DispatcherCommand>,
    // TODO: might want a differnet mechanism, to wake them one after each other to go check
    // But each poller also needs to be able to check if there are still cores available,
    // in case the remote sends a message that there is work available
    local_available: watch::Receiver<Vec<(EngineType, u32)>>,
    engines: Vec<(EngineType, u32)>,
) {
    // local state keeping variables
    let mut remote_had_work = true;
    let available_engines = local_available.borrow().clone();

    // set up the connection by sending a single node info
    let proto_engines = engines
        .into_iter()
        .map(|(dandelion_type, number)| Engine {
            engine_type: engine_type_dtop(dandelion_type) as i32,
            engine_capacity: number as u32,
        })
        .collect();
    let node_info_buffer = serialize_node_info(NodeInfo {
        version: 1,
        engines: proto_engines,
    });

    let (read_socket, mut write_socket) = split(socket);

    send_message(&node_info_buffer, &mut write_socket).await;

    // stream for the promises of the local requests wait for them to resolve
    // Once stream interfaces stabilize would be nice to move this to merged streams, but for now this seems
    let mut merged_stream = FuturesUnordered::new();
    merged_stream.push(Either::Right(Either::Right(poll_idle_core(
        available_engines,
        local_available.clone(),
    ))));
    merged_stream.push(Either::Right(
        receive_queue_message(read_socket).left_future(),
    ));

    while let Some(current_future) = merged_stream.next().await {
        match current_future {
            PollingOption::Message(read_socket, Ok(message)) => {
                match message {
                    // remote did not have work, can ignore local capacity to work until we get a message the more is available
                    queue_message::QueueMessage::NoWork(true) => remote_had_work = false,
                    // remote signals it may have work so can ask for it, if we have capacity
                    queue_message::QueueMessage::NoWork(false) => {
                        remote_had_work = true;
                        let engines = local_available
                            .borrow()
                            .iter()
                            .filter_map(|(engine_type, engine_capacity)| {
                                if *engine_capacity > 0 {
                                    Some(proto::Engine {
                                        engine_type: engine_type_dtop(*engine_type) as i32,
                                        engine_capacity: *engine_capacity,
                                    })
                                } else {
                                    None
                                }
                            })
                            .collect();
                        let request_buffer = serialize_remote_message(RemoteMessage {
                            remote_message: Some(remote_message::RemoteMessage::WorkRequest(
                                NodeInfo {
                                    version: 1,
                                    engines,
                                },
                            )),
                        });
                        send_message(&request_buffer, &mut write_socket).await;
                    }
                    queue_message::QueueMessage::Invocation(invocation) => {
                        let Invocation {
                            invocation_id,
                            function_id,
                            data_sets,
                            caching,
                        } = invocation;
                        let function_arc = Arc::new(function_id);
                        let inputs = proto_data_sets_to_composition_sets(data_sets);
                        let recorder = Recorder::new(function_arc.clone(), Instant::now());
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
                        merged_stream.push(Either::Left(poll_results(
                            callback_receriver,
                            invocation_id,
                        )));
                    }
                }
                merged_stream.push(Either::Right(Either::Left(receive_queue_message(
                    read_socket,
                ))));
            }
            PollingOption::Message(_, Err(error)) => {
                // TODO: recover from message reception failure
                panic!("Receiving remote queue message faied with: {}", error);
            }
            PollingOption::Results(results) => {
                send_message(&serialize_remote_message(results), &mut write_socket).await;
            }
            // getting a notification so should poll the queue
            PollingOption::PollFor(receiver, available_engines) => {
                // send message to get work if we have not asked already
                if remote_had_work {
                    let engines = available_engines
                        .iter()
                        .filter_map(|(engine_type, engine_capacity)| {
                            if *engine_capacity > 0 {
                                Some(proto::Engine {
                                    engine_type: engine_type_dtop(*engine_type) as i32,
                                    engine_capacity: *engine_capacity,
                                })
                            } else {
                                None
                            }
                        })
                        .collect();
                    let request_buffer = serialize_remote_message(RemoteMessage {
                        remote_message: Some(remote_message::RemoteMessage::WorkRequest(
                            NodeInfo {
                                version: 1,
                                engines,
                            },
                        )),
                    });
                    send_message(&request_buffer, &mut write_socket).await;
                }
                merged_stream.push(Either::Right(Either::Right(poll_idle_core(
                    available_engines,
                    receiver,
                ))));
            }
        }
    }
}

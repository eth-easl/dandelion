use crate::{
    deserialize_node_info, deserialize_queue_message, deserialize_remote_message,
    proto::{
        self, queue_message, remote_message, Engine, Invocation, NodeInfo, QueueMessage,
        RemoteMessage, Response,
    },
    serialize_node_info, serialize_queue_message,
    util::{
        composition_sets_to_proto, engine_type_dtop, engine_type_ptod, proto_data_sets_to_context,
    },
};
use dandelion_commons::{err_dandelion, DandelionError, DandelionResult, MultinodeError};
use dispatcher::queue::{get_engine_flag, WorkQueue};
use futures::{
    future::Either,
    stream::{repeat_with, select_all, FuturesUnordered},
    FutureExt, Stream, StreamExt,
};
use log::{debug, warn};
use machine_interface::{
    composition::CompositionSet,
    function_driver::{WorkDone, WorkToDo},
    machine_config::EngineType,
};
use prost::bytes::{Bytes, BytesMut};
use std::{
    collections::{BTreeMap, BinaryHeap},
    future::Future,
    process::Output,
    sync::Arc,
    task::Poll,
};
use tokio::{
    io::{split, AsyncReadExt, AsyncWriteExt},
    sync::mpsc::{channel, Receiver},
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

/// Handler for one remote node, polling the local queue for them.
/// The first message from the remote should contain the possible engines it will poll for,
/// and the maximum number of requests for those engines it will poll.
/// Protocol for polling is sending a poll message, saying which engines are polled for.
/// Response is either a single available task or if none are available immediately,
/// a message that notifies the remote of that. If the response is yes,
/// Each task sent out is associated with an id.
/// When a task if finished, the response is to carry that same id, so the promise can be fulfilled.
pub async fn remote_queue_handler<Stream: AsyncReadExt + AsyncWriteExt + std::marker::Unpin>(
    mut socket: Stream,
    queue: WorkQueue,
) {
    // first ask for the information about the other node
    let node_info_buffer = receive_message(&mut socket).await;

    // Currently not using engine information
    let NodeInfo {
        version,
        engines: _,
    } = deserialize_node_info(node_info_buffer).unwrap();
    assert_eq!(version, 1);

    let mut debt_map = BTreeMap::new();
    let mut free_debt_ids = BinaryHeap::new();
    let mut max_debt_id = 0;

    // are ready, wait for the remote to ask for work or return completed tasks
    loop {
        let message_buffer = receive_message(&mut socket).await;
        let message = deserialize_remote_message(message_buffer).unwrap();
        match message.remote_message {
            Some(remote_message::RemoteMessage::WorkRequest(work_request)) => {
                // For now just send one matching function
                let NodeInfo {
                    version: _,
                    engines,
                } = work_request;
                let mut engine_flags = 0;
                for engine in engines {
                    engine_flags |= get_engine_flag(engine_type_ptod(engine.engine_type).unwrap());
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
                    let (function_id, data_sets) = match work {
                        // Todo send along relevant information, like caching bool and recorder start time
                        WorkToDo::FunctionArguments {
                            function_id,
                            function_alternatives: _,
                            input_sets,
                            metadata: _,
                            caching: _,
                            recorder: _,
                        } => (function_id, input_sets),
                        WorkToDo::Shutdown(_) => {
                            panic!("Should never get shutdown in remote engine queue")
                        }
                    };
                    QueueMessage {
                        queue_message: Some(queue_message::QueueMessage::Invocation(Invocation {
                            data_sets: composition_sets_to_proto(&data_sets),
                            function_id: function_id.to_string(),
                            invocation_id: promise_id,
                        })),
                    }
                } else {
                    // there is no work, so send message accordingly
                    QueueMessage {
                        queue_message: Some(queue_message::QueueMessage::NoWork(false)),
                    }
                };
                let message_bytes = serialize_queue_message(queue_message);
                send_message(&message_bytes, &mut socket).await;
            }
            Some(remote_message::RemoteMessage::Response(response)) => {
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
                        err_dandelion!(DandelionError::Multinode(MultinodeError::RequestFailed(
                            error_message
                        )))
                    }
                };
                debt.fulfill(result);
            }
            None => panic!("Should always have a message"),
        }
    }
}

enum PollingOption<Stream: AsyncReadExt + std::marker::Unpin> {
    Message(Stream, DandelionResult<QueueMessage>),
    PollFor(
        Receiver<(machine_interface::machine_config::EngineType, u32)>,
        EngineType,
        u32,
    ),
    Results(RemoteMessage),
}

async fn poll_message<Stream: AsyncReadExt + std::marker::Unpin>(
    mut receiver: Stream,
) -> PollingOption<Stream> {
    let new_message = deserialize_queue_message(receive_message(&mut receiver).await);
    PollingOption::Message(receiver, new_message)
}

async fn poll_idle_core<Stream: AsyncReadExt + std::marker::Unpin>(
    mut receiver: Receiver<(machine_interface::machine_config::EngineType, u32)>,
) -> PollingOption<Stream> {
    let (engine_type, engine_numer) = receiver.recv().await.unwrap();
    PollingOption::PollFor(receiver, engine_type, engine_numer)
}

async fn poll_results<Stream: AsyncReadExt + std::marker::Unpin>(
    result_future: impl Future<Output = DandelionResult<WorkDone>>,
) -> PollingOption<Stream> {
    let response = match result_future.await {
        Ok(WorkDone::Context(context)) => {
            let context_arc = Arc::new(context);
            let composition_sets = context_arc
                .content
                .iter()
                .enumerate()
                .map(|(function_set_id, data_option)| {
                    data_option.as_ref().and_then(|_| {
                        Some(CompositionSet::from((
                            function_set_id,
                            vec![context_arc.clone()],
                        )))
                    })
                })
                .collect();
            Response::DataSets(composition_sets_to_proto(&composition_sets))
        }
        Ok(WorkDone::Resources(_)) => panic!("Should never receive resources at the remote queue"),
        Err(err) => Response::ErrorMsg(err.error.to_string()),
    };
    PollingOption::Response(RemoteMessage {
        remote_message: Some(remote_message::RemoteMessage(response)),
    })
}

async fn remote_queue_poller<Stream: AsyncReadExt + AsyncWriteExt + std::marker::Unpin>(
    socket: Stream,
    queue: WorkQueue,
    // TODO: change to receive the number of nodes to ask work for
    mut local_available: Receiver<(machine_interface::machine_config::EngineType, u32)>,
    engines: Vec<(EngineType, usize)>,
) {
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

    let (mut read_socket, mut write_socket) = split(socket);

    send_message(&node_info_buffer, &mut write_socket).await;

    // stream for the promises of the local requests wait for them to resolve
    // Once stream interfaces stabilize would be nice to move this to merged streams, but for now this seems
    let mut merged_stream = FuturesUnordered::new();
    merged_stream.push(Either::Right(Either::Right(poll_idle_core(
        local_available,
    ))));
    merged_stream.push(Either::Right(poll_message(read_socket).left_future()));
    // let debt_stream = FuturesUnordered::new();

    while let Some(current_future) = merged_stream.next().await {
        match current_future {
            PollingOption::Message(read_socket, Ok(message)) => {
                match message.queue_message.unwrap() {
                    // Should notify that this remote does not have work, should ask another one
                    queue_message::QueueMessage::NoWork(_) => (),
                    queue_message::QueueMessage::Invocation(invocation) => {
                        let Invocation {
                            invocation_id,
                            function_id,
                            data_sets,
                        } = invocation;
                        merged_stream.push(Either::Left(poll_results(queue.do_work(
                            WorkToDo::FunctionArguments {
                                function_id,
                                function_alternatives: (),
                                input_sets: (),
                                metadata: (),
                                caching: (),
                                recorder: (),
                            },
                        ))))
                    }
                }
                merged_stream.push(Either::Right(Either::Left(poll_message(read_socket))));
            }
            PollingOption::Message(read_socket, Err(error)) => {
                merged_stream.push(Either::Right(Either::Left(poll_message(read_socket))));
            }
            // getting a notification so should poll the queue
            PollingOption::PollFor(receiver, engine_type, number) => {
                // send message to get work
                let request_buffer = serialize_node_info(NodeInfo {
                    version: 1,
                    engines: vec![proto::Engine {
                        engine_type: engine_type_dtop(engine_type) as i32,
                        engine_capacity: number,
                    }],
                });
                send_message(&node_info_buffer, &mut write_socket).await;
                merged_stream.push(Either::Right(Either::Right(poll_idle_core(receiver))));
            }
        }
    }
}

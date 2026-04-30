use crate::{
    deserialize_node_info, deserialize_queue_message, deserialize_remote_message,
    proto::{
        self, queue_message,
        remote_message::{self, RemoteMessage},
        Engine, Invocation, NodeInfo, QueueMessage, Response,
    },
    serialize_node_info, serialize_queue_message,
    util::{
        composition_sets_to_proto, engine_type_dtop, engine_type_ptod,
        pack_metadata_size_and_flags, proto_data_sets_to_context, unpack_metadata_size_and_flags,
        ADDITIONAL_DATA_BUFFER, NO_FLAGS,
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
    composition::CompositionSetData,
    function_driver::{WorkDone, WorkToDo},
    machine_config::EngineType,
};
use prost::bytes::{Bytes, BytesMut};
use std::{
    collections::{BTreeMap, BinaryHeap},
    future::Future,
    process::Output,
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
async fn send_message(
    metadata_buffer: &Bytes,
    mut sender: impl AsyncWriteExt + std::marker::Unpin,
    data_buffer: Option<(Vec<CompositionSetData>, u64)>,
) {
    let metadata_size: u32 = metadata_buffer.len().try_into().unwrap();
    let data_buffer_present = match data_buffer {
        Some((_, total_size)) => total_size > 0,
        None => false,
    };

    let flags = data_buffer_present
        .then(|| ADDITIONAL_DATA_BUFFER)
        .unwrap_or(NO_FLAGS);
    let packed_metadata = pack_metadata_size_and_flags(metadata_size, flags);

    sender.write_u64(packed_metadata).await.unwrap();
    sender.write_all(metadata_buffer).await.unwrap();

    if let Some((data_set, total_size)) = data_buffer {
        if total_size > 0 {
            sender.write_u64(total_size).await.unwrap();
        }
        for mut data in data_set {
            if data.size > 0 {
                sender.write_u64(data.size as u64).await.unwrap();
                while let Some(data_slice) = data.read_next_chunk() {
                    sender.write_all(data_slice).await.unwrap();
                }
            }
        }
    }

    sender.flush().await.unwrap();
}

// TODO handle connection failure
// For small messages we are expecting repeteatly, could have spezial read function with permanent preallocated buffers
// Issue: serialization does not give constant sizes, so would need to find an upper bound first
async fn receive_message(
    mut receiver: impl AsyncReadExt + std::marker::Unpin,
) -> (Bytes, Option<Bytes>) {
    let packed_metadata = receiver.read_u64().await.unwrap();
    let (metadata_size, flags) = unpack_metadata_size_and_flags(packed_metadata);

    // new buffer with size of message
    let mut metadata_buffer = BytesMut::zeroed(metadata_size as usize);
    // Using read_exact here to ensure that we do not read a part of the next message,
    // which could happen if we use read_buf here.
    assert_eq!(metadata_size as usize, receiver.read_exact(&mut metadata_buffer).await.unwrap());

    if (flags & ADDITIONAL_DATA_BUFFER) != 0 {
        let total_data_size = receiver.read_u64().await.unwrap();
        let mut data_buffer = BytesMut::zeroed(total_data_size as usize);
        assert_eq!(total_data_size as usize, receiver.read_exact(&mut data_buffer).await.unwrap());
        return (metadata_buffer.freeze(), Some(data_buffer.freeze()));
    } else {
        return (metadata_buffer.freeze(), None);
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
pub async fn remote_queue_handler<Stream: AsyncReadExt + AsyncWriteExt + std::marker::Unpin>(
    mut socket: Stream,
    queue: WorkQueue,
) {
    // first ask for the information about the other node
    let (node_info_buffer, data_buf) = receive_message(&mut socket).await;
    assert_eq!(data_buf, None);

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
        let (message_buffer, data_buf) = receive_message(&mut socket).await;
        let message = deserialize_remote_message(message_buffer).unwrap();
        match message.remote_message {
            Some(RemoteMessage::WorkRequest(work_request)) => {
                assert_eq!(data_buf, None);

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
                let (queue_message, data) =
                    if let Some((work, debt)) = queue.try_get_work_no_shutdown(engine_flags) {
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
                        let (metadata_sets, data) = composition_sets_to_proto(&data_sets);
                        (
                            QueueMessage {
                                queue_message: Some(
                                    proto::queue_message::QueueMessage::Invocation(Invocation {
                                        metadata_sets,
                                        function_id: function_id.to_string(),
                                        invocation_id: promise_id,
                                    }),
                                ),
                            },
                            Some(data),
                        )
                    } else {
                        // there is no work, so send message accordingly
                        (
                            QueueMessage {
                                queue_message: Some(queue_message::QueueMessage::NoWork(false)),
                            },
                            None,
                        )
                    };
                let message_bytes = serialize_queue_message(queue_message);
                send_message(&message_bytes, &mut socket, data).await;
            }

            Some(RemoteMessage::Response(response)) => {
                let Response {
                    invocation_id,
                    response,
                } = response;
                // TODO: handle failure
                let debt = debt_map
                    .remove(&invocation_id)
                    .expect("Should always get back function response for a present debt");
                let result = match response.unwrap() {
                    proto::response::Response::MetadataSets(metadata_sets) => {
                        Ok(WorkDone::Context(proto_data_sets_to_context(
                            metadata_sets.metadata_sets,
                            data_buf,
                        )))
                    }
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
    Message(Stream, DandelionResult<QueueMessage>, Option<Bytes>),
    PollFor(
        Receiver<(machine_interface::machine_config::EngineType, u32)>,
        EngineType,
        u32,
    ),
}

async fn poll_message<Stream: AsyncReadExt + std::marker::Unpin>(
    mut receiver: Stream,
) -> PollingOption<Stream> {
    let (message_buffer, data_buf) = receive_message(&mut receiver).await;
    let new_message = deserialize_queue_message(message_buffer);
    PollingOption::Message(receiver, new_message, data_buf)
}

async fn poll_idle_core<Stream: AsyncReadExt + std::marker::Unpin>(
    mut receiver: Receiver<(machine_interface::machine_config::EngineType, u32)>,
) -> PollingOption<Stream> {
    let (engine_type, engine_numer) = receiver.recv().await.unwrap();
    PollingOption::PollFor(receiver, engine_type, engine_numer)
}

// struct MessageStream<ReadSocket: AsyncReadExt + std::marker::Unpin> {
//     socket: ReadSocket,
// }

// impl<ReadSocket: AsyncReadExt + std::marker::Unpin> MessageStream<ReadSocket> {
//     fn new(mut socket: ReadSocket) -> Self {
//         Self { socket }
//     }
// }

// struct PollStream {
//     receiver: tokio_stream::wrappers::ReceiverStream<(EngineType, u32)>,
// }

// impl Stream for PollStream {
//     type Item = PollinOption;
//     fn poll_next(
//         mut self: std::pin::Pin<&mut Self>,
//         cx: &mut std::task::Context<'_>,
//     ) -> Poll<Option<Self::Item>> {
//         match self.receiver.poll_next_unpin(cx) {
//             Poll::Ready(Some(tuple)) => Poll::Ready(Some(PollinOption::PollFor(tuple))),
//             Poll::Ready(None) => Poll::Ready(None),
//             Poll::Pending => Poll::Pending,
//         }
//     }
// }

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

    send_message(&node_info_buffer, &mut write_socket, None).await;

    // stream for the promises of the local requests wait for them to resolve
    // Once stream interfaces stabilize would be nice to move this to merged streams, but for now this seems
    let mut merged_stream = FuturesUnordered::new();
    merged_stream.push(Either::Left(poll_message(read_socket)));
    merged_stream.push(Either::Right(poll_idle_core(local_available)));
    // let debt_stream = FuturesUnordered::new();

    while let Some(current_future) = merged_stream.next().await {
        match current_future {
            PollingOption::Message(read_socket, message, data_buf) => {
                merged_stream.push(Either::Left(poll_message(read_socket)));
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
                send_message(&node_info_buffer, &mut write_socket, None).await;
                merged_stream.push(Either::Right(poll_idle_core(receiver)));
            }
        }
    }
}

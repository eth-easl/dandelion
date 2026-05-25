use std::{
    future::Future,
    sync::{Arc, Mutex, RwLock},
    task::Poll,
    time::Instant,
};

use crate::{
    client::{
        receive_message, remote_queue_client, remote_queue_server_logic, send_message, QueueOption,
    },
    data::ExportRegistry,
    deserialize_node_info, deserialize_remote_message,
    proto::{
        queue_message, remote_message, response, Engine, Invocation, QueueMessage, RepeatedEngines,
        Response,
    },
    serialize_queue_message, DispatcherCommand,
};
use dandelion_commons::{err_dandelion, records::Recorder, DandelionError};
use dispatcher::queue::{get_engine_flag, WorkQueue};
use futures::{
    task::{Context, Waker},
    FutureExt,
};
use machine_interface::{
    function_driver::{functions::FunctionAlternative, Metadata},
    machine_config::{self, IntoEnumIterator},
    memory_domain::malloc::MallocMemoryDomain,
};
use tokio::{
    io::{AsyncRead, ReadBuf},
    sync::mpsc,
    task::yield_now,
};

const EXPECTED_ERROR: DandelionError = DandelionError::NotImplemented;

async fn mock_dispatcher(work_queue: WorkQueue, engine_type: machine_config::EngineType) {
    let function_id = Arc::new("dummy_function".to_string());
    let result = work_queue
        .do_work(
            machine_interface::function_driver::WorkToDo::FunctionArguments {
                function_id: function_id.clone(),
                function_alternatives: vec![Arc::new(FunctionAlternative {
                    engine: engine_type,
                    context_size: 0,
                    path: "".to_string(),
                    domain: Arc::new(Box::new(MallocMemoryDomain {})),
                    function: RwLock::new(None),
                })],
                input_sets: vec![],
                metadata: Arc::new(Metadata {
                    input_sets: vec![],
                    output_sets: vec![],
                    min_set_bytes: vec![],
                }),
                caching: false,
                recorder: Recorder::new(function_id, Instant::now()),
            },
        )
        .await;
    let error = if let Err(error) = result {
        error.error
    } else {
        panic!("Received work response instead of expected err")
    };
    assert_eq!(
        DandelionError::Multinode(dandelion_commons::MultinodeError::RequestFailed(
            EXPECTED_ERROR.to_string()
        ),),
        error
    );
}

#[test_log::test]
fn test_remote_queue_server() {
    let work_queue = WorkQueue::init();
    // add an initial amount of remote cores, since that message is processed before the main state machine starts
    work_queue.add_remote_cores(2);
    let engine_type = machine_config::EngineType::iter().next().unwrap();
    let (remote_data_deletion_sender, _remote_data_deletion_receiver) = mpsc::unbounded_channel();

    let (queue_option_sender, queue_option_receiver) = mpsc::channel(64);
    let (queue_message_sender, mut queue_message_receiver) = mpsc::channel(64);

    let mut context = Context::from_waker(Waker::noop());
    let mut server_future = Box::pin(remote_queue_server_logic(
        queue_option_receiver,
        queue_message_sender,
        work_queue.clone(),
        ExportRegistry::new(1),
        remote_data_deletion_sender,
        0,
        2,
    ));

    // add functions to the work queue and wait for their resolution
    let mut test_composition_1 = Box::pin(mock_dispatcher(work_queue.clone(), engine_type));
    let mut test_composition_2 = Box::pin(mock_dispatcher(work_queue.clone(), engine_type));
    let mut test_composition_3 = Box::pin(mock_dispatcher(work_queue.clone(), engine_type));

    // enqueue two functions
    assert_eq!(
        Poll::Pending,
        test_composition_1.as_mut().poll(&mut context)
    );
    assert_eq!(
        Poll::Pending,
        test_composition_2.as_mut().poll(&mut context)
    );

    // send first request for work
    queue_option_sender
        .try_send(QueueOption::Message(
            remote_message::RemoteMessage::WorkRequest(RepeatedEngines {
                engines: vec![Engine {
                    engine_type: engine_type as i32,
                    engine_capacity: 1,
                }],
            }),
            None,
        ))
        .unwrap();

    // poll server so the work reqwuest can be handled, then
    // check that there should now be a response from the server
    assert_eq!(Poll::Pending, server_future.as_mut().poll(&mut context));
    let first_invocation_id = match queue_message_receiver
        .try_recv()
        .unwrap()
        .queue_message
        .unwrap()
    {
        queue_message::QueueMessage::Invocation(Invocation {
            invocation_id,
            function_id,
            metadata_sets: _,
            caching: _,
        }) => {
            assert_eq!("dummy_function", function_id);
            invocation_id
        }
        queue_message::QueueMessage::NoWork(_) => panic!("should not receive no work message"),
    };

    // ask for more work before sending a response for the first one
    queue_option_sender
        .try_send(QueueOption::Message(
            remote_message::RemoteMessage::WorkRequest(RepeatedEngines {
                engines: vec![Engine {
                    engine_type: engine_type as i32,
                    engine_capacity: 1,
                }],
            }),
            None,
        ))
        .unwrap();

    // check that we get work again
    assert_eq!(Poll::Pending, server_future.as_mut().poll(&mut context));
    let second_invocation_id = match queue_message_receiver
        .try_recv()
        .unwrap()
        .queue_message
        .unwrap()
    {
        queue_message::QueueMessage::Invocation(Invocation {
            invocation_id,
            function_id,
            metadata_sets: _,
            caching: _,
        }) => {
            assert_eq!("dummy_function", function_id);
            invocation_id
        }
        queue_message::QueueMessage::NoWork(_) => panic!("should not receive no work message"),
    };

    // ask for more work again, queue should now be empty
    queue_option_sender
        .try_send(QueueOption::Message(
            remote_message::RemoteMessage::WorkRequest(RepeatedEngines {
                engines: vec![Engine {
                    engine_type: engine_type as i32,
                    engine_capacity: 1,
                }],
            }),
            None,
        ))
        .unwrap();

    // expect message that there was no work
    assert_eq!(Poll::Pending, server_future.as_mut().poll(&mut context));
    match queue_message_receiver
        .try_recv()
        .unwrap()
        .queue_message
        .unwrap()
    {
        queue_message::QueueMessage::NoWork(no_work) => assert!(no_work),
        queue_message::QueueMessage::Invocation(_) => panic!("Should not receive more work"),
    }

    // send back results out of order, start with second
    queue_option_sender
        .try_send(QueueOption::Message(
            remote_message::RemoteMessage::Response(Response {
                invocation_id: second_invocation_id,
                response: Some(response::Response::ErrorMsg(EXPECTED_ERROR.to_string())),
            }),
            None,
        ))
        .unwrap();

    // let the server process the response
    assert_eq!(Poll::Pending, server_future.as_mut().poll(&mut context));
    // expect the result to be passed through the future
    assert_eq!(
        Poll::Ready(()),
        test_composition_2.as_mut().poll(&mut context)
    );

    // add another function to the queue
    assert_eq!(
        Poll::Pending,
        test_composition_3.as_mut().poll(&mut context)
    );
    // send the queuing notification
    queue_option_sender
        .try_send(QueueOption::WorkAvailable)
        .unwrap();

    // check that the server sends out another work available message
    assert_eq!(Poll::Pending, server_future.as_mut().poll(&mut context));
    match queue_message_receiver
        .try_recv()
        .unwrap()
        .queue_message
        .unwrap()
    {
        queue_message::QueueMessage::NoWork(no_work) => assert!(!no_work),
        queue_message::QueueMessage::Invocation(_) => panic!("Should not receive more work"),
    }

    // ask for that work
    queue_option_sender
        .try_send(QueueOption::Message(
            remote_message::RemoteMessage::WorkRequest(RepeatedEngines {
                engines: vec![Engine {
                    engine_type: engine_type as i32,
                    engine_capacity: 1,
                }],
            }),
            None,
        ))
        .unwrap();

    // check the third function is also sent out
    assert_eq!(Poll::Pending, server_future.as_mut().poll(&mut context));
    let third_invocation_id = match queue_message_receiver
        .try_recv()
        .unwrap()
        .queue_message
        .unwrap()
    {
        queue_message::QueueMessage::Invocation(Invocation {
            invocation_id,
            function_id,
            metadata_sets: _,
            caching: _,
        }) => {
            assert_eq!("dummy_function", function_id);
            invocation_id
        }
        queue_message::QueueMessage::NoWork(_) => panic!("should not receive no work message"),
    };

    // send back the remaining results
    queue_option_sender
        .try_send(QueueOption::Message(
            remote_message::RemoteMessage::Response(Response {
                invocation_id: third_invocation_id,
                response: Some(response::Response::ErrorMsg(EXPECTED_ERROR.to_string())),
            }),
            None,
        ))
        .unwrap();
    queue_option_sender
        .try_send(QueueOption::Message(
            remote_message::RemoteMessage::Response(Response {
                invocation_id: first_invocation_id,
                response: Some(response::Response::ErrorMsg(EXPECTED_ERROR.to_string())),
            }),
            None,
        ))
        .unwrap();

    // poll server to process
    assert_eq!(Poll::Pending, server_future.as_mut().poll(&mut context));
    assert_eq!(
        Poll::Ready(()),
        test_composition_1.as_mut().poll(&mut context)
    );
    assert_eq!(
        Poll::Ready(()),
        test_composition_3.as_mut().poll(&mut context)
    )
}

async fn mock_queue_server(
    function_id: String,
    mut socket: tokio::io::DuplexStream,
    progress_point: Arc<Mutex<usize>>,
) {
    const INVOCATION_ID: u32 = 7;
    // receive first message, expect node info to register node
    let (node_info_buffer, node_info_data) = receive_message(&mut socket).await;
    assert!(node_info_data.is_none());
    let registration_messge = deserialize_node_info(node_info_buffer).unwrap();
    assert_eq!(1, registration_messge.version);
    assert_eq!(2, registration_messge.num_local_cores);
    *progress_point.lock().unwrap() = 1;

    // wait for second message asking for work
    let (message_buffer, message_data) = receive_message(&mut socket).await;
    assert!(message_data.is_none());
    let remote_message = deserialize_remote_message(message_buffer)
        .unwrap()
        .remote_message
        .unwrap();
    match remote_message {
        remote_message::RemoteMessage::WorkRequest(_) => (),
        _ => panic!("Expected work request not {:?}", remote_message),
    }
    *progress_point.lock().unwrap() = 2;
    // send back a message with work
    let work_message = serialize_queue_message(QueueMessage {
        queue_message: Some(queue_message::QueueMessage::Invocation(Invocation {
            invocation_id: INVOCATION_ID,
            function_id: function_id.clone(),
            metadata_sets: vec![],
            caching: true,
        })),
    });
    send_message(&work_message, &mut socket, None).await;

    // expect to be asked for work again, send more work
    let (message_buffer, message_data) = receive_message(&mut socket).await;
    assert!(message_data.is_none());
    let remote_message = deserialize_remote_message(message_buffer)
        .unwrap()
        .remote_message
        .unwrap();
    match remote_message {
        remote_message::RemoteMessage::WorkRequest(_) => (),
        _ => panic!("Expected work request not {:?}", remote_message),
    }
    *progress_point.lock().unwrap() = 3;
    // send back a message with work
    let work_message = serialize_queue_message(QueueMessage {
        queue_message: Some(queue_message::QueueMessage::Invocation(Invocation {
            invocation_id: INVOCATION_ID,
            function_id,
            metadata_sets: vec![],
            caching: true,
        })),
    });
    send_message(&work_message, &mut socket, None).await;

    // receive new message from the server with the result
    let (remote_message, remote_data) = receive_message(&mut socket).await;
    assert!(remote_data.is_none());
    match deserialize_remote_message(remote_message)
        .unwrap()
        .remote_message
        .unwrap()
    {
        remote_message::RemoteMessage::WorkRequest(_) => panic!("Should not receive work request"),
        remote_message::RemoteMessage::Response(Response {
            invocation_id,
            response,
        }) => {
            assert_eq!(INVOCATION_ID, invocation_id);
            match response.unwrap() {
                response::Response::ErrorMsg(error_message) => {
                    assert_eq!(DandelionError::NotImplemented.to_string(), error_message)
                }
                _ => panic!("expected error message"),
            }
        }
        remote_message::RemoteMessage::NodeUpdate(_) => {
            panic!("Should not receive node update in this test.")
        }
    }

    *progress_point.lock().unwrap() = 4;
    yield_now().await;

    let (remote_message, remote_data) = receive_message(&mut socket).await;
    assert!(remote_data.is_none());
    // receive new message from the server asking for work
    match deserialize_remote_message(remote_message)
        .unwrap()
        .remote_message
        .unwrap()
    {
        remote_message::RemoteMessage::WorkRequest(_) => (),
        remote_message::RemoteMessage::Response(_) => panic!("Should receive work request"),
        remote_message::RemoteMessage::NodeUpdate(_) => {
            panic!("Should not receive node update in this test.")
        }
    }
    // send back no work available
    let no_work_message = serialize_queue_message(QueueMessage {
        queue_message: Some(queue_message::QueueMessage::NoWork(true)),
    });
    send_message(&no_work_message, &mut socket, None).await;

    *progress_point.lock().unwrap() = 5;
    yield_now().await;

    let mut context = Context::from_waker(Waker::noop());
    let mut buf = [0u8];
    assert!(Box::pin(&mut socket)
        .as_mut()
        .poll_read(&mut context, &mut ReadBuf::new(&mut buf))
        .is_pending());

    // send message that more work can be asked for
    let more_work_message = serialize_queue_message(QueueMessage {
        queue_message: Some(queue_message::QueueMessage::NoWork(false)),
    });
    send_message(&more_work_message, &mut socket, None).await;
    *progress_point.lock().unwrap() = 6;

    // expect to receive message asking for more work
    let (remote_message, remote_data) = receive_message(&mut socket).await;
    assert!(remote_data.is_none());
    match deserialize_remote_message(remote_message)
        .unwrap()
        .remote_message
        .unwrap()
    {
        remote_message::RemoteMessage::WorkRequest(_) => (),
        _ => panic!("Should receive work request"),
    }
}

#[test_log::test]
fn test_remote_queue_client() {
    // constants used in the test
    let expected_function_id = "dummy function".to_string();

    // create a mock socket for the handler
    let (client, server) = tokio::io::duplex(4096);

    let work_queue = WorkQueue::init();
    work_queue.add_local_cores(2);
    let (dispatcher_sender, mut dispatcher_receiver) = mpsc::channel(1);
    let progress = Arc::new(Mutex::new(0));
    let engine_type = machine_config::EngineType::iter().next().unwrap();
    let engine_flags = get_engine_flag(engine_type);

    let mut context = Context::from_waker(Waker::noop());
    let mut poller_future = Box::pin(remote_queue_client(
        client,
        dispatcher_sender,
        ExportRegistry::new(1),
        work_queue.clone(),
    ));
    let mut test_server_future = Box::pin(mock_queue_server(
        expected_function_id.clone(),
        server,
        progress.clone(),
    ));

    // poll the poller to check if it registered
    assert_eq!(Poll::Pending, poller_future.as_mut().poll(&mut context));

    // poll server to see if the message was received
    assert_eq!(
        Poll::Pending,
        test_server_future.as_mut().poll(&mut context)
    );
    // expect server already has sent the initial message with the node info and then the request for work
    assert_eq!(2, *progress.lock().unwrap());

    // poll the server to receive work
    assert_eq!(Poll::Pending, poller_future.as_mut().poll(&mut context));

    // should now have work in the receiver, need to keep the receiver since the client doesn't handle dropping it.
    let (result_future_1, _sender) = match dispatcher_receiver.poll_recv(&mut context) {
        Poll::Ready(Some(DispatcherCommand::RemoteFunctionRequest {
            function_id,
            inputs: _,
            is_cold,
            recorder: _,
            callback,
        })) => {
            assert!(!is_cold);
            assert_eq!(expected_function_id, function_id.as_str());
            (
                work_queue.do_work(
                    machine_interface::function_driver::WorkToDo::FunctionArguments {
                        function_id: function_id.clone(),
                        function_alternatives: vec![Arc::new(FunctionAlternative {
                            engine: engine_type,
                            context_size: 0,
                            path: "".to_string(),
                            domain: Arc::new(Box::new(MallocMemoryDomain {})),
                            function: RwLock::new(None),
                        })],
                        input_sets: vec![],
                        metadata: Arc::new(Metadata {
                            input_sets: vec![],
                            output_sets: vec![],
                            min_set_bytes: vec![],
                        }),
                        caching: false,
                        recorder: Recorder::new(function_id, Instant::now()),
                    },
                ),
                callback,
            )
        }
        Poll::Pending | Poll::Ready(None) => panic!("Should receive work now"),
        Poll::Ready(Some(_)) => panic!("Received unexpected command"),
    };
    let mut result_poller_1 = Box::pin(result_future_1);
    match result_poller_1.poll_unpin(&mut context) {
        Poll::Pending => (),
        Poll::Ready(_) => panic!("Should not have work done yet"),
    }

    // should now send request for more work, since one engine left the queue and there is one remaining
    assert_eq!(Poll::Pending, poller_future.poll_unpin(&mut context));
    // check node asked for more work and send it back
    assert_eq!(
        Poll::Pending,
        test_server_future.as_mut().poll(&mut context)
    );
    assert_eq!(3, *progress.lock().unwrap());
    // poll the client to make sure it had time to receive
    assert_eq!(Poll::Pending, poller_future.poll_unpin(&mut context));

    // check that we have more work on the reciever
    let (result_future_2, work_sender_2) = match dispatcher_receiver.poll_recv(&mut context) {
        Poll::Ready(Some(DispatcherCommand::RemoteFunctionRequest {
            function_id,
            inputs: _,
            is_cold,
            recorder: _,
            callback,
        })) => {
            assert!(!is_cold);
            assert_eq!(expected_function_id, function_id.as_str());
            (
                work_queue.do_work(
                    machine_interface::function_driver::WorkToDo::FunctionArguments {
                        function_id: function_id.clone(),
                        function_alternatives: vec![Arc::new(FunctionAlternative {
                            engine: engine_type,
                            context_size: 0,
                            path: "".to_string(),
                            domain: Arc::new(Box::new(MallocMemoryDomain {})),
                            function: RwLock::new(None),
                        })],
                        input_sets: vec![],
                        metadata: Arc::new(Metadata {
                            input_sets: vec![],
                            output_sets: vec![],
                            min_set_bytes: vec![],
                        }),
                        caching: false,
                        recorder: Recorder::new(function_id, Instant::now()),
                    },
                ),
                callback,
            )
        }
        Poll::Pending | Poll::Ready(None) => panic!("Should receive work now"),
        Poll::Ready(Some(_)) => panic!("Received unexpected command"),
    };
    let mut result_poller_2 = Box::pin(result_future_2);
    match result_poller_2.poll_unpin(&mut context) {
        Poll::Pending => (),
        Poll::Ready(_) => panic!("Should not have work done yet"),
    }

    // send back a result and poll to get it processed
    assert!(work_sender_2
        .send(err_dandelion!(DandelionError::NotImplemented))
        .is_ok());
    assert_eq!(Poll::Pending, poller_future.as_mut().poll(&mut context));

    // check the results arrived
    assert_eq!(
        Poll::Pending,
        test_server_future.as_mut().poll(&mut context)
    );
    assert_eq!(4, *progress.lock().unwrap());

    // take work from the queue and check that client polls for more
    let mut another_engine_future = Box::pin(work_queue.get_compute_work(engine_flags));
    assert!(match another_engine_future.as_mut().poll(&mut context) {
        Poll::Ready(_) => true,
        Poll::Pending => false,
    });
    assert_eq!(Poll::Pending, poller_future.as_mut().poll(&mut context));

    // check node asked for more work and send back that there is none
    assert_eq!(
        Poll::Pending,
        test_server_future.as_mut().poll(&mut context)
    );
    assert_eq!(5, *progress.lock().unwrap());

    // give poller option to receive the message an react
    assert_eq!(Poll::Pending, poller_future.as_mut().poll(&mut context));

    // send that more work is now available
    assert_eq!(
        Poll::Pending,
        test_server_future.as_mut().poll(&mut context)
    );
    assert_eq!(6, *progress.lock().unwrap());

    // process the notification that there is more work and ask for it
    assert_eq!(Poll::Pending, poller_future.as_mut().poll(&mut context));

    // check the mock server received another work request and has finished
    assert_eq!(
        Poll::Ready(()),
        test_server_future.as_mut().poll(&mut context)
    );
}

// #[test_log::test]
// fn test_combined() {
//     // create socket connecting the two sides
//     let (client_socket, server_socket) = tokio::io::duplex(4096);

//     // create variable needed for server side
//     let work_queue = WorkQueue::init();
//     work_queue.add_local_cores(2);
//     let engine_type = machine_config::EngineType::iter().next().unwrap();

//     // create variable needed for client side
//     let (dispatcher_sender, mut dispatcher_receiver) = mpsc::channel(1);

//     // spawn both on a new runtime
//     let runtime = tokio::runtime::Builder::new_multi_thread().build().unwrap();
//     let (remote_data_deletion_sender, _remote_data_deletion_receiver) = mpsc::unbounded_channel();
//     runtime.spawn(remote_queue_server(
//         client_socket,
//         work_queue.clone(),
//         ExportRegistry::new(1),
//         remote_data_deletion_sender,
//     ));
//     runtime.spawn(remote_queue_client(
//         server_socket,
//         dispatcher_sender,
//         ExportRegistry::new(2),
//         work_queue.clone(),
//     ));

//     // create one waiting engine
//     let mut context = Context::from_waker(Waker::noop());
//     let mut engine_future = Box::pin(work_queue.get_compute_work(get_engine_flag(engine_type)));
//     assert!(match engine_future.as_mut().poll(&mut context) {
//         Poll::Pending => true,
//         _ => false,
//     });

//     // send work on the work queue
//     let mut test_dispatcher_future = Box::pin(mock_dispatcher(work_queue.clone(), engine_type));
//     assert_eq!(
//         Poll::Pending,
//         test_dispatcher_future.poll_unpin(&mut context)
//     );

//     // should now have something on the dispatcher receiver
//     let callback = match dispatcher_receiver.blocking_recv().unwrap() {
//         DispatcherCommand::RemoteFunctionRequest {
//             function_id,
//             inputs,
//             is_cold,
//             recorder: _,
//             callback,
//         } => {
//             assert_eq!("dummy_function", function_id.as_str());
//             assert_eq!(0, inputs.len());
//             assert!(is_cold);
//             callback
//         }
//         _ => panic!("Received unexpeceted dispatcher command"),
//     };

//     // send back a result
//     assert!(callback.send(err_dandelion!(EXPECTED_ERROR)).is_ok());
//     loop {
//         match test_dispatcher_future.poll_unpin(&mut context) {
//             Poll::Pending => (),
//             Poll::Ready(()) => break,
//         }
//     }
// }

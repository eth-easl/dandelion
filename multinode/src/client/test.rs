use std::{
    future::Future,
    sync::{Arc, Mutex, RwLock},
    task::Poll,
    time::Instant,
};

use crate::{
    client::{receive_message, remote_queue_client, remote_queue_server, send_message},
    deserialize_node_info, deserialize_queue_message, deserialize_remote_message,
    proto::{
        self, queue_message, remote_message, response, Engine, Invocation, NodeInfo, QueueMessage,
        RemoteMessage, RepeatedEngines, Response,
    },
    serialize_node_info, serialize_queue_message, serialize_remote_message,
    util::engine_type_dtop,
    DispatcherCommand,
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

async fn mock_queue_client(mut client: tokio::io::DuplexStream, engine_type: proto::EngineType) {
    // send the node info to the handler
    let node_info = NodeInfo {
        version: 1,
        num_local_cores: 0,
    };
    let node_info_serial = serialize_node_info(node_info.clone());
    send_message(&node_info_serial, &mut client, None).await;
    // yield so the registration can be handled
    yield_now().await;

    // send a request for work, expect there to be work
    let remote_message = serialize_remote_message(RemoteMessage {
        remote_message: Some(remote_message::RemoteMessage::WorkRequest(
            RepeatedEngines {
                engines: vec![Engine {
                    engine_type: engine_type as i32,
                    engine_capacity: 1,
                }],
            },
        )),
    });
    send_message(&remote_message, &mut client, None).await;
    let (work_bytes, should_be_none) = receive_message(&mut client).await;
    assert!(should_be_none.is_none());
    let invocation_id = match deserialize_queue_message(work_bytes)
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

    // ask for work again, but expect there to be none
    send_message(&remote_message, &mut client, None).await;
    let (work_bytes, _) = receive_message(&mut client).await;
    match deserialize_queue_message(work_bytes)
        .unwrap()
        .queue_message
        .unwrap()
    {
        queue_message::QueueMessage::NoWork(no_work) => assert!(no_work),
        queue_message::QueueMessage::Invocation(_) => panic!("Should not receive more work"),
    }

    // send back the results of the first invocation
    let function_result = serialize_remote_message(RemoteMessage {
        remote_message: Some(remote_message::RemoteMessage::Response(Response {
            invocation_id,
            response: Some(response::Response::ErrorMsg(EXPECTED_ERROR.to_string())),
        })),
    });
    send_message(&function_result, &mut client, None).await;

    // should receive a message that there is more work available
    let (message_buffer, message_data) = receive_message(&mut client).await;
    assert!(message_data.is_none());
    let work_available = deserialize_queue_message(message_buffer)
        .unwrap()
        .queue_message
        .unwrap();
    match work_available {
        queue_message::QueueMessage::NoWork(false) => (),
        _ => panic!("did not get expected message:{:?}", work_available),
    }
}

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
    // create a mock socket for the handler
    let (client, server) = tokio::io::duplex(4096);

    let work_queue = WorkQueue::init();
    let engine_type = machine_config::EngineType::iter().next().unwrap();

    let mut context = Context::from_waker(Waker::noop());
    let mut server_future = Box::pin(remote_queue_server(server, work_queue.clone()));
    let mut test_client_future = Box::pin(mock_queue_client(client, engine_type_dtop(engine_type)));
    let mut test_dispatcher_future = Box::pin(mock_dispatcher(work_queue.clone(), engine_type));

    assert_eq!(
        Poll::Pending,
        test_dispatcher_future.as_mut().poll(&mut context)
    );

    assert_eq!(
        Poll::Pending,
        test_client_future.as_mut().poll(&mut context)
    );

    // poll the handle once to make sure the node info was processed
    assert_eq!(Poll::Pending, server_future.as_mut().poll(&mut context));
    // send the request for work
    assert_eq!(
        Poll::Pending,
        test_client_future.as_mut().poll(&mut context)
    );
    // poll again so the work can be sent
    assert_eq!(Poll::Pending, server_future.as_mut().poll(&mut context));
    // poll to process work and ask for more
    assert_eq!(
        Poll::Pending,
        test_client_future.as_mut().poll(&mut context)
    );
    // should send message that there is no more work
    assert_eq!(Poll::Pending, server_future.as_mut().poll(&mut context));
    // receive notifcation that there is no more work, return the finished work for the first invocation
    assert_eq!(
        Poll::Pending,
        test_client_future.as_mut().poll(&mut context)
    );
    // receive the result for the invocation
    assert_eq!(Poll::Pending, server_future.as_mut().poll(&mut context));
    // check the result has been forwarded correctly
    assert_eq!(
        Poll::Ready(()),
        test_dispatcher_future.as_mut().poll(&mut context)
    );

    // send notification to the server that more work has become available
    work_queue.queueing_notifier().notify_one();
    assert_eq!(Poll::Pending, server_future.as_mut().poll(&mut context));

    // check the client has terminated succcessfully
    assert_eq!(
        Poll::Ready(()),
        test_client_future.as_mut().poll(&mut context)
    );
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
    *progress_point.lock().unwrap() = 1;

    // wait for second message asking for work
    let (message_buffer, message_data) = receive_message(&mut socket).await;
    assert!(message_data.is_none());
    match deserialize_remote_message(message_buffer)
        .unwrap()
        .remote_message
        .unwrap()
    {
        remote_message::RemoteMessage::WorkRequest(_) => (),
        _ => panic!("Expected work request"),
    }
    *progress_point.lock().unwrap() = 2;
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

    *progress_point.lock().unwrap() = 3;
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

    *progress_point.lock().unwrap() = 4;
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
    *progress_point.lock().unwrap() = 5;

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

#[test]
fn test_remote_queue_client() {
    // constants used in the test
    let expected_function_id = "dummy function".to_string();

    // create a mock socket for the handler
    let (client, server) = tokio::io::duplex(4096);

    let work_queue = WorkQueue::init();
    let (dispatcher_sender, mut dispatcher_receiver) = mpsc::channel(1);
    let progress = Arc::new(Mutex::new(0));
    let engine_flags = get_engine_flag(machine_config::EngineType::iter().next().unwrap());

    let mut context = Context::from_waker(Waker::noop());
    let mut poller_future = Box::pin(remote_queue_client(
        client,
        dispatcher_sender,
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
    assert_eq!(*progress.lock().unwrap(), 1);

    // poke client to see it does not send spurious messages
    assert_eq!(Poll::Pending, poller_future.as_mut().poll(&mut context));
    assert_eq!(
        Poll::Pending,
        test_server_future.as_mut().poll(&mut context)
    );
    assert_eq!(*progress.lock().unwrap(), 1);

    // poke client to go fetch work
    let mut engine_future = Box::pin(work_queue.get_work(engine_flags));
    assert!(match engine_future.as_mut().poll(&mut context) {
        Poll::Pending => true,
        _ => false,
    });

    // poll the client so it can send
    assert_eq!(Poll::Pending, poller_future.as_mut().poll(&mut context));
    // poll the test server to check that the message was received and send back an invocation
    assert_eq!(
        Poll::Pending,
        test_server_future.as_mut().poll(&mut context)
    );
    assert_eq!(2, *progress.lock().unwrap());

    // poll the server to receive work
    assert_eq!(Poll::Pending, poller_future.as_mut().poll(&mut context));
    // should now have work in the receiver
    let work_sender = match dispatcher_receiver.poll_recv(&mut context) {
        Poll::Ready(Some(DispatcherCommand::RemoteFunctionRequest {
            function_id,
            inputs: _,
            is_cold,
            recorder: _,
            callback,
        })) => {
            assert!(!is_cold);
            assert_eq!(expected_function_id, function_id.as_str());
            callback
        }
        Poll::Pending | Poll::Ready(None) => panic!("Should receive work now"),
        Poll::Ready(Some(_)) => panic!("Received unexpected command"),
    };
    // make sure nothing breaks in the poller in the mean time
    assert_eq!(Poll::Pending, poller_future.as_mut().poll(&mut context));

    // send back a result and poll to get it processed
    assert!(work_sender
        .send(err_dandelion!(DandelionError::NotImplemented))
        .is_ok());
    assert_eq!(Poll::Pending, poller_future.as_mut().poll(&mut context));

    // check the results arrived
    assert_eq!(
        Poll::Pending,
        test_server_future.as_mut().poll(&mut context)
    );
    assert_eq!(3, *progress.lock().unwrap());

    // have the server ask for more work
    let mut another_engine_future = Box::pin(work_queue.get_work(engine_flags));
    assert!(match another_engine_future.as_mut().poll(&mut context) {
        Poll::Pending => true,
        _ => false,
    });
    assert_eq!(Poll::Pending, poller_future.as_mut().poll(&mut context));

    // check node asked for more work and send back that there is none
    assert_eq!(
        Poll::Pending,
        test_server_future.as_mut().poll(&mut context)
    );
    assert_eq!(4, *progress.lock().unwrap());

    // give poller option to receive the message an react
    assert_eq!(Poll::Pending, poller_future.as_mut().poll(&mut context));

    // send that more work is now available
    assert_eq!(
        Poll::Pending,
        test_server_future.as_mut().poll(&mut context)
    );
    assert_eq!(5, *progress.lock().unwrap());

    // process the notification that there is more work and ask for it
    assert_eq!(Poll::Pending, poller_future.as_mut().poll(&mut context));

    // check the mock server received another work request and has finished
    assert_eq!(
        Poll::Ready(()),
        test_server_future.as_mut().poll(&mut context)
    );
}

#[test_log::test]
fn test_combined() {
    // create socket connecting the two sides
    let (client_socket, server_socket) = tokio::io::duplex(4096);

    // create variable needed for server side
    let work_queue = WorkQueue::init();
    let engine_type = machine_config::EngineType::iter().next().unwrap();

    // create variable needed for client side
    let (dispatcher_sender, mut dispatcher_receiver) = mpsc::channel(1);

    // spawn both on a new runtime
    let runtime = tokio::runtime::Builder::new_multi_thread().build().unwrap();
    runtime.spawn(remote_queue_server(client_socket, work_queue.clone()));
    runtime.spawn(remote_queue_client(
        server_socket,
        dispatcher_sender,
        work_queue.clone(),
    ));

    // create one waiting engine
    let mut context = Context::from_waker(Waker::noop());
    let mut engine_future = Box::pin(work_queue.get_work(get_engine_flag(engine_type)));
    assert!(match engine_future.as_mut().poll(&mut context) {
        Poll::Pending => true,
        _ => false,
    });

    // send work on the work queue
    let mut test_dispatcher_future = Box::pin(mock_dispatcher(work_queue.clone(), engine_type));
    assert_eq!(
        Poll::Pending,
        test_dispatcher_future.poll_unpin(&mut context)
    );

    // should now have something on the dispatcher receiver
    let callback = match dispatcher_receiver.blocking_recv().unwrap() {
        DispatcherCommand::RemoteFunctionRequest {
            function_id,
            inputs,
            is_cold,
            recorder: _,
            callback,
        } => {
            assert_eq!("dummy_function", function_id.as_str());
            assert_eq!(0, inputs.len());
            assert!(is_cold);
            callback
        }
        _ => panic!("Received unexpeceted dispatcher command"),
    };

    // send back a result
    assert!(callback.send(err_dandelion!(EXPECTED_ERROR)).is_ok());
    loop {
        match test_dispatcher_future.poll_unpin(&mut context) {
            Poll::Pending => (),
            Poll::Ready(()) => break,
        }
    }
}

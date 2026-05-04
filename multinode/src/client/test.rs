use std::{
    future::Future,
    sync::{Arc, Mutex, RwLock},
    task::Poll,
    time::Instant,
};

use crate::{
    client::{receive_message, remote_queue_handler, remote_queue_poller, send_message},
    deserialize_node_info, deserialize_queue_message, deserialize_remote_message,
    proto::{
        queue_message, remote_message, response, Engine, EngineType, Invocation, NodeInfo,
        QueueMessage, RemoteMessage, Response,
    },
    serialize_node_info, serialize_queue_message, serialize_remote_message, DispatcherCommand,
};
use dandelion_commons::{err_dandelion, records::Recorder, DandelionError};
use dispatcher::queue::WorkQueue;
use futures::task::{Context, Waker};
use machine_interface::{
    function_driver::{functions::FunctionAlternative, Metadata},
    machine_config::{self, IntoEnumIterator},
    memory_domain::malloc::MallocMemoryDomain,
};
use tokio::{
    io::{AsyncRead, ReadBuf},
    sync::{mpsc, watch},
    task::yield_now,
};

const EXPECTED_ERROR: &str = "test message";

async fn test_client(mut client: tokio::io::DuplexStream) {
    // send the node info to the handler
    let node_info = NodeInfo {
        version: 1,
        engines: vec![Engine {
            engine_type: EngineType::EngineKvm as i32,
            engine_capacity: 2,
        }],
    };
    let node_info_serial = serialize_node_info(node_info.clone());
    send_message(&node_info_serial, &mut client).await;
    // yield so the registration can be handled
    yield_now().await;

    // send a request for work, expect there to be work
    let remote_message = serialize_remote_message(RemoteMessage {
        remote_message: Some(remote_message::RemoteMessage::WorkRequest(node_info)),
    });
    send_message(&remote_message, &mut client).await;
    let work_bytes = receive_message(&mut client).await;
    let invocation_id = match deserialize_queue_message(work_bytes)
        .unwrap()
        .queue_message
        .unwrap()
    {
        queue_message::QueueMessage::Invocation(Invocation {
            invocation_id,
            function_id,
            data_sets: _,
            caching: _,
        }) => {
            assert_eq!("dummy_function", function_id);
            invocation_id
        }
        queue_message::QueueMessage::NoWork(_) => panic!("should not receive no work message"),
    };

    // ask for work again, but expect there to be none
    send_message(&remote_message, &mut client).await;
    let work_bytes = receive_message(&mut client).await;
    match deserialize_queue_message(work_bytes)
        .unwrap()
        .queue_message
        .unwrap()
    {
        queue_message::QueueMessage::NoWork(_) => (),
        queue_message::QueueMessage::Invocation(_) => panic!("Should not receive more work"),
    }

    // send back the results of the first invocation
    let function_result = serialize_remote_message(RemoteMessage {
        remote_message: Some(remote_message::RemoteMessage::Response(Response {
            invocation_id,
            response: Some(response::Response::ErrorMsg(EXPECTED_ERROR.to_string())),
        })),
    });
    send_message(&function_result, &mut client).await;

    // yield so the message buffer is not dropped while the handler is still polled
    yield_now().await;
}

async fn test_queue_client(work_queue: WorkQueue) {
    let function_id = Arc::new("dummy_function".to_string());
    let result = work_queue
        .do_work(
            machine_interface::function_driver::WorkToDo::FunctionArguments {
                function_id: function_id.clone(),
                function_alternatives: vec![Arc::new(FunctionAlternative {
                    engine: machine_interface::machine_config::EngineType::Kvm,
                    context_size: 0,
                    path: "".to_string(),
                    domain: Arc::new(Box::new(MallocMemoryDomain {})),
                    function: RwLock::new(None),
                })],
                input_sets: vec![],
                metadata: Arc::new(Metadata {
                    input_sets: vec![],
                    output_sets: vec![],
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
fn test_remote_queue_handler() {
    // create a mock socket for the handler
    let (client, server) = tokio::io::duplex(4096);

    let stub = || {};
    let work_queue = WorkQueue::init(stub, stub);

    let mut context = Context::from_waker(Waker::noop());
    let mut handler_future = Box::pin(remote_queue_handler(server, work_queue.clone()));
    let mut test_client_future = Box::pin(test_client(client));
    let mut queue_poller = Box::pin(test_queue_client(work_queue.clone()));

    assert_eq!(Poll::Pending, queue_poller.as_mut().poll(&mut context));

    assert_eq!(
        Poll::Pending,
        test_client_future.as_mut().poll(&mut context)
    );

    // poll the handle once to make sure the node info was processed
    assert_eq!(Poll::Pending, handler_future.as_mut().poll(&mut context));
    // send the request for work
    assert_eq!(
        Poll::Pending,
        test_client_future.as_mut().poll(&mut context)
    );
    // poll again so the work can be sent
    assert_eq!(Poll::Pending, handler_future.as_mut().poll(&mut context));
    // poll to process work and ask for more
    assert_eq!(
        Poll::Pending,
        test_client_future.as_mut().poll(&mut context)
    );
    // should send message that there is no more work
    assert_eq!(Poll::Pending, handler_future.as_mut().poll(&mut context));
    // receive notifcation that there is no more work, return the finished work for the first invocation
    assert_eq!(
        Poll::Pending,
        test_client_future.as_mut().poll(&mut context)
    );
    // receive the result for the invocation
    assert_eq!(Poll::Pending, handler_future.as_mut().poll(&mut context));
    // check the result has been forwarded correctly
    assert_eq!(Poll::Ready(()), queue_poller.as_mut().poll(&mut context));

    // check the client has terminated succcessfully
    assert_eq!(
        Poll::Ready(()),
        test_client_future.as_mut().poll(&mut context)
    );
}

async fn test_queue_server(
    function_id: String,
    mut socket: tokio::io::DuplexStream,
    progress_point: Arc<Mutex<usize>>,
) {
    const INVOCATION_ID: u32 = 7;
    // receive first message, expect node info to register node
    let registration_messge = deserialize_node_info(receive_message(&mut socket).await).unwrap();
    assert_eq!(1, registration_messge.version);
    *progress_point.lock().unwrap() = 1;

    // wait for second message asking for work
    let registration_messge = deserialize_node_info(receive_message(&mut socket).await).unwrap();
    assert_eq!(1, registration_messge.version);
    *progress_point.lock().unwrap() = 2;
    // send back a message with work
    let work_message = serialize_queue_message(QueueMessage {
        queue_message: Some(queue_message::QueueMessage::Invocation(Invocation {
            invocation_id: INVOCATION_ID,
            function_id,
            data_sets: vec![],
            caching: true,
        })),
    });
    send_message(&work_message, &mut socket).await;

    // receive new message from the server with the result
    let RemoteMessage { remote_message } =
        deserialize_remote_message(receive_message(&mut socket).await).unwrap();
    match remote_message.unwrap() {
        remote_message::RemoteMessage::WorkRequest(_) => panic!("Should not receive work request"),
        remote_message::RemoteMessage::Response(Response {
            invocation_id,
            response,
        }) => {
            assert_eq!(INVOCATION_ID, invocation_id);
            match response.unwrap() {
                response::Response::DataSets(_) => panic!("expected error message"),
                response::Response::ErrorMsg(error_message) => {
                    assert_eq!(DandelionError::NotImplemented.to_string(), error_message)
                }
            }
        }
    }

    *progress_point.lock().unwrap() = 3;
    yield_now().await;

    // receive new message from the server asking for work
    let registration_messge = deserialize_node_info(receive_message(&mut socket).await).unwrap();
    assert_eq!(1, registration_messge.version);
    // send back no work available
    let no_work_message = serialize_queue_message(QueueMessage {
        queue_message: Some(queue_message::QueueMessage::NoWork(true)),
    });
    send_message(&no_work_message, &mut socket).await;

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
    send_message(&more_work_message, &mut socket).await;
    *progress_point.lock().unwrap() = 5;

    // expect to receive message asking for more work
    let registration_messge = deserialize_node_info(receive_message(&mut socket).await).unwrap();
}

#[test]
fn test_remote_queue_poller() {
    // constants used in the test
    let expected_function_id = "dummy function".to_string();
    let mut engines: Vec<_> = machine_config::EngineType::iter()
        .map(|e_type| (e_type, 0u32))
        .collect();

    // create a mock socket for the handler
    let (client, server) = tokio::io::duplex(4096);

    let (dispatcher_sender, mut dispatcher_receiver) = mpsc::channel(1);
    let (watch_sender, watch_receiver) = watch::channel(engines.clone());
    let progress = Arc::new(Mutex::new(0));

    let mut context = Context::from_waker(Waker::noop());
    let max_engines = engines.iter().map(|(e_type, _)| (*e_type, 1)).collect();
    let mut poller_future = Box::pin(remote_queue_poller(
        client,
        dispatcher_sender,
        watch_receiver,
        max_engines,
    ));
    let mut test_server_future = Box::pin(test_queue_server(
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
    engines[0].1 = 1;
    watch_sender.send_replace(engines.clone());

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
    watch_sender.send_replace(engines.clone());
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

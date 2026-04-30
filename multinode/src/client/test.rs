use std::{
    future::Future,
    sync::{Arc, Mutex, RwLock},
    task::Poll,
    time::Instant,
};

use crate::{
    client::{receive_message, remote_queue_handler, remote_queue_poller, send_message},
    deserialize_node_info, deserialize_queue_message,
    proto::{
        queue_message, remote_message, response, Engine, EngineType, Invocation, NodeInfo,
        QueueMessage, RemoteMessage, Response,
    },
    serialize_node_info, serialize_queue_message, serialize_remote_message,
};
use dandelion_commons::{err_dandelion, records::Recorder, DandelionError};
use dispatcher::queue::{get_engine_flag, WorkQueue};
use futures::{
    channel,
    task::{Context, Waker},
};
use machine_interface::{
    function_driver::{functions::FunctionAlternative, Metadata, WorkToDo},
    memory_domain::malloc::MallocMemoryDomain,
};
use tokio::task::yield_now;

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
    send_message(&node_info_serial, &mut client, None).await;
    // yield so the registration can be handled
    yield_now().await;

    // send a request for work, expect there to be work
    let remote_message = serialize_remote_message(RemoteMessage {
        remote_message: Some(remote_message::RemoteMessage::WorkRequest(node_info)),
    });
    send_message(&remote_message, &mut client, None).await;
    let (work_bytes, _) = receive_message(&mut client).await;
    let invocation_id = match deserialize_queue_message(work_bytes)
        .unwrap()
        .queue_message
        .unwrap()
    {
        queue_message::QueueMessage::Invocation(Invocation {
            invocation_id,
            function_id,
            metadata_sets: _,
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
    send_message(&function_result, &mut client, None).await;

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
    // check the result has be forwarded correctly
    assert_eq!(Poll::Ready(()), queue_poller.as_mut().poll(&mut context));

    // check the client has terminated succcessfully
    assert_eq!(
        Poll::Ready(()),
        test_client_future.as_mut().poll(&mut context)
    );
}

async fn test_queue_server(mut socket: tokio::io::DuplexStream, progress_point: Arc<Mutex<usize>>) {
    // receive first message, expect node info to register node
    let (node_info_buffer, _) = receive_message(&mut socket).await;
    let registration_messge = deserialize_node_info(node_info_buffer).unwrap();
    assert_eq!(1, registration_messge.version);
    *progress_point.lock().unwrap() = 1;

    // wait for second message asking for work
    let (node_info_buffer, _) = receive_message(&mut socket).await;
    let registration_messge = deserialize_node_info(node_info_buffer).unwrap();
    assert_eq!(1, registration_messge.version);
    *progress_point.lock().unwrap() = 2;
    // send back a message with work
    let work_message = serialize_queue_message(QueueMessage {
        queue_message: Some(queue_message::QueueMessage::Invocation(Invocation {
            invocation_id: 1,
            function_id: "dummy name".to_string(),
            metadata_sets: vec![],
        })),
    });
    send_message(&work_message, &mut socket, None).await;
    yield_now().await;
}

#[test]
fn test_remote_queue_poller() {
    // create a mock socket for the handler
    let (client, server) = tokio::io::duplex(4096);

    let stub = || {};
    let work_queue = WorkQueue::init(stub, stub);
    let (poll_sender, poll_receiver) =
        tokio::sync::mpsc::channel::<(machine_interface::machine_config::EngineType, u32)>(1);
    let engines = vec![(machine_interface::machine_config::EngineType::Kvm, 3)];
    let progress = Arc::new(Mutex::new(0));

    let mut context = Context::from_waker(Waker::noop());
    let mut poller_future = Box::pin(remote_queue_poller(
        client,
        work_queue.clone(),
        poll_receiver,
        engines,
    ));

    let mut test_server_future = Box::pin(test_queue_server(server, progress.clone()));

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
    let mut poll_sender_future =
        Box::pin(poll_sender.send((machine_interface::machine_config::EngineType::Kvm, 1)));
    assert_eq!(
        Poll::Ready(Ok(())),
        poll_sender_future.as_mut().poll(&mut context)
    );
    // poll the client so it can send
    assert_eq!(Poll::Pending, poller_future.as_mut().poll(&mut context));
    // poll the test server to check that the message was received and send back an invocation
    assert_eq!(
        Poll::Ready(()),
        test_server_future.as_mut().poll(&mut context)
    );
    assert_eq!(2, *progress.lock().unwrap());

    // poll the server to receive work
    assert_eq!(Poll::Pending, poller_future.as_mut().poll(&mut context));
    // should now have work in the work queue
    let mut work_queue_future = Box::pin(work_queue.get_work(get_engine_flag(
        machine_interface::machine_config::EngineType::Kvm,
    )));
    if let Poll::Ready((work, debt)) = work_queue_future.as_mut().poll(&mut context) {
        match work {
            WorkToDo::FunctionArguments {
                function_id,
                function_alternatives,
                input_sets,
                metadata,
                caching,
                recorder,
            } => (),
            WorkToDo::Shutdown(_) => panic!("Should not receive shutdown from remote"),
        }
        debt.fulfill(err_dandelion!(DandelionError::UserError(
            dandelion_commons::UserError::FunctionError(-1)
        )));
    } else {
        panic!("Should find work in queue");
    }
}

use std::{
    sync::{Arc, RwLock},
    task::Poll,
    time::Instant,
};

use crate::{
    client::{remote_queue_client_logic, remote_queue_server_logic, QueueOption},
    data::ExportRegistry,
    proto::{
        queue_message, remote_message, response, Engine, Invocation, RepeatedEngines,
        RepeatedInvocations, Response,
    },
    DispatcherCommand,
};
use dandelion_commons::{err_dandelion, records::Recorder, DandelionError};
use dispatcher::queue::WorkQueue;
use futures::{
    task::{Context, Waker},
    FutureExt,
};
use machine_interface::{
    function_driver::{functions::FunctionAlternative, Metadata},
    machine_config::{self, IntoEnumIterator},
    memory_domain::malloc::MallocMemoryDomain,
};
use tokio::sync::mpsc;

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
    assert_eq!(Poll::Pending, test_composition_1.poll_unpin(&mut context));
    assert_eq!(Poll::Pending, test_composition_2.poll_unpin(&mut context));

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
    assert_eq!(Poll::Pending, server_future.poll_unpin(&mut context));
    let first_invocation_id = match queue_message_receiver.try_recv().unwrap() {
        queue_message::QueueMessage::Invocations(RepeatedInvocations { mut invocations }) => {
            assert_eq!(1, invocations.len());
            let Invocation {
                invocation_id,
                function_id,
                metadata_sets: _,
                caching: _,
            } = invocations.pop().unwrap();
            assert_eq!("dummy_function", function_id);
            invocation_id
        }
        other => panic!("Should not receive other message: {:?}", other),
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
    assert_eq!(Poll::Pending, server_future.poll_unpin(&mut context));
    let second_invocation_id = match queue_message_receiver.try_recv().unwrap() {
        queue_message::QueueMessage::Invocations(RepeatedInvocations { mut invocations }) => {
            assert_eq!(1, invocations.len());
            let Invocation {
                invocation_id,
                function_id,
                metadata_sets: _,
                caching: _,
            } = invocations.pop().unwrap();
            assert_eq!("dummy_function", function_id);
            invocation_id
        }
        other => panic!("Should not receive other message: {:?}", other),
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
    assert_eq!(Poll::Pending, server_future.poll_unpin(&mut context));
    match queue_message_receiver.try_recv().unwrap() {
        queue_message::QueueMessage::NoWork(no_work) => assert!(no_work),
        other => panic!("Should not receive other message: {:?}", other),
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
    assert_eq!(Poll::Pending, server_future.poll_unpin(&mut context));
    // expect the result to be passed through the future
    assert_eq!(Poll::Ready(()), test_composition_2.poll_unpin(&mut context));

    // add another function to the queue
    assert_eq!(Poll::Pending, test_composition_3.poll_unpin(&mut context));
    // send the queuing notification
    queue_option_sender
        .try_send(QueueOption::WorkAvailable)
        .unwrap();

    // check that the server sends out another work available message
    assert_eq!(Poll::Pending, server_future.poll_unpin(&mut context));
    match queue_message_receiver.try_recv().unwrap() {
        queue_message::QueueMessage::NoWork(no_work) => assert!(!no_work),
        other => panic!("Should not receive other message: {:?}", other),
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
    assert_eq!(Poll::Pending, server_future.poll_unpin(&mut context));
    let third_invocation_id = match queue_message_receiver.try_recv().unwrap() {
        queue_message::QueueMessage::Invocations(RepeatedInvocations { mut invocations }) => {
            assert_eq!(1, invocations.len());
            let Invocation {
                invocation_id,
                function_id,
                metadata_sets: _,
                caching: _,
            } = invocations.pop().unwrap();
            assert_eq!("dummy_function", function_id);
            invocation_id
        }
        other => panic!("should not receive other message: {:?}", other),
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
    assert_eq!(Poll::Pending, server_future.poll_unpin(&mut context));
    assert_eq!(Poll::Ready(()), test_composition_1.poll_unpin(&mut context));
    assert_eq!(Poll::Ready(()), test_composition_3.poll_unpin(&mut context))
}

#[test_log::test]
fn test_remote_queue_client() {
    // constants used in the test
    let expected_function_id = "dummy function".to_string();
    const INVOCATION_ID: u32 = 7;

    // let work_queue = WorkQueue::init();
    // work_queue.add_local_cores(2);
    // let engine_type = machine_config::EngineType::iter().next().unwrap();
    // let engine_flags = get_engine_flag(engine_type);

    let (dispatcher_sender, mut dispatcher_receiver) = mpsc::channel(64);
    let (poll_option_sender, poll_option_receiver) = mpsc::channel(64);
    let (remote_message_sender, mut remote_message_receiver) = mpsc::channel(64);

    let mut context = Context::from_waker(Waker::noop());
    let mut client_future = Box::pin(remote_queue_client_logic(
        poll_option_receiver,
        remote_message_sender,
        dispatcher_sender,
        poll_option_sender.clone(),
        // work_queue.queue_state_watcher(),
        ExportRegistry::new(1),
        0,
    ));

    // send message with the number of local cores and that the local queue state has changed
    poll_option_sender
        .try_send(crate::client::PollingOption::LocalCoreCountChanged(2))
        .unwrap();

    // receive the message about the updated core count
    assert_eq!(Poll::Pending, client_future.poll_unpin(&mut context));
    match remote_message_receiver.try_recv().unwrap() {
        remote_message::RemoteMessage::NodeUpdate(update) => assert_eq!(2, update.num_local_cores),
        remote_message => panic!("Expected work request not {:?}", remote_message),
    }

    // update local queue state
    poll_option_sender
        .try_send(crate::client::PollingOption::QueueStateChanged(0))
        .unwrap();
    assert_eq!(Poll::Pending, client_future.poll_unpin(&mut context));
    match remote_message_receiver.try_recv().unwrap() {
        remote_message::RemoteMessage::WorkRequest(_) => (),
        remote_message => panic!("Expected work request not {:?}", remote_message),
    }

    // send work
    poll_option_sender
        .try_send(crate::client::PollingOption::Message(
            Ok(queue_message::QueueMessage::Invocations(
                RepeatedInvocations {
                    invocations: vec![Invocation {
                        invocation_id: INVOCATION_ID,
                        function_id: expected_function_id.clone(),
                        metadata_sets: vec![],
                        caching: true,
                    }],
                },
            )),
            None,
        ))
        .unwrap();

    // poll client and check dispatcher queue for the work that was received
    assert_eq!(Poll::Pending, client_future.poll_unpin(&mut context));
    // let (result_future_1, _work_sender_1) = match dispatcher_receiver.poll_recv(&mut context) {
    let _work_sender_1 = match dispatcher_receiver.poll_recv(&mut context) {
        Poll::Ready(Some(DispatcherCommand::RemoteFunctionRequest {
            function_id,
            inputs: _,
            is_cold,
            recorder: _,
            callback,
        })) => {
            assert!(!is_cold);
            assert_eq!(expected_function_id, function_id.as_str());
            // (
            //     work_queue.do_work(
            //         machine_interface::function_driver::WorkToDo::FunctionArguments {
            //             function_id: function_id.clone(),
            //             function_alternatives: vec![Arc::new(FunctionAlternative {
            //                 engine: engine_type,
            //                 context_size: 0,
            //                 path: "".to_string(),
            //                 domain: Arc::new(Box::new(MallocMemoryDomain {})),
            //                 function: RwLock::new(None),
            //             })],
            //             input_sets: vec![],
            //             metadata: Arc::new(Metadata {
            //                 input_sets: vec![],
            //                 output_sets: vec![],
            //                 min_set_bytes: vec![],
            //             }),
            //             caching: false,
            //             recorder: Recorder::new(function_id, Instant::now()),
            //         },
            //     ),
            callback
            // )
        }
        Poll::Pending | Poll::Ready(None) => panic!("Should receive work now"),
        Poll::Ready(Some(_)) => panic!("Received unexpected command"),
    };
    // put work into workqueue to trigger asking for more work
    // let mut result_poller_1 = Box::pin(result_future_1);
    // match result_poller_1.poll_unpin(&mut context) {
    //     Poll::Pending => (),
    //     Poll::Ready(_) => panic!("Should not have work done yet"),
    // }

    // notify that queue state has changed
    poll_option_sender
        .try_send(crate::client::PollingOption::QueueStateChanged(1))
        .unwrap();

    // should now send request for more work
    assert_eq!(Poll::Pending, client_future.poll_unpin(&mut context));
    match remote_message_receiver.try_recv().unwrap() {
        remote_message::RemoteMessage::WorkRequest(_) => (),
        remote_message => panic!("Expected work request not {:?}", remote_message),
    }

    // send work
    poll_option_sender
        .try_send(crate::client::PollingOption::Message(
            Ok(queue_message::QueueMessage::Invocations(
                RepeatedInvocations {
                    invocations: vec![Invocation {
                        invocation_id: INVOCATION_ID,
                        function_id: expected_function_id.clone(),
                        metadata_sets: vec![],
                        caching: true,
                    }],
                },
            )),
            None,
        ))
        .unwrap();

    // check that we have more work on the reciever
    assert_eq!(Poll::Pending, client_future.poll_unpin(&mut context));
    let work_sender_2 = match dispatcher_receiver.poll_recv(&mut context) {
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
    // notify that queue state has changed, should not trigger asking for more work
    poll_option_sender
        .try_send(crate::client::PollingOption::QueueStateChanged(2))
        .unwrap();

    // should not lead to asking for more work
    assert_eq!(Poll::Pending, client_future.poll_unpin(&mut context));
    assert!(dispatcher_receiver.is_empty());

    // send back a result, mark the queue as changed and poll to get it processed
    assert!(
        Box::pin(work_sender_2.callback(err_dandelion!(DandelionError::NotImplemented)))
            .poll_unpin(&mut context)
            .is_ready()
    );
    poll_option_sender
        .try_send(crate::client::PollingOption::QueueStateChanged(1))
        .unwrap();
    // let mut another_engine_future = Box::pin(work_queue.get_compute_work(engine_flags));
    // assert!(another_engine_future.poll_unpin(&mut context).is_ready());

    assert_eq!(Poll::Pending, client_future.poll_unpin(&mut context));

    // check the results has been sent out
    match remote_message_receiver.try_recv().unwrap() {
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
        remote_message => panic!("Expected work request not {:?}", remote_message),
    }

    // check that client send out a request for more.
    // (since it got the result and the queue change, should be able to process both)
    // assert_eq!(Poll::Pending, client_future.poll_unpin(&mut context));
    match remote_message_receiver.try_recv().unwrap() {
        remote_message::RemoteMessage::WorkRequest(_) => (),
        remote_message => panic!("Expected work request not {:?}", remote_message),
    }

    // send back that there is none
    poll_option_sender
        .try_send(crate::client::PollingOption::Message(
            Ok(queue_message::QueueMessage::NoWork(true)),
            None,
        ))
        .unwrap();

    // give poller option to receive the message an react
    assert_eq!(Poll::Pending, client_future.poll_unpin(&mut context));
    // check there was no spurious message
    assert!(remote_message_receiver.is_empty());
    assert!(dispatcher_receiver.is_empty());

    // send that more work is now available
    poll_option_sender
        .try_send(crate::client::PollingOption::Message(
            Ok(queue_message::QueueMessage::NoWork(false)),
            None,
        ))
        .unwrap();

    // receive the message asking for more work
    assert_eq!(Poll::Pending, client_future.poll_unpin(&mut context));
    match remote_message_receiver.try_recv().unwrap() {
        remote_message::RemoteMessage::WorkRequest(_) => (),
        remote_message => panic!("Expected work request not {:?}", remote_message),
    }
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
//     assert!(match engine_future.poll_unpin(&mut context) {
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

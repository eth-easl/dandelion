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
};
use dandelion_commons::{records::Recorder, DandelionError};
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
            0,
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
    let mut test_composition_4 = Box::pin(mock_dispatcher(work_queue.clone(), engine_type));

    // enqueue two functions
    assert_eq!(Poll::Pending, test_composition_1.poll_unpin(&mut context));
    assert_eq!(Poll::Pending, test_composition_2.poll_unpin(&mut context));
    assert_eq!(Poll::Pending, test_composition_3.poll_unpin(&mut context));

    // send first request for work
    queue_option_sender
        .try_send(QueueOption::Message(
            remote_message::RemoteMessage::WorkRequest(RepeatedEngines {
                engines: vec![Engine {
                    engine_type: engine_type as i32,
                    engine_capacity: 2,
                }],
            }),
            None,
        ))
        .unwrap();

    // poll server so the work reqwuest can be handled, then
    // check that there should now be a response from the server with two invocations
    assert_eq!(Poll::Pending, server_future.poll_unpin(&mut context));
    let (first_invocation_id, _second_invocation_id) =
        match queue_message_receiver.try_recv().unwrap() {
            queue_message::QueueMessage::Invocations(RepeatedInvocations { mut invocations }) => {
                assert_eq!(2, invocations.len());
                let Invocation {
                    invocation_id: second_id,
                    function_id,
                    metadata_sets: _,
                    caching: _,
                } = invocations.pop().unwrap();
                assert_eq!("dummy_function", function_id);
                let Invocation {
                    invocation_id: first_id,
                    function_id,
                    metadata_sets: _,
                    caching: _,
                } = invocations.pop().unwrap();
                assert_eq!("dummy_function", function_id);
                (first_id, second_id)
            }
            other => panic!("Should not receive other message: {:?}", other),
        };

    // ask for more work before sending a response for the first one
    queue_option_sender
        .try_send(QueueOption::Message(
            remote_message::RemoteMessage::WorkRequest(RepeatedEngines {
                engines: vec![Engine {
                    engine_type: engine_type as i32,
                    engine_capacity: 2,
                }],
            }),
            None,
        ))
        .unwrap();

    // check that we get work again
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
        other => panic!("Should not receive other message: {:?}", other),
    };

    // ask for more work again, queue should now be empty
    queue_option_sender
        .try_send(QueueOption::Message(
            remote_message::RemoteMessage::WorkRequest(RepeatedEngines {
                engines: vec![Engine {
                    engine_type: engine_type as i32,
                    engine_capacity: 2,
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
                invocation_id: third_invocation_id,
                response: Some(response::Response::ErrorMsg(EXPECTED_ERROR.to_string())),
            }),
            None,
        ))
        .unwrap();

    // let the server process the response
    assert_eq!(Poll::Pending, server_future.poll_unpin(&mut context));
    // expect the result to be passed through the future
    assert_eq!(Poll::Ready(()), test_composition_3.poll_unpin(&mut context));

    // add another function to the queue
    assert_eq!(Poll::Pending, test_composition_4.poll_unpin(&mut context));
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
                    engine_capacity: 2,
                }],
            }),
            None,
        ))
        .unwrap();

    // check the third function is also sent out
    assert_eq!(Poll::Pending, server_future.poll_unpin(&mut context));
    let fourth_invocation_id = match queue_message_receiver.try_recv().unwrap() {
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
                invocation_id: fourth_invocation_id,
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
    assert_eq!(Poll::Ready(()), test_composition_4.poll_unpin(&mut context))
}

#[test_log::test]
fn test_remote_queue_client() {
    // constants used in the test
    let expected_function_id = "dummy function".to_string();
    const INVOCATION_ID: u32 = 7;

    let (dispatcher_sender, mut dispatcher_receiver) = mpsc::channel(64);
    let (poll_option_sender, poll_option_receiver) = mpsc::channel(64);
    let (remote_message_sender, mut remote_message_receiver) = mpsc::channel(64);

    let dispatcher_send =
        |registry, duration, invocation_id, function_id, inputs, is_cold, recorder| {
            dispatcher_sender
                .blocking_send((
                    registry,
                    duration,
                    invocation_id,
                    function_id,
                    inputs,
                    is_cold,
                    recorder,
                ))
                .unwrap();
        };

    let mut context = Context::from_waker(Waker::noop());
    let mut client_future = Box::pin(remote_queue_client_logic(
        poll_option_receiver,
        remote_message_sender,
        dispatcher_send,
        0,
        ExportRegistry::new(1),
        0,
    ));

    // send message with the number of local cores and that the local queue state has changed
    poll_option_sender
        .try_send(crate::client::PollingOption::LocalCoreCountChanged(3))
        .unwrap();

    // receive the message about the updated core count
    assert_eq!(Poll::Pending, client_future.poll_unpin(&mut context));
    match remote_message_receiver.try_recv().unwrap() {
        remote_message::RemoteMessage::NodeUpdate(update) => assert_eq!(3, update.num_local_cores),
        remote_message => panic!("Expected work request not {:?}", remote_message),
    }

    // update local queue state, expect message asking for work
    poll_option_sender
        .try_send(crate::client::PollingOption::QueueStateChanged(0))
        .unwrap();
    assert_eq!(Poll::Pending, client_future.poll_unpin(&mut context));
    match remote_message_receiver.try_recv().unwrap() {
        remote_message::RemoteMessage::WorkRequest(engines) => {
            assert!(engines.engines.len() > 0);
            assert_eq!(3, engines.engines[0].engine_capacity);
        }
        remote_message => panic!("Expected work request not {:?}", remote_message),
    }

    // send work
    poll_option_sender
        .try_send(crate::client::PollingOption::Message(
            Ok(queue_message::QueueMessage::Invocations(
                RepeatedInvocations {
                    invocations: vec![
                        Invocation {
                            invocation_id: INVOCATION_ID,
                            function_id: expected_function_id.clone(),
                            metadata_sets: vec![],
                            caching: true,
                        },
                        Invocation {
                            invocation_id: INVOCATION_ID + 1,
                            function_id: expected_function_id.clone(),
                            metadata_sets: vec![],
                            caching: true,
                        },
                    ],
                },
            )),
            None,
        ))
        .unwrap();

    // poll client and check dispatcher queue for the work that was received
    assert_eq!(Poll::Pending, client_future.poll_unpin(&mut context));
    let _invocation_id_1 = match dispatcher_receiver.poll_recv(&mut context) {
        Poll::Ready(Some((
            _registry,
            _duration,
            invocation_id,
            function_id,
            _inputs,
            is_cold,
            _recorder,
        ))) => {
            assert!(!is_cold);
            assert_eq!(expected_function_id, function_id.as_str());
            invocation_id
        }
        Poll::Pending | Poll::Ready(None) => panic!("Should receive work now"),
    };
    // there should be another function in the queue
    let _invocation_id_2 = match dispatcher_receiver.poll_recv(&mut context) {
        Poll::Ready(Some((
            _registry,
            _duration,
            invocation_id,
            function_id,
            _inputs,
            is_cold,
            _recorder,
        ))) => {
            assert!(!is_cold);
            assert_eq!(expected_function_id, function_id.as_str());
            invocation_id
        }
        Poll::Pending | Poll::Ready(None) => panic!("Should receive work now"),
    };

    // notify that queue state has changed
    poll_option_sender
        .try_send(crate::client::PollingOption::QueueStateChanged(2))
        .unwrap();

    // should now send request for more work
    assert_eq!(Poll::Pending, client_future.poll_unpin(&mut context));
    match remote_message_receiver.try_recv().unwrap() {
        remote_message::RemoteMessage::WorkRequest(engines) => {
            assert!(engines.engines.len() > 0);
            assert_eq!(1, engines.engines[0].engine_capacity);
        }
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
    let invocation_id_3 = match dispatcher_receiver.poll_recv(&mut context) {
        Poll::Ready(Some((
            _registry,
            _duration,
            invocation_id,
            function_id,
            _inputs,
            is_cold,
            _recorder,
        ))) => {
            assert!(!is_cold);
            assert_eq!(expected_function_id, function_id.as_str());
            invocation_id
        }
        Poll::Pending | Poll::Ready(None) => panic!("Should receive work now"),
    };
    // Notify that queue state has changed, should not trigger asking for more work,
    // since prefetch capacity is 0.
    poll_option_sender
        .try_send(crate::client::PollingOption::QueueStateChanged(3))
        .unwrap();
    assert_eq!(Poll::Pending, client_future.poll_unpin(&mut context));
    assert!(dispatcher_receiver.is_empty());

    // send back a result, mark the queue as changed and poll to get it processed
    poll_option_sender
        .try_send(crate::client::PollingOption::Results(
            remote_message::RemoteMessage::Response(Response {
                invocation_id: invocation_id_3,
                response: Some(response::Response::ErrorMsg(
                    DandelionError::NotImplemented.to_string(),
                )),
            }),
        ))
        .unwrap();

    poll_option_sender
        .try_send(crate::client::PollingOption::QueueStateChanged(2))
        .unwrap();

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
        remote_message => panic!("Expected reponse not {:?}", remote_message),
    }

    // check that client send out a request for more.
    // (since it got the result and the queue change, should be able to process both)
    match remote_message_receiver.try_recv().unwrap() {
        remote_message::RemoteMessage::WorkRequest(_) => (),
        remote_message => panic!("Expected work request not {:?}", remote_message),
    }

    // send back that there is some
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

#[test_log::test]
fn test_remote_queue_client_prefetch() {
    // constants used in the test
    let expected_function_id = "dummy function".to_string();
    const INVOCATION_ID: u32 = 7;

    let (dispatcher_sender, mut dispatcher_receiver) = mpsc::channel(64);
    let (poll_option_sender, poll_option_receiver) = mpsc::channel(64);
    let (remote_message_sender, mut remote_message_receiver) = mpsc::channel(64);

    let dispatcher_send =
        |registry, duration, invocation_id, function_id, inputs, is_cold, recorder| {
            dispatcher_sender
                .blocking_send((
                    registry,
                    duration,
                    invocation_id,
                    function_id,
                    inputs,
                    is_cold,
                    recorder,
                ))
                .unwrap();
        };

    let mut context = Context::from_waker(Waker::noop());
    let mut client_future = Box::pin(remote_queue_client_logic(
        poll_option_receiver,
        remote_message_sender,
        dispatcher_send,
        1,
        ExportRegistry::new(1),
        0,
    ));

    // send message with the number of local cores and that the local queue state has changed
    poll_option_sender
        .try_send(crate::client::PollingOption::LocalCoreCountChanged(1))
        .unwrap();

    assert_eq!(Poll::Pending, client_future.poll_unpin(&mut context));
    match remote_message_receiver.try_recv().unwrap() {
        remote_message::RemoteMessage::NodeUpdate(update) => assert_eq!(1, update.num_local_cores),
        remote_message => panic!("Expected work request not {:?}", remote_message),
    }

    // expect to ask for work for 1 core
    assert_eq!(Poll::Pending, client_future.poll_unpin(&mut context));
    match remote_message_receiver.try_recv().unwrap() {
        remote_message::RemoteMessage::WorkRequest(engines) => {
            assert!(engines.engines.len() > 0);
            assert_eq!(1, engines.engines[0].engine_capacity);
        }
        remote_message => panic!("Expected work request not {:?}", remote_message),
    }

    // send back work for 1 core
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
    let _invocation_id_1 = match dispatcher_receiver.poll_recv(&mut context) {
        Poll::Ready(Some((
            _registry,
            _duration,
            invocation_id,
            function_id,
            _inputs,
            is_cold,
            _recorder,
        ))) => {
            assert!(!is_cold);
            assert_eq!(expected_function_id, function_id.as_str());
            invocation_id
        }
        Poll::Pending | Poll::Ready(None) => panic!("Should receive work now"),
    };

    // expect it to ask for 1 more work given the prefetch capacity
    match remote_message_receiver.try_recv().unwrap() {
        remote_message::RemoteMessage::WorkRequest(engines) => {
            assert!(engines.engines.len() > 0);
            assert_eq!(1, engines.engines[0].engine_capacity);
        }
        remote_message => panic!("Expected work request not {:?}", remote_message),
    }
}

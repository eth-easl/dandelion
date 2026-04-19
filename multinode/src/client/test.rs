use std::{future::Future, task::Poll};

use crate::{
    client::{remote_queue_handler, send_message},
    proto::{Engine, EngineType, NodeInfo},
    serialize_node_info,
};
use futures::task::{Context, Waker};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[test]
fn test_remote_queue_handler() {
    // create a mock socket for the handler
    let (mut client, mut server) = tokio::io::duplex(4096);

    let mut handler_context = Context::from_waker(Waker::noop());
    let mut handler_future = Box::pin(remote_queue_handler(server));

    // send the node info to the handler
    let node_info = NodeInfo {
        version: 1,
        engines: vec![Engine {
            engine_type: EngineType::EngineKvm as i32,
            engine_capacity: 2,
        }],
    };
    let node_info_serial = serialize_node_info(node_info);

    match Box::pin(send_message(node_info_serial, client))
        .as_mut()
        .poll(&mut handler_context)
    {
        Poll::Ready(()) => (),
        Poll::Pending => panic!("Should be able to write entire node info"),
    }

    // poll the handle once to make sure the node info was processed
    handler_future.as_mut().poll(&mut handler_context);

    // ask for a as many tasks as we have engines
    match Box::pin(send_message(node_info_serial, client))
        .as_mut()
        .poll(&mut handler_context)
    {
        Poll::Ready(()) => (),
        Poll::Pending => panic!("Should be able to write entire node info"),
    }
}

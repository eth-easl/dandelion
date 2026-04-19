use dandelion_commons::{
    err_dandelion, DandelionError, DandelionResult, FunctionId, MultinodeError,
};
use log::{debug, warn};
use machine_interface::{
    composition::CompositionSet,
    machine_config::{EngineType, EnumCount},
    memory_domain::Context,
};
use prost::bytes::{buf, Buf, Bytes, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    //     net::TcpStream,
    //     sync::watch::Receiver,
    //     time::{sleep, Duration},
};

use crate::{
    deserialize_node_info,
    proto::{self, NodeInfo},
    util::{self, composition_sets_to_proto, proto_data_sets_to_context},
};

#[cfg(test)]
mod test;

// TODO: should only registered remotes be allowed to make requests?
//       -> if so we need to make sure we don't allow requests after we deregister a node (introduce
//          an active flag in the RemoteNode struct)
// TODO: do we need to send periodic heartbeats to the remotes (to check if a node is still alive)?

/// Represents a remote dandelion node.
///
/// Create a new `RemoteNode` using either the `try_create_client` or `create_client` function.
/// Use the `RemoteNode` instance to communicate with the remote dandelion instance using the
/// multinode api.
// #[derive(Debug)]
// pub struct RemoteNode {
//     /// Remote connection info
//     remote_host: String,
//     remote_port: u16,
//     /// The client struct used to execute the http requests
//     client: reqwest::Client,
// }

// async fn try_connect(
//     client: &reqwest::Client,
//     remote_host: &String,
//     remote_port: u16,
// ) -> DandelionResult<()> {
//     match client
//         .post(format!("http://{}:{}/multinode/", remote_host, remote_port))
//         .body("")
//         .send()
//         .await
//     {
//         Ok(resp) => {
//             if resp.status().is_success() {
//                 return Ok(());
//             } else {
//                 return err_dandelion!(DandelionError::Multinode(
//                     MultinodeError::ConnectionFailed(format!(
//                         "Probe returned status code {}",
//                         resp.status()
//                     ),)
//                 ));
//             }
//         }
//         Err(err) => err_dandelion!(DandelionError::Multinode(MultinodeError::ConnectionFailed(
//             format!("Probe request failed: {:?}", err),
//         ))),
//     }
// }

// /// Tries to establish connection with the given remote and returns the corresponding `RemoteNode`
// /// instance on success.
// pub async fn try_create_client(
//     remote_host: String,
//     remote_port: u16,
// ) -> DandelionResult<RemoteNode> {
//     let client = reqwest::Client::new();
//     match try_connect(&client, &remote_host, remote_port).await {
//         Ok(_) => Ok(RemoteNode {
//             remote_host,
//             remote_port,
//             client,
//         }),
//         Err(err) => Err(err),
//     }
// }

// /// Creates a `RemoteNode` instance representing the given remote node by continuosly trying to
// /// establish a connection and retrying every `retry_timout_ms` ms on failure until success.
// pub async fn create_client(
//     remote_host: String,
//     remote_port: u16,
//     retry_timout_ms: u64,
// ) -> RemoteNode {
//     let client = reqwest::Client::new();
//     loop {
//         match try_connect(&client, &remote_host, remote_port).await {
//             Ok(_) => {
//                 return RemoteNode {
//                     remote_host,
//                     remote_port,
//                     client,
//                 }
//             }
//             Err(err) => {
//                 warn!(
//                     "Failed to probe remote node! Retrying in {} ms... (Error: {})",
//                     retry_timout_ms, err
//                 );
//                 sleep(Duration::from_millis(retry_timout_ms)).await;
//             }
//         }
//     }
// }

// impl RemoteNode {
//     async fn send_request(&self, url: String, request: Bytes) -> DandelionResult<Bytes> {
//         match self.client.post(url).body(request).send().await {
//             Ok(response) => {
//                 if !response.status().is_success() {
//                     return err_dandelion!(DandelionError::Multinode(
//                         MultinodeError::RequestFailed(format!(
//                             "Response status {:?}",
//                             response.status()
//                         ),)
//                     ));
//                 }
//                 let body = response
//                     .bytes()
//                     .await
//                     .expect("Failed to collect response body");
//                 Ok(body)
//             }
//             Err(err) => {
//                 return err_dandelion!(DandelionError::Multinode(MultinodeError::RequestFailed(
//                     format!("{:?}", err),
//                 )))
//             }
//         }
//     }

//     /// Runs the given function with the given input data on the remote node and returns the
//     /// resulting output sets.
//     pub async fn invoke_function(
//         &self,
//         function_id: FunctionId,
//         data_sets: &Vec<Option<CompositionSet>>,
//     ) -> DandelionResult<Context> {
//         // prepare request
//         let serialized_data = composition_sets_to_proto(data_sets);
//         let inv_req = proto::InvocationRequest {
//             function_id: (*function_id).clone(),
//             data_sets: serialized_data,
//         };
//         let request = super::serialize_invocation_request(inv_req);

//         // execute
//         let response_body = self
//             .send_request(
//                 format!(
//                     "http://{}:{}/multinode/invoke",
//                     self.remote_host, self.remote_port
//                 ),
//                 request,
//             )
//             .await?;

//         // process response
//         let inv_resp = match super::deserialize_invocation_response(response_body.clone()) {
//             Ok(resp) => resp,
//             Err(_) => {
//                 // might also return a non-successful ActionStatus on internal errors
//                 let action_status = super::deserialize_action_status(response_body.clone())?;
//                 assert!(!action_status.success);
//                 return err_dandelion!(DandelionError::Multinode(MultinodeError::RequestFailed(
//                     action_status.message,
//                 )));
//             }
//         };
//         if !inv_resp.success {
//             return err_dandelion!(DandelionError::Multinode(MultinodeError::RequestFailed(
//                 inv_resp.error_msg,
//             )));
//         }
//         Ok(proto_data_sets_to_context(inv_resp.data_sets))
//     }
// }

// // TODO: should this be another object, having a remote worker and a remote master?
// // Then can implement deregister on drop for example
// /// Registers this node at the remote node.
// pub async fn register_as_remote(
//     local_host: String,
//     local_port: u32,
//     remote_url: String,
//     engines: Vec<(EngineType, u32)>,
// ) -> DandelionResult<()> {
//     debug!("Registering as remote on {}", remote_url);
//     // prepare request
//     let proto_engines = engines
//         .iter()
//         .map(|(t, c)| proto::Engine {
//             engine_type: util::engine_type_dtop(*t) as i32,
//             engine_capacity: *c,
//         })
//         .collect();
//     let node_info = proto::NodeInfo {
//         host: local_host,
//         port: local_port,
//         engines: proto_engines,
//     };
//     let request = super::serialize_node_info(node_info);

//     let response_body = match reqwest::Client::new()
//         .post(format!("http://{}/multinode/register", remote_url))
//         .body(request)
//         .send()
//         .await
//     {
//         Ok(response) => {
//             if !response.status().is_success() {
//                 return err_dandelion!(DandelionError::Multinode(MultinodeError::RequestFailed(
//                     format!("Response status {:?}", response.status()),
//                 )));
//             } else {
//                 response
//                     .bytes()
//                     .await
//                     .expect("Failed to collect response body")
//             }
//         }
//         Err(err) => {
//             return err_dandelion!(DandelionError::Multinode(MultinodeError::RequestFailed(
//                 format!("{:?}", err),
//             )))
//         }
//     };

//     // process response
//     let action_status = super::deserialize_action_status(response_body)?;
//     match action_status.success {
//         true => Ok(()),
//         false => err_dandelion!(DandelionError::Multinode(MultinodeError::RequestFailed(
//             action_status.message,
//         ))),
//     }
// }

const _: () = assert!(size_of::<u64>() == size_of::<usize>());

/// To send a message between nodes, always first send the length of the message,
/// then the message, so the other side knows when one message ends.
async fn send_message(buffer: Bytes, mut sender: impl AsyncWriteExt + std::marker::Unpin) {
    let message_size = buffer.len() as u64;
    sender.write_u64(message_size).await;
    sender.write_all(&buffer).await;
    sender.flush().await;
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
/// Response is either a single available task if none are available immediately,
/// a delayed message if the other node still wants tasks. If the response is yes,
/// then send a task. Each task sent out is associated with an id.
/// When a task if finished, the response also carries that id, so the promise can be fulfilled.
pub async fn remote_queue_handler<Stream: AsyncReadExt + AsyncWriteExt + std::marker::Unpin>(
    mut socket: Stream,
) {
    // first ask for the information about the other node
    let node_info_buffer = receive_message(socket).await;
    // let node_info_result = socket.read(node_info_buffer.as_mut_slice()).await;
    // TODO: could we handle error differently?
    // assert_eq!(size_of::<proto::NodeInfo>(), node_info_result.unwrap());

    let NodeInfo { version, engines } = deserialize_node_info(node_info_buffer).unwrap();
    assert_eq!(version, 1);
    // storage for the promises of the functions we hand out
    let mut per_engine_promises = Vec::new();
    for engine in engines {
        // for each engine, make sure there is an entry and it has the correct number of vec slots
        let index = usize::try_from(engine.engine_type).unwrap();
        if per_engine_promises.len() <= index {
            per_engine_promises.resize(index + 1, vec![]);
        }
        per_engine_promises[index].resize(engine.engine_capacity as usize, None);
    }

    // are ready, wait for the remote to ask for work
    // For the work request just rereceive the node info, but with an amount of engines to the currently requested number of tasks
    let work_request_buffer = receive_message(socket).await;
    let work_request = deserialize_node_info(work_request_buffer).unwrap();

    // send back that we don't have any work
}

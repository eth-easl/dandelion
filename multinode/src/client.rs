use dandelion_commons::{DandelionError, DandelionResult, FunctionId, MultinodeError};
use log::warn;
use machine_interface::{
    composition::CompositionSet, machine_config::EngineType, memory_domain::Context,
};
use prost::bytes::Bytes;
use tokio::time::{sleep, Duration};

use crate::{
    proto,
    util::{self, composition_sets_to_proto, proto_data_sets_to_context},
};

// TODO: should only registered remotes be allowed to make requests?
//       -> if so we need to make sure we don't allow requests after we deregister a node (introduce
//          an active flag in the RemoteNode struct)
// TODO: do we need to send periodic heartbeats to the remotes (to check if a node is still alive)?

/// Represents a remote dandelion node.
///
/// Create a new `RemoteNode` using either the `try_create_client` or `create_client` function.
/// Use the `RemoteNode` instance to communicate with the remote dandelion instance using the
/// multinode api.
#[derive(Debug)]
pub struct RemoteNode {
    /// Local connection info
    local_host: String,
    local_port: u16,
    /// Remote connection info
    remote_host: String,
    remote_port: u16,
    /// The client struct used to execute the http requests
    client: reqwest::Client,
}

async fn try_connect(
    client: &reqwest::Client,
    remote_host: &String,
    remote_port: u16,
) -> DandelionResult<()> {
    match client
        .post(format!("http://{}:{}/multinode/", remote_host, remote_port))
        .body("")
        .send()
        .await
    {
        Ok(resp) => {
            if resp.status().is_success() {
                return Ok(());
            } else {
                return Err(DandelionError::Multinode(MultinodeError::ConnectionFailed(
                    format!("Probe returned status code {}", resp.status()),
                )));
            }
        }
        Err(err) => Err(DandelionError::Multinode(MultinodeError::ConnectionFailed(
            format!("Probe request failed: {:?}", err),
        ))),
    }
}

/// Tries to establish connection with the given remote and returns the corresponding `RemoteNode`
/// instance on success.
pub async fn try_create_client(
    local_host: String,
    local_port: u16,
    remote_host: String,
    remote_port: u16,
) -> DandelionResult<RemoteNode> {
    let client = reqwest::Client::new();
    match try_connect(&client, &remote_host, remote_port).await {
        Ok(_) => Ok(RemoteNode {
            local_host,
            local_port,
            remote_host,
            remote_port,
            client,
        }),
        Err(err) => Err(err),
    }
}

/// Creates a `RemoteNode` instance representing the given remote node by continuosly trying to
/// establish a connection and retrying every `retry_timout_ms` ms on failure until success.
pub async fn create_client(
    local_host: String,
    local_port: u16,
    remote_host: String,
    remote_port: u16,
    retry_timout_ms: u64,
) -> RemoteNode {
    let client = reqwest::Client::new();
    loop {
        match try_connect(&client, &remote_host, remote_port).await {
            Ok(_) => {
                return RemoteNode {
                    local_host,
                    local_port,
                    remote_host,
                    remote_port,
                    client,
                }
            }
            Err(err) => {
                warn!(
                    "Failed to probe remote node! Retrying in {} ms... (Error: {})",
                    retry_timout_ms, err
                );
                sleep(Duration::from_millis(retry_timout_ms)).await;
            }
        }
    }
}

impl RemoteNode {
    async fn send_request(&self, url: String, request: Bytes) -> DandelionResult<Bytes> {
        match self.client.post(url).body(request).send().await {
            Ok(response) => {
                if !response.status().is_success() {
                    return Err(DandelionError::Multinode(MultinodeError::RequestFailed(
                        format!("Response status {:?}", response.status()),
                    )));
                }
                let body = response
                    .bytes()
                    .await
                    .expect("Failed to collect response body");
                Ok(body)
            }
            Err(err) => {
                return Err(DandelionError::Multinode(MultinodeError::RequestFailed(
                    format!("{:?}", err),
                )))
            }
        }
    }

    /// Registers this node at the remote node.
    pub async fn register_at_remote(&self, engines: Vec<(EngineType, u32)>) -> DandelionResult<()> {
        // prepare request
        let proto_engines = engines
            .iter()
            .map(|(t, c)| proto::Engine {
                engine_type: util::engine_type_dtop(*t) as i32,
                engine_capacity: *c,
            })
            .collect();
        let node_info = proto::NodeInfo {
            host: self.local_host.clone(),
            port: self.local_port as u32,
            engines: proto_engines,
        };
        let request = super::serialize_node_info(node_info);

        // execute
        let response_body = self
            .send_request(
                format!(
                    "http://{}:{}/multinode/register",
                    self.remote_host, self.remote_port
                ),
                request,
            )
            .await?;

        // process response
        let action_status = super::deserialize_action_status(response_body)?;
        match action_status.success {
            true => Ok(()),
            false => Err(DandelionError::Multinode(MultinodeError::RequestFailed(
                action_status.message,
            ))),
        }
    }

    /// Deregisters this node at the remote node.
    pub async fn deregister_at_remote(&self) -> DandelionResult<()> {
        // prepare request
        let node_info = proto::NodeInfo {
            host: self.local_host.clone(),
            port: self.local_port as u32,
            engines: vec![],
        };
        let request = super::serialize_node_info(node_info);

        // execute
        let response_body = self
            .send_request(
                format!(
                    "http://{}:{}/multinode/deregister",
                    self.remote_host, self.remote_port
                ),
                request,
            )
            .await?;

        // process response
        let action_status = super::deserialize_action_status(response_body)?;
        match action_status.success {
            true => Ok(()),
            false => Err(DandelionError::Multinode(MultinodeError::RequestFailed(
                action_status.message,
            ))),
        }
    }

    /// Runs the given function with the given input data on the remote node and returns the
    /// resulting output sets.
    pub async fn invoke_function(
        &self,
        function_id: FunctionId,
        data_sets: &Vec<Option<CompositionSet>>,
    ) -> DandelionResult<Context> {
        // prepare request
        let serialized_data = composition_sets_to_proto(data_sets);
        let inv_req = proto::InvocationRequest {
            function_id: (*function_id).clone(),
            data_sets: serialized_data,
        };
        let request = super::serialize_invocation_request(inv_req);

        // execute
        let response_body = self
            .send_request(
                format!(
                    "http://{}:{}/multinode/invoke",
                    self.remote_host, self.remote_port
                ),
                request,
            )
            .await?;

        // process response
        let inv_resp = match super::deserialize_invocation_response(response_body.clone()) {
            Ok(resp) => resp,
            Err(_) => {
                // might also return a non-successful ActionStatus on internal errors
                let action_status = super::deserialize_action_status(response_body.clone())?;
                assert!(!action_status.success);
                return Err(DandelionError::Multinode(MultinodeError::RequestFailed(
                    action_status.message,
                )));
            }
        };
        if !inv_resp.success {
            return Err(DandelionError::Multinode(MultinodeError::RequestFailed(
                inv_resp.error_msg,
            )));
        }
        Ok(proto_data_sets_to_context(
            &inv_resp.data_sets,
            response_body,
        ))
    }
}

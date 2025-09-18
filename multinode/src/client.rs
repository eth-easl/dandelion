use log::warn;
use reqwest::Response;
use tokio::time::{sleep, Duration};

use crate::proto::{DataSet, NodeInfo, Task, TaskResult};

#[derive(Debug)]
pub enum ClientError {
    ConnectionFailed(String),
    RequestFailed(String),
    InvalidResponse(String),
    ActionFailed,
    BufferGone,
}
impl core::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        return f.write_fmt(format_args!("{:?}", self));
    }
}
impl std::error::Error for ClientError {}

#[derive(Debug)]
pub struct Client {
    local_name: String,
    remote_name: String,
    remote_host: String,
    remote_port: u16,
    client: reqwest::Client,
}

pub async fn try_create_client(
    local_name: String,
    remote_name: String,
    remote_host: String,
    remote_port: u16,
) -> Result<Client, ClientError> {
    let client = reqwest::Client::new();
    match client
        .post(format!("http://{}:{}/multinode/", remote_host, remote_port))
        .body("")
        .send()
        .await
    {
        Ok(resp) => {
            if resp.status().is_success() {
                return Ok(Client {
                    local_name,
                    remote_name,
                    remote_host,
                    remote_port,
                    client,
                });
            } else {
                return Err(ClientError::ConnectionFailed(format!(
                    "Probe returned status code {}",
                    resp.status()
                )));
            }
        }
        Err(err) => Err(ClientError::ConnectionFailed(format!(
            "Probe request failed: {:?}",
            err
        ))),
    }
}

pub async fn create_client(
    local_name: String,
    remote_name: String,
    remote_host: String,
    remote_port: u16,
    retry_timout_ms: u64,
) -> Client {
    loop {
        // TODO: better handle this cloning below
        match try_create_client(
            local_name.clone(),
            remote_name.clone(),
            remote_host.clone(),
            remote_port,
        )
        .await
        {
            Ok(client) => return client,
            Err(err) => {
                warn!(
                    "Failed to probe remote node! Retrying in {} ms... (Error: {:?})",
                    retry_timout_ms, err
                );
                sleep(Duration::from_millis(retry_timout_ms)).await;
            }
        }
    }
}

async fn handle_response_action_status(response: Response) -> Result<(), ClientError> {
    if !response.status().is_success() {
        return Err(ClientError::RequestFailed(format!(
            "Response status {:?}",
            response.status()
        )));
    }
    let body = response
        .bytes()
        .await
        .expect("Failed to collect response body");
    match super::deserialize_action_status(body) {
        Ok(action_status) => {
            if action_status.success {
                Ok(())
            } else {
                Err(ClientError::ActionFailed)
            }
        }
        Err(err) => return Err(ClientError::InvalidResponse(format!("{:?}", err))),
    }
}

impl Client {
    pub async fn register_at_remote(
        &self,
        name: String,
        local_host: String,
        local_port: u16,
        engine_type: i32,
        engine_capacity: u32,
    ) -> Result<(), ClientError> {
        let request = super::serialize_node_info(NodeInfo {
            name,
            host: local_host,
            port: local_port as u32,
            engine_type,
            engine_capacity,
        });
        match self
            .client
            .post(format!(
                "http://{}:{}/multinode/register",
                self.remote_host, self.remote_port
            ))
            .body(request)
            .send()
            .await
        {
            Ok(response) => handle_response_action_status(response).await,
            Err(err) => return Err(ClientError::RequestFailed(format!("{:?}", err))),
        }
    }

    pub async fn deregister_at_remote(&self, name: String) -> Result<(), ClientError> {
        let mut node_info = NodeInfo::default();
        node_info.name = name;
        let request = super::serialize_node_info(node_info);
        match self
            .client
            .post(format!(
                "http://{}:{}/multinode/deregister",
                self.remote_host, self.remote_port
            ))
            .body(request)
            .send()
            .await
        {
            Ok(response) => handle_response_action_status(response).await,
            Err(err) => return Err(ClientError::RequestFailed(format!("{:?}", err))),
        }
    }

    pub async fn enqueue_task(
        &self,
        function_id: u64,
        promise_index: usize,
        data_sets: Vec<DataSet>,
    ) -> Result<(), ClientError> {
        let request = super::serialize_task(Task {
            client_name: self.local_name.clone(),
            worker_name: self.remote_name.clone(),
            function_id,
            client_promise_idx: promise_index as u64,
            data_sets,
        });
        match self
            .client
            .post(format!(
                "http://{}:{}/multinode/schedule",
                self.remote_host, self.remote_port
            ))
            .body(request)
            .send()
            .await
        {
            Ok(response) => handle_response_action_status(response).await,
            Err(err) => return Err(ClientError::RequestFailed(format!("{:?}", err))),
        }
    }

    pub async fn return_task_result(
        &self,
        promise_index: usize,
        data_sets: Vec<DataSet>,
    ) -> Result<(), ClientError> {
        let request = super::serialize_task_result(TaskResult {
            worker_name: self.remote_name.clone(),
            promise_idx: promise_index as u64,
            data_sets,
        });
        match self
            .client
            .post(format!(
                "http://{}:{}/multinode/result",
                self.remote_host, self.remote_port
            ))
            .body(request)
            .send()
            .await
        {
            Ok(response) => handle_response_action_status(response).await,
            Err(err) => return Err(ClientError::RequestFailed(format!("{:?}", err))),
        }
    }
}

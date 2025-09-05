use tokio::sync::mpsc::Sender;
use tonic::transport::{server::Router, Channel};
use tonic::{transport::Server, Request, Response, Status};

use multinode_proto::main_node_interface_client::MainNodeInterfaceClient;
use multinode_proto::main_node_interface_server::{MainNodeInterface, MainNodeInterfaceServer};
use multinode_proto::worker_interface_client::WorkerInterfaceClient;
use multinode_proto::worker_interface_server::{WorkerInterface, WorkerInterfaceServer};
use multinode_proto::{ActionStatus, NodeInfo, Task, TaskResult};

pub mod multinode_proto {
    tonic::include_proto!("multinode_proto");
}

use super::DispatcherCommand;

#[derive(Debug)]
pub enum RequestError {
    ActionFailed(String),
    RequestFailed(tonic::Status),
}

fn handle_action_request(resp: Result<Response<ActionStatus>, Status>) -> Result<(), RequestError> {
    match resp {
        Ok(response) => {
            let action_status = response.into_inner();
            if action_status.success {
                Ok(())
            } else {
                Err(RequestError::ActionFailed(action_status.message))
            }
        }
        Err(err) => Err(RequestError::RequestFailed(err)),
    }
}

// leader node service

#[derive(Debug)]
pub struct MultinodeLeaderServer {
    dispatcher_sender: Sender<DispatcherCommand>,
}

impl MultinodeLeaderServer {
    pub fn new(dispatcher_sender: Sender<DispatcherCommand>) -> Self {
        MultinodeLeaderServer { dispatcher_sender }
    }
}

#[tonic::async_trait]
impl MainNodeInterface for MultinodeLeaderServer {
    async fn register_worker(
        &self,
        request: Request<NodeInfo>,
    ) -> Result<Response<ActionStatus>, Status> {
        let node_info = request.into_inner();
        self.dispatcher_sender
            .send(DispatcherCommand::WorkerRegistration {
                name: node_info.name,
            })
            .await
            .unwrap();

        let response = ActionStatus {
            success: true,
            message: "".to_string(),
        };

        Ok(Response::new(response))
    }

    async fn return_task_result(
        &self,
        request: Request<TaskResult>,
    ) -> Result<Response<ActionStatus>, Status> {
        let task_result = request.into_inner();
        self.dispatcher_sender
            .send(DispatcherCommand::RemoteTaskResult {
                name: task_result.name,
            })
            .await
            .unwrap();

        let response = ActionStatus {
            success: true,
            message: "".to_string(),
        };

        Ok(Response::new(response))
    }
}

pub fn build_leader_server(dispatcher_sender: Sender<DispatcherCommand>) -> Router {
    let leader = MultinodeLeaderServer::new(dispatcher_sender);
    Server::builder().add_service(MainNodeInterfaceServer::new(leader))
}

#[derive(Debug)]
pub struct MultinodeLeaderClient {
    client: MainNodeInterfaceClient<Channel>,
}

impl MultinodeLeaderClient {
    pub async fn new(addr: String) -> Result<MultinodeLeaderClient, tonic::transport::Error> {
        let client = MainNodeInterfaceClient::connect(addr).await?;
        Ok(MultinodeLeaderClient { client })
    }

    pub async fn register_worker(&mut self, name: String) -> Result<(), RequestError> {
        let request = Request::new(NodeInfo { name });
        handle_action_request(self.client.register_worker(request).await)
    }

    pub async fn return_task_result(&mut self, name: String) -> Result<(), RequestError> {
        let request = Request::new(TaskResult { name });
        handle_action_request(self.client.return_task_result(request).await)
    }
}

// worker node service

#[derive(Debug)]
pub struct MultinodeWorker {
    dispatcher_sender: Sender<DispatcherCommand>,
}

impl MultinodeWorker {
    pub fn new(dispatcher_sender: Sender<DispatcherCommand>) -> Self {
        MultinodeWorker { dispatcher_sender }
    }
}

#[tonic::async_trait]
impl WorkerInterface for MultinodeWorker {
    async fn enqueue_task(&self, request: Request<Task>) -> Result<Response<ActionStatus>, Status> {
        let task = request.into_inner();
        self.dispatcher_sender
            .send(DispatcherCommand::RemoteTask { name: task.name })
            .await
            .unwrap();

        let response = ActionStatus {
            success: true,
            message: "".to_string(),
        };

        Ok(Response::new(response))
    }
}

pub fn build_worker_server(dispatcher_sender: Sender<DispatcherCommand>) -> Router {
    let leader = MultinodeWorker::new(dispatcher_sender);
    Server::builder().add_service(WorkerInterfaceServer::new(leader))
}

#[derive(Debug)]
pub struct MultinodeWorkerClient {
    client: WorkerInterfaceClient<Channel>,
}

impl MultinodeWorkerClient {
    pub async fn new(addr: String) -> Result<MultinodeWorkerClient, tonic::transport::Error> {
        let client = WorkerInterfaceClient::connect(addr).await?;
        Ok(MultinodeWorkerClient { client })
    }

    pub async fn enqueue_task(&mut self, name: String) -> Result<(), RequestError> {
        let request = Request::new(Task { name });
        handle_action_request(self.client.enqueue_task(request).await)
    }
}

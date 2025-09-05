use tonic::{
    transport::server::Router, transport::Channel, transport::Server, Request, Response, Status,
};

use multinode_proto::main_node_interface_client::MainNodeInterfaceClient;
use multinode_proto::main_node_interface_server::{MainNodeInterface, MainNodeInterfaceServer};
use multinode_proto::worker_interface_client::WorkerInterfaceClient;
use multinode_proto::worker_interface_server::{WorkerInterface, WorkerInterfaceServer};
use multinode_proto::{ActionStatus, NodeInfo, Task, TaskResult};

pub mod multinode_proto {
    tonic::include_proto!("multinode_proto");
}

#[derive(Debug, Default)]
pub struct MultinodeLeaderServer {}

#[tonic::async_trait]
impl MainNodeInterface for MultinodeLeaderServer {
    async fn register_worker(
        &self,
        request: Request<NodeInfo>,
    ) -> Result<Response<ActionStatus>, Status> {
        println!("Got a registration request: {:?}", request);

        let response = ActionStatus {
            success: true,
            message: "".to_string(),
        };

        Ok(Response::new(response))
    }

    async fn register_task_result(
        &self,
        request: Request<TaskResult>,
    ) -> Result<Response<ActionStatus>, Status> {
        println!("Got back a task result: {:?}", request);

        let response = ActionStatus {
            success: true,
            message: "".to_string(),
        };

        Ok(Response::new(response))
    }
}

pub fn build_leader_server() -> Router {
    let leader = MultinodeLeaderServer::default();
    Server::builder().add_service(MainNodeInterfaceServer::new(leader))
}

#[derive(Debug)]
pub struct MultinodeLeaderClient {
    client: MainNodeInterfaceClient<Channel>,
}

impl MultinodeLeaderClient {
    pub async fn new(addr: &str) -> MultinodeLeaderClient {
        // TODO: use addr to connect
        let client = MainNodeInterfaceClient::connect("http://0.0.0.0:8081")
            .await
            .expect("Failed to connect to multinode leader server"); // TODO: probably better to return result
        MultinodeLeaderClient { client }
    }

    pub async fn register_worker(&mut self, name: String) {
        println!("trying to register worker");
        let request = Request::new(NodeInfo { name });
        let response = self
            .client
            .register_worker(request)
            .await
            .expect("Failed to register worker"); // TODO: return result
        println!(
            "Sucessfully registered worker and got response {:?}",
            response
        );
    }
}

#[derive(Debug, Default)]
pub struct MultinodeWorker {}

#[tonic::async_trait]
impl WorkerInterface for MultinodeWorker {
    async fn enqueue_task(&self, request: Request<Task>) -> Result<Response<ActionStatus>, Status> {
        println!("Got a task from leader: {:?}", request);

        let response = ActionStatus {
            success: true,
            message: "".to_string(),
        };

        Ok(Response::new(response))
    }
}

pub fn build_worker_server() -> Router {
    let leader = MultinodeWorker::default();
    Server::builder().add_service(WorkerInterfaceServer::new(leader))
}

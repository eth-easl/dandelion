use std::thread::{self, JoinHandle};

use dandelion_commons::DandelionResult;
use log::error;
use machine_interface::machine_config::EngineType;
use multinode::client::{create_client, RemoteClient};
use tokio::runtime::Builder;

use crate::queue::{get_engine_flag, WorkQueue};

static RETRY_TIMEOUT: u64 = 1000;

enum Status {
    INACTIVE,
    ACTIVE,
    STOPPED,
    FINISHED,
}

pub struct RemoteNode {
    client: RemoteClient,
    work_queue: WorkQueue,
    engines: Vec<(EngineType, u32)>,
    status: Status,
    polling_thread: Option<JoinHandle<()>>,
}

impl RemoteNode {
    pub async fn new(
        local_host: String,
        local_port: u16,
        remote_host: String,
        remote_port: u16,
        work_queue: WorkQueue,
        engines: Vec<(EngineType, u32)>,
    ) -> RemoteNode {
        let client = create_client(
            local_host,
            local_port,
            remote_host,
            remote_port,
            RETRY_TIMEOUT,
        )
        .await;
        RemoteNode {
            client,
            work_queue,
            engines,
            status: Status::INACTIVE,
            polling_thread: None,
        }
    }

    fn start_polling(&mut self) {
        let queue = self.work_queue.clone();
        self.polling_thread = Some(thread::spawn(move || {
            let rt = Builder::new_current_thread()
                .enable_io()
                .build()
                .expect("Failed to create polling thread!");
            rt.block_on(async {
                loop {
                    let (work, dept) = queue.get_work(0);
                }
            })
        }));
    }

    async fn stop_polling(&mut self) {
        self.status = Status::STOPPED;
        if let Some(handle) = self.polling_thread.take() {
            let res = handle.join();
            if let Err(err) = res {
                error!("Polling thread exited with error: {:?}", err);
            }
        }
    }

    pub fn add_remote(
        &mut self,
        remote_host: String,
        remote_port: u16,
        engines: Vec<(EngineType, u32)>,
    ) -> DandelionResult<()> {
        // TODO: add node to list of remotes

        if self.polling_thread.is_none() {
            self.start_polling();
        }

        Ok(())
    }

    pub fn remove_remote(&mut self, remote_host: String, remote_port: u16) -> DandelionResult<()> {
        // TODO: remove remote from list of remotes

        // TODO: wait for all jobs to complete
        // TODO: if list becomes empty stop the polling thread

        Ok(())
    }
}

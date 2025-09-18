use log::error;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, RwLock,
    },
};

use crossbeam::channel::{TryRecvError, TrySendError};
use dandelion_commons::{DandelionError, DandelionResult, MultinodeError};
use machine_interface::{
    function_driver::WorkDone,
    promise::{Debt, PromiseBuffer},
};
use multinode::{client::Client, proto::DataSet};

use crate::composition::CompositionSet;

const MAX_QUEUE: usize = 4096;

pub struct RemoteEngineQueue {
    queue_in: crossbeam::channel::Sender<(u64, Vec<Option<CompositionSet>>, Debt)>,
    queue_out: crossbeam::channel::Receiver<(u64, Vec<Option<CompositionSet>>, Debt)>,
    promise_buffer: PromiseBuffer,
    remote_clients: RwLock<HashMap<String, (u32, Arc<Client>)>>,
    remote_capacity: AtomicU32,
}

fn serialize_data_sets(sets: Vec<Option<CompositionSet>>) -> Vec<DataSet> {
    let mut out_vector = Vec::with_capacity(sets.len());
    for set_opt in sets.iter() {
        if let Some(set) = set_opt {
            out_vector.push(set.serialize());
        }
    }
    out_vector
}

impl RemoteEngineQueue {
    pub fn new() -> Self {
        let (sender, receiver) = crossbeam::channel::bounded(MAX_QUEUE);
        RemoteEngineQueue {
            queue_in: sender,
            queue_out: receiver,
            promise_buffer: PromiseBuffer::init(MAX_QUEUE),
            remote_clients: RwLock::new(HashMap::new()),
            remote_capacity: AtomicU32::new(0),
        }
    }

    pub fn available(&self) -> bool {
        let clients = self
            .remote_clients
            .read()
            .expect("Failed to get read lock for remote_clients (lock is poisoned)!");
        clients.len() > 0
    }

    // TODO: refactor arg names
    pub async fn add_remote(
        &self,
        key: String,
        host: String,
        port: u16,
        cap: u32,
    ) -> DandelionResult<()> {
        // client creation also checks if the remote resource can be reached
        let local_name = "leader".to_string(); // TODO
        let client =
            match multinode::client::try_create_client(local_name, key.clone(), host, port).await {
                Ok(c) => c,
                Err(_) => {
                    return Err(DandelionError::MultinodeError(
                        MultinodeError::ResourceNotReached,
                    ))
                }
            };

        let mut clients = self
            .remote_clients
            .write()
            .expect("Failed to get write lock for remote_clients (lock is poisoned)!");
        if clients.contains_key(&key) {
            return Err(DandelionError::MultinodeError(
                MultinodeError::ResourceDuplicate,
            ));
        }
        clients.insert(key, (cap, Arc::new(client)));
        self.remote_capacity.fetch_add(cap, Ordering::AcqRel);
        Ok(())
    }

    pub fn remove_remote(&self, key: &String) -> DandelionResult<()> {
        let mut clients = self
            .remote_clients
            .write()
            .expect("Failed to get write lock for remote_clients (lock is poisoned)!");
        match clients.remove(key) {
            Some((client_cap, _)) => {
                self.remote_capacity.fetch_sub(client_cap, Ordering::AcqRel);
                Ok(())
            }
            None => Err(DandelionError::MultinodeError(
                MultinodeError::ResourceNotFound,
            )),
        }
    }

    pub async fn enqueue_work(
        &self,
        f_id: u64,
        f_inputs: Vec<Option<CompositionSet>>,
    ) -> DandelionResult<WorkDone> {
        let (promise, debt) = self.promise_buffer.get_promise()?;

        // check if some remote has capacity and queue is empty -> if so send the work directly
        if self.queue_out.is_empty() {
            let current_cap = self.remote_capacity.load(Ordering::Acquire);
            if current_cap > 0
                && self
                    .remote_capacity
                    .compare_exchange(
                        current_cap,
                        current_cap - 1,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
            {
                let promise_idx = self
                    .promise_buffer
                    .get_promise_idx(&promise)
                    .expect("Promise is somehow out of bounds!");
                let mut selected_client = None;
                {
                    // select a client that has capacity and release lock afterwards
                    let mut clients = self
                        .remote_clients
                        .write()
                        .expect("Failed to get write lock for remote_clients (lock is poisoned)!");
                    for (_key, (cap, client)) in clients.iter_mut() {
                        if *cap > 0 {
                            *cap -= 1;
                            selected_client = Some((*client).clone());
                            break;
                        }
                    }
                }

                // send task to remote node
                let data_sets = serialize_data_sets(f_inputs);
                if let Err(err) = selected_client
                    .expect("Did not find a client with capacity even though there should be one!")
                    .enqueue_task(f_id, promise_idx, data_sets)
                    .await
                {
                    return Err(DandelionError::MultinodeError(MultinodeError::ClientError(
                        format!("{:?}", err),
                    )));
                }
                return promise.await;
            }
        }

        // if no remote has capacity enqueue it
        match self.queue_in.try_send((f_id, f_inputs, debt)) {
            Ok(()) => (),
            Err(TrySendError::Disconnected(_)) => {
                error!("Failed to enqueue work, workqueue has been disconnected")
            }
            Err(TrySendError::Full(_)) => return Err(DandelionError::WorkQueueFull),
        }
        promise.await
    }

    pub async fn return_work_result(
        &self,
        remote_key: &String,
    ) -> DandelionResult<Option<multinode::proto::Task>> {
        // if there is work in the queue take work and return it
        if !self.queue_out.is_empty() {
            // make sure worker client is still here
            let mut worker_client = None;
            {
                // get the client and release lock afterwards
                let clients = self
                    .remote_clients
                    .read()
                    .expect("Failed to get read lock for remote_clients (lock is poisoned)!");
                if let Some((_, client)) = clients.get(remote_key) {
                    worker_client = Some((*client).clone());
                }
            };

            // get next task and send it to remote
            if worker_client.is_some() {
                match self.queue_out.try_recv() {
                    Err(TryRecvError::Disconnected) => panic!("Remote workqueue disconnected!"),
                    Err(TryRecvError::Empty) => (), // possible due to concurrency -> just continue
                    Ok((next_f_id, next_f_inputs, debt)) => {
                        if debt.is_alive() {
                            let debt_idx = self
                                .promise_buffer
                                .get_debt_idx(&debt)
                                .expect("Debt is somehow out of bounds!");
                            let data_sets = serialize_data_sets(next_f_inputs);
                            if let Err(err) = worker_client
                                .expect("Did not find a client with capacity even though there should be one!")
                                .enqueue_task(next_f_id, debt_idx, data_sets)
                                .await
                            {
                                return Err(DandelionError::MultinodeError(MultinodeError::ClientError(
                                    format!("{:?}", err),
                                )));
                            }
                        }
                    }
                }
            }
        }

        // otherwise return the capacity of the remote resource
        let mut clients = self
            .remote_clients
            .write()
            .expect("Failed to get write lock for remote_clients (lock is poisoned)!");
        match clients.get_mut(remote_key) {
            Some((cap, _)) => {
                *cap += 1;
                Ok(None)
                // FIXME: it's possible that the queue has gotten work in the meantime
            }
            None => Err(DandelionError::MultinodeError(
                MultinodeError::ResourceNotFound,
            )),
        }
    }
}

use crate::{
    function_driver::{EngineWorkQueue, WorkToDo},
    promise::{Debt, Promise, PromiseBuffer},
};
use std::sync::Arc;
use tokio::sync::{
    mpsc::{channel, Receiver, Sender},
    Mutex,
};

#[derive(Clone)]
pub struct TestQueue {
    sender: Sender<(WorkToDo, Debt)>,
    reciever: Arc<Mutex<Receiver<(WorkToDo, Debt)>>>,
    promise_buffer: PromiseBuffer,
}

impl TestQueue {
    pub fn new() -> Self {
        let (sender, reciever) = channel(1);
        return TestQueue {
            sender,
            reciever: Arc::new(Mutex::new(reciever)),
            promise_buffer: PromiseBuffer::init(128),
        };
    }
    pub fn enqueu(&self, args: WorkToDo) -> Promise {
        let (promise, debt) = self.promise_buffer.get_promise().unwrap();
        self.sender.blocking_send((args, debt)).unwrap();
        return promise;
    }
}

impl EngineWorkQueue for TestQueue {
    async fn get_compute_engine_args(&self) -> (WorkToDo, Debt) {
        self.reciever.lock().await.recv().await.unwrap()
    }
    async fn get_io_engine_args(&self) -> (WorkToDo, Debt, Option<usize>) {
        let (work, debt) = self.reciever.lock().await.recv().await.unwrap();
        (work, debt, Some(0))
    }

    fn requeu_engine_args(
        &self,
        _work: WorkToDo,
        _debt: crate::promise::Debt,
        _composition_id: usize,
    ) {
        panic!("should not requeue on the test queue");
    }

    fn remove_self_from_queue(&self) {}
}

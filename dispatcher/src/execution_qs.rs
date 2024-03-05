use dandelion_commons::{records::Recorder, DandelionResult};
use machine_interface::{
    function_driver::{EngineArguments, WorkQueue},
    memory_domain::Context,
    promise::{Debt, Promise},
};
use std::sync::Arc;

/// Datastructure that implements priority queueing
/// Highest priority queue holds promises if there are any
#[derive(Clone)]
pub struct EngineQueue {
    queue_in: Arc<std::sync::mpsc::Sender<(EngineArguments, Debt)>>,
    queue_out: Arc<std::sync::Mutex<std::sync::mpsc::Receiver<(EngineArguments, Debt)>>>,
}

/// This is run on the engine so it performs asyncornous access to the local state
impl WorkQueue for EngineQueue {
    fn get_engine_args(&self) -> (EngineArguments, Debt) {
        loop {
            let (recieved_args, recieved_debt) = self.queue_out.lock().unwrap().recv().unwrap();
            if recieved_debt.is_alive() {
                return (recieved_args, recieved_debt);
            }
        }
    }
}

impl EngineQueue {
    pub fn new() -> Self {
        let (sender, receiver) = std::sync::mpsc::channel();
        return EngineQueue {
            queue_in: Arc::new(sender),
            queue_out: Arc::new(std::sync::Mutex::new(receiver)),
        };
    }

    pub async fn enqueu_work(&self, args: EngineArguments) -> DandelionResult<(Context, Recorder)> {
        let (promise, debt) = Promise::new();
        self.queue_in.send((args, debt)).unwrap();
        return *promise.await;
    }
}

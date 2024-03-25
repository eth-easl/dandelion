use crossbeam::channel::TryRecvError;
use dandelion_commons::{records::Recorder, DandelionResult};
use log::error;
use machine_interface::{
    function_driver::{EngineArguments, WorkQueue},
    memory_domain::Context,
    promise::{Debt, Promise},
};

/// Datastructure that implements priority queueing
/// Highest priority queue holds promises if there are any
#[derive(Clone)]
pub struct EngineQueue {
    queue_in: crossbeam::channel::Sender<(EngineArguments, Debt)>,
    queue_out: crossbeam::channel::Receiver<(EngineArguments, Debt)>,
}

/// This is run on the engine so it performs asyncornous access to the local state
impl WorkQueue for EngineQueue {
    fn get_engine_args(&self) -> (EngineArguments, Debt) {
        loop {
            match self.queue_out.try_recv() {
                Err(TryRecvError::Disconnected) => panic!("Work queue disconnected"),
                Err(TryRecvError::Empty) => continue,
                Ok(recieved) => {
                    let (recieved_args, recevied_dept) = recieved;
                    if recevied_dept.is_alive() {
                        return (recieved_args, recevied_dept);
                    }
                }
            }
        }
    }
}

impl EngineQueue {
    pub fn new() -> Self {
        let (sender, receiver) = crossbeam::channel::bounded(4096);
        return EngineQueue {
            queue_in: sender,
            queue_out: receiver,
        };
    }

    pub async fn enqueu_work(&self, args: EngineArguments) -> DandelionResult<(Context, Recorder)> {
        let (promise, debt) = Promise::new();
        match self.queue_in.try_send((args, debt)) {
            Ok(()) => (),
            Err(err) => error!("Failed to enqueu work with error: {:?}", err),
        }
        return *promise.await;
    }
}

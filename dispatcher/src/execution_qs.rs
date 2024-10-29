use crossbeam::channel::{TryRecvError, TrySendError};
use dandelion_commons::{DandelionError, DandelionResult};
use log::error;
use machine_interface::{
    function_driver::{WorkDone, WorkQueue, WorkToDo},
    promise::{Debt, Promise},
};

/// Datastructure that implements priority queueing
/// Highest priority queue holds promises if there are any
#[derive(Clone)]
pub struct EngineQueue {
    queue_in: crossbeam::channel::Sender<(WorkToDo, Debt)>,
    queue_out: crossbeam::channel::Receiver<(WorkToDo, Debt)>,
}

/// This is run on the engine so it performs asyncornous access to the local state
impl WorkQueue for EngineQueue {
    fn clone_box(&self) -> Box<dyn WorkQueue + Send> {
        Box::new(self.clone())
    }
    
    fn get_engine_args(&self) -> (WorkToDo, Debt) {
        loop {
            // match self.queue_out.try_recv() {
            //     Err(TryRecvError::Disconnected) => panic!("Work queue disconnected"),
            //     Err(TryRecvError::Empty) => continue,
            //     Ok(recieved) => {
            //         let (recieved_args, recevied_dept) = recieved;
            //         if recevied_dept.is_alive() {
            //             return (recieved_args, recevied_dept);
            //         }
            //     }
            // }
            match self.queue_out.recv() { // Blocking receive
                Err(_) => panic!("Work queue disconnected"), // Handle disconnection
                Ok((received_args, received_debt)) => {
                    if received_debt.is_alive() {
                        return (received_args, received_debt);
                    }
                }
            }
        }
    }

    fn try_get_engine_args(&self) -> Option<(WorkToDo, Debt)> {
        return match self.queue_out.try_recv() {
            Err(TryRecvError::Disconnected) => panic!("Work queue disconnected"),
            Err(TryRecvError::Empty) => None,
            Ok(received) => {
                let (args, dept) = received;
                if dept.is_alive() {
                    Some((args, dept))
                } else {
                    None
                }
            }
        };
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

    pub async fn enqueu_work(&self, args: WorkToDo) -> DandelionResult<WorkDone> {
        let (promise, debt) = Promise::new();
        match self.queue_in.try_send((args, debt)) {
            Ok(()) => (),
            Err(TrySendError::Disconnected(_)) => {
                error!("Failed to enqueu work, workqueue has been disconnected")
            }
            Err(TrySendError::Full(_)) => return Err(DandelionError::WorkQueueFull),
        }
        return *promise.await;
    }
}

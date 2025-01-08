use core::sync::atomic::{AtomicUsize, Ordering};
use crossbeam::channel::{TryRecvError, TrySendError};
use dandelion_commons::{DandelionError, DandelionResult};
use log::{error, debug};
use machine_interface::{
    function_driver::{WorkDone, WorkQueue, WorkToDo},
    promise::{Debt, PromiseBuffer},
};
use std::{hint, sync::Arc};

const MAX_QUEUE: usize = 4096;

struct AtomicTickets {
    start: AtomicUsize,
    end: AtomicUsize,
}

#[derive(Clone)]
pub struct EngineQueue {
    queue_in: crossbeam::channel::Sender<(WorkToDo, Debt)>,
    queue_out: crossbeam::channel::Receiver<(WorkToDo, Debt)>,
    worker_queue: Arc<AtomicTickets>,
    promise_buffer: PromiseBuffer,
}

/// This is run on the engine so it performs asyncornous access to the local state
impl WorkQueue for EngineQueue {
    fn get_engine_args(&self) -> (WorkToDo, Debt) {
        // make sure only one thread spins on lock and work gets distributed in order of workers getting free
        let local_ticket = self.worker_queue.end.fetch_add(1, Ordering::AcqRel);
        while local_ticket != self.worker_queue.start.load(Ordering::Acquire) {
            hint::spin_loop();
        }
        let work = loop {
            match self.queue_out.try_recv() {
                Err(TryRecvError::Disconnected) => panic!("Work queue disconnected"),
                Err(TryRecvError::Empty) => continue,
                Ok(recieved) => {
                    debug!("Received work");
                    let (recieved_args, recevied_dept) = recieved;
                    if recevied_dept.is_alive() {
                        break (recieved_args, recevied_dept);
                    }
                }
            }
        };
        self.worker_queue.start.fetch_add(1, Ordering::Release);
        return work;
    }

    fn try_get_engine_args(&self) -> Option<(WorkToDo, Debt)> {
        let queue_head = self.worker_queue.start.load(Ordering::Acquire);
        if self
            .worker_queue
            .end
            .compare_exchange(
                queue_head,
                queue_head + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            let try_result = self.queue_out.try_recv();
            self.worker_queue.start.fetch_add(1, Ordering::AcqRel);
            return match try_result {
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
        } else {
            return None;
        }
    }
}

impl EngineQueue {
    pub fn new() -> Self {
        let (sender, receiver) = crossbeam::channel::bounded(MAX_QUEUE);
        let tickets = AtomicTickets {
            start: AtomicUsize::new(0),
            end: AtomicUsize::new(0),
        };
        let queue = Arc::new(tickets);
        return EngineQueue {
            queue_in: sender,
            queue_out: receiver,
            worker_queue: queue,
            promise_buffer: PromiseBuffer::init(MAX_QUEUE),
        };
    }

    pub async fn enqueu_work(&self, args: WorkToDo) -> DandelionResult<WorkDone> {
        let (promise, debt) = self.promise_buffer.get_promise()?;
        match self.queue_in.try_send((args, debt)) {
            Ok(()) => (),
            Err(TrySendError::Disconnected(_)) => {
                error!("Failed to enqueu work, workqueue has been disconnected")
            }
            Err(TrySendError::Full(_)) => return Err(DandelionError::WorkQueueFull),
        }
        return promise.await;
    }
}

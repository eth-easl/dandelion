use dandelion_commons::DandelionResult;
use futures::{
    lock::{Mutex, MutexLockFuture},
    FutureExt,
};
use log::trace;
use machine_interface::{
    function_driver::{EngineWorkQueue, WorkDone, WorkToDo},
    machine_config::EngineType,
    promise::{Debt, PromiseBuffer},
};
use std::{
    collections::LinkedList,
    future::Future,
    sync::Arc,
    task::{Poll, Waker},
};

pub enum QueueFlag {
    EngineReqwestIO = 0b1,
    EngineCheri = 0b10,
    EngineProcess = 0b100,
    EngineKvm = 0b1000,
}

pub fn get_engine_flag(t: EngineType) -> u32 {
    match t {
        #[cfg(feature = "reqwest_io")]
        EngineType::Reqwest => QueueFlag::EngineReqwestIO as u32,
        #[cfg(feature = "cheri")]
        EngineType::Cheri => QueueFlag::EngineCheri as u32,
        #[cfg(feature = "mmu")]
        EngineType::Process => QueueFlag::EngineProcess as u32,
        #[cfg(feature = "kvm")]
        EngineType::Kvm => QueueFlag::EngineKvm as u32,
    }
}

struct QueueElement {
    /// Flags indicating which engines can run this task
    flags: u32,
    /// The WorkToDo content of the queue element
    work: WorkToDo,
    /// The Debt content of the queue element
    debt: Debt,
}

struct WakerElement {
    flags: u32,
    waker: Waker,
}

const MAX_QUEUE: usize = 4096;

/// Producers can push new work to the end of the queue using the `push` function.
/// Consumers can pop elements using the `aquire` function.
#[derive(Clone)]
pub struct WorkQueue {
    // Holds the two queues, first one for work to be done, second one for engines waiting for fitting work to arrive
    queues: Arc<Mutex<(LinkedList<QueueElement>, LinkedList<WakerElement>)>>,
    promise_buffer: PromiseBuffer,
    /// This is used to notify that the number of idle threads has increased or decreased
    add_idle: fn(),
    remove_idle: fn(),
}

struct WaitFuture<'list> {
    flags: u32,
    queues: &'list Arc<Mutex<(LinkedList<QueueElement>, LinkedList<WakerElement>)>>,
    lock: MutexLockFuture<'list, (LinkedList<QueueElement>, LinkedList<WakerElement>)>,
}

impl<'list> WaitFuture<'list> {
    fn new(
        flags: u32,
        queues: &'list Arc<Mutex<(LinkedList<QueueElement>, LinkedList<WakerElement>)>>,
    ) -> WaitFuture<'list> {
        Self {
            flags,
            queues,
            lock: queues.lock(),
        }
    }
}

impl Future for WaitFuture<'_> {
    type Output = (WorkToDo, Debt);

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        // check if there is a lock option and if so if it is ready
        if let Poll::Ready(mut lock_guard) = self.lock.poll_unpin(cx) {
            // check if there is any work with the flags we are looking for
            let result = lock_guard
                .0
                .extract_if(|queue_element| queue_element.flags & self.flags == self.flags)
                .next()
                .map(|queue_element| (queue_element.work, queue_element.debt));
            if let Some(result_tupple) = result {
                // Found some work, so core is not idle
                // (self.remove_idle)();
                Poll::Ready(result_tupple)
            } else {
                // Did not find any work, so need to add to waker queue
                let waker_element = WakerElement {
                    flags: self.flags,
                    waker: cx.waker().clone(),
                };
                lock_guard.1.push_back(waker_element);
                // lock was ready once, need to set new one
                self.lock = self.queues.lock();
                Poll::Pending
            }
        } else {
            Poll::Pending
        }
    }
}

impl WorkQueue {
    /// Creates a new WorkQueue of given size.
    pub fn init(add_idle: fn(), remove_idle: fn()) -> Self {
        WorkQueue {
            queues: Arc::new(Mutex::new((LinkedList::new(), LinkedList::new()))),
            promise_buffer: PromiseBuffer::init(MAX_QUEUE),
            add_idle,
            remove_idle,
        }
    }

    /// Pushes the work and debt to the back of the queue and sets the flags accordingly.
    /// Returns an error if the queue is full.
    /// TODO: check or define here and other places, if the flags need to match fully, just checking that any flag is set would be enough
    async fn push(&self, work: WorkToDo, debt: Debt, flags: u32) {
        let mut queue_guard = self.queues.lock().await;
        queue_guard.0.push_back(QueueElement { flags, work, debt });
        // call first waker with matching flags if there are any
        if let Some(waker_to_call) = queue_guard
            .1
            .extract_if(|queue_element| queue_element.flags & flags == flags)
            .next()
        {
            waker_to_call.waker.wake();
        }
    }

    /// Inserts the work into the queue setting the flags according to the supported engines and
    /// awaits the future before returning the result.
    pub async fn do_work(&self, work: WorkToDo) -> DandelionResult<WorkDone> {
        let flags = match &work {
            WorkToDo::Shutdown(engine_type) => get_engine_flag(*engine_type),
            WorkToDo::FunctionArguments {
                function_id: _,
                function_alternatives,
                input_sets: _,
                metadata: _,
                caching: _,
                recorder: _,
            } => {
                let mut flags = 0;
                trace!(
                    "found function arguments with alternatives: {:?}",
                    function_alternatives
                );
                for alternative in function_alternatives {
                    flags |= get_engine_flag(alternative.engine);
                }
                flags
            }
        };
        log::trace!("Enqueueing with flags: {}", flags);

        let (promise, debt) = self.promise_buffer.get_promise()?;
        self.push(work, debt, flags).await;

        return promise.await;
    }

    /// Tries to acquire some work that matches the given flags starting from the head of the queue.
    pub fn try_get_work(&self, engine_flags: u32) -> Option<(WorkToDo, Debt)> {
        // May want to try lock here too, instead of lock, but since we have the ticket, should always succeed on locking
        self.queues.try_lock().and_then(|mut guard| {
            guard
                .0
                .extract_if(|queue_element| queue_element.flags & engine_flags == engine_flags)
                .next()
                .map(|queue_element| (queue_element.work, queue_element.debt))
        })
    }

    /// Spins on the queue until it manages to acquire some work that matches the given flags.
    pub async fn get_work(&self, engine_flags: u32) -> (WorkToDo, Debt) {
        // try to get work, if there is none, insert self into waker and try again
        WaitFuture::new(engine_flags, &self.queues).await
    }
}

/// Engine specific wrapper for the `WorkQueue` that implements the `EngineWorkQueue` trait.
#[derive(Clone)]
pub struct EngineQueue {
    work_queue: WorkQueue,
    engine_flags: u32,
}

impl EngineQueue {
    /// Wraps the work queue and specialized it for the given engine type.
    pub fn init(work_queue: WorkQueue, engine_type: EngineType) -> Self {
        EngineQueue {
            work_queue,
            engine_flags: get_engine_flag(engine_type),
        }
    }
}

impl EngineWorkQueue for EngineQueue {
    fn get_engine_args(
        &self,
    ) -> impl Future<Output = (WorkToDo, machine_interface::promise::Debt)> {
        self.work_queue.get_work(self.engine_flags)
    }

    fn try_get_engine_args(&self) -> Option<(WorkToDo, machine_interface::promise::Debt)> {
        self.work_queue.try_get_work(self.engine_flags)
    }
}

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
use tokio::sync::{watch, Notify};

pub enum QueueFlag {
    EngineSystem = 0b1,
    EngineCheri = 0b10,
    EngineProcess = 0b100,
    EngineKvm = 0b1000,
}

pub fn get_engine_flag(t: EngineType) -> u32 {
    match t {
        #[cfg(feature = "reqwest_io")]
        EngineType::System => QueueFlag::EngineSystem as u32,
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

struct InnerQueue {
    /// Queueu holding work for which some data still needs to be fetched
    compute_queue: LinkedList<QueueElement>,
    /// Queueu holding work for which all data is local
    io_queue: LinkedList<QueueElement>,
    /// List of compute engines which are idle
    compute_waker_list: LinkedList<WakerElement>,
    /// List of io engines ready to take more work
    io_waker_list: LinkedList<Waker>,
}

const MAX_QUEUE: usize = 4096;

/// Producers can push new work to the end of the queue using the `push` function.
/// Consumers can pop elements using the `aquire` function.
#[derive(Clone)]
pub struct WorkQueue {
    /// Holds the two queues, first one for work ready to be run locally, second one for engines waiting for fitting work to arrive
    inner: Arc<Mutex<InnerQueue>>,
    promise_buffer: PromiseBuffer,
    /// Used to keep track of idle cores
    idle_sender: watch::Sender<u32>,
    idle_receiver: watch::Receiver<u32>,
    /// Notifier to send out notification, that idle resource count changed
    /// Notifier to send out notification that queueing is happening
    queuing_notifier: Arc<Notify>,
}

struct ComputeWaitFuture<'queue> {
    flags: u32,
    work_queue: &'queue WorkQueue,
    lock: MutexLockFuture<'queue, InnerQueue>,
    was_set_idle: bool,
}

impl<'list> ComputeWaitFuture<'list> {
    fn new(flags: u32, work_queue: &'list WorkQueue) -> ComputeWaitFuture<'list> {
        Self {
            flags,
            work_queue,
            lock: work_queue.inner.lock(),
            was_set_idle: false,
        }
    }
}

impl Future for ComputeWaitFuture<'_> {
    type Output = (WorkToDo, Debt);

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        // check if there is a lock option and if so if it is ready
        if let Poll::Ready(mut lock_guard) = self.lock.poll_unpin(cx) {
            // check if there is any work with the flags we are looking for
            let result = lock_guard
                .compute_queue
                .extract_if(|queue_element| queue_element.flags & self.flags != 0)
                .next()
                .map(|queue_element| (queue_element.work, queue_element.debt));
            if let Some(result_tupple) = result {
                // Found some work, so core is not idle
                if self.was_set_idle {
                    self.work_queue.idle_sender.send_modify(|idle| *idle -= 1);
                }
                Poll::Ready(result_tupple)
            } else {
                // Did not find any work, so need to add to waker queue
                let waker_element = WakerElement {
                    flags: self.flags,
                    waker: cx.waker().clone(),
                };
                if !self.was_set_idle {
                    self.was_set_idle = true;
                    self.work_queue.idle_sender.send_modify(|idle| *idle += 1);
                }
                lock_guard.compute_waker_list.push_back(waker_element);
                // lock was ready once, need to set new one
                self.lock = self.work_queue.inner.lock();
                Poll::Pending
            }
        } else {
            Poll::Pending
        }
    }
}

struct IoWaitFuture<'queue> {
    work_queue: &'queue WorkQueue,
    lock: MutexLockFuture<'queue, InnerQueue>,
}

impl<'list> IoWaitFuture<'list> {
    fn new(work_queue: &'list WorkQueue) -> IoWaitFuture<'list> {
        Self {
            work_queue,
            lock: work_queue.inner.lock(),
        }
    }
}

impl Future for IoWaitFuture<'_> {
    type Output = (WorkToDo, Debt);

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        // check if there is a lock option and if so if it is ready
        if let Poll::Ready(mut lock_guard) = self.lock.poll_unpin(cx) {
            // take first one, since we don't care about the flags, only to resolve the reference sets
            let result = lock_guard
                .io_queue
                .pop_front()
                .map(|queue_element| (queue_element.work, queue_element.debt));
            if let Some(result_tupple) = result {
                // Found some work, so core is not idle
                Poll::Ready(result_tupple)
            } else {
                // Did not find any work, so need to add to waker queue
                lock_guard.io_waker_list.push_back(cx.waker().clone());
                // lock was ready once, need to set new one
                self.lock = self.work_queue.inner.lock();
                Poll::Pending
            }
        } else {
            Poll::Pending
        }
    }
}

impl WorkQueue {
    /// Creates a new WorkQueue of given size.
    pub fn init() -> Self {
        let (idle_sender, idle_receiver) = watch::channel(0);
        WorkQueue {
            inner: Arc::new(Mutex::new(InnerQueue {
                compute_queue: LinkedList::new(),
                io_queue: LinkedList::new(),
                compute_waker_list: LinkedList::new(),
                io_waker_list: LinkedList::new(),
            })),
            promise_buffer: PromiseBuffer::init(MAX_QUEUE),
            idle_sender,
            idle_receiver,
            queuing_notifier: Arc::new(Notify::new()),
        }
    }

    pub fn idle_watcher(&self) -> watch::Receiver<u32> {
        self.idle_receiver.clone()
    }

    pub fn queueing_notifier(&self) -> Arc<Notify> {
        self.queuing_notifier.clone()
    }

    /// Pushes the work and debt to the back of the queue and sets the flags accordingly.
    /// Returns an error if the queue is full.
    /// TODO: check or define here and other places, if the flags need to match fully, just checking that any flag is set would be enough
    async fn push_compute(&self, work: WorkToDo, debt: Debt, flags: u32) {
        let mut queue_guard = self.inner.lock().await;
        queue_guard
            .compute_queue
            .push_back(QueueElement { flags, work, debt });
        // call first waker with matching flags if there are any
        if let Some(waker_to_call) = queue_guard
            .compute_waker_list
            .extract_if(|queue_element| queue_element.flags & flags == flags)
            .next()
        {
            waker_to_call.waker.wake();
        } else {
            self.queuing_notifier.notify_waiters();
        }
    }

    async fn push_io(&self, work: WorkToDo, debt: Debt, flags: u32) {
        let mut queue_guard = self.inner.lock().await;
        queue_guard
            .io_queue
            .push_back(QueueElement { flags, work, debt });
        // call first waker with matching flags if there are any
        if let Some(waker_to_call) = queue_guard.io_waker_list.pop_front() {
            waker_to_call.wake();
        } else {
            self.queuing_notifier.notify_waiters();
        }
    }

    /// Inserts the work into the queue setting the flags according to the supported engines and
    /// awaits the future before returning the result.
    pub async fn do_work(&self, work: WorkToDo) -> DandelionResult<WorkDone> {
        let (flags, local) = match &work {
            WorkToDo::Shutdown(engine_type) => (get_engine_flag(*engine_type), true),
            WorkToDo::SetsToResolve { input_sets: _ } => (0, false),
            WorkToDo::FunctionArguments {
                function_id: _,
                function_alternatives,
                input_sets,
                metadata: _,
                caching: _,
                recorder: _,
            } => {
                // check if all the sets are already fully locally available
                let local = input_sets.into_iter().all(|set_option| {
                    if let Some(set) = set_option {
                        set.is_local()
                    } else {
                        true
                    }
                });
                let mut flags = 0;
                trace!(
                    "found function arguments with alternatives: {:?}",
                    function_alternatives
                );
                for alternative in function_alternatives {
                    flags |= get_engine_flag(alternative.engine);
                }
                // check if system flag is set, if so, put in system queue
                (flags, local)
            }
        };
        log::trace!(
            "Enqueueing function with all local data {}, with engine flags: {}",
            local,
            flags
        );

        let (promise, debt) = self.promise_buffer.get_promise()?;
        if local {
            self.push_compute(work, debt, flags).await;
        } else {
            self.push_io(work, debt, flags).await;
        }
        return promise.await;
    }

    /// Tries to acquire some work that matches the given flags starting from the head of the queue.
    /// Ignores shutdown and fetch work, since that only makes sense to execute locally
    pub fn try_get_work_for_remote(&self, engine_flags: u32) -> Option<(WorkToDo, Debt)> {
        if let Some(mut lock) = self.inner.try_lock() {
            // first check the queue with unresolved references, since those are easier to steal
            if let Some(work_with_references) = lock
                .io_queue
                .extract_if(|queue_element| {
                    if let WorkToDo::FunctionArguments {
                        function_id: _,
                        function_alternatives: _,
                        input_sets: _,
                        metadata: _,
                        caching: _,
                        recorder: _,
                    } = &queue_element.work
                    {
                        queue_element.flags & engine_flags != 0
                    } else {
                        false
                    }
                })
                .next()
            {
                return Some((work_with_references.work, work_with_references.debt));
            }
            // did not find any work in the io_queue so check compute queue
            if let Some(local_work) = lock
                .compute_queue
                .extract_if(|queue_element| {
                    if let WorkToDo::FunctionArguments {
                        function_id: _,
                        function_alternatives: _,
                        input_sets: _,
                        metadata: _,
                        caching: _,
                        recorder: _,
                    } = &queue_element.work
                    {
                        queue_element.flags & engine_flags != 0
                    } else {
                        false
                    }
                })
                .next()
            {
                return Some((local_work.work, local_work.debt));
            }
        }
        None
    }

    /// Spins on the queue until it manages to acquire some work that matches the given flags.
    pub async fn get_compute_work(&self, engine_flags: u32) -> (WorkToDo, Debt) {
        // try to get work, if there is none, insert self into waker and try again
        ComputeWaitFuture::new(engine_flags, &self).await
    }

    pub async fn get_io_work(&self) -> (WorkToDo, Debt) {
        // try to get work, if there is none, insert self into waker and try again
        IoWaitFuture::new(&self).await
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
    fn get_compute_engine_args(
        &self,
    ) -> impl Future<Output = (WorkToDo, machine_interface::promise::Debt)> {
        self.work_queue.get_compute_work(self.engine_flags)
    }

    fn get_io_engine_args(
        &self,
    ) -> impl Future<Output = (WorkToDo, machine_interface::promise::Debt)> {
        self.work_queue.get_io_work()
    }

    fn requeu_engine_args(&self, work: WorkToDo, debt: Debt) -> impl Future<Output = ()> {
        let flags = match &work {
            WorkToDo::FunctionArguments {
                function_id: _,
                function_alternatives,
                input_sets: _,
                metadata: _,
                caching: _,
                recorder: _,
            } => function_alternatives
                .iter()
                .fold(0, |flags, alt| flags | get_engine_flag(alt.engine)),
            _ => panic!("Should not reenqueue non function arguments"),
        };
        trace!("Reenqueue with flags: {}", flags);
        self.work_queue.push_compute(work, debt, flags)
    }
}

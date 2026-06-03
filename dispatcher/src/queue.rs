use dandelion_commons::{
    err_dandelion, DandelionError, DandelionResult, DispatcherError, RequestCancellation,
};
use futures::{
    lock::{Mutex, MutexLockFuture},
    FutureExt,
};
use log::{debug, trace};
use machine_interface::{
    composition::SystemInfo,
    function_driver::{EngineWorkQueue, WorkDone, WorkToDo},
    machine_config::EngineType,
    promise::{Debt, PromiseBuffer},
};
use std::{
    collections::LinkedList,
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Poll, Waker},
};
use tokio::sync::{watch, Notify};

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
    /// Cancellation token for the queued request.
    cancellation: Option<RequestCancellation>,
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
    /// Holds the two queues, first one for work to be done, second one for engines waiting for fitting work to arrive
    queues: Arc<Mutex<(LinkedList<QueueElement>, LinkedList<WakerElement>)>>,
    promise_buffer: PromiseBuffer,
    /// Used to keep track of idle cores
    idle_sender: watch::Sender<u32>,
    idle_receiver: watch::Receiver<u32>,
    /// Notifier to send out notification, that idle resource count changed
    /// Notifier to send out notification that queueing is happening
    queuing_notifier: Arc<Notify>,
    /// Tracks current system informations used by the any sharding policy.
    pub system_info: Arc<SystemInfo>,
}

struct WaitFuture<'queue> {
    flags: u32,
    work_queue: &'queue WorkQueue,
    lock: MutexLockFuture<'queue, (LinkedList<QueueElement>, LinkedList<WakerElement>)>,
    was_set_idle: bool,
}

impl<'list> WaitFuture<'list> {
    fn new(flags: u32, work_queue: &'list WorkQueue) -> WaitFuture<'list> {
        Self {
            flags,
            work_queue,
            lock: work_queue.queues.lock(),
            was_set_idle: false,
        }
    }
}

fn fulfill_cancelled_work(cancelled_work: Vec<QueueElement>) {
    if !cancelled_work.is_empty() {
        debug!("Cancelling {} queued work items", cancelled_work.len());
    }
    for queue_element in cancelled_work {
        queue_element
            .debt
            .fulfill(err_dandelion!(DandelionError::Dispatcher(
                DispatcherError::Cancelled,
            )));
    }
}

fn take_cancelled_work(queue: &mut LinkedList<QueueElement>) -> Vec<QueueElement> {
    queue
        .extract_if(|queue_element| {
            queue_element
                .cancellation
                .as_ref()
                .is_some_and(|cancellation| cancellation.is_cancelled())
        })
        .collect()
}

impl Future for WaitFuture<'_> {
    type Output = (WorkToDo, Debt);

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        // check if there is a lock option and if so if it is ready
        if let Poll::Ready(mut lock_guard) = self.lock.poll_unpin(cx) {
            fulfill_cancelled_work(take_cancelled_work(&mut lock_guard.0));
            // check if there is any work with the flags we are looking for
            let result = lock_guard
                .0
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
                lock_guard.1.push_back(waker_element);
                // lock was ready once, need to set new one
                self.lock = self.work_queue.queues.lock();
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
        let (num_local_cores_sender, num_local_cores_watcher) = watch::channel(0);
        WorkQueue {
            queues: Arc::new(Mutex::new((LinkedList::new(), LinkedList::new()))),
            promise_buffer: PromiseBuffer::init(MAX_QUEUE),
            idle_sender,
            idle_receiver,
            queuing_notifier: Arc::new(Notify::new()),
            system_info: Arc::new(SystemInfo {
                num_local_cores_watcher,
                num_local_cores_sender,
                num_remote_cores: AtomicUsize::new(0),
            }),
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
    async fn push(
        &self,
        work: WorkToDo,
        debt: Debt,
        flags: u32,
        cancellation: Option<RequestCancellation>,
    ) {
        let mut queue_guard = self.queues.lock().await;
        queue_guard.0.push_back(QueueElement {
            flags,
            work,
            debt,
            cancellation,
        });
        // call first waker with matching flags if there are any
        if let Some(waker_to_call) = queue_guard
            .1
            .extract_if(|queue_element| queue_element.flags & flags == flags)
            .next()
        {
            waker_to_call.waker.wake();
        } else {
            self.queuing_notifier.notify_waiters();
        }
    }

    /// Inserts the work into the queue setting the flags according to the supported engines and
    /// awaits the future before returning the result.
    pub async fn do_work(&self, work: WorkToDo) -> DandelionResult<WorkDone> {
        self.do_work_cancellable(work, None).await
    }

    /// Inserts the work into the queue setting the flags according to the supported engines and
    /// awaits the future before returning the result.
    pub async fn do_work_cancellable(
        &self,
        work: WorkToDo,
        cancellation: Option<RequestCancellation>,
    ) -> DandelionResult<WorkDone> {
        if cancellation.as_ref().is_some_and(|c| c.is_cancelled()) {
            return err_dandelion!(DandelionError::Dispatcher(
                DispatcherError::Cancelled,
            ));
        }
        let flags = match &work {
            WorkToDo::Shutdown(engine_type) => get_engine_flag(*engine_type),
            WorkToDo::FunctionArguments {
                function_id: _,
                function_alternatives,
                input_sets: _,
                metadata: _,
                caching: _,
                recorder: _,
                cancellation: _,
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
        self.push(work, debt, flags, cancellation).await;
        return promise.await;
    }

    /// Tries to acquire some work that matches the given flags starting from the head of the queue.
    pub fn try_get_work(&self, engine_flags: u32) -> Option<(WorkToDo, Debt)> {
        // May want to try lock here too, instead of lock, but since we have the ticket, should always succeed on locking
        self.queues.try_lock().and_then(|mut guard| {
            fulfill_cancelled_work(take_cancelled_work(&mut guard.0));
            guard
                .0
                .extract_if(|queue_element| queue_element.flags & engine_flags != 0)
                .next()
                .map(|queue_element| (queue_element.work, queue_element.debt))
        })
    }

    /// Tries to acquire some work that matches the given flags starting from the head of the queue.
    /// Ignores shutdown (to use for remote nodes for example)
    pub fn try_get_work_no_shutdown(&self, engine_flags: u32) -> Option<(WorkToDo, Debt)> {
        // May want to try lock here too, instead of lock, but since we have the ticket, should always succeed on locking
        self.queues.try_lock().and_then(|mut guard| {
            fulfill_cancelled_work(take_cancelled_work(&mut guard.0));
            guard
                .0
                .extract_if(|queue_element| {
                    if let WorkToDo::Shutdown(_) = queue_element.work {
                        false
                    } else {
                        queue_element.flags & engine_flags != 0
                    }
                })
                .next()
                .map(|queue_element| (queue_element.work, queue_element.debt))
        })
    }

    /// Spins on the queue until it manages to acquire some work that matches the given flags.
    pub async fn get_work(&self, engine_flags: u32) -> (WorkToDo, Debt) {
        // try to get work, if there is none, insert self into waker and try again
        WaitFuture::new(engine_flags, &self).await
    }

    /// Increases the number of local cores.
    pub fn add_local_cores(&self, num_cores: usize) {
        self.system_info.num_local_cores_sender.send_modify(|curr| {
            trace!(
                "Added {} local core(s). New number of local cores: {}",
                num_cores,
                *curr + num_cores
            );
            *curr += num_cores
        });
    }

    /// Decreases the number of local cores.
    pub fn remove_local_cores(&self, num_cores: usize) -> DandelionResult<()> {
        match !self
            .system_info
            .num_local_cores_sender
            .send_if_modified(|curr| {
                if *curr < num_cores {
                    false
                } else {
                    *curr -= num_cores;
                    trace!(
                        "Removed {} local core(s). New number of local cores: {}",
                        num_cores,
                        *curr - num_cores
                    );
                    true
                }
            }) {
            false => err_dandelion!(DandelionError::Dispatcher(
                DispatcherError::InvalidSytemInformation
            )),
            true => Ok(()),
        }
    }

    /// Increases the number of local cores.
    pub fn add_remote_cores(&self, num_cores: usize) {
        let prev_num_cores = self
            .system_info
            .num_remote_cores
            .fetch_add(num_cores, Ordering::AcqRel);
        trace!(
            "Added {} remote core(s). New number of remote cores: {}",
            num_cores,
            prev_num_cores + num_cores
        );
    }

    /// Decreases the number of local cores.
    pub fn remove_remote_cores(&self, num_cores: usize) -> DandelionResult<()> {
        let mut curr_remote_cores = self.system_info.num_remote_cores.load(Ordering::Acquire);
        loop {
            if curr_remote_cores < num_cores {
                return err_dandelion!(DandelionError::Dispatcher(
                    DispatcherError::InvalidSytemInformation
                ));
            }
            let new_val = curr_remote_cores - num_cores;
            match self.system_info.num_remote_cores.compare_exchange(
                curr_remote_cores,
                new_val,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(val) => curr_remote_cores = val,
            }
        }
        trace!(
            "Removed {} remote core(s). New number of remote cores: {}",
            num_cores,
            curr_remote_cores + num_cores
        );
        Ok(())
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

    fn remove_self_from_queue(&self) {
        self.work_queue
            .remove_local_cores(1)
            .expect("Failed to remove itself from the work queue.");
    }
}

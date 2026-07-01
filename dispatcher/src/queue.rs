mod policy;
pub use policy::PREFETCH_PER_CORE;

use dandelion_commons::{
    err_dandelion, records::RecordPoint, DandelionError, DandelionResult, DispatcherError,
};
use log::trace;
use machine_interface::{
    composition::SystemInfo,
    function_driver::{EngineWorkQueue, WorkDone, WorkToDo},
    machine_config::EngineType,
    promise::{Debt, PromiseBuffer},
};
use std::{
    collections::{BTreeMap, LinkedList},
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::{Poll, Waker},
};
use tokio::sync::{mpsc, watch, Notify};

pub enum QueueFlag {
    EngineSystem = 0b1,
    EngineCheri = 0b10,
    EngineProcess = 0b100,
    EngineKvm = 0b1000,
}

pub fn get_engine_flag(t: EngineType) -> u32 {
    match t {
        EngineType::System => QueueFlag::EngineSystem as u32,
        #[cfg(feature = "cheri")]
        EngineType::Cheri => QueueFlag::EngineCheri as u32,
        #[cfg(feature = "mmu")]
        EngineType::Process => QueueFlag::EngineProcess as u32,
        #[cfg(feature = "kvm")]
        EngineType::Kvm => QueueFlag::EngineKvm as u32,
    }
}

struct ComputeQueueElement {
    /// ID of the composition this work belongs to.
    composition_id: usize,
    /// Flags indicating which engines can run this task
    flags: u32,
    /// The WorkToDo content of the queue element
    work: WorkToDo,
    /// The Debt content of the queue element
    debt: Debt,
}

struct IoQueueElement {
    /// ID of the composition this work belongs to.
    composition_id: usize,
    /// Flags indicating which engines can run this task
    flags: u32,
    /// The WorkToDo content of the queue element
    work: WorkToDo,
    /// The Debt content of the queue element
    debt: Debt,
    /// Policy-specific metadata for this element
    policy_data: policy::IOElementData,
}

struct WakerElement {
    flags: u32,
    waker: Waker,
}

struct InnerQueue {
    /// Queue holding work for which some data still needs to be fetched
    compute_queue: LinkedList<ComputeQueueElement>,
    /// Queue holding work for which all data is local
    io_queue: LinkedList<IoQueueElement>,
    /// List of compute engines which are idle
    compute_waker_list: LinkedList<WakerElement>,
    /// List of io engines ready to take more work
    io_waker_list: LinkedList<Waker>,
    /// The number of functions for which we are currently prefetching
    prefetching_in_progress: usize,
}

const MAX_QUEUE: usize = 4096;

/// Producers can push new work to the end of the queue using the `push` function.
/// Consumers can pop elements using the `aquire` function.
/// TODO: move all things behind Arcs into a single one.
#[derive(Clone)]
pub struct WorkQueue {
    /// Holds the two queues, first one for work ready to be run locally, second one for engines waiting for fitting work to arrive
    inner: Arc<Mutex<InnerQueue>>,
    promise_buffer: PromiseBuffer,
    /// Notifier to indicate that the number of idle cores has changed
    idle_notifier: Arc<Notify>,
    /// Notifier to send out notification that queueing is happening
    queuing_notifier: Arc<Notify>,
    /// Tracks current system informations used by the any sharding policy.
    pub system_info: Arc<SystemInfo>,
    /// Channels for asking remote node to take work
    remote_nodes: Arc<Mutex<BTreeMap<u64, mpsc::UnboundedSender<(WorkToDo, Debt, usize)>>>>,
}

struct ComputeWaitFuture<'queue> {
    was_idle: bool,
    flags: u32,
    work_queue: &'queue WorkQueue,
}

impl<'list> ComputeWaitFuture<'list> {
    fn new(flags: u32, work_queue: &'list WorkQueue) -> ComputeWaitFuture<'list> {
        Self {
            was_idle: false,
            flags,
            work_queue,
        }
    }
}

impl Future for ComputeWaitFuture<'_> {
    type Output = (WorkToDo, Debt);

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        let mut lock_guard = self.work_queue.inner.lock().unwrap();
        // check if there is any work with the flags we are looking for
        let result = lock_guard
            .compute_queue
            .extract_if(|queue_element| queue_element.flags & self.flags != 0)
            .next()
            .map(|queue_element| (queue_element.work, queue_element.debt));
        if let Some(mut result_tupple) = result {
            // Poke the IO queue in case they were waiting for space to produce more results
            if let Some(waker) = lock_guard.io_waker_list.pop_front() {
                waker.wake();
            }

            if let WorkToDo::FunctionArguments {
                function_id: _,
                function_alternatives: _,
                input_sets: _,
                metadata: _,
                caching: _,
                recorder,
            } = &mut result_tupple.0
            {
                recorder.record(RecordPoint::ComputeQueueEnd);
            }
            if self.was_idle {
                self.work_queue.idle_notifier.notify_waiters();
            }
            Poll::Ready(result_tupple)
        } else {
            // Did not find any work, so need to add to waker queue
            let waker_element = WakerElement {
                flags: self.flags,
                waker: cx.waker().clone(),
            };
            lock_guard.compute_waker_list.push_back(waker_element);
            // have newly become idle
            if !self.was_idle {
                self.work_queue.idle_notifier.notify_waiters();
                self.was_idle = true;
            }
            // lock was ready once, need to set new one
            Poll::Pending
        }
    }
}

struct IoWaitFuture<'queue> {
    work_queue: &'queue WorkQueue,
}

impl<'list> IoWaitFuture<'list> {
    fn new(work_queue: &'list WorkQueue) -> IoWaitFuture<'list> {
        Self { work_queue }
    }
}

impl Future for IoWaitFuture<'_> {
    type Output = (WorkToDo, Debt, Option<usize>);

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let mut lock_guard = self.work_queue.inner.lock().unwrap();
        // Always take work that is not FunctionArguments.
        // Take FunctionArguments only if the local queue is smaller than a certain threshold.
        // Do this to avoid overloading the IO cores with too much parallel fetching and to make it easier for remotes
        // to find work where they can do their own fetching instead.
        let compute_pending = lock_guard.compute_queue.len();
        let local_cores = *self.work_queue.system_info.num_local_cores_watcher.borrow();
        let active_fetch_count = lock_guard.prefetching_in_progress;
        let idle_compute_cores = lock_guard.compute_waker_list.len();
        let mut is_prefetching = false;
        let result = lock_guard
            .io_queue
            .extract_if(|queue_element| match &queue_element.work {
                WorkToDo::FunctionArguments { .. } => {
                    is_prefetching = true;
                    policy::should_io_take(
                        &queue_element.policy_data,
                        compute_pending,
                        active_fetch_count,
                        local_cores,
                        idle_compute_cores,
                    )
                }
                _ => {
                    is_prefetching = false;
                    true
                }
            })
            .next()
            .map(|queue_element| {
                (
                    queue_element.work,
                    queue_element.debt,
                    queue_element.composition_id,
                )
            });
        // If the task is a prefetching task increase the counter accordingly
        if is_prefetching {
            lock_guard.prefetching_in_progress += 1;
        }
        if let Some((mut work, debt, composition_id)) = result {
            let composition_id_option =
                if let WorkToDo::FunctionArguments { recorder, .. } = &mut work {
                    recorder.record(RecordPoint::IOQueueEnd);
                    Some(composition_id)
                } else {
                    None
                };

            // Found some work, so core is not idle
            Poll::Ready((work, debt, composition_id_option))
        } else {
            // Did not find any work, so need to add to waker queue
            lock_guard.io_waker_list.push_back(cx.waker().clone());
            Poll::Pending
        }
    }
}

impl WorkQueue {
    /// Creates a new WorkQueue of given size.
    pub fn init() -> Self {
        let (num_local_cores_sender, num_local_cores_watcher) = watch::channel(0);
        WorkQueue {
            inner: Arc::new(Mutex::new(InnerQueue {
                compute_queue: LinkedList::new(),
                io_queue: LinkedList::new(),
                compute_waker_list: LinkedList::new(),
                io_waker_list: LinkedList::new(),
                prefetching_in_progress: 0,
            })),
            promise_buffer: PromiseBuffer::init(MAX_QUEUE),
            idle_notifier: Arc::new(Notify::new()),
            queuing_notifier: Arc::new(Notify::new()),
            system_info: Arc::new(SystemInfo {
                num_local_cores_watcher,
                num_local_cores_sender,
                num_remote_cores: AtomicUsize::new(0),
            }),
            remote_nodes: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn queueing_notifier(&self) -> Arc<Notify> {
        self.queuing_notifier.clone()
    }

    pub fn idle_notifier(&self) -> Arc<Notify> {
        self.idle_notifier.clone()
    }

    pub fn idle_cores(&self) -> usize {
        self.inner.lock().unwrap().compute_waker_list.len()
    }

    /// Pushes the work and debt to the back of the queue and sets the flags accordingly.
    /// Returns an error if the queue is full.
    /// TODO: check or define here and other places, if the flags need to match fully, just checking that any flag is set would be enough
    fn push_compute(
        &self,
        mut work: WorkToDo,
        debt: Debt,
        flags: u32,
        composition_id: usize,
        had_fetching: bool,
    ) {
        if let WorkToDo::FunctionArguments {
            function_id: _,
            function_alternatives: _,
            input_sets: _,
            metadata: _,
            caching: _,
            recorder,
        } = &mut work
        {
            recorder.record(RecordPoint::ComputeQueueStart);
        }

        let mut queue_guard = self.inner.lock().unwrap();
        if had_fetching {
            queue_guard.prefetching_in_progress -= 1;
        }

        let new_element = ComputeQueueElement {
            composition_id,
            flags,
            work,
            debt,
        };
        // if list empty or the back has the same or a smaller id just push to the back
        if queue_guard.compute_queue.is_empty()
            || queue_guard.compute_queue.back().unwrap().composition_id <= composition_id
        {
            queue_guard.compute_queue.push_back(new_element);
        // check the front has same or bigger id so can just push front
        } else if queue_guard.compute_queue.front().unwrap().composition_id >= composition_id {
            queue_guard.compute_queue.push_front(new_element);
        // find the place to insert
        } else {
            let index = queue_guard
                .compute_queue
                .iter()
                .position(|elem| elem.composition_id > composition_id)
                .unwrap();
            let mut tail = queue_guard.compute_queue.split_off(index);
            queue_guard.compute_queue.push_back(new_element);
            queue_guard.compute_queue.append(&mut tail);
        }

        // call first waker with matching flags if there are any
        if let Some(waker_to_call) = queue_guard
            .compute_waker_list
            .extract_if(|queue_element| queue_element.flags & flags == flags)
            .next()
        {
            log::trace!("Notifying one waker");
            waker_to_call.waker.wake();
        } else {
            self.queuing_notifier.notify_waiters();
        }
    }

    fn push_io(
        &self,
        mut work: WorkToDo,
        debt: Debt,
        flags: u32,
        composition_id: usize,
        try_offload: bool,
    ) {
        if let WorkToDo::FunctionArguments {
            function_id: _,
            function_alternatives: _,
            input_sets: _,
            metadata: _,
            caching: _,
            recorder,
        } = &mut work
        {
            recorder.record(RecordPoint::IOQueueStart);
        }

        // Policy: compute element metadata and optionally offload to a remote node
        let Some((work, debt, policy_data)) =
            policy::prepare_io_element(work, debt, try_offload, composition_id, &self.remote_nodes)
        else {
            // if prepare_io_element returned None it has already offloaded the work to a remote
            return;
        };

        let mut queue_guard = self.inner.lock().unwrap();
        let new_element = IoQueueElement {
            composition_id,
            flags,
            work,
            debt,
            policy_data,
        };
        // check if an io core would take the element or not;
        // if not, no reason to call waker
        let compute_length = queue_guard.compute_queue.len();
        let local_cores = *self.system_info.num_local_cores_watcher.borrow();
        let already_fetching = queue_guard.prefetching_in_progress;
        let idle_compute_cores = queue_guard.compute_waker_list.len();
        let would_process = match &new_element.work {
            WorkToDo::FunctionArguments { .. } => policy::should_io_take(
                &new_element.policy_data,
                compute_length,
                already_fetching,
                local_cores,
                idle_compute_cores,
            ),
            _ => true,
        };

        // if list empty or the back has the same or a smaller id just push to the back
        if queue_guard.io_queue.is_empty()
            || queue_guard.io_queue.back().unwrap().composition_id <= composition_id
        {
            queue_guard.io_queue.push_back(new_element);
        // check the front has same or smaller id so can just push front
        } else if queue_guard.io_queue.front().unwrap().composition_id >= composition_id {
            queue_guard.io_queue.push_front(new_element);
        // find the place to insert
        } else {
            let index = queue_guard
                .io_queue
                .iter()
                .position(|elem| elem.composition_id > composition_id)
                .unwrap();
            let mut tail = queue_guard.io_queue.split_off(index);
            queue_guard.io_queue.push_back(new_element);
            queue_guard.io_queue.append(&mut tail);
        }

        // call first waker with matching flags if there are any
        // only call waker if we know the core wants to take this element
        if !queue_guard.io_waker_list.is_empty() && would_process {
            queue_guard.io_waker_list.pop_front().unwrap().wake();
        } else {
            self.queuing_notifier.notify_waiters();
        }
    }

    fn push(&self, work: WorkToDo, debt: Debt, composition_id: usize, try_offload: bool) {
        let (flags, local) = match &work {
            WorkToDo::Shutdown(engine_type) => (get_engine_flag(*engine_type), true),
            WorkToDo::SetsToResolve { .. } => (0, false),
            WorkToDo::RemoteToDelete { .. } => (0, false),
            WorkToDo::FunctionArguments {
                function_alternatives,
                input_sets,
                ..
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

        if local {
            self.push_compute(work, debt, flags, composition_id, false);
        } else {
            self.push_io(work, debt, flags, composition_id, try_offload);
        }
    }

    /// Inserts the work into the queue setting the flags according to the supported engines and
    /// awaits the future before returning the result.
    pub async fn do_work(
        &self,
        work: WorkToDo,
        composition_id: usize,
    ) -> DandelionResult<WorkDone> {
        let (promise, debt) = self.promise_buffer.get_promise()?;
        self.push(work, debt, composition_id, true);
        return promise.await;
    }

    /// Tries to acquire some work that matches the given flags starting from the head of the queue.
    /// Ignores shutdown and fetch work, since that only makes sense to execute locally.
    pub fn try_get_work_for_remote(
        &self,
        engine_flags: u32,
        node_id: u64,
        number_of_functions: usize,
    ) -> Vec<(WorkToDo, Debt, usize)> {
        let mut guard = self.inner.lock().unwrap();
        let InnerQueue {
            compute_queue,
            io_queue,
            ..
        } = &mut *guard;
        policy::get_work_for_remote(
            io_queue,
            compute_queue,
            engine_flags,
            node_id,
            number_of_functions,
        )
    }

    /// Spins on the queue until it manages to acquire some work that matches the given flags.
    pub async fn get_compute_work(&self, engine_flags: u32) -> (WorkToDo, Debt) {
        // try to get work, if there is none, insert self into waker and try again
        ComputeWaitFuture::new(engine_flags, &self).await
    }

    pub async fn get_io_work(&self) -> (WorkToDo, Debt, Option<usize>) {
        // try to get work, if there is none, insert self into waker and try again
        IoWaitFuture::new(&self).await
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

    pub fn add_remote_channel(
        &self,
        node_id: u64,
        channel: mpsc::UnboundedSender<(WorkToDo, Debt, usize)>,
    ) {
        self.remote_nodes.lock().unwrap().insert(node_id, channel);
    }

    /// Put work back into queue after trying to offload without success.
    pub async fn reenqueue(&self, work: WorkToDo, debt: Debt, composition_id: usize) {
        self.push(work, debt, composition_id, false);
    }

    /// Increases the number of remote cores.
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

    /// Decreases the number of remote cores.
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
    fn get_compute_engine_args(
        &self,
    ) -> impl Future<Output = (WorkToDo, machine_interface::promise::Debt)> {
        self.work_queue.get_compute_work(self.engine_flags)
    }

    fn get_io_engine_args(
        &self,
    ) -> impl Future<Output = (WorkToDo, machine_interface::promise::Debt, Option<usize>)> {
        self.work_queue.get_io_work()
    }

    fn requeu_engine_args(&self, work: WorkToDo, debt: Debt, composition_id: usize) {
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
        self.work_queue
            .push_compute(work, debt, flags, composition_id, true)
    }

    fn remove_self_from_queue(&self) {
        self.work_queue
            .remove_local_cores(1)
            .expect("Failed to remove itself from the work queue.");
    }
}

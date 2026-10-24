mod policy;

pub use policy::PREFETCH_PER_CORE;

use dandelion_commons::{records::RecordPoint, DandelionResult, SystemInfo};
use log::trace;
use machine_interface::{
    function_driver::{EngineWorkQueue, WorkToDo},
    machine_config::EngineType,
    promise::{Debt, Promise, PromiseBuffer},
};
use std::{
    collections::{BTreeMap, LinkedList},
    future::Future,
    sync::{Arc, Mutex},
    task::{Poll, Waker},
};
use tokio::sync::{mpsc, Notify};

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

/// Work that was prepared for the queues, but not yet inserted.
#[derive(Default)]
struct PreparedElements {
    compute: LinkedList<ComputeQueueElement>,
    io: LinkedList<IoQueueElement>,
}

impl PreparedElements {
    fn is_empty(&self) -> bool {
        self.compute.is_empty() && self.io.is_empty()
    }
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
        trace!(
            "Compute trying to take task with {} in queue",
            lock_guard.compute_queue.len()
        );
        // check if there is any work with the flags we are looking for
        let result = lock_guard
            .compute_queue
            .extract_if(|queue_element| {
                !queue_element.debt.is_alive() || queue_element.flags & self.flags != 0
            })
            .find(|queue_element| queue_element.debt.is_alive())
            .map(|queue_element| (queue_element.work, queue_element.debt));
        // Poke the IO queue in case they were waiting either for more space in the compute queue or an idle compute core
        if let Some(waker) = lock_guard.io_waker_list.pop_front() {
            waker.wake();
        }
        if let Some(mut result_tupple) = result {
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
            self.work_queue.idle_notifier.notify_one();
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
                self.work_queue.idle_notifier.notify_one();
                self.was_idle = true;
            }
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
        trace!(
            "Io trying to take task with {} in queue",
            lock_guard.io_queue.len()
        );
        // Always take work that is not FunctionArguments.
        // Take FunctionArguments only if the local queue is smaller than a certain threshold.
        // Do this to avoid overloading the IO cores with too much parallel fetching and to make it easier for remotes
        // to find work where they can do their own fetching instead.
        let compute_pending = lock_guard.compute_queue.len();
        let local_cores = self.work_queue.system_info.local_cores();
        let active_fetch_count = lock_guard.prefetching_in_progress;
        let idle_compute_cores = lock_guard.compute_waker_list.len();
        let extracted = lock_guard
            .io_queue
            .extract_if(|queue_element| {
                !queue_element.debt.is_alive()
                    || match &queue_element.work {
                        WorkToDo::FunctionArguments { .. } => policy::should_io_take(
                            &queue_element.policy_data,
                            compute_pending,
                            active_fetch_count,
                            local_cores,
                            idle_compute_cores,
                        ),
                        _ => true,
                    }
            })
            .find(|queue_element| queue_element.debt.is_alive());
        let result = extracted.map(|queue_element| {
            (
                queue_element.work,
                queue_element.debt,
                queue_element.composition_id,
            )
        });
        if let Some((mut work, debt, composition_id)) = result {
            let composition_id_option =
                if let WorkToDo::FunctionArguments { recorder, .. } = &mut work {
                    recorder.record(RecordPoint::IOQueueEnd);
                    // If the extracted task is a prefetching task increase the counter accordingly
                    lock_guard.prefetching_in_progress += 1;
                    Some(composition_id)
                } else {
                    None
                };
            trace!(
                "Io did find task, {} in queue, idle_compute_cores {}, compute_pending {}, active_fetch_count {}, local_cores {}",
                lock_guard.io_queue.len(),
                idle_compute_cores,
                compute_pending,
                active_fetch_count,
                local_cores,
            );
            // Found some work, so core is not idle
            Poll::Ready((work, debt, composition_id_option))
        } else {
            // Did not find any work, so need to add to waker queue
            trace!(
                "Io did not find task, {} in queue, idle_compute_cores {}, compute_pending {}, active_fetch_count {}, local_cores {}",
                lock_guard.io_queue.len(),
                idle_compute_cores,
                compute_pending,
                active_fetch_count,
                local_cores,
            );
            lock_guard.io_waker_list.push_back(cx.waker().clone());
            Poll::Pending
        }
    }
}

/// Records that the work enters the compute queue, done before taking the queue lock.
fn record_compute_queue_start(work: &mut WorkToDo) {
    if let WorkToDo::FunctionArguments {
        input_sets: _input_sets,
        recorder,
        ..
    } = work
    {
        #[cfg(feature = "timestamp")]
        {
            let (total_items, total_size) =
                _input_sets.iter().fold((0, 0), |(number, size), set| {
                    (number + set.len(), size + set.total_size)
                });
            recorder.record_input(total_items as u64, total_size as u64);
        }
        recorder.record(RecordPoint::ComputeQueueStart);
    }
}

/// Inserts all `elements` of the composition with `composition_id` into `queue`, keeping the queue
/// sorted by composition id and the elements in their order.
fn insert_by_composition<T>(
    queue: &mut LinkedList<T>,
    mut elements: LinkedList<T>,
    composition_id: usize,
    id_of: impl Fn(&T) -> usize,
) {
    // if list empty or the back has the same or a smaller id just append to the back
    if queue
        .back()
        .is_none_or(|back| id_of(back) <= composition_id)
    {
        queue.append(&mut elements);
    // check the front has same or bigger id so can just prepend
    } else if queue
        .front()
        .is_some_and(|front| id_of(front) >= composition_id)
    {
        elements.append(queue);
        *queue = elements;
    // find the place to insert
    } else {
        let index = queue
            .iter()
            .position(|elem| id_of(elem) > composition_id)
            .unwrap();
        let mut tail = queue.split_off(index);
        queue.append(&mut elements);
        queue.append(&mut tail);
    }
}

fn get_flags_and_local(work: &WorkToDo) -> (u32, bool) {
    match &work {
        WorkToDo::Shutdown(engine_type) => (get_engine_flag(*engine_type), true),
        WorkToDo::SetsToResolve { .. } => (0, false),
        WorkToDo::RemoteToDelete { .. } => (0, false),
        WorkToDo::FunctionArguments {
            function_alternatives,
            input_sets,
            ..
        } => {
            // check if all the sets are already fully locally available
            let local = input_sets.iter().all(|set| set.is_local());
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
    }
}

impl WorkQueue {
    pub fn init() -> Self {
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
            system_info: Arc::new(SystemInfo::new()),
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

    /// Prepares the work for the queue (without yet taking the queue lock).
    /// NOTE: This does run the io policy which may already offload work to a remote node.
    fn prepare(
        &self,
        mut work: WorkToDo,
        debt: Debt,
        composition_id: usize,
        try_offload: bool,
        elements: &mut PreparedElements,
    ) {
        let (flags, local) = get_flags_and_local(&work);
        trace!(
            "Enqueueing function with all local data {}, with engine flags: {}",
            local,
            flags
        );
        if local {
            record_compute_queue_start(&mut work);
            elements.compute.push_back(ComputeQueueElement {
                composition_id,
                flags,
                work,
                debt,
            });
        } else {
            if let WorkToDo::FunctionArguments { recorder, .. } = &mut work {
                recorder.record(RecordPoint::IOQueueStart);
            }
            if let Some((work, debt, policy_data)) = policy::prepare_io_element(
                work,
                debt,
                try_offload,
                composition_id,
                &self.remote_nodes,
            ) {
                elements.io.push_back(IoQueueElement {
                    composition_id,
                    flags,
                    work,
                    debt,
                    policy_data,
                });
            }
        }
    }

    /// Inserts the prepared elements of the composition with `composition_id` into the queues and
    /// wakes up the cores that should take them, taking the queue lock only once.
    /// `had_fetching` indicates that the elements come from an io core that finished prefetching.
    // TODO: check or define here and other places, if the flags need to match fully, just checking that any flag is set would be enough
    fn insert_prepared(
        &self,
        elements: PreparedElements,
        composition_id: usize,
        had_fetching: bool,
    ) {
        // nothing to do if all the work was offloaded already
        if elements.is_empty() && !had_fetching {
            return;
        }
        let PreparedElements { compute, io } = elements;
        let mut queue_guard = self.inner.lock().unwrap();
        if had_fetching {
            queue_guard.prefetching_in_progress -= 1;
        }
        let mut notify = false;

        if !compute.is_empty() {
            // wake up one idle compute core with matching flags per element
            for flags in compute.iter().map(|elem| elem.flags) {
                match queue_guard
                    .compute_waker_list
                    .extract_if(|queue_element| queue_element.flags & flags == flags)
                    .next()
                {
                    Some(waker_to_call) => waker_to_call.waker.wake(),
                    None => notify = true,
                }
            }
            insert_by_composition(
                &mut queue_guard.compute_queue,
                compute,
                composition_id,
                |elem| elem.composition_id,
            );
        }

        if !io.is_empty() {
            // only wake up io cores for elements we know they would take
            let compute_length = queue_guard.compute_queue.len();
            let local_cores = self.system_info.local_cores();
            let already_fetching = queue_guard.prefetching_in_progress;
            let idle_compute_cores = queue_guard.compute_waker_list.len();
            let num_io_elements = io.len();
            let would_process = io
                .iter()
                .filter(|elem| match &elem.work {
                    WorkToDo::FunctionArguments { .. } => policy::should_io_take(
                        &elem.policy_data,
                        compute_length,
                        already_fetching,
                        local_cores,
                        idle_compute_cores,
                    ),
                    _ => true,
                })
                .count();
            insert_by_composition(&mut queue_guard.io_queue, io, composition_id, |elem| {
                elem.composition_id
            });
            let mut woken = 0;
            while woken < would_process {
                let Some(waker) = queue_guard.io_waker_list.pop_front() else {
                    break;
                };
                waker.wake();
                woken += 1;
            }
            notify |= woken < num_io_elements;
        }
        drop(queue_guard);

        if notify {
            self.queuing_notifier.notify_waiters();
        }
    }

    /// Inserts the work into the queue setting the flags according to the supported engines and
    /// returns the promise that resolves to its result.
    pub fn enqueue(&self, work: WorkToDo, composition_id: usize) -> DandelionResult<Promise> {
        let (promise, debt) = self.promise_buffer.get_promise()?;
        let mut elements = PreparedElements::default();
        self.prepare(work, debt, composition_id, true, &mut elements);
        self.insert_prepared(elements, composition_id, false);
        Ok(promise)
    }

    /// Inserts all the work into the queue and returns the promises for their results, in the same
    /// order as the work.
    /// Everything that does not depend on the queue state is done up front, so the queue lock is
    /// only taken once and only held for the insertion and waking up the engines.
    pub fn enqueue_many(
        &self,
        work_iter: impl IntoIterator<Item = WorkToDo>,
        composition_id: usize,
    ) -> DandelionResult<Vec<Promise>> {
        let work_iter = work_iter.into_iter();
        // If we run out of promises midway, none of the work gets inserted into the queue. Work
        // the io policy already offloaded to a remote node will be discarded.
        let mut promises = Vec::with_capacity(work_iter.size_hint().0);
        let mut elements = PreparedElements::default();
        for work in work_iter {
            let (promise, debt) = self.promise_buffer.get_promise()?;
            promises.push(promise);
            self.prepare(work, debt, composition_id, true, &mut elements);
        }
        self.insert_prepared(elements, composition_id, false);
        Ok(promises)
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
        trace!(
            "Trying to get work for remote from io queue ({}), compute queue ({})",
            io_queue.len(),
            compute_queue.len()
        );
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
        ComputeWaitFuture::new(engine_flags, self).await
    }

    pub async fn get_io_work(&self) -> (WorkToDo, Debt, Option<usize>) {
        // try to get work, if there is none, insert self into waker and try again
        IoWaitFuture::new(self).await
    }

    pub fn add_remote_channel(
        &self,
        node_id: u64,
        channel: mpsc::UnboundedSender<(WorkToDo, Debt, usize)>,
    ) {
        self.remote_nodes.lock().unwrap().insert(node_id, channel);
    }

    /// Removes the offload channel for a remote node, e.g. when it has disconnected.
    pub fn remove_remote_channel(&self, node_id: u64) {
        self.remote_nodes.lock().unwrap().remove(&node_id);
    }

    /// Put work back into queue after trying to offload without success.
    pub fn reenqueue(&self, work: WorkToDo, debt: Debt, composition_id: usize) {
        // The abort handle refers to the failed offload attempt, whose remote invocation id may be reused.
        debt.clear_abort_handle();
        let mut elements = PreparedElements::default();
        self.prepare(work, debt, composition_id, false, &mut elements);
        self.insert_prepared(elements, composition_id, false);
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

    fn reenqueue_engine_args(&self, mut work: WorkToDo, debt: Debt, composition_id: usize) {
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
        // the io core fetched all the data, so the work always goes to the compute queue now
        record_compute_queue_start(&mut work);
        let mut elements = PreparedElements::default();
        elements.compute.push_back(ComputeQueueElement {
            composition_id,
            flags,
            work,
            debt,
        });
        self.work_queue
            .insert_prepared(elements, composition_id, true)
    }

    fn remove_self_from_queue(&self) {
        self.work_queue
            .system_info
            .remove_local_cores(1)
            .expect("Failed to remove itself from the work queue.");
    }
}

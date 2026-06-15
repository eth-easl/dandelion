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

struct ComputeQueueElement {
    /// Flags indicating which engines can run this task
    flags: u32,
    /// The WorkToDo content of the queue element
    work: WorkToDo,
    /// The Debt content of the queue element
    debt: Debt,
}

struct IoQueueElement {
    /// Flags indicating which engines can run this task
    flags: u32,
    /// The WorkToDo content of the queue element
    work: WorkToDo,
    /// The Debt content of the queue element
    debt: Debt,
    /// Sizes of remote references if any
    #[cfg(feature = "data_locallity")]
    remote_data: std::collections::BTreeMap<u64, usize>,
    /// Total size of all inputs
    #[cfg(feature = "data_locallity")]
    total_input_size: usize,
}

struct WakerElement {
    flags: u32,
    waker: Waker,
}

struct InnerQueue {
    /// Queueu holding work for which some data still needs to be fetched
    compute_queue: LinkedList<ComputeQueueElement>,
    /// Queueu holding work for which all data is local
    io_queue: LinkedList<IoQueueElement>,
    /// List of compute engines which are idle
    compute_waker_list: LinkedList<WakerElement>,
    /// List of io engines ready to take more work
    io_waker_list: LinkedList<Waker>,
    /// The number of functions for which we are currently prefetching
    fetching_in_progress: usize,
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
    /// Used to keep track of the total number of jobs currently in the queue
    queue_state_sender: watch::Sender<usize>,
    queue_state_receiver: watch::Receiver<usize>,
    /// Notifier to send out notification, that idle resource count changed
    /// Notifier to send out notification that queueing is happening
    queuing_notifier: Arc<Notify>,
    /// Tracks current system informations used by the any sharding policy.
    pub system_info: Arc<SystemInfo>,
    /// Channels for asking remote node to take work
    remote_nodes: Arc<Mutex<BTreeMap<u64, mpsc::UnboundedSender<(WorkToDo, Debt)>>>>,
}

struct ComputeWaitFuture<'queue> {
    flags: u32,
    work_queue: &'queue WorkQueue,
}

impl<'list> ComputeWaitFuture<'list> {
    fn new(flags: u32, work_queue: &'list WorkQueue) -> ComputeWaitFuture<'list> {
        Self { flags, work_queue }
    }
}

impl Future for ComputeWaitFuture<'_> {
    type Output = (WorkToDo, Debt);

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
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
            self.work_queue.queue_state_decrease();

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
            Poll::Ready(result_tupple)
        } else {
            // Did not find any work, so need to add to waker queue
            let waker_element = WakerElement {
                flags: self.flags,
                waker: cx.waker().clone(),
            };
            lock_guard.compute_waker_list.push_back(waker_element);
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

const LOCAL_WORK_PER_CORE: usize = 2;

#[inline]
fn io_extract_if(
    queue_element: &IoQueueElement,
    compute_length: usize,
    already_fetching: usize,
    local_cores: usize,
    #[cfg(feature = "data_locallity")] idle_compute_cores: usize,
) -> bool {
    match queue_element.work {
        WorkToDo::FunctionArguments {
            function_id: _,
            function_alternatives: _,
            input_sets: _,
            metadata: _,
            caching: _,
            recorder: _,
        } => {
            #[cfg(not(feature = "data_locallity"))]
            let should_take = compute_length + already_fetching < LOCAL_WORK_PER_CORE * local_cores;

            #[cfg(feature = "data_locallity")]
            // additionally want to prevent fetching, if there is remote data and no local core is idle
            // always take it if there are idle cores, only prefetch if it is prefetching via IO, not from other nodes
            let should_take = idle_compute_cores > 0
                || (compute_length + already_fetching < LOCAL_WORK_PER_CORE * local_cores
                    && !queue_element.remote_data.is_empty());
            // need to do it like this, because attributes on expressions are still experimental
            should_take
        }
        // always take resolver work
        _ => true,
    }
}

impl Future for IoWaitFuture<'_> {
    type Output = (WorkToDo, Debt);

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let mut lock_guard = self.work_queue.inner.lock().unwrap();
        // Always take work that is not FunctionArguments.
        // Take FunctionArguments only if the local queue is smaller than a certain thershold.
        // Do this to avoid overloading the IO cores with too much parallel fetching and to make it easier for remotes
        // to find work where they can do their own fetching instead.
        let compute_length = lock_guard.compute_queue.len();
        let local_cores = *self.work_queue.system_info.num_local_cores_watcher.borrow();
        let already_fetching = lock_guard.fetching_in_progress;
        #[cfg(feature = "data_locallity")]
        let idle_compute_cores = lock_guard.compute_waker_list.len();
        let mut is_fetching = false;
        let result = lock_guard
            .io_queue
            .extract_if(|queue_element| {
                #[cfg(not(feature = "data_locallity"))]
                let should_take =
                    io_extract_if(queue_element, compute_length, already_fetching, local_cores);
                #[cfg(feature = "data_locallity")]
                let should_take = io_extract_if(
                    queue_element,
                    compute_length,
                    already_fetching,
                    local_cores,
                    idle_compute_cores,
                );
                // If will take task that has prefetching, increase counter accordingly
                is_fetching = should_take;
                should_take
            })
            .next()
            .map(|queue_element| (queue_element.work, queue_element.debt));
        if is_fetching {
            lock_guard.fetching_in_progress += 1;
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
                recorder.record(RecordPoint::IOQueueEnd);
            }

            // Found some work, so core is not idle
            Poll::Ready(result_tupple)
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
        let (queue_state_sender, queue_state_receiver) = watch::channel(0);
        let (num_local_cores_sender, num_local_cores_watcher) = watch::channel(0);
        WorkQueue {
            inner: Arc::new(Mutex::new(InnerQueue {
                compute_queue: LinkedList::new(),
                io_queue: LinkedList::new(),
                compute_waker_list: LinkedList::new(),
                io_waker_list: LinkedList::new(),
                fetching_in_progress: 0,
            })),
            promise_buffer: PromiseBuffer::init(MAX_QUEUE),
            queue_state_sender,
            queue_state_receiver,
            queuing_notifier: Arc::new(Notify::new()),
            system_info: Arc::new(SystemInfo {
                num_local_cores_watcher,
                num_local_cores_sender,
                num_remote_cores: AtomicUsize::new(0),
            }),
            remote_nodes: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn queue_state_watcher(&self) -> watch::Receiver<usize> {
        self.queue_state_receiver.clone()
    }

    pub fn queueing_notifier(&self) -> Arc<Notify> {
        self.queuing_notifier.clone()
    }

    fn queue_state_decrease(&self) {
        self.queue_state_sender.send_if_modified(|current_state| {
            *current_state -= 1;
            *current_state < *self.system_info.num_local_cores_watcher.borrow()
        });
    }

    fn queue_state_increase(&self) {
        self.queue_state_sender.send_if_modified(|current_state| {
            *current_state += 1;
            *current_state < *self.system_info.num_local_cores_watcher.borrow()
        });
    }

    /// Pushes the work and debt to the back of the queue and sets the flags accordingly.
    /// Returns an error if the queue is full.
    /// TODO: check or define here and other places, if the flags need to match fully, just checking that any flag is set would be enough
    fn push_compute(&self, mut work: WorkToDo, debt: Debt, flags: u32, had_fetching: bool) {
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
            queue_guard.fetching_in_progress -= 1;
        }
        queue_guard
            .compute_queue
            .push_back(ComputeQueueElement { flags, work, debt });
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
        #[cfg(feature = "data_locallity")] try_offload: bool,
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

        let mut queue_guard = self.inner.lock().unwrap();
        #[cfg(feature = "data_locallity")]
        let (remote_data, total_input_size) = if let WorkToDo::FunctionArguments {
            function_id: _,
            function_alternatives: _,
            input_sets,
            metadata: _,
            caching: _,
            recorder: _,
        } = &work
        {
            let mut ref_map = std::collections::BTreeMap::new();
            let mut total_input_size = 0;
            for (item, data) in input_sets
                .iter()
                .filter_map(|s| s.as_ref().map(|s| s.into_iter()))
                .flatten()
            {
                total_input_size += item.data.size;
                if let machine_interface::composition::ItemData::RemoteData(remote_data) = data {
                    if item.data.size > 0 {
                        use std::collections::btree_map::Entry;
                        match ref_map.entry(remote_data.node_id) {
                            Entry::Occupied(mut value) => *value.get_mut() += item.data.size,
                            Entry::Vacant(value) => {
                                value.insert(item.data.size);
                            }
                        }
                    }
                }
            }
            (ref_map, total_input_size)
        } else {
            (std::collections::BTreeMap::new(), 0)
        };
        // TODO: think about if there is a better place to do this
        #[cfg(feature = "data_locallity")]
        {
            if try_offload {
                if let Some((node_id, max_size)) = remote_data.iter().max() {
                    // if more than half of the data is on one specific node try to offload to that one
                    if max_size * 2 > total_input_size {
                        if let Some(node_sender) = self.remote_nodes.lock().unwrap().get(node_id) {
                            // Doing try send to avoid compiler issues with holding mutex
                            node_sender.send((work, debt)).unwrap();
                            return;
                        }
                    }
                }
            }
        }

        let new_element = IoQueueElement {
            flags,
            work,
            debt,
            #[cfg(feature = "data_locallity")]
            remote_data,
            #[cfg(feature = "data_locallity")]
            total_input_size,
        };
        // check if an io core would take the element or not,
        // if not, no reason to call waker
        let compute_length = queue_guard.compute_queue.len();
        let local_cores = *self.system_info.num_local_cores_watcher.borrow();
        let already_fetching = queue_guard.fetching_in_progress;
        #[cfg(feature = "data_locallity")]
        let idle_compute_cores = queue_guard.compute_waker_list.len();
        #[cfg(feature = "data_locallity")]
        let would_process = io_extract_if(
            &new_element,
            compute_length,
            already_fetching,
            local_cores,
            idle_compute_cores,
        );
        #[cfg(not(feature = "data_locallity"))]
        let would_process =
            io_extract_if(&new_element, compute_length, already_fetching, local_cores);

        queue_guard.io_queue.push_back(new_element);

        // call first waker with matching flags if there are any
        // only call waker if we know the core wants to take this element
        if !queue_guard.io_waker_list.is_empty() && would_process {
            queue_guard.io_waker_list.pop_front().unwrap().wake();
        } else {
            self.queuing_notifier.notify_waiters();
        }
    }

    fn push(
        &self,
        work: WorkToDo,
        debt: Debt,
        #[cfg(feature = "data_locallity")] try_offload: bool,
    ) {
        let (flags, local) = match &work {
            WorkToDo::Shutdown(engine_type) => (get_engine_flag(*engine_type), true),
            WorkToDo::SetsToResolve { input_sets: _ } => (0, false),
            WorkToDo::RemoteToDelete { remote_data: _ } => (0, false),
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
                // only want to count the actual functions to execute
                self.queue_state_increase();
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
            self.push_compute(work, debt, flags, false);
        } else {
            #[cfg(feature = "data_locallity")]
            self.push_io(work, debt, flags, try_offload);
            #[cfg(not(feature = "data_locallity"))]
            self.push_io(work, debt, flags);
        }
    }

    /// Inserts the work into the queue setting the flags according to the supported engines and
    /// awaits the future before returning the result.
    pub async fn do_work(&self, work: WorkToDo) -> DandelionResult<WorkDone> {
        let (promise, debt) = self.promise_buffer.get_promise()?;
        #[cfg(feature = "data_locallity")]
        self.push(work, debt, true);
        #[cfg(not(feature = "data_locallity"))]
        self.push(work, debt);
        return promise.await;
    }

    /// Tries to acquire some work that matches the given flags starting from the head of the queue.
    /// Ignores shutdown and fetch work, since that only makes sense to execute locally
    /// Version that takes node locality into account
    #[cfg(feature = "data_locallity")]
    pub fn try_get_work_for_remote(
        &self,
        engine_flags: u32,
        node_id: u64,
        number_of_functions: usize,
    ) -> Vec<(WorkToDo, Debt)> {
        let mut functions = Vec::with_capacity(number_of_functions);
        let mut lock = self.inner.lock().unwrap();
        // go through all the input sets and find the index of the one with the most data already on the node asking for work
        functions.extend(
            lock.io_queue
                .extract_if(|queue_element| {
                    if let WorkToDo::FunctionArguments {
                        function_id: _,
                        function_alternatives: _,
                        input_sets: _,
                        metadata: _,
                        caching: _,
                        recorder,
                    } = &mut queue_element.work
                    {
                        let node_has_most_data = queue_element
                            .remote_data
                            .iter()
                            .max_by_key(|(_, v)| **v)
                            .map(|(max_id, _)| node_id == *max_id)
                            .unwrap_or(false);
                        if queue_element.flags & engine_flags != 0 && node_has_most_data {
                            // we are taking the element so can set the recorder
                            recorder.record(RecordPoint::IOQueueEnd);
                            self.queue_state_decrease();
                            true
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                })
                .map(|queue_element| (queue_element.work, queue_element.debt))
                .take(number_of_functions),
        );

        // did not find enough work where the node has the majority of data, so take anything left over
        if functions.len() < number_of_functions {
            let still_needed = number_of_functions - functions.len();
            functions.extend(
                lock.io_queue
                    .extract_if(|queue_element| {
                        if let WorkToDo::FunctionArguments {
                            function_id: _,
                            function_alternatives: _,
                            input_sets: _,
                            metadata: _,
                            caching: _,
                            recorder,
                        } = &mut queue_element.work
                        {
                            if queue_element.flags & engine_flags != 0 {
                                recorder.record(RecordPoint::IOQueueEnd);
                                self.queue_state_decrease();
                                true
                            } else {
                                false
                            }
                        } else {
                            false
                        }
                    })
                    .map(|queue_element| (queue_element.work, queue_element.debt))
                    .take(still_needed),
            );
        }

        // did not find enough work in the io_queue so check compute queue
        if functions.len() < number_of_functions {
            let still_needed = number_of_functions - functions.len();
            functions.extend(
                lock.compute_queue
                    .extract_if(|queue_element| {
                        if let WorkToDo::FunctionArguments {
                            function_id: _,
                            function_alternatives: _,
                            input_sets: _,
                            metadata: _,
                            caching: _,
                            recorder,
                        } = &mut queue_element.work
                        {
                            if queue_element.flags & engine_flags != 0 {
                                recorder.record(RecordPoint::ComputeQueueEnd);
                                self.queue_state_decrease();
                                // Don't need to wake IO cores to do more prefetching,
                                // since those queues must be empty if we are taking from here.
                                true
                            } else {
                                false
                            }
                        } else {
                            false
                        }
                    })
                    .map(|queue_element| (queue_element.work, queue_element.debt))
                    .take(still_needed),
            );
        }

        functions
    }

    /// Tries to acquire some work that matches the given flags starting from the head of the queue.
    /// Ignores shutdown and fetch work, since that only makes sense to execute locally
    /// Base Version
    #[cfg(not(feature = "data_locallity"))]
    pub fn try_get_work_for_remote(
        &self,
        engine_flags: u32,
        _node_id: u64,
        number_of_functions: usize,
    ) -> Vec<(WorkToDo, Debt)> {
        let mut functions = Vec::with_capacity(number_of_functions);
        let mut lock_guard = self.inner.lock().unwrap();
        // first check the queue with unresolved references, since those are easier to steal
        functions.extend(
            lock_guard
                .io_queue
                .extract_if(|queue_element| {
                    if let WorkToDo::FunctionArguments {
                        function_id: _,
                        function_alternatives: _,
                        input_sets: _,
                        metadata: _,
                        caching: _,
                        recorder,
                    } = &mut queue_element.work
                    {
                        if queue_element.flags & engine_flags != 0 {
                            self.queue_state_decrease();
                            recorder.record(RecordPoint::IOQueueEnd);
                            true
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                })
                .map(|queue_element| (queue_element.work, queue_element.debt))
                .take(number_of_functions),
        );
        // did not find enough work in the io_queue so check compute queue
        if functions.len() < number_of_functions {
            let still_needed = number_of_functions - functions.len();
            functions.extend(
                lock_guard
                    .compute_queue
                    .extract_if(|queue_element| {
                        if let WorkToDo::FunctionArguments {
                            function_id: _,
                            function_alternatives: _,
                            input_sets: _,
                            metadata: _,
                            caching: _,
                            recorder,
                        } = &mut queue_element.work
                        {
                            if queue_element.flags & engine_flags != 0 {
                                recorder.record(RecordPoint::ComputeQueueEnd);
                                self.queue_state_decrease();
                                // don't need to poke IO cores to do more prefetching,
                                // since those queues are empty if we are taking from here
                                true
                            } else {
                                false
                            }
                        } else {
                            false
                        }
                    })
                    .map(|queue_element| (queue_element.work, queue_element.debt))
                    .take(still_needed),
            )
        }
        functions
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
        channel: mpsc::UnboundedSender<(WorkToDo, Debt)>,
    ) {
        self.remote_nodes.lock().unwrap().insert(node_id, channel);
    }

    /// Put work back into queue after trying to offload without success
    pub async fn reenqueue(&self, work: WorkToDo, debt: Debt) {
        #[cfg(feature = "data_locallity")]
        self.push(work, debt, false);
        #[cfg(not(feature = "data_locallity"))]
        self.push(work, debt);
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

    fn requeu_engine_args(&self, work: WorkToDo, debt: Debt) {
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
        self.work_queue.push_compute(work, debt, flags, true)
    }

    fn remove_self_from_queue(&self) {
        self.work_queue
            .remove_local_cores(1)
            .expect("Failed to remove itself from the work queue.");
    }
}

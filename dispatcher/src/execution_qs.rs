use crate::dispatcher::{EnqueueWork, FullQueue};
use core::sync::atomic::{AtomicUsize, Ordering};
use crossbeam::channel::{TryRecvError, TrySendError};
use dandelion_commons::{DandelionError, DandelionResult, FunctionId};
use log::error;
use machine_interface::{
    function_driver::{WorkQueue, WorkToDo},
    promise::{Debt, Promise, PromiseBuffer},
};
use std::{
    cell::Cell,
    cmp,
    collections::{HashMap, VecDeque},
    env,
    hint,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const MAX_QUEUE: usize = 16384;
const FRONT_QUEUE: usize = 10; // Specify how many requests can be looked at each polling iteration; limit it to lower polling time
const MAX_QUEUE_TIME: Duration = Duration::new(0, 30_000_000); // 30 ms
const MAX_IDLE_TIME: Duration = Duration::new(0, 5_000_000); // 5 ms
const IDLE_BEFORE_BATCH_FORCED: Duration = Duration::new(0, 5_000_000); // 5 ms
const SOFT_MIN_BATCHED: usize = 32;
const MAX_BATCHED: usize = 512;

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

impl EnqueueWork for EngineQueue {
    fn enqueue_work(&self, args: WorkToDo, function_id: FunctionId) -> DandelionResult<Promise> {
        let (promise, debt) = self.promise_buffer.get_promise()?;
        match self.queue_in.try_send((args, debt)) {
            Ok(()) => (),
            Err(TrySendError::Disconnected(_)) => {
                error!("Failed to enqueu work, workqueue has been disconnected")
            }
            Err(TrySendError::Full(_)) => return Err(DandelionError::WorkQueueFull),
        }
        return Ok(promise);
    }
}

unsafe impl Send for EngineQueue {}
unsafe impl Sync for EngineQueue {}

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
}

pub struct EngineQueueGPU {
    engine_counter: Cell<u8>,
    engine_id: u8,
    general_queue_in: crossbeam::channel::Sender<(WorkToDo, Debt)>,
    general_queue_out: crossbeam::channel::Receiver<(WorkToDo, Debt)>,
    function_queue: Arc<Mutex<VecDeque<(Instant, FunctionId, WorkToDo, Debt)>>>,
    last_function: Arc<Mutex<HashMap<u8, FunctionId>>>,
    idle_since: Cell<Instant>,
    worker_queue: Arc<AtomicTickets>,
    promise_buffer: PromiseBuffer,
}

impl WorkQueue for EngineQueueGPU {
    fn get_engine_args(&self) -> (WorkToDo, Debt) {
        self.idle_since.set(Instant::now());
        let work = loop {
            // Make sure that engines acquire the lock in a round-robin fashion
            let local_ticket = self.worker_queue.end.fetch_add(1, Ordering::AcqRel);
            while local_ticket != self.worker_queue.start.load(Ordering::Acquire) {
                hint::spin_loop();
            }

            // If some non-FunctionArguments is ready, run it
            let _ = match self.general_queue_out.try_recv() {
                Err(TryRecvError::Disconnected) => panic!("Work queue disconnected"),
                Ok(recieved) => {
                    let (recieved_args, recevied_dept) = recieved;
                    if recevied_dept.is_alive() {
                        break (recieved_args, recevied_dept);
                    } else {
                        ()
                    }
                }
                Err(TryRecvError::Empty) => (),
            };
            // Else, try to run FunctionArguments WorkToDo

            let mut function_queue = self.function_queue.lock().unwrap();

            let mut last_function = self.last_function.lock().unwrap();
            let last_function_id = last_function.get(&self.engine_id).unwrap();

            if !function_queue.is_empty() {
                let (first_timestamp, first_function_id, _, _) = &function_queue[0];

                let now = Instant::now();
                let mut send_idx = usize::MAX;

                // Send first in the queue if:
                if *last_function_id == u64::MAX {
                    // The engine hasn't run any function yet
                    send_idx = 0;
                } else if now.duration_since(self.idle_since.get()) > MAX_IDLE_TIME {
                    // The engine has been idle for too much time
                    send_idx = 0;
                } else if now.duration_since(*first_timestamp) > MAX_QUEUE_TIME {
                    // The first request has waited too long
                    send_idx = 0;
                }

                if send_idx == usize::MAX {
                    let loop_max = std::cmp::min(FRONT_QUEUE, function_queue.len());
                    // Send i-th request if:
                    for i in 0..loop_max {
                        let (_, function_id, _, _) = &function_queue[i];

                        // The i-th request is of the model loaded by the engine
                        if *last_function_id == *function_id {
                            send_idx = i;
                            break;
                        }
                    }
                }

                // Send first request if:
                // 1) no other request has been chosen
                if send_idx == usize::MAX {
                    let mut someone_has_it = false;
                    for key in last_function.keys() {
                        if *key != self.engine_id {
                            let last_function_id = last_function.get(key).unwrap();
                            if *last_function_id == *first_function_id {
                                someone_has_it = true;
                                break;
                            }
                        }
                    }
                    // 2) no other engine has the function loaded
                    if !someone_has_it {
                        send_idx = 0;
                    }
                }

                // Actual send, if:
                if send_idx != usize::MAX {
                    match function_queue.remove(send_idx) {
                        Some((_, function_id, args, debt)) => {
                            // The debt is still alive
                            if debt.is_alive() {
                                last_function.insert(self.engine_id, function_id);
                                break (args, debt);
                            }
                        }
                        None => (),
                    }
                }
            }

            // If no WorkToDo is selected at all, update the counter
            self.worker_queue.start.fetch_add(1, Ordering::Release);
        };
        // Exiting the loop, update the counter
        self.worker_queue.start.fetch_add(1, Ordering::Release);
        return work;
    }
    /// models called with inter-call times that are randomly selected, to "overlap differently"

    fn try_get_engine_args(&self) -> Option<(WorkToDo, Debt)> {
        // TODO
        return None;
    }
}

impl EnqueueWork for EngineQueueGPU {
    fn enqueue_work(&self, args: WorkToDo, function_id: FunctionId) -> DandelionResult<Promise> {
        let (promise, debt) = self.promise_buffer.get_promise()?;

        match args {
            WorkToDo::FunctionArguments { .. } => {
                let now = Instant::now();
                self.function_queue
                    .lock()
                    .unwrap()
                    .push_back((now, function_id, args, debt));
            }
            _ => match self.general_queue_in.try_send((args, debt)) {
                Ok(()) => (),
                Err(TrySendError::Disconnected(_)) => {
                    error!("Failed to enqueu work, workqueue has been disconnected")
                }
                Err(TrySendError::Full(_)) => return Err(DandelionError::WorkQueueFull),
            },
        }

        return Ok(promise);
    }
}

unsafe impl Send for EngineQueueGPU {}
unsafe impl Sync for EngineQueueGPU {}

impl EngineQueueGPU {
    pub fn new() -> Self {
        let engine_counter = Cell::new(0);
        let engine_id = 0;

        let (sender, receiver) = crossbeam::channel::bounded(MAX_QUEUE);

        let function_queue = Arc::new(Mutex::new(VecDeque::with_capacity(MAX_QUEUE)));

        let last_function = Arc::new(Mutex::new(HashMap::new()));

        let idle_since = Cell::new(Instant::now());

        let tickets = AtomicTickets {
            start: AtomicUsize::new(0),
            end: AtomicUsize::new(0),
        };
        let worker_queue = Arc::new(tickets);

        let promise_buffer = PromiseBuffer::init(MAX_QUEUE);

        return EngineQueueGPU {
            engine_counter,
            engine_id,
            general_queue_in: sender,
            general_queue_out: receiver,
            function_queue,
            last_function,
            idle_since,
            worker_queue,
            promise_buffer,
        };
    }
}

impl Clone for EngineQueueGPU {
    fn clone(&self) -> EngineQueueGPU {
        self.engine_counter.set(self.engine_counter.get() + 1);
        {
            let mut last_function = self.last_function.lock().unwrap();
            last_function.insert(self.engine_counter.get(), u64::MAX);
        }

        EngineQueueGPU {
            engine_counter: self.engine_counter.clone(),
            engine_id: self.engine_counter.get(),
            general_queue_in: self.general_queue_in.clone(),
            general_queue_out: self.general_queue_out.clone(),
            function_queue: self.function_queue.clone(),
            idle_since: Cell::new(Instant::now()),
            last_function: self.last_function.clone(),
            worker_queue: self.worker_queue.clone(),
            promise_buffer: self.promise_buffer.clone(),
        }
    }
}

#[cfg(feature = "auto_batching")]
pub struct BatchingQueue {
    engine_counter: Cell<u8>,
    engine_id: u8,
    general_queue_in: crossbeam::channel::Sender<(WorkToDo, Debt)>,
    general_queue_out: crossbeam::channel::Receiver<(WorkToDo, Debt)>,
    atoms_times: Arc<Mutex<VecDeque<(Instant, FunctionId)>>>,
    atoms_map: Arc<Mutex<HashMap<FunctionId, VecDeque<(WorkToDo, Debt)>>>>,
    last_function: Arc<Mutex<HashMap<u8, FunctionId>>>,
    idle_since: Cell<Instant>,
    worker_queue: Arc<AtomicTickets>,
    promise_buffer: PromiseBuffer,
}

#[cfg(feature = "auto_batching")]
impl WorkQueue for BatchingQueue {
    fn get_engine_args(&self) -> (WorkToDo, Debt) {
        let FIXED_BATCH_SIZE: usize = match env::var("BATCH_SIZE") {
            Ok(value) => value.parse::<usize>().unwrap(),
            Err(e) => 2,
        };

        self.idle_since.set(Instant::now());
        let work = loop {
            // Make sure that engines acquire the lock in a round-robin fashion
            let local_ticket = self.worker_queue.end.fetch_add(1, Ordering::AcqRel);
            while local_ticket != self.worker_queue.start.load(Ordering::Acquire) {
                hint::spin_loop();
            }

            // If some non-FunctionArguments is ready, run it
            let _ = match self.general_queue_out.try_recv() {
                Err(TryRecvError::Disconnected) => panic!("Work queue disconnected"),
                Ok(recieved) => {
                    let (recieved_args, recevied_dept) = recieved;
                    if recevied_dept.is_alive() {
                        break (recieved_args, recevied_dept);
                    } else {
                        ()
                    }
                }
                Err(TryRecvError::Empty) => (),
            };
            // Else, try to run BatchAtom WorkToDo

            let mut atoms_times = self.atoms_times.lock().unwrap();

            let mut last_function = self.last_function.lock().unwrap();
            let last_function_id = last_function.get(&self.engine_id).unwrap();

            let mut atoms_map = self.atoms_map.lock().unwrap();

            if !atoms_times.is_empty() {
                let (first_timestamp, first_function_id) = &atoms_times[0];

                let now = Instant::now();
                let mut send_idx = usize::MAX;
                let mut send_function_id = u64::MAX;

                // Send first in the queue if:
                if *last_function_id == u64::MAX {
                    // The engine hasn't run any function yet
                    send_idx = 0;
                    send_function_id = *first_function_id;
                } else if now.duration_since(self.idle_since.get()) > MAX_IDLE_TIME {
                    // The engine has been idle for too much time
                    send_idx = 0;
                    send_function_id = *first_function_id;
                } else if now.duration_since(*first_timestamp) > MAX_QUEUE_TIME {
                    // The first request has waited too long
                    send_idx = 0;
                    send_function_id = *first_function_id;
                }

                if send_idx == usize::MAX {
                    let loop_max = std::cmp::min(FRONT_QUEUE, atoms_times.len());
                    // Send i-th request if:
                    for i in 0..loop_max {
                        let (_, function_id) = &atoms_times[i];

                        // The i-th request is of the model loaded by the engine
                        if *last_function_id == *function_id {
                            send_idx = i;
                            send_function_id = *function_id;
                            break;
                        }
                    }
                }

                // Send first request if:
                // 1) no other request has been chosen, AND
                if send_idx == usize::MAX {
                    let mut someone_has_it = false;
                    for key in last_function.keys() {
                        if *key != self.engine_id {
                            let last_function_id = last_function.get(key).unwrap();
                            if *last_function_id == *first_function_id {
                                someone_has_it = true;
                                break;
                            }
                        }
                    }
                    // 2) no other engine has the function loaded
                    if !someone_has_it {
                        send_idx = 0;
                        send_function_id = *first_function_id;
                    }
                }

                // Skip if batch would be too small:
                if send_function_id != u64::MAX {
                    let mut requests_list = atoms_map.get_mut(&send_function_id).unwrap();
                    if self.idle_since.get().elapsed() < IDLE_BEFORE_BATCH_FORCED && requests_list.len() < SOFT_MIN_BATCHED {
                        send_function_id = u64::MAX;
                    }
                }

                // Actual send, if:
                if send_function_id != u64::MAX {
                    let mut requests_list = atoms_map.get_mut(&send_function_id).unwrap();
                    let (mut head_args, head_debt) = requests_list.pop_front().unwrap();

                    if head_debt.is_alive() {
                        // Prepare the batch:
                        let mut batched = 1;
                        let mut tmp_inputs_vec = Vec::new();
                        let mut call_children_debts = Vec::new();

                        // Copy inputs of first request
                        if let WorkToDo::BatchAtom {
                            ref inputs,
                            function_id: _,
                            recorder: _,
                            inputs_vec: _,
                            children_debts: _,
                        } = head_args
                        {
                            for input in inputs {
                                tmp_inputs_vec.push(input.clone().unwrap());
                            }
                        }

                        // Copy inputs of other requests
                        let inner_loop_max = cmp::min(requests_list.len(), MAX_BATCHED - 1);
                        for i in 1..=inner_loop_max {
                            let (child_args, child_debt) = requests_list.pop_front().unwrap();
                            if let WorkToDo::BatchAtom {
                                inputs,
                                function_id: _,
                                recorder: _,
                                inputs_vec: _,
                                children_debts: _,
                            } = child_args
                            {
                                for (i, input) in inputs.into_iter().enumerate() {
                                    tmp_inputs_vec[i].item_list.extend(input.unwrap().item_list);
                                }
                                call_children_debts.push(child_debt);
                            }
                        }
                        batched += inner_loop_max;
                        // println!("{} requests batched", batched);

                        // Workaround to support batch size 1
                        if batched == 1 {
                            let copy_first_input = tmp_inputs_vec[0].item_list[0].clone();
                            tmp_inputs_vec[0].item_list = vec![copy_first_input; FIXED_BATCH_SIZE];
                        }

                        // Remove the batched requests from the time-based queue
                        let mut idx = 0;
                        let mut removed = 0;
                        while removed < batched {
                            let (_, function_id) = &atoms_times[idx];
                            if *function_id == send_function_id {
                                atoms_times.remove(idx);
                                removed += 1;
                            } else {
                                idx += 1;
                            }
                        }

                        let mut call_inputs_vec = Vec::new();
                        for set in tmp_inputs_vec {
                            call_inputs_vec.push(Some(set));
                        }

                        // Make head request ready to run whole batch
                        if let WorkToDo::BatchAtom {
                            ref mut inputs_vec,
                            ref mut children_debts,
                            function_id: _,
                            inputs: _,
                            recorder: _,
                        } = head_args
                        {
                            *inputs_vec = Some(call_inputs_vec);
                            *children_debts = Some(call_children_debts);
                        }

                        break (head_args, head_debt);
                    }
                }
            }

            // If no WorkToDo is selected at all, update the counter
            self.worker_queue.start.fetch_add(1, Ordering::Release);
        };
        // Exiting the loop, update the counter
        self.worker_queue.start.fetch_add(1, Ordering::Release);
        return work;
    }
    /// models called with inter-call times that are randomly selected, to "overlap differently"

    fn try_get_engine_args(&self) -> Option<(WorkToDo, Debt)> {
        // TODO
        return None;
    }
}

#[cfg(feature = "auto_batching")]
impl EnqueueWork for BatchingQueue {
    fn enqueue_work(&self, args: WorkToDo, function_id: FunctionId) -> DandelionResult<Promise> {
        let (promise, debt) = self.promise_buffer.get_promise()?;

        match args {
            #[cfg(feature = "auto_batching")]
            WorkToDo::BatchAtom { .. } => {
                let now = Instant::now();

                let mut atoms_times = self.atoms_times.lock().unwrap();
                atoms_times.push_back((now, function_id));

                let mut atoms_map = self.atoms_map.lock().unwrap();
                atoms_map
                    .entry(function_id)
                    .or_insert(VecDeque::with_capacity(MAX_QUEUE));
                atoms_map
                    .get_mut(&function_id)
                    .unwrap()
                    .push_back((args, debt));
            }
            _ => match self.general_queue_in.try_send((args, debt)) {
                Ok(()) => (),
                Err(TrySendError::Disconnected(_)) => {
                    error!("Failed to enqueu work, workqueue has been disconnected")
                }
                Err(TrySendError::Full(_)) => return Err(DandelionError::WorkQueueFull),
            },
        }

        return Ok(promise);
    }
}

#[cfg(feature = "auto_batching")]
unsafe impl Send for BatchingQueue {}
#[cfg(feature = "auto_batching")]
unsafe impl Sync for BatchingQueue {}

#[cfg(feature = "auto_batching")]
impl BatchingQueue {
    pub fn new() -> Self {
        let engine_counter = Cell::new(0);
        let engine_id = 0;

        let (sender, receiver) = crossbeam::channel::bounded(MAX_QUEUE);

        let atoms_times = Arc::new(Mutex::new(VecDeque::with_capacity(MAX_QUEUE)));
        let atoms_map = Arc::new(Mutex::new(HashMap::new()));

        let last_function = Arc::new(Mutex::new(HashMap::new()));

        let idle_since = Cell::new(Instant::now());

        let tickets = AtomicTickets {
            start: AtomicUsize::new(0),
            end: AtomicUsize::new(0),
        };
        let worker_queue = Arc::new(tickets);

        let promise_buffer = PromiseBuffer::init(MAX_QUEUE);

        return BatchingQueue {
            engine_counter,
            engine_id,
            general_queue_in: sender,
            general_queue_out: receiver,
            atoms_times,
            atoms_map,
            last_function,
            idle_since,
            worker_queue,
            promise_buffer,
        };
    }
}

#[cfg(feature = "auto_batching")]
impl Clone for BatchingQueue {
    fn clone(&self) -> BatchingQueue {
        self.engine_counter.set(self.engine_counter.get() + 1);
        {
            let mut last_function = self.last_function.lock().unwrap();
            last_function.insert(self.engine_counter.get(), u64::MAX);
        }

        BatchingQueue {
            engine_counter: self.engine_counter.clone(),
            engine_id: self.engine_counter.get(),
            general_queue_in: self.general_queue_in.clone(),
            general_queue_out: self.general_queue_out.clone(),
            atoms_times: self.atoms_times.clone(),
            atoms_map: self.atoms_map.clone(),
            idle_since: Cell::new(Instant::now()),
            last_function: self.last_function.clone(),
            worker_queue: self.worker_queue.clone(),
            promise_buffer: self.promise_buffer.clone(),
        }
    }
}

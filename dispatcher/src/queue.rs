use std::{
    cell::UnsafeCell,
    pin::Pin,
    sync::{
        atomic::{AtomicU32, AtomicUsize, Ordering},
        Arc,
    },
};

use dandelion_commons::{DandelionError, DandelionResult};
use machine_interface::{
    function_driver::{EngineWorkQueue, WorkDone, WorkToDo},
    machine_config::EngineType,
    promise::{Debt, PromiseBuffer},
};

enum ElementState {
    Empty = 0,
    Available = 1,
    Taken = 2,
}

pub enum QueueFlag {
    EngineReqwestIO = 0b1,
    EngineCheri = 0b10,
    EngineWasm = 0b100,
    EngineProcess = 0b1000,
    EngineKvm = 0b1_0000,
}

pub fn get_engine_flag(t: EngineType) -> u32 {
    match t {
        #[cfg(feature = "reqwest_io")]
        EngineType::Reqwest => QueueFlag::EngineReqwestIO as u32,
        #[cfg(feature = "cheri")]
        EngineType::Cheri => QueueFlag::EngineCheri as u32,
        #[cfg(feature = "wasm")]
        EngineType::RWasm => QueueFlag::EngineWasm as u32,
        #[cfg(feature = "mmu")]
        EngineType::Process => QueueFlag::EngineProcess as u32,
        #[cfg(feature = "kvm")]
        EngineType::Kvm => QueueFlag::EngineKvm as u32,
    }
}

struct QueueElement {
    /// Atomic ElementState allowing different engine queues to check it
    state: AtomicU32,
    /// Flags indicating which engines can run this task
    flags: u32,
    /// The buffer lap in which this element was last updated (solves the issue where the head is
    /// advanced over an element that has been reserved, i.e. tail has moved, but not yet been assigned)
    lap: AtomicUsize,
    /// The WorkToDo content of the queue element
    work: Option<WorkToDo>,
    /// The Debt content of the queue element
    debt: Option<Debt>,
}

struct QueueInternal {
    /// The data buffer used as a ring buffer
    buffer: Pin<Box<[UnsafeCell<QueueElement>]>>,
    /// The head index of the queue
    head: AtomicUsize,
    /// The tail index of the queue
    tail: AtomicUsize,
}

impl QueueInternal {
    /// Sets the data and flags of the element at the given queue index.
    ///
    /// ***ASSUMES the element is empty and the calling thread has ownership of the index.***
    fn set_elem(&self, idx: usize, work: WorkToDo, debt: Debt, flags: u32, lap: usize) {
        let buf_idx = idx % self.buffer.len();
        let elem = self.buffer[buf_idx].get();
        unsafe {
            (*elem).work = Some(work);
            (*elem).debt = Some(debt);
            (*elem).flags = flags;
            (*elem)
                .state
                .store(ElementState::Available as u32, Ordering::Release);
            (*elem).lap.store(lap, Ordering::Release);
        }
    }

    /// Tries to acquire the element at the given queue index setting its state to taken.
    /// Returns true on success, false otherwise.
    fn acquire_elem(&self, idx: usize) -> bool {
        let buf_idx = idx % self.buffer.len();
        let elem = self.buffer[buf_idx].get();
        unsafe {
            let state = (*elem).state.load(Ordering::Acquire);
            if state != (ElementState::Available as u32) {
                return false;
            }
            if (*elem)
                .state
                .compare_exchange(
                    ElementState::Available as u32,
                    ElementState::Taken as u32,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
            {
                return false;
            }
            true
        }
    }

    /// Gets the data from the queue element then sets its state to empty and returns the data.
    ///
    /// ***ASSUMES the element is valid and the calling thread has acquired it already.***
    fn extract_elem(&self, idx: usize) -> (WorkToDo, Debt) {
        let buf_idx = idx % self.buffer.len();
        let elem = self.buffer[buf_idx].get();
        unsafe {
            let work = (*elem).work.take();
            let debt = (*elem).debt.take();
            (*elem).work = None;
            (*elem).debt = None;
            (*elem)
                .state
                .store(ElementState::Empty as u32, Ordering::Release);
            return (work.unwrap(), debt.unwrap());
        }
    }

    #[inline]
    fn get_elem_state(&self, idx: usize) -> u32 {
        let buf_idx = idx % self.buffer.len();
        let elem = self.buffer[buf_idx].get();
        unsafe {
            return (*elem).state.load(Ordering::Acquire);
        }
    }

    #[inline]
    fn get_elem_lap(&self, idx: usize) -> usize {
        let buf_idx = idx % self.buffer.len();
        let elem = self.buffer[buf_idx].get();
        unsafe {
            return (*elem).lap.load(Ordering::Acquire);
        }
    }

    #[inline]
    fn get_elem_flags(&self, idx: usize) -> u32 {
        let buf_idx = idx % self.buffer.len();
        let elem = self.buffer[buf_idx].get();
        unsafe {
            return (*elem).flags;
        }
    }
}

unsafe impl Sync for QueueInternal {}
unsafe impl Send for QueueInternal {}

/// Thread safe, lock-free ring buffer with fixed size.
///
/// Producers can push new work to the end of the queue using the `push` function.
/// Consumers can inspect elements using the `inspect` function and pop elements using the `aquire`
/// and `aquire_next` functions.
pub struct WorkQueue {
    inner: Arc<QueueInternal>,
    promise_buffer: PromiseBuffer,
}

impl WorkQueue {
    /// Creates a new WorkQueue of given size.
    pub fn init(capacity: usize) -> Self {
        let buffer_vec: Vec<UnsafeCell<QueueElement>> = std::iter::repeat_with(|| {
            UnsafeCell::new(QueueElement {
                state: AtomicU32::new(ElementState::Empty as u32),
                flags: 0,
                lap: AtomicUsize::new(0),
                work: None,
                debt: None,
            })
        })
        .take(capacity + 1) // capacity + 1 for buffer size
        .collect();
        let buffer = Pin::new(buffer_vec.into_boxed_slice());
        WorkQueue {
            inner: Arc::new(QueueInternal {
                buffer,
                head: AtomicUsize::new(0),
                tail: AtomicUsize::new(0),
            }),
            promise_buffer: PromiseBuffer::init(capacity),
        }
    }

    /// Pushes the work and debt to the back of the queue and sets the flags accordingly.
    /// Returns an error if the queue is full.
    pub fn push(&self, work: WorkToDo, debt: Debt, flags: u32) -> DandelionResult<()> {
        let buffer_len = self.inner.buffer.len();

        // acquire a buffer index to store the data
        let mut idx = self.inner.tail.load(Ordering::Acquire);
        loop {
            let new_tail = idx + 1;
            let head = self.inner.head.load(Ordering::Acquire);
            if new_tail >= head + buffer_len {
                return Err(DandelionError::WorkQueueFull);
            }

            match self.inner.tail.compare_exchange(
                idx,
                new_tail,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => {
                    idx = actual;
                    continue;
                }
            }
        }

        // store the data
        let lap = (idx / buffer_len) + 1;
        self.inner.set_elem(idx, work, debt, flags, lap);
        Ok(())
    }

    /// Tries to get the work and debt from the queue at given index.
    /// Returns None if the element is not available (empty or already taken) or the data if successful.
    pub fn acquire(&self, idx: usize) -> Option<(WorkToDo, Debt)> {
        let buffer_len = self.inner.buffer.len();

        // try to acquire the index
        if !self.inner.acquire_elem(idx) {
            return None;
        }

        // get content and release element
        let content = self.inner.extract_elem(idx);

        // move head forward
        loop {
            let curr_head = self.inner.head.load(Ordering::Acquire);
            let curr_tail = self.inner.tail.load(Ordering::Acquire);

            let mut new_head = curr_head;
            while new_head < curr_tail {
                let state = self.inner.get_elem_state(new_head);

                if state == (ElementState::Empty as u32) {
                    let expected_lap = self.inner.get_elem_lap(new_head);
                    let actual_lap = (new_head / buffer_len) + 1;

                    if actual_lap == expected_lap {
                        new_head += 1;
                    } else {
                        // in this case the producer has reserved this queue element but not yet written the data
                        break;
                    }
                } else {
                    // element is not empty
                    break;
                }
            }

            if self
                .inner
                .head
                .compare_exchange(curr_head, new_head, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
        }

        return Some(content);
    }

    /// Returns the queue element flags of given index if the element is available and None otherwise.
    pub fn inspect(&self, idx: usize) -> Option<u32> {
        let state = self.inner.get_elem_state(idx);
        if state == ElementState::Available as u32 {
            return Some(self.inner.get_elem_flags(idx));
        } else {
            return None;
        }
    }

    /// Inserts the work into the queue setting the flags according to the supported engines and
    /// awaits the future before returning the result.
    pub async fn do_work(
        &self,
        work: WorkToDo,
        engines: Vec<EngineType>,
    ) -> DandelionResult<WorkDone> {
        let mut flags = 0;
        for e in engines {
            flags |= get_engine_flag(e);
        }

        let (promise, debt) = self.promise_buffer.get_promise()?;
        self.push(work, debt, flags)?;

        return promise.await;
    }

    /// Inserts the work into the queue with the given engine flags and awaits the future before
    /// returning the result.
    pub async fn do_work_flags(
        &self,
        work: WorkToDo,
        engine_flags: u32,
    ) -> DandelionResult<WorkDone> {
        let (promise, debt) = self.promise_buffer.get_promise()?;
        self.push(work, debt, engine_flags)?;
        return promise.await;
    }

    /// Tries to acquire some work that matches the given flags starting from the head of the queue.
    pub fn try_get_work(&self, engine_flags: u32) -> Option<(WorkToDo, Debt)> {
        let buffer_len = self.inner.buffer.len();
        let mut idx = self.inner.head.load(Ordering::Acquire);
        let q_tail = self.inner.tail.load(Ordering::Acquire);
        while idx <= q_tail {
            let actual_lap = self.inner.get_elem_lap(idx);
            let expected_lap = (idx / buffer_len) + 1;
            if actual_lap != expected_lap {
                break;
            }

            let elem_flags = self.inner.get_elem_flags(idx);
            if elem_flags & engine_flags == engine_flags {
                let content = self.acquire(idx);
                if content.is_some() {
                    return content;
                }
            }

            idx += 1;
        }
        return None;
    }

    /// Spins on the queue until it manages to acquire some work that matches the given flags.
    pub fn get_work(&self, engine_flags: u32) -> (WorkToDo, Debt) {
        loop {
            if let Some(content) = self.try_get_work(engine_flags) {
                return content;
            }
        }
    }

    /// Returns true if the queue is empty and false otherwise.
    pub fn is_empty(&self) -> bool {
        let q_head = self.inner.head.load(Ordering::Acquire);
        let q_tail = self.inner.tail.load(Ordering::Acquire);
        return q_head == q_tail;
    }
}

impl Clone for WorkQueue {
    fn clone(&self) -> Self {
        WorkQueue {
            inner: Arc::clone(&self.inner),
            promise_buffer: self.promise_buffer.clone(),
        }
    }
}

/// Engine specific wrapper for the `WorkQueue` that implements the `EngineWorkQueue` trait.
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

    /// Inserts the work into the queue and awaits the result.
    pub async fn do_work(&self, work: WorkToDo) -> DandelionResult<WorkDone> {
        self.work_queue.do_work_flags(work, self.engine_flags).await
    }
}

impl EngineWorkQueue for EngineQueue {
    fn get_engine_args(&self) -> (WorkToDo, machine_interface::promise::Debt) {
        // TODO: add some ticket system for each engine type to reduce the number of workers
        // constantly accessing the queue elements
        self.work_queue.get_work(self.engine_flags)
    }

    fn try_get_engine_args(&self) -> Option<(WorkToDo, machine_interface::promise::Debt)> {
        self.work_queue.try_get_work(self.engine_flags)
    }
}

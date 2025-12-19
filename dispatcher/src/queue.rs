use dandelion_commons::DandelionResult;
use machine_interface::{
    function_driver::{EngineWorkQueue, WorkDone, WorkToDo},
    machine_config::EngineType,
    promise::{Debt, PromiseBuffer},
};
#[cfg(feature = "spin_queue")]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{
    collections::LinkedList,
    fmt,
    sync::{Arc, Mutex},
};

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
    /// Flags indicating which engines can run this task
    flags: u32,
    /// The WorkToDo content of the queue element
    work: WorkToDo,
    /// The Debt content of the queue element
    debt: Debt,
}

#[cfg(feature = "spin_queue")]
struct AtomicTickets {
    start: AtomicUsize,
    end: AtomicUsize,
}

/// Producers can push new work to the end of the queue using the `push` function.
/// Consumers can pop elements using the `aquire` function.
#[derive(Clone)]
pub struct WorkQueue {
    inner: Arc<Mutex<std::collections::LinkedList<QueueElement>>>,
    promise_buffer: PromiseBuffer,
    #[cfg(feature = "spin_queue")]
    tickets: Arc<AtomicTickets>,
}

impl WorkQueue {
    /// Creates a new WorkQueue of given size.
    pub fn init(capacity: usize) -> Self {
        WorkQueue {
            inner: Arc::new(Mutex::new(LinkedList::new())),
            promise_buffer: PromiseBuffer::init(capacity),
            #[cfg(feature = "spin_queue")]
            tickets: Arc::new(AtomicTickets {
                start: AtomicUsize::new(0),
                end: AtomicUsize::new(0),
            }),
        }
    }

    /// Pushes the work and debt to the back of the queue and sets the flags accordingly.
    /// Returns an error if the queue is full.
    fn push(&self, work: WorkToDo, debt: Debt, flags: u32) -> DandelionResult<()> {
        let mut queue_guard = self.inner.lock().expect("Work queue lock poisoned");
        queue_guard.push_back(QueueElement { flags, work, debt });
        Ok(())
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
        #[cfg(feature = "spin_queue")]
        {
            let queue_head = self.tickets.start.load(Ordering::Acquire);
            if self
                .tickets
                .end
                .compare_exchange(
                    queue_head,
                    queue_head + 1,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
            {
                return None;
            }
        }
        // May want to try lock here too, instead of lock, but since we have the ticket, should always succeed on locking
        let mut queue_guard = self.inner.lock().unwrap();
        let result = queue_guard
            .extract_if(|queue_element| queue_element.flags & engine_flags == engine_flags)
            .next()
            .map(|queue_element| (queue_element.work, queue_element.debt));
        #[cfg(feature = "spin_queue")]
        self.tickets.start.fetch_add(1, Ordering::AcqRel);
        result
    }

    /// Spins on the queue until it manages to acquire some work that matches the given flags.
    pub fn get_work(&self, engine_flags: u32) -> (WorkToDo, Debt) {
        loop {
            #[cfg(feature = "spin_queue")]
            {
                let local_ticket = self.tickets.end.fetch_add(1, Ordering::AcqRel);
                while local_ticket != self.tickets.start.load(Ordering::Acquire) {
                    core::hint::spin_loop();
                }
            }
            let mut queue_guard = self.inner.lock().unwrap();
            let result = queue_guard
                .extract_if(|queue_element| queue_element.flags & engine_flags == engine_flags)
                .next()
                .map(|queue_element| (queue_element.work, queue_element.debt));
            #[cfg(feature = "spin_queue")]
            self.tickets.start.fetch_add(1, Ordering::Release);
            if let Some(result_tupple) = result {
                return result_tupple;
            }
        }
    }
}

impl fmt::Debug for WorkQueue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "WorkQueue{{ length: {} }}",
            self.inner.lock().unwrap().len(),
        )
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

    /// Inserts the work into the queue and awaits the result.
    pub async fn do_work(&self, work: WorkToDo) -> DandelionResult<WorkDone> {
        self.work_queue.do_work_flags(work, self.engine_flags).await
    }
}

impl EngineWorkQueue for EngineQueue {
    fn get_engine_args(&self) -> (WorkToDo, machine_interface::promise::Debt) {
        self.work_queue.get_work(self.engine_flags)
    }

    fn try_get_engine_args(&self) -> Option<(WorkToDo, machine_interface::promise::Debt)> {
        self.work_queue.try_get_work(self.engine_flags)
    }
}

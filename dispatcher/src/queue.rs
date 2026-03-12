use dandelion_commons::DandelionResult;
use log::trace;
use machine_interface::{
    function_driver::{EngineWorkQueue, WorkDone, WorkToDo},
    machine_config::{EngineType, EnumCount},
    promise::{Debt, PromiseBuffer},
};
#[cfg(feature = "spin_queue")]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{
    collections::LinkedList,
    fmt,
    sync::{Arc, Mutex},
};

#[derive(Debug, Clone, Copy)]
pub enum Priority {
    High,
    BestEffort,
}

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

#[cfg(feature = "spin_queue")]
struct AtomicTickets {
    start: AtomicUsize,
    end: AtomicUsize,
}

/// Producers can push new work to the end of the queue using the `push` function.
/// Consumers can pop elements using the `aquire` function.
/// check high-priority queue first, falls back to best-effort
#[derive(Clone)]
pub struct WorkQueue {
    high: Arc<Mutex<std::collections::LinkedList<QueueElement>>>,
    best_effort: Arc<Mutex<std::collections::LinkedList<QueueElement>>>,
    promise_buffer: PromiseBuffer,
    #[cfg(feature = "spin_queue")]
    tickets: Arc<Box<[AtomicTickets]>>,
}

impl WorkQueue {
    /// Creates a new WorkQueue of given size.
    pub fn init(capacity: usize) -> Self {
        WorkQueue {
            high: Arc::new(Mutex::new(LinkedList::new())),
            best_effort: Arc::new(Mutex::new(LinkedList::new())),
            promise_buffer: PromiseBuffer::init(capacity),
            #[cfg(feature = "spin_queue")]
            tickets: Arc::new(
                (0..EngineType::COUNT)
                    .map(|_| AtomicTickets {
                        start: AtomicUsize::new(0),
                        end: AtomicUsize::new(0),
                    })
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            ),
        }
    }

    /// Pushes the work and debt to the back of the queue and sets the flags accordingly.
    /// Returns an error if the queue is full.
    fn push(&self, work: WorkToDo, debt: Debt, flags: u32, priority: Priority) -> DandelionResult<()> {
        let element = QueueElement { flags, work, debt };

        match priority {
            Priority::High => {
                let mut queue = self.high.lock().expect("High priority queue lock poisoned");
                queue.push_back(element);
            }
            Priority::BestEffort => {
                let mut queue = self.best_effort.lock().expect("Best effort priority queue lock poisoned");
                queue.push_back(element);
            }
        }
        Ok(())
    }

    // 
    fn extract_matching(queue: &mut LinkedList<QueueElement>, engine_flags: u32,) -> Option<(WorkToDo, Debt)> {
        queue
            .extract_if(|queue_element| queue_element.flags & engine_flags == engine_flags)
            .next()
            .map(|queue_element| (queue_element.work, queue_element.debt))
    }

    /// Inserts the work into the queue setting the flags according to the supported engines and
    /// awaits the future before returning the result.
    pub async fn do_work(&self, work: WorkToDo, priority: Priority) -> DandelionResult<WorkDone> {
        let flags = match &work {
            WorkToDo::Shutdown(engine_type) => get_engine_flag(*engine_type),
            WorkToDo::FunctionArguments {
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
        log::trace!("Enqueueing with flags: {} priority: {:?}", flags, priority);

        let (promise, debt) = self.promise_buffer.get_promise()?;
        self.push(work, debt, flags, priority)?;

        return promise.await;
    }

    /// Inserts the work into the queue with the given engine flags and awaits the future before
    /// returning the result.
    pub async fn do_work_flags(
        &self,
        work: WorkToDo,
        engine_flags: u32,
        priority: Priority
    ) -> DandelionResult<WorkDone> {
        let (promise, debt) = self.promise_buffer.get_promise()?;
        self.push(work, debt, engine_flags, priority)?;
        return promise.await;
    }

    /// Tries to acquire some work that matches the given flags
    /// check high-prio queue first, falls back to best effort
    pub fn try_get_work(
        &self,
        engine_flags: u32,
        engine_type: EngineType,
    ) -> Option<(WorkToDo, Debt)> {
        #[cfg(feature = "spin_queue")]
        {
            let queue_head = self.tickets[engine_type as usize]
                .start
                .load(Ordering::Acquire);
            if self.tickets[engine_type as usize]
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
        // check high-priority first
        {
                let mut queue = self.high.lock().unwrap();
                if let Some(result) = Self::extract_matching(&mut queue, engine_flags) {
                    #[cfg(feature = "spin_queue")]
                    self.tickets[engine_type as usize]
                        .start
                        .fetch_add(1, Ordering::AcqRel);
                    return Some(result);
                }
        }
        
        let result = {
            let mut queue = self.best_effort.lock().unwrap();
            Self::extract_matching(&mut queue, engine_flags)
        };
        #[cfg(feature = "spin_queue")]
        self.tickets[engine_type as usize]
            .start
            .fetch_add(1, Ordering::AcqRel);
        result
    }

    /// Spins on the queue until it manages to acquire some work that matches the given flags.
    pub fn get_work(&self, engine_flags: u32, engine_type: EngineType) -> (WorkToDo, Debt) {
        loop {
            #[cfg(feature = "spin_queue")]
            {
                let local_ticket = self.tickets[engine_type as usize]
                    .end
                    .fetch_add(1, Ordering::AcqRel);
                while local_ticket
                    != self.tickets[engine_type as usize]
                        .start
                        .load(Ordering::Acquire)
                {
                    core::hint::spin_loop();
                }
            }
            // Check high-priority queue first
            {
                let mut queue = self.high.lock().unwrap();
                if let Some(result) = Self::extract_matching(&mut queue, engine_flags) {
                    #[cfg(feature = "spin_queue")]
                    self.tickets[engine_type as usize]
                        .start
                        .fetch_add(1, Ordering::Release);
                    return result;
                }
            }
            // Then check best-effort queue
            {
                let mut queue = self.best_effort.lock().unwrap();
                if let Some(result) = Self::extract_matching(&mut queue, engine_flags) {
                    #[cfg(feature = "spin_queue")]
                    self.tickets[engine_type as usize]
                        .start
                        .fetch_add(1, Ordering::Release);
                    return result;
                }
            }
            #[cfg(feature = "spin_queue")]
            self.tickets[engine_type as usize]
                .start
                .fetch_add(1, Ordering::Release);
        }
    }
}

impl fmt::Debug for WorkQueue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "WorkQueue{{ high: {}, best_effort: {} }}",
            self.high.lock().unwrap().len(),
            self.best_effort.lock().unwrap().len(),
        )
    }
}

/// Engine specific wrapper for the `WorkQueue` that implements the `EngineWorkQueue` trait.
#[derive(Clone)]
pub struct EngineQueue {
    work_queue: WorkQueue,
    engine_flags: u32,
    engine_type: EngineType,
}

impl EngineQueue {
    /// Wraps the work queue and specialized it for the given engine type.
    pub fn init(work_queue: WorkQueue, engine_type: EngineType) -> Self {
        EngineQueue {
            work_queue,
            engine_flags: get_engine_flag(engine_type),
            engine_type,
        }
    }

    /// Inserts the work into the queue and awaits the result.
    pub async fn do_work(&self, work: WorkToDo, priority: Priority) -> DandelionResult<WorkDone> {
        self.work_queue
            .do_work_flags(work, self.engine_flags, priority)
            .await
    }
}

impl EngineWorkQueue for EngineQueue {
    fn get_engine_args(&self) -> (WorkToDo, machine_interface::promise::Debt) {
        self.work_queue
            .get_work(self.engine_flags, self.engine_type)
    }

    fn try_get_engine_args(&self) -> Option<(WorkToDo, machine_interface::promise::Debt)> {
        self.work_queue
            .try_get_work(self.engine_flags, self.engine_type)
    }
}

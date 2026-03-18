use crate::{
    function_driver::{EngineWorkQueue, WorkToDo},
    preemption::PreemptionRegistry,
    promise::{Debt, Promise, PromiseBuffer},
};
use dandelion_commons::Priority;
use std::sync::{Arc, Condvar, Mutex};

struct TestQueueInternal {
    args: Option<(WorkToDo, Debt)>,
}

#[derive(Clone)]
pub struct TestQueue {
    internal: Arc<(Mutex<TestQueueInternal>, Condvar)>,
    promise_buffer: PromiseBuffer,
    preemption_registry: Arc<PreemptionRegistry>,
}

impl TestQueue {
    pub fn new() -> Self {
        return TestQueue {
            internal: Arc::new((Mutex::new(TestQueueInternal { args: None }), Condvar::new())),
            promise_buffer: PromiseBuffer::init(128),
            preemption_registry: Arc::new(PreemptionRegistry::new()),
        };
    }
    pub fn enqueu(&self, args: WorkToDo) -> Promise {
        let (lock, arg_var) = self.internal.as_ref();
        let mut lock_guard = lock.lock().expect("Test queue failed to lock on enqueuing");
        if lock_guard.args.is_some() {
            lock_guard = arg_var
                .wait_while(lock_guard, |guard| guard.args.is_some())
                .expect("Test queue enqueue failed waiting on inserting args");
        }
        let (promise, debt) = self.promise_buffer.get_promise().unwrap();
        if lock_guard.args.replace((args, debt)).is_some() {
            panic!("Test queue replace args still present")
        };
        arg_var.notify_all();
        return promise;
    }
}

impl EngineWorkQueue for TestQueue {
    fn get_engine_args(&self) -> (WorkToDo, Debt, Priority) {
        let (lock, arg_var) = self.internal.as_ref();
        let mut lock_guard = lock
            .lock()
            .expect("Test queue failed to lock on get_engine_args");
        if lock_guard.args.is_none() {
            lock_guard = arg_var
                .wait_while(lock_guard, |guard| guard.args.is_none())
                .expect("Test queue failed waiting to take args");
        }
        let args = lock_guard
            .args
            .take()
            .expect("Test queue tried to take args from empty queue");
        arg_var.notify_all();

        return (args.0, args.1, Priority::BestEffort);
    }

    fn try_get_engine_args(&self) -> Option<(WorkToDo, crate::promise::Debt, Priority)> {
        return Some(self.get_engine_args());
    }

    fn preemption_registry(&self) -> &Arc<PreemptionRegistry> {
        &self.preemption_registry
    }
}

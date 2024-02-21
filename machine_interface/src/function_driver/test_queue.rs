use crate::{
    function_driver::{EngineArguments, WorkQueue},
    promise::Promise,
};
use std::sync::{Arc, Condvar, Mutex};

struct TestQueueInternal {
    args: Option<EngineArguments>,
    promise: Option<Promise>,
}

#[derive(Clone)]
pub struct TestQueue {
    internal: Arc<(Mutex<TestQueueInternal>, Condvar, Condvar)>,
}

impl TestQueue {
    pub fn new() -> Self {
        return TestQueue {
            internal: Arc::new((
                Mutex::new(TestQueueInternal {
                    args: None,
                    promise: None,
                }),
                Condvar::new(),
                Condvar::new(),
            )),
        };
    }
    pub fn enqueu(&self, args: EngineArguments) -> Promise {
        let (lock, arg_var, promise_var) = self.internal.as_ref();
        let mut lock_guard = lock.lock().expect("Test queue failed to lock on enqueuing");
        if lock_guard.args.is_some() {
            lock_guard = arg_var
                .wait_while(lock_guard, |guard| guard.args.is_some())
                .expect("Test queue enqueue failed waiting on inserting args");
        }
        if lock_guard.args.replace(args).is_some() {
            panic!("Test queue replace args still present")
        };
        arg_var.notify_all();
        if lock_guard.promise.is_none() {
            lock_guard = promise_var
                .wait_while(lock_guard, |guard| guard.promise.is_none())
                .expect("Test queue failed to lock on taking promise");
        }
        let promise = lock_guard
            .promise
            .take()
            .expect("Test queue return promis should be there because of condvar");
        promise_var.notify_all();
        return promise;
    }
}

impl WorkQueue for TestQueue {
    fn get_engine_args(&self, promise: Promise) -> EngineArguments {
        let (lock, arg_var, promise_var) = self.internal.as_ref();
        let mut lock_guard = lock
            .lock()
            .expect("Test queue failed to lock on get_engine_args");
        if lock_guard.promise.is_some() {
            lock_guard = promise_var
                .wait_while(lock_guard, |guard| guard.promise.is_some())
                .expect("Test queue failed to wait to place new promise");
        }
        if lock_guard.promise.replace(promise).is_some() {
            panic!("Test queue no promise should be present")
        };
        promise_var.notify_all();
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
        return args;
    }
}

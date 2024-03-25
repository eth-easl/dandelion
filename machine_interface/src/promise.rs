use crate::memory_domain::Context;

use core::{
    pin::Pin,
    ptr,
    sync::atomic::{AtomicPtr, AtomicU8, Ordering},
    task::Poll,
};
use dandelion_commons::{records::Recorder, DandelionError, DandelionResult};

pub struct Promise {
    data: *const PromiseData,
}
unsafe impl Send for Promise {}

impl Promise {
    pub fn new() -> (Promise, Debt) {
        let data = Box::new(PromiseData {
            abort_handle: AtomicPtr::new(ptr::null_mut()),
            results: AtomicPtr::new(ptr::null_mut()),
            waker: AtomicPtr::new(ptr::null_mut()),
            references: AtomicU8::new(2),
        });
        let data_ptr = Box::into_raw(data);
        let promise = Promise { data: data_ptr };
        let debt = Debt { data: data_ptr };
        return (promise, debt);
    }
    pub fn abort(self) -> () {
        core::mem::drop(self);
    }
    fn abort_internal(&mut self) {
        let data = unsafe { &*self.data };
        let abort_handle = data.abort_handle.swap(ptr::null_mut(), Ordering::SeqCst);
        if !abort_handle.is_null() {
            unsafe { (*abort_handle)() }
        }
    }
}

impl futures::future::Future for Promise {
    type Output = Box<DandelionResult<(Context, Recorder)>>;
    // as per documentation calling after it has resolved once is undefined
    // handle this by returning pending again
    fn poll(self: Pin<&mut Self>, cx: &mut core::task::Context<'_>) -> Poll<Self::Output> {
        let data = unsafe { &*self.data };
        // update the waker
        let old_waker_ptr = data.waker.swap(
            Box::into_raw(Box::new(cx.waker().clone())),
            Ordering::SeqCst,
        );
        if !old_waker_ptr.is_null() {
            let _ = unsafe { Box::from_raw(old_waker_ptr) };
        }
        // check if the result was put there in the mean time
        let results_ptr = data.results.swap(ptr::null_mut(), Ordering::SeqCst);
        if !results_ptr.is_null() {
            return Poll::Ready(unsafe { Box::from_raw(results_ptr) });
        } else {
            return Poll::Pending;
        }
    }
}

pub struct Debt {
    data: *const PromiseData,
}
unsafe impl Send for Debt {}

impl Debt {
    pub fn is_alive(&self) -> bool {
        let data = unsafe { &*self.data };
        return data.references.load(Ordering::Acquire) > 1;
    }

    pub fn fulfill(self, results: Box<DandelionResult<(Context, Recorder)>>) {
        let data = unsafe { &*self.data };
        // make sure we are not aborted by this promise anymore
        data.abort_handle.store(ptr::null_mut(), Ordering::Release);
        // write a result
        data.results.store(Box::into_raw(results), Ordering::SeqCst);
        let waker_ptr = data.waker.swap(ptr::null_mut(), Ordering::SeqCst);
        if !waker_ptr.is_null() {
            let waker = unsafe { Box::from_raw(waker_ptr) };
            waker.wake();
        }
    }
    pub fn install_abort_handle(&self, handle: fn()) {
        let data = unsafe { &*self.data };
        data.abort_handle
            .store(handle as *mut fn(), Ordering::Release);
    }
}

fn drop_promise_data(data_ptr: *const PromiseData) {
    let data = unsafe { &*data_ptr };
    if data.references.fetch_sub(1, Ordering::SeqCst) == 0 {
        let _ = unsafe { Box::from_raw(data_ptr as *mut PromiseData) };
    }
}

impl Drop for Promise {
    fn drop(&mut self) {
        self.abort_internal();
        drop_promise_data(self.data);
    }
}
impl Drop for Debt {
    fn drop(&mut self) {
        let data = unsafe { &*self.data };
        // make sure we can't get aborted by this handle anymore
        data.abort_handle.store(ptr::null_mut(), Ordering::Release);
        // if abort_handle is not null, there is still a promise waiting for a result
        if data.results.load(Ordering::Acquire).is_null() {
            // always pay your debts
            let error_box = Box::new(Err(DandelionError::PromiseDroppedDebt));
            // only need to store, because the debt is being dropped and only the debts could be used to fill in a result
            data.results
                .store(Box::into_raw(error_box), Ordering::SeqCst);
            let waker_ptr = data.waker.swap(ptr::null_mut(), Ordering::SeqCst);
            if !waker_ptr.is_null() {
                let waker = unsafe { Box::from_raw(waker_ptr) };
                waker.wake();
            }
        }
        drop_promise_data(self.data);
    }
}

struct PromiseData {
    /// Abort handle, only to be called once, as long as this value
    ///non null that means the function has not been aborted or terminated on it's own
    abort_handle: AtomicPtr<fn() -> ()>,
    /// Points to raw box of the results the engine has put in there
    results: AtomicPtr<DandelionResult<(Context, Recorder)>>,
    waker: AtomicPtr<core::task::Waker>,
    references: AtomicU8,
}

impl PromiseData {}

impl Drop for PromiseData {
    fn drop(&mut self) {
        // don't do anything for the abort handler, as the function does not need to be dropped,
        // and calling it is likely not useful, as the debt was also dropped
        // make sure to drop every pointer we still own
        let results_ptr = self.results.load(Ordering::Acquire);
        if !results_ptr.is_null() {
            let _ = unsafe { Box::from_raw(results_ptr) };
        }
        let waker_ptr = self.waker.load(Ordering::Acquire);
        if !waker_ptr.is_null() {
            // don't need to call before dropping, as the promise was dropped anyway
            let _ = unsafe { Box::from_raw(waker_ptr) };
        }
    }
}

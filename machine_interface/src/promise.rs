use crate::function_driver::WorkDone;

use core::{
    cell::Cell,
    mem::ManuallyDrop,
    pin::Pin,
    ptr,
    sync::atomic::{AtomicPtr, AtomicU8, Ordering},
    task::Poll,
};
use dandelion_commons::{err_dandelion, DandelionError, DandelionResult, PromiseError};
use futures::task::AtomicWaker;
use std::sync::{Arc, Mutex};

// debt sets content on drop so this is both the alive flag and the content lock
static DEBT_ALIVE: u8 = 0b0000_0001;
static PROMISE_ALIVE: u8 = 0b0000_0010;
// set while the debt still accesses the slot, cleared by the debt after its last access (the wake)
static DEBT_REF: u8 = 0b0000_0100;

type AbortHandle = Box<dyn FnOnce() + Send + 'static>;

struct PromiseData {
    /// Abort handle, taken out when called, dropped together with the data otherwise
    abort_handle: Mutex<Option<AbortHandle>>,
    /// Points to raw box of the results the engine has put in there
    results: Cell<DandelionResult<WorkDone>>,
    waker: AtomicWaker,
    flags: AtomicU8,
}

union DataWrapper {
    data: ManuallyDrop<PromiseData>,
    next: *mut DataWrapper,
}
unsafe impl Sync for DataWrapper {}
unsafe impl Send for DataWrapper {}

struct PromiseBufferInternal {
    head: AtomicPtr<DataWrapper>,
    _buffer: Pin<Box<[DataWrapper]>>,
}

impl PromiseBufferInternal {
    fn init(size: usize) -> Self {
        if size == 0 {
            panic!("Promisebuffer with 0 entries")
        }

        let mut vec_buffer = Vec::with_capacity(size);
        vec_buffer.resize_with(size, || DataWrapper {
            next: ptr::null_mut(),
        });
        let mut buffer = Pin::new(vec_buffer.into_boxed_slice());
        let head = AtomicPtr::new(ptr::addr_of!(buffer[0]).cast_mut());
        for index in 0..size - 1 {
            buffer[index].next = ptr::addr_of!(buffer[index + 1]).cast_mut();
        }
        return Self {
            head,
            _buffer: buffer,
        };
    }

    pub fn get_promise_data(&self) -> DandelionResult<*mut DataWrapper> {
        let mut current = self.head.load(Ordering::Acquire);
        if current.is_null() {
            return err_dandelion!(DandelionError::PromiseError(PromiseError::NoneAvailable));
        }
        let mut new_head = unsafe { (*current).next };
        while let Err(current_stored) =
            self.head
                .compare_exchange(current, new_head, Ordering::AcqRel, Ordering::Acquire)
        {
            current = current_stored;
            if current.is_null() {
                return err_dandelion!(DandelionError::PromiseError(PromiseError::NoneAvailable));
            }
            new_head = unsafe { (*current).next };
        }
        return Ok(current);
    }

    fn drop_promise_data(&self, data_ptr: *mut DataWrapper) {
        // drop data in union so we can reuse
        unsafe { ManuallyDrop::<PromiseData>::drop(&mut (*data_ptr).data) };
        // reinsert at head
        let mut head = self.head.load(Ordering::Acquire);
        unsafe { (*data_ptr).next = head };
        while let Err(current_head) =
            self.head
                .compare_exchange(head, data_ptr, Ordering::AcqRel, Ordering::Acquire)
        {
            head = current_head;
            unsafe { (*data_ptr).next = head };
        }
    }
}

#[derive(Clone)]
pub struct PromiseBuffer {
    internal: Arc<PromiseBufferInternal>,
}

impl PromiseBuffer {
    pub fn init(size: usize) -> Self {
        return Self {
            internal: Arc::new(PromiseBufferInternal::init(size)),
        };
    }

    pub fn get_promise(&self) -> DandelionResult<(Promise, Debt)> {
        let data_ptr = self.internal.get_promise_data()?;
        let data = unsafe { &mut (&mut *data_ptr).data };
        let default = ManuallyDrop::new(PromiseData {
            abort_handle: Mutex::new(None),
            results: Cell::new(err_dandelion!(DandelionError::PromiseError(
                PromiseError::DroppedDebt
            ))),
            waker: AtomicWaker::new(),
            flags: AtomicU8::new(DEBT_ALIVE | DEBT_REF | PROMISE_ALIVE),
        });
        *data = default;

        let promise = Promise {
            data: data_ptr,
            origin: self.internal.clone(),
        };
        let debt = Debt {
            data: data_ptr,
            origin: self.internal.clone(),
        };
        return Ok((promise, debt));
    }
}

pub struct Promise {
    data: *mut DataWrapper,
    origin: Arc<PromiseBufferInternal>,
}
unsafe impl Send for Promise {}

impl Promise {
    pub fn abort(self) -> () {
        core::mem::drop(self);
    }
    fn abort_internal(&mut self) {
        let data = unsafe { &(&*self.data).data };
        let abort_handler = take_abort_handle(data);
        // only abort if the debt has not already been dropped
        if data.flags.load(Ordering::Acquire) & DEBT_ALIVE != 0 {
            if let Some(abort_handler) = abort_handler {
                abort_handler();
            }
        }
    }
}

impl futures::future::Future for Promise {
    type Output = DandelionResult<WorkDone>;
    // as per documentation calling after it has resolved once is undefined
    // handle this by returning pending again
    fn poll(self: Pin<&mut Self>, cx: &mut core::task::Context<'_>) -> Poll<Self::Output> {
        let data = unsafe { &(&*self.data).data };

        // update the waker
        data.waker.register(cx.waker());

        let flags = data.flags.load(Ordering::Acquire);
        // the only changes the debt ever does is set the content or get dropped, which also sets content
        // if there was an error it could only have been seting the content.
        if flags & DEBT_ALIVE == 0 {
            return Poll::Ready(data.results.replace(err_dandelion!(
                DandelionError::PromiseError(PromiseError::TakenPromise,)
            )));
        } else {
            return Poll::Pending;
        }
    }
}

impl Drop for Promise {
    fn drop(&mut self) {
        self.abort_internal();
        let data = unsafe { &(&*self.data).data };
        let previous_flags = data.flags.fetch_and(!PROMISE_ALIVE, Ordering::SeqCst);
        // the debt may still be calling the waker after clearing DEBT_ALIVE, only free once it is done
        if (previous_flags & DEBT_REF) == 0 {
            self.origin.drop_promise_data(self.data);
        }
    }
}

pub struct Debt {
    data: *mut DataWrapper,
    origin: Arc<PromiseBufferInternal>,
}
unsafe impl Send for Debt {}

impl Debt {
    pub fn is_alive(&self) -> bool {
        let data = unsafe { &(&*self.data).data };
        return data.flags.load(Ordering::SeqCst) & PROMISE_ALIVE != 0;
    }

    pub fn fulfill(self, results: DandelionResult<WorkDone>) {
        let data = unsafe { &(&*self.data).data };
        // write a result, the flag will be set and the waker called when the drop is executed
        data.results.set(results);
    }

    // The installer needs to be able to deal with the abort arriving after the debt has been dropped.
    // This can happen due to race conditions when dropping / fulfilling the debt and calls to the abort.
    pub fn install_abort_handle<F>(&self, handle: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let data = unsafe { &(&*self.data).data };
        let old_handle = data
            .abort_handle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .replace(Box::new(handle));
        drop(old_handle);
    }

    /// Remove the abort handle, for example when the work it refers to is no longer in flight.
    pub fn clear_abort_handle(&self) {
        let data = unsafe { &(&*self.data).data };
        drop(take_abort_handle(data));
    }
}

fn take_abort_handle(data: &PromiseData) -> Option<AbortHandle> {
    return data
        .abort_handle
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .take();
}

impl Drop for Debt {
    fn drop(&mut self) {
        let data = unsafe { &(&*self.data).data };
        // mark debt as dropped, this publishes the result to the promise
        data.flags.fetch_and(!DEBT_ALIVE, Ordering::SeqCst);

        // call waker, if the promise has been dropped the waker should be able to deal with a delayed signal
        data.waker.wake();

        // release the reference to the slot, after this the debt must not touch the data anymore
        let previous_flags = data.flags.fetch_and(!DEBT_REF, Ordering::SeqCst);
        if previous_flags & PROMISE_ALIVE == 0 {
            self.origin.as_ref().drop_promise_data(self.data);
        }
    }
}

use crate::function_driver::WorkDone;

use core::{
    cell::Cell,
    mem::ManuallyDrop,
    pin::Pin,
    ptr,
    sync::atomic::{AtomicPtr, AtomicU8, Ordering},
    task::{Poll, Waker},
};
use dandelion_commons::{DandelionError, DandelionResult, PromiseError};
use std::sync::Arc;

static WAKER_INDEX: u8 = 0b0000_0001;
static DEBT_ALIVE: u8 = 0b0001_0000;
static PROMISE_ALIVE: u8 = 0b0010_0000;
static CONTENT_SET: u8 = 0b0100_0000;
static ALIVE: u8 = DEBT_ALIVE | PROMISE_ALIVE;

// pub struct PromiseBufferInternal {
//     head: AtomicPtr<ManuallyDrop<DataWrapper>>,
//     buffer: Box<[ManuallyDrop<DataWrapper>]>,
// }

// pub struct PromiseBuffer {
//     internal: Arc<PromiseBufferInternal>,
// }
// unsafe impl Send for PromiseBuffer {}
// unsafe impl Sync for PromiseBuffer {}

// impl PromiseBuffer {
//     pub fn init(size: usize) -> Self {
//         let mut vec_buffer = Vec::with_capacity(size);
//         vec_buffer.resize_with(size, || {
//             ManuallyDrop::new(DataWrapper {
//                 next: ptr::null_mut(),
//             })
//         });
//         let mut buffer = vec_buffer.into_boxed_slice();
//         let head_ptr = ptr::addr_of_mut!(buffer[0]);
//         // for index in 0..size - 1 {
//         // buffer[index].next = ptr::addr_of_mut!(buffer[index + 1]);
//         // }
//         return Self {
//             internal: Arc::new(PromiseBufferInternal {
//                 head: AtomicPtr::new(head_ptr),
//                 buffer,
//             }),
//         };
//     }

//     pub fn get_promise(&self) -> DandelionResult<(Promise, Debt)> {
//         // read head so we can take the next one
//         // let mut data_ptr = self.internal.head.load(Ordering::Acquire);
//         // if data_ptr.is_null() {
//         //     return Err(DandelionError::PromiseError(PromiseError::NoneAvailable));
//         // }
//         // let mut new_head = unsafe { (*data_ptr).next };
//         // loop {
//         //     data_ptr = match self.internal.head.compare_exchange(
//         //         data_ptr,
//         //         new_head,
//         //         Ordering::AcqRel,
//         //         Ordering::Acquire,
//         //     ) {
//         //         Ok(_) => break,
//         //         Err(new_head) => new_head,
//         //     };
//         //     if data_ptr.is_null() {
//         //         return Err(DandelionError::PromiseError(PromiseError::NoneAvailable));
//         //     }
//         //     new_head = unsafe { (*data_ptr).next };
//         // }
//         let data = Box::into_raw(Box::new(PromiseData {
//             abort_handle: AtomicPtr::new(ptr::null_mut()),
//             results: Cell::new(Err(DandelionError::PromiseError(PromiseError::Default))),
//             wakers: [Cell::new(None), Cell::new(None)],
//             flags: AtomicU8::new(DEBT_ALIVE | PROMISE_ALIVE),
//         }));
//         // let data;
//         // unsafe {
//         //     // data = &mut (*data_ptr).data as *mut ManuallyDrop<PromiseData>;
//         //     (*data).abort_handle = AtomicPtr::new(ptr::null_mut());
//         //     (*data).results = Cell::new(Err(DandelionError::PromiseError(PromiseError::Default)));
//         //     (*data).wakers = [Cell::new(None), Cell::new(None)];
//         //     (*data).flags = AtomicU8::new(DEBT_ALIVE | PROMISE_ALIVE);
//         // }
//         // println!(
//         //     "found space at {:?} (converted: {:?}), with buffer from {:?} to {:?}",
//         //     data_ptr,
//         //     data,
//         //     ptr::addr_of!(self.internal.buffer[0]),
//         //     ptr::addr_of!(self.internal.buffer[self.internal.buffer.len() - 1])
//         // );
//         let promise = Promise {
//             data,
//             origin: self.internal.clone(),
//         };
//         let debt = Debt {
//             data,
//             origin: self.internal.clone(),
//         };
//         return Ok((promise, debt));
//     }
// }

// union DataWrapper {
//     data: ManuallyDrop<PromiseData>,
//     next: *mut ManuallyDrop<DataWrapper>,
// }

struct PromiseData {
    /// Abort handle, only to be called once, as long as this value
    ///non null that means the function has not been aborted or terminated on it's own
    abort_handle: AtomicPtr<fn() -> ()>,
    /// Points to raw box of the results the engine has put in there
    results: Cell<DandelionResult<WorkDone>>,
    /// TODO replace Option<Waker> with only waker when Waker::noop stabilizes,
    /// and we can use it as default and use clone_from() on the cells
    wakers: [Cell<Option<Waker>>; 2],
    flags: AtomicU8,
}

fn drop_promise_data(data_ptr: *const PromiseData, drop_origin: u8) {
    let data = unsafe { &*data_ptr };
    let previous_flags = data.flags.fetch_and(!drop_origin, Ordering::SeqCst);
    if ((previous_flags & !drop_origin) & ALIVE) == 0 {
        let _ = unsafe { Box::from_raw(data_ptr as *mut PromiseData) };
    }
}

pub struct Promise {
    data: *const PromiseData,
    // origin: Arc<PromiseBufferInternal>,
}
unsafe impl Send for Promise {}

impl Promise {
    pub fn new() -> (Promise, Debt) {
        let data = Box::new(PromiseData {
            abort_handle: AtomicPtr::new(ptr::null_mut()),
            results: Cell::new(Err(DandelionError::PromiseError(PromiseError::Default))),
            wakers: [Cell::new(None), Cell::new(None)],
            flags: AtomicU8::new(DEBT_ALIVE | PROMISE_ALIVE),
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
    type Output = DandelionResult<WorkDone>;
    // as per documentation calling after it has resolved once is undefined
    // handle this by returning pending again
    fn poll(self: Pin<&mut Self>, cx: &mut core::task::Context<'_>) -> Poll<Self::Output> {
        let data = unsafe { &*self.data };
        let flags = data.flags.load(Ordering::SeqCst);

        // update the waker
        let waker_index = (flags & WAKER_INDEX) ^ WAKER_INDEX;
        data.wakers[usize::from(waker_index)].set(Some(cx.waker().clone()));

        // set waker to next one, to ensure the fulfill never reads a waker wrie in progress,
        // because of 2 polls after each other where the fulfill has the index of the second
        // waker being written. This is automatically avoided, by checking for changes in the
        // debt flags.
        // want to take content if there is some and flip index
        let new_flags = (flags ^ WAKER_INDEX) & !CONTENT_SET;
        let current_flags =
            match data
                .flags
                .compare_exchange(flags, new_flags, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(changed_flags) | Err(changed_flags) => changed_flags,
            };

        // the only changes the debt ever does is set the content or get dropped, which also sets content
        // if there was an error it could only have been seting the content.
        if current_flags & CONTENT_SET != 0 {
            return Poll::Ready(data.results.replace(Err(DandelionError::PromiseError(
                PromiseError::TakenPromise,
            ))));
        } else {
            return Poll::Pending;
        }
    }
}

pub struct Debt {
    data: *const PromiseData,
    // origin: Arc<PromiseBufferInternal>,
}
unsafe impl Send for Debt {}

impl Debt {
    pub fn is_alive(&self) -> bool {
        let data = unsafe { &*self.data };
        return data.flags.load(Ordering::SeqCst) & PROMISE_ALIVE != 0;
    }

    pub fn fulfill(self, results: DandelionResult<WorkDone>) {
        let data = unsafe { &*self.data };
        // make sure we are not aborted by this promise anymore
        data.abort_handle.store(ptr::null_mut(), Ordering::SeqCst);
        // write a result
        data.results.set(results);
        let flags = data.flags.fetch_or(CONTENT_SET, Ordering::SeqCst);
        let waker_index = flags & WAKER_INDEX;
        if let Some(waker) = data.wakers[usize::from(waker_index)].take() {
            waker.wake();
        }
    }
    pub fn install_abort_handle(&self, handle: fn()) {
        let data = unsafe { &*self.data };
        data.abort_handle
            .store(handle as *mut fn(), Ordering::SeqCst);
    }
}

impl Drop for Promise {
    fn drop(&mut self) {
        self.abort_internal();
        drop_promise_data(self.data, PROMISE_ALIVE);
    }
}
impl Drop for Debt {
    fn drop(&mut self) {
        let data = unsafe { &*self.data };
        // make sure we can't get aborted by this handle anymore
        data.abort_handle.store(ptr::null_mut(), Ordering::SeqCst);
        // if promise is still alive, there is still a promise waiting for a result
        let flags = data.flags.load(Ordering::SeqCst);
        if flags & PROMISE_ALIVE == 1 {
            // always pay your debts
            if flags & CONTENT_SET == 0 {
                data.results
                    .set(Err(DandelionError::PromiseError(PromiseError::DroppedDebt)));
                data.flags.fetch_or(CONTENT_SET, Ordering::SeqCst);
            }
            let waker_index = data.flags.load(Ordering::SeqCst) & WAKER_INDEX;
            if let Some(waker) = data.wakers[usize::from(waker_index)].take() {
                waker.wake();
            }
        }
        drop_promise_data(self.data, DEBT_ALIVE);
    }
}

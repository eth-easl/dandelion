// Preemption infrastructure: when high-priority work arrives, the queue calls
// registry.preempt_one() which sets a flag and sends SIGUSR1 to a best-effort
// engine thread, causing vcpu.run() to return VcpuExit::Intr.

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

use libc::pthread_t;
use log::debug;

pub struct PreemptionHandle {
    pub thread_id: pthread_t,
    pub preempt_flag: Arc<AtomicBool>,
}

/// Registry of engine threads currently running best-effort work.
pub struct PreemptionRegistry {
    handles: Mutex<Vec<PreemptionHandle>>,
}

impl PreemptionRegistry {
    pub fn new() -> Self {
        PreemptionRegistry {
            handles: Mutex::new(Vec::new()),
        }
    }

    /// Register a thread as running best-effort work.
    pub fn register(&self, thread_id: pthread_t, preempt_flag: Arc<AtomicBool>) {
        let mut handles = self.handles.lock().expect("PreemptionRegistry lock poisoned");
        handles.push(PreemptionHandle {
            thread_id,
            preempt_flag,
        });
        debug!("Registered thread {:?}, total: {}", thread_id, handles.len());
    }

    /// Deregister a thread (finished or preempted).
    pub fn deregister(&self, thread_id: pthread_t) {
        let mut handles = self.handles.lock().expect("PreemptionRegistry lock poisoned");
        handles.retain(|h| h.thread_id != thread_id);
        debug!("Deregistered thread {:?}, remaining: {}", thread_id, handles.len());
    }

    /// Preempt one best-effort thread: set its flag and send SIGUSR1.
    pub fn preempt_one(&self) -> bool {
        let handles = self.handles.lock().expect("PreemptionRegistry lock poisoned");
        if let Some(handle) = handles.first() {
            handle.preempt_flag.store(true, Ordering::Release);

            let ret = unsafe { libc::pthread_kill(handle.thread_id, libc::SIGUSR1) };
            if ret != 0 {
                debug!("Failed to send SIGUSR1 to thread {:?}, error: {}", handle.thread_id, ret);
                return false;
            }

            debug!("Sent preemption signal to thread {:?}", handle.thread_id);
            return true;
        }

        debug!("No best-effort threads to preempt");
        false
    }
}

/// Install empty SIGUSR1 handler. The signal just interrupts vcpu.run() (returns VcpuExit::Intr).
/// Must be called once per engine thread before entering the run loop.
pub fn install_signal_handler() {
    unsafe {
        let mut sa: libc::sigaction = std::mem::zeroed();
        sa.sa_sigaction = empty_signal_handler as *const () as usize;
        sa.sa_flags = 0; // No SA_RESTART — we want syscalls to be interrupted
        libc::sigemptyset(&mut sa.sa_mask);
        let ret = libc::sigaction(libc::SIGUSR1, &sa, std::ptr::null_mut());
        if ret != 0 {
            panic!("Failed to install SIGUSR1 handler: {}", ret);
        }
    }
    debug!("Installed SIGUSR1 signal handler");
}

/// Empty handler — the signal itself is what matters, not the handler logic.
extern "C" fn empty_signal_handler(_signum: libc::c_int) {}

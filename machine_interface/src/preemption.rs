// Preemption infrastructure for signaling engine threads to stop running
//
// When the queue receives high-priority work, it calls
// registry.preempt_one(), which:
// 1. Sets the preemption flag on a registered thread
// 2. Sends SIGUSR1 to that thread, interrupting vcpu.run()
//
// The signal causes vcpu.run() to return VcpuExit::Intr. The run loop
// checks the preemption flag and returns Err(Preempted).

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

use libc::pthread_t;
use log::debug;

/// Handle for a single engine thread that can be preempted.
pub struct PreemptionHandle {
    /// The pthread ID of the engine thread, used to send SIGUSR1.
    pub thread_id: pthread_t,
    /// The preemption flag shared with the KVM run loop.
    /// Setting this to `true` causes the run loop to exit at the next check.
    pub preempt_flag: Arc<AtomicBool>,
}

/// Thread-safe registry of engine threads currently running best-effort work.
/// Used by the queue to find and preempt a thread when high-priority work arrives.
pub struct PreemptionRegistry {
    /// List of currently active (running best-effort) engine thread handles.
    handles: Mutex<Vec<PreemptionHandle>>,
}

impl PreemptionRegistry {
    pub fn new() -> Self {
        PreemptionRegistry {
            handles: Mutex::new(Vec::new()),
        }
    }

    /// Register an engine thread as running best-effort work.
    /// Called by the engine thread when it starts executing a best-effort function.
    pub fn register(&self, thread_id: pthread_t, preempt_flag: Arc<AtomicBool>) {
        let mut handles = self.handles.lock().expect("PreemptionRegistry lock poisoned");
        handles.push(PreemptionHandle {
            thread_id,
            preempt_flag,
        });
        debug!(
            "Registered preemption handle for thread {:?}, total handles: {}",
            thread_id,
            handles.len()
        );
    }

    /// Deregister an engine thread (it finished or was preempted).
    /// Called by the engine thread when it finishes the current function.
    pub fn deregister(&self, thread_id: pthread_t) {
        let mut handles = self.handles.lock().expect("PreemptionRegistry lock poisoned");
        handles.retain(|h| h.thread_id != thread_id);
        debug!(
            "Deregistered preemption handle for thread {:?}, remaining handles: {}",
            thread_id,
            handles.len()
        );
    }

    /// Preempt one engine thread currently running best-effort work.
    /// Sets the preemption flag and sends SIGUSR1 to interrupt `vcpu.run()`.
    /// Returns `true` if a thread was preempted, `false` if none were available.
    pub fn preempt_one(&self) -> bool {
        let handles = self.handles.lock().expect("PreemptionRegistry lock poisoned");
        if let Some(handle) = handles.first() {
            // Set the preemption flag — the run loop will check this
            handle.preempt_flag.store(true, Ordering::Release);

            // Send SIGUSR1 to the thread to kick it out of vcpu.run()
            let ret = unsafe { libc::pthread_kill(handle.thread_id, libc::SIGUSR1) };
            if ret != 0 {
                debug!(
                    "Failed to send SIGUSR1 to thread {:?}, error: {}",
                    handle.thread_id, ret
                );
                return false;
            }

            debug!(
                "Sent preemption signal to thread {:?}",
                handle.thread_id
            );
            return true;
        }

        debug!("No best-effort threads to preempt");
        false
    }
}

/// Install the SIGUSR1 signal handler for the current thread.
/// The handler is intentionally empty — the signal's only purpose is to
/// interrupt the `vcpu.run()` syscall (KVM_RUN ioctl), causing it to
/// return with `VcpuExit::Intr`. The actual preemption logic is handled
/// by the AtomicBool flag check in the run loop.
///
/// Must be called once per engine thread, before entering the run loop.
pub fn install_signal_handler() {
    unsafe {
        let mut sa: libc::sigaction = std::mem::zeroed();
        // Empty handler — just needs to exist so the signal is "handled"
        // rather than using the default action (which would terminate the process)
        sa.sa_sigaction = empty_signal_handler as *const () as usize;
        sa.sa_flags = 0; // No SA_RESTART — we want syscalls to be interrupted
        libc::sigemptyset(&mut sa.sa_mask);
        let ret = libc::sigaction(libc::SIGUSR1, &sa, std::ptr::null_mut());
        if ret != 0 {
            panic!("Failed to install SIGUSR1 handler: {}", ret);
        }
    }
    debug!("Installed SIGUSR1 signal handler for preemption");
}

/// Empty signal handler. The signal itself is what matters (interrupting KVM_RUN),
/// not the handler logic. The preemption flag is already set before the signal is sent.
extern "C" fn empty_signal_handler(_signum: libc::c_int) {
    // Intentionally empty.
    // The preempt_flag AtomicBool is set by preempt_one() BEFORE sending the signal,
    // so the run loop will see it when it checks after VcpuExit::Intr.
}

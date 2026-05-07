// Preemption infrastructure: when high-priority work arrives, the queue calls
// registry.preempt_one() which sets a flag and sends SIGUSR1 to a best-effort
// engine thread, causing vcpu.run() to return VcpuExit::Intr.

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::Instant;

use libc::pthread_t;
use log::{debug, info};

pub struct PreemptionHandle {
    pub thread_id: pthread_t,
    pub preempt_flag: Arc<AtomicBool>,
    pub started_at: Instant,
    /// Inactive handles stay in the registry so per-thread state (preempt_count)
    /// survives register/deregister cycles. preempt_one() filters by this flag.
    pub active: bool,
    pub preempt_count: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PreemptionPolicy {
    LongestRunning,
    Lifo,
    Fair,
}

impl Default for PreemptionPolicy {
    fn default() -> Self {
        PreemptionPolicy::LongestRunning
    }
}

pub struct PreemptionRegistry {
    handles: Mutex<Vec<PreemptionHandle>>,
    policy: PreemptionPolicy,
}

impl PreemptionRegistry {
    /// Read DANDELION_PREEMPTION_POLICY ("lifo", "longest_running", "fair") and
    /// fall back to LongestRunning if unset or unrecognised.
    pub fn new() -> Self {
        let raw = std::env::var("DANDELION_PREEMPTION_POLICY").unwrap_or_default();
        let policy = match raw.to_ascii_lowercase().as_str() {
            "lifo" => PreemptionPolicy::Lifo,
            "longest_running" | "longest" => PreemptionPolicy::LongestRunning,
            "fair" => PreemptionPolicy::Fair,
            _ => PreemptionPolicy::default(),
        };
        info!("PreemptionRegistry initialised with policy {:?}", policy);
        Self::with_policy(policy)
    }

    pub fn with_policy(policy: PreemptionPolicy) -> Self {
        PreemptionRegistry {
            handles: Mutex::new(Vec::new()),
            policy,
        }
    }

    pub fn policy(&self) -> PreemptionPolicy {
        self.policy
    }

    pub fn register(&self, thread_id: pthread_t, preempt_flag: Arc<AtomicBool>, started_at: Instant) {
        let mut handles = self.handles.lock().expect("PreemptionRegistry lock poisoned");
        if let Some(existing) = handles.iter_mut().find(|h| h.thread_id == thread_id) {
            existing.preempt_flag = preempt_flag;
            existing.started_at = started_at;
            existing.active = true;
        } else {
            handles.push(PreemptionHandle {
                thread_id,
                preempt_flag,
                started_at,
                active: true,
                preempt_count: 0,
            });
        }
        let active = handles.iter().filter(|h| h.active).count();
        debug!("Registered thread {:?}, active: {}", thread_id, active);
    }

    pub fn deregister(&self, thread_id: pthread_t) {
        let mut handles = self.handles.lock().expect("PreemptionRegistry lock poisoned");
        if let Some(h) = handles.iter_mut().find(|h| h.thread_id == thread_id) {
            h.active = false;
        }
        let active = handles.iter().filter(|h| h.active).count();
        debug!("Deregistered thread {:?}, active: {}", thread_id, active);
    }

    pub fn preempt_one(&self) -> bool {
        let mut handles = self.handles.lock().expect("PreemptionRegistry lock poisoned");
        let active_idxs: Vec<usize> = (0..handles.len()).filter(|&i| handles[i].active).collect();
        let chosen_idx: Option<usize> = match self.policy {
            PreemptionPolicy::LongestRunning => {
                active_idxs.into_iter().min_by_key(|&i| handles[i].started_at)
            }
            PreemptionPolicy::Lifo => {
                active_idxs.into_iter().max_by_key(|&i| handles[i].started_at)
            }
            PreemptionPolicy::Fair => {
                active_idxs.into_iter().min_by(|&a, &b| {
                    handles[a].preempt_count
                        .cmp(&handles[b].preempt_count)
                        .then_with(|| handles[a].started_at.cmp(&handles[b].started_at))
                })
            }
        };

        if let Some(idx) = chosen_idx {
            handles[idx].preempt_count += 1;
            let handle = &handles[idx];
            handle.preempt_flag.store(true, Ordering::Release);

            let ret = unsafe { libc::pthread_kill(handle.thread_id, libc::SIGUSR1) };
            if ret != 0 {
                debug!("Failed to send SIGUSR1 to thread {:?}, error: {}", handle.thread_id, ret);
                return false;
            }

            debug!(
                "Sent preemption signal to thread {:?} (policy: {:?}, preempt_count now {})",
                handle.thread_id, self.policy, handle.preempt_count
            );
            return true;
        }

        debug!("No best-effort threads to preempt");
        false
    }
}

/// Install empty SIGUSR1 handler. The signal interrupts vcpu.run() (returns
/// VcpuExit::Intr); the handler itself does nothing. Must be called once per
/// engine thread before entering the run loop.
pub fn install_signal_handler() {
    unsafe {
        let mut sa: libc::sigaction = std::mem::zeroed();
        sa.sa_sigaction = empty_signal_handler as *const () as usize;
        // No SA_RESTART so syscalls get interrupted.
        sa.sa_flags = 0;
        libc::sigemptyset(&mut sa.sa_mask);
        let ret = libc::sigaction(libc::SIGUSR1, &sa, std::ptr::null_mut());
        if ret != 0 {
            panic!("Failed to install SIGUSR1 handler: {}", ret);
        }
    }
    debug!("Installed SIGUSR1 signal handler");
}

extern "C" fn empty_signal_handler(_signum: libc::c_int) {}

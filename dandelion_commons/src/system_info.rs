use crate::{err_dandelion, DandelionError, DandelionResult, DispatcherError};
use log::trace;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::watch;

/// Contains system information about the available compute cores.
pub struct SystemInfo {
    /// The number of local compute cores in the system (as a watcher so we can update remote nodes
    /// if the local core count changes).
    num_local_cores_watcher: watch::Receiver<usize>,
    num_local_cores_sender: watch::Sender<usize>,
    /// The number of remote compute cores in the system.
    num_remote_cores: AtomicUsize,
}

impl SystemInfo {
    /// Creates a new system info without any local or remote cores.
    pub fn new() -> Self {
        let (num_local_cores_sender, num_local_cores_watcher) = watch::channel(0);
        SystemInfo {
            num_local_cores_watcher,
            num_local_cores_sender,
            num_remote_cores: AtomicUsize::new(0),
        }
    }

    /// The current number of local compute cores.
    pub fn local_cores(&self) -> usize {
        *self.num_local_cores_watcher.borrow()
    }

    /// The current number of remote compute cores.
    pub fn remote_cores(&self) -> usize {
        self.num_remote_cores.load(Ordering::Acquire)
    }

    /// A watcher that gets notified whenever the number of local compute cores changes.
    pub fn local_cores_watcher(&self) -> watch::Receiver<usize> {
        self.num_local_cores_watcher.clone()
    }

    /// Increases the number of local cores.
    pub fn add_local_cores(&self, num_cores: usize) {
        self.num_local_cores_sender.send_modify(|curr| {
            *curr += num_cores;
            trace!(
                "Added {} local core(s). New number of local cores: {}",
                num_cores,
                *curr
            );
        });
    }

    /// Decreases the number of local cores.
    pub fn remove_local_cores(&self, num_cores: usize) -> DandelionResult<()> {
        let removed = self.num_local_cores_sender.send_if_modified(|curr| {
            if *curr < num_cores {
                false
            } else {
                *curr -= num_cores;
                trace!(
                    "Removed {} local core(s). New number of local cores: {}",
                    num_cores,
                    *curr
                );
                true
            }
        });
        if removed {
            Ok(())
        } else {
            err_dandelion!(DandelionError::Dispatcher(
                DispatcherError::InvalidSytemInformation
            ))
        }
    }

    /// Increases the number of remote cores.
    pub fn add_remote_cores(&self, num_cores: usize) {
        let prev_num_cores = self.num_remote_cores.fetch_add(num_cores, Ordering::AcqRel);
        trace!(
            "Added {} remote core(s). New number of remote cores: {}",
            num_cores,
            prev_num_cores + num_cores
        );
    }

    /// Decreases the number of remote cores.
    pub fn remove_remote_cores(&self, num_cores: usize) -> DandelionResult<()> {
        let mut curr_remote_cores = self.num_remote_cores.load(Ordering::Acquire);
        loop {
            if curr_remote_cores < num_cores {
                return err_dandelion!(DandelionError::Dispatcher(
                    DispatcherError::InvalidSytemInformation
                ));
            }
            let new_val = curr_remote_cores - num_cores;
            match self.num_remote_cores.compare_exchange(
                curr_remote_cores,
                new_val,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    trace!(
                        "Removed {} remote core(s). New number of remote cores: {}",
                        num_cores,
                        new_val
                    );
                    return Ok(());
                }
                Err(val) => curr_remote_cores = val,
            }
        }
    }
}

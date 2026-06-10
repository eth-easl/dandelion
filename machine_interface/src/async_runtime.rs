use log::debug;
use nix::{sched::CpuSet, unistd::Pid};
use std::{
    collections::BTreeSet,
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, LazyLock, Mutex, OnceLock,
    },
};

pub static MAX_SYS_CORES: OnceLock<usize> = OnceLock::new();
pub static MIN_SYS_CORESET: OnceLock<CpuSet> = OnceLock::new();

/// Global runtime for all asynchronous
/// The runtime is initiallized the first time it is dereferenced.
/// At that point it will also check if the number of MAX_ASYNC_CORES has been set.
/// If no limit has been set, it will initialize itself to possibly use all cores on the server.
/// This means it spawns threads and pins them to each core, but blocks them from running until they are specifically enabled.
pub static GLOBAL_RUNTIME: LazyLock<AysncRuntime> = LazyLock::new(|| AysncRuntime::new());

/// The single async runtime for dandelion
pub struct AysncRuntime {
    /// The runtime to use to drive the async tasks.
    runtime: tokio::runtime::Runtime,
    core_set: Mutex<nix::sched::CpuSet>,
    threads: Arc<Mutex<BTreeSet<Pid>>>,
}

impl AysncRuntime {
    pub fn new() -> Self {
        let max_io_cores = *MAX_SYS_CORES.get_or_init(|| num_cpus::get_physical());
        // TODO: should document the defaults better / think if these are sensible
        let min_core_set = *MIN_SYS_CORESET.get_or_init(|| {
            let mut set = CpuSet::new();
            set.set(0).unwrap();
            set
        });
        let threads = Arc::new(Mutex::new(BTreeSet::new()));
        let threads_function = threads.clone();
        // set to +1 so we can subtract 1 enought times before the result is 0
        let static_cores = Arc::new(AtomicUsize::new(max_io_cores + 1));
        let start_func = move || {
            log::debug!("starting a new thread");
            // register all the worker threads to the thread set
            let thread_index = static_cores
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |previous| {
                    Some(previous.saturating_sub(1))
                })
                .unwrap();
            // For worker threads this should be > 0, then register to get set expanded if more cores are added later
            if thread_index > 0 {
                log::debug!("starting another thread that will block");
                let tid = nix::unistd::gettid();
                threads_function.lock().unwrap().insert(tid);
            }
            // Temporary blocking threads are spawned by the existing runtime threads,
            // i.e. they inherit their affinity, could leave out setting the affinity for those, if we don't expect to change the set.
            // TODO: check what happens on thread panic,
            // the runtime might try to restart a new workwer thread that now does not register to get the set extended.
            nix::sched::sched_setaffinity(nix::unistd::Pid::from_raw(0), &min_core_set).unwrap();
            log::debug!("Finish starting new thread");
        };
        AysncRuntime {
            runtime: tokio::runtime::Builder::new_multi_thread()
                .worker_threads(max_io_cores)
                .enable_io()
                .enable_time()
                .thread_name("GLOBAL_RUNTIME_THREAD")
                .on_thread_start(start_func)
                .build()
                .unwrap(),
            threads,
            core_set: Mutex::new(min_core_set),
        }
    }

    pub fn add_core(&self, index: usize) {
        let mut core_set_guard = self.core_set.lock().unwrap();
        core_set_guard.set(index).unwrap();
        let new_set = *core_set_guard;
        debug!("Updated async runtime core set now: {:?}", new_set);
        drop(core_set_guard);
        // adjust all core sets for all tids
        for tid in self.threads.lock().unwrap().iter() {
            nix::sched::sched_setaffinity(*tid, &new_set).unwrap();
        }
    }

    pub fn spawn<F>(&self, future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.runtime.spawn(future)
    }

    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.runtime.block_on(future)
    }
}

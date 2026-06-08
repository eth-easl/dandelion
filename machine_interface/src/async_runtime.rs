use nix::sched::CpuSet;
use std::{
    future::Future,
    sync::{
        mpsc::{channel, Receiver, Sender},
        LazyLock, Mutex, OnceLock,
    },
};

// / Number of worker threads to use, this means that cores 0 up to # will have a thread pinned to them.
static CORE_INDEX: Mutex<usize> = Mutex::new(0);
/// The set of cores that is used to limit on which cores spawn_blocking tasks can run.
// TODO: check if this would need to change over time so we can adapt this set of cores also.
pub static PERMANENT_IO_CORES: OnceLock<CpuSet> = OnceLock::new();

// // TODO: use mpmc instead once it becomes stable
// // TODO: think how to wake up specific core and if that is necessary
// // TODO: think about using thread local state instead of global state to make it less fragile
static CORE_BLOCKERS: OnceLock<Vec<Mutex<Receiver<()>>>> = OnceLock::new();

/// Global runtime for all asynchronous
/// The runtime is initiallized the first time it is dereferenced.
/// At that point it will also check if the number of MAX_ASYNC_CORES has been set.
/// If no limit has been set, it will initialize itself to possibly use all cores on the server.
/// This means it spawns threads and pins them to each core, but blocks them from running until they are specifically enabled.
pub static GLOBAL_RUNTIME: LazyLock<AysncRuntime> = LazyLock::new(|| {
    let max_cores = num_cpus::get_physical();
    *CORE_INDEX.lock().unwrap() = max_cores;
    let mut core_unblockers = Vec::with_capacity(max_cores);
    let mut core_blockers = Vec::with_capacity(max_cores);
    for _ in 0..max_cores {
        let (sender, receiver) = channel();
        core_unblockers.push(sender);
        core_blockers.push(Mutex::new(receiver));
    }
    // if no other permanent cores are set, only assume core 0
    PERMANENT_IO_CORES.get_or_init(|| {
        let mut coreset = nix::sched::CpuSet::new();
        coreset.set(0).unwrap();
        coreset
    });
    assert!(CORE_BLOCKERS.get().is_none());
    CORE_BLOCKERS.set(core_blockers).unwrap();
    AysncRuntime::new(core_unblockers)
});

/// The single async runtime for dandelion
pub struct AysncRuntime {
    /// The runtime to use to drive the async tasks.
    runtime: tokio::runtime::Runtime,
    /// The senders to wake up blocked threads on the runtime.
    core_unblockers: Vec<Sender<()>>,
}

impl AysncRuntime {
    pub fn new(core_unblockers: Vec<Sender<()>>) -> Self {
        let start_func = || {
            log::debug!("starting a new thread");
            let current_affinity =
                nix::sched::sched_getaffinity(nix::unistd::Pid::from_raw(0)).unwrap();
            log::debug!("previous affinity: {:?}", current_affinity);
            // Get the index of a core and pin thread to it
            // all the non blocking threads should be spawned before any of the blocklig
            // this means, that if the CORE_INDEX is at zero we only expect temporary blocking threads
            let mut index_guard = CORE_INDEX.lock().unwrap();
            if *index_guard > 0 {
                log::debug!("starting another thread that will block");
                *index_guard -= 1;
                let core_index = *index_guard;
                drop(index_guard);
                let mut coreset = nix::sched::CpuSet::new();
                coreset.set(core_index).unwrap();
                nix::sched::sched_setaffinity(nix::unistd::Pid::from_raw(0), &coreset).unwrap();
                // Whenever a thread starts, it should take a blocker out of the CORE_BLOCKERS,
                // pin the thread to the associated core and run recv on it, to start in a blocked state.
                todo!("This currenlty stops the progress at some point. If it is commented out the tests pass.");
                todo!("It is unclear why that happens, as there is at least one thread working correclty that can be observed");
                CORE_BLOCKERS.get().unwrap()[core_index]
                    .lock()
                    .unwrap()
                    .recv()
                    .unwrap();
                log::debug!("Core {} unblocked", core_index);
            } else {
                // Temporary blocking threads are spawned by the existing runtime threads,
                // i.e. they inherit their affinity, don't need to set again.
                // TODO: check what happens on thread panic,
                // the runtime might try to restart a new workwer thread that now also falls into this case.
            }
        };
        AysncRuntime {
            runtime: tokio::runtime::Builder::new_multi_thread()
                .worker_threads(core_unblockers.len())
                .enable_io()
                .enable_time()
                .thread_name("GLOBAL_RUNTIME_THREAD")
                .on_thread_start(start_func)
                .build()
                .unwrap(),
            core_unblockers,
        }
    }

    pub fn wake_core_by_index(&self, index: usize) {
        log::trace!("Unblocking core: {}", index);
        self.core_unblockers[index].send(()).unwrap();
    }

    pub fn spawn<F>(&self, future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.runtime.spawn(future)
    }
}

use std::sync::{
    mpsc::{channel, Receiver, Sender},
    LazyLock, Mutex, OnceLock,
};

use nix::unistd::Pid;

/// The single async runtime for dandelion
pub struct AysncRuntime {
    /// The runtime to use to drive the async tasks.
    runtime: tokio::runtime::Runtime,
    /// The senders to wake up blocked threads on the runtime.
    core_unblockers: Vec<Sender<()>>,
}

/// Number of worker threads to use, this means that cores 0 up to # will have a thread pinned to them.
pub static MAX_ASYNC_CORES: OnceLock<usize> = OnceLock::new();

// TODO: use mpmc instead once it becomes stable
// TODO: think how to wake up specific core and if that is necessary
static CORE_BLOCKERS: Mutex<Vec<Receiver<()>>> = Mutex::new(Vec::new());

/// Global runtime for all asynchronous
/// The runtime is initiallized the first time it is dereferenced.
/// At that point it will also check if the number of MAX_ASYNC_CORES has been set.
/// If no limit has been set, it will initialize itself to possibly use all cores on the server.
/// This means it spawns threads and pins them to each core, but blocks them from running until they are specifically enabled.
pub static GLOBAL_RUNTIME: LazyLock<AysncRuntime> = LazyLock::new(|| {
    let max_cores = *MAX_ASYNC_CORES.get_or_init(|| num_cpus::get_physical());
    let mut core_unblockers = Vec::with_capacity(max_cores);
    let mut core_blockers = Vec::with_capacity(max_cores);
    for _ in 0..max_cores {
        let (sender, receiver) = channel();
        core_unblockers.push(sender);
        core_blockers.push(receiver);
    }
    let mut blocker_guard = CORE_BLOCKERS.lock().unwrap();
    assert_eq!(
        0,
        blocker_guard.len(),
        "CORE_BLOCKERS should not be initialized if GLOBAL_RUNTIME is running the lazy lock"
    );
    *blocker_guard = core_blockers;

    AysncRuntime {
        runtime: tokio::runtime::Builder::new_multi_thread()
            .worker_threads(max_cores)
            .enable_io()
            .enable_time()
            .on_thread_start(|| {
                // Whenever a thread starts, it should take a blocker out of the CORE_BLOCKERS,
                // pin the thread to the associated core and run recv on it, to start in a blocked state.
                let mut blocker_guard = CORE_BLOCKERS.lock().unwrap();
                let new_blocker = blocker_guard.pop().unwrap();
                let core_index = blocker_guard.len();
                drop(blocker_guard);
                let mut coreset = nix::sched::CpuSet::new();
                coreset.set(core_index);
                nix::sched::sched_setaffinity(Pid::from_raw(0), &coreset).unwrap();
                let _ = new_blocker.recv();
                // after receiveing the block message, put blocker back into vec
            })
            .build()
            .unwrap(),
        core_unblockers,
    }
});

use dandelion_commons::{
    records::{Archive, Recorder},
    DandelionResult,
};
use dispatcher::{
    dispatcher::{Dispatcher, DispatcherInput},
    queue::WorkQueue,
    resource_pool::ResourcePool,
};
use futures::{
    task::{AtomicWaker, Context, Poll},
    Stream,
};
use log::{debug, error, info, warn};
use machine_interface::{
    composition::CompositionSet,
    function_driver::{ComputeResource, Metadata},
    machine_config::{DomainType, EngineType},
    memory_domain::MemoryResource,
};
use multinode::client::register_as_remote;
use nix::unistd::Pid;
use std::{
    collections::BTreeMap,
    fs::read_to_string,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, OnceLock,
    },
};
use tokio::{
    runtime::Builder,
    select, spawn,
    sync::{mpsc, oneshot},
};

mod frontend;

pub enum DispatcherCommand {
    FunctionRequest {
        function_id: Arc<String>,
        inputs: Vec<DispatcherInput>,
        is_cold: bool,
        recorder: Recorder,
        callback: oneshot::Sender<DandelionResult<(Vec<Option<CompositionSet>>, Recorder)>>,
    },
    FunctionRegistration {
        name: String,
        engine_type: EngineType,
        context_size: usize,
        path: String,
        metadata: Metadata,
        callback: oneshot::Sender<DandelionResult<()>>,
    },
    CompositionRegistration {
        composition: String,
        callback: oneshot::Sender<DandelionResult<()>>,
    },
    RemoteRegistration {
        callback: oneshot::Sender<DandelionResult<WorkQueue>>,
    },
    RemoteFunctionRequest {
        function_id: Arc<String>,
        inputs: Vec<Option<CompositionSet>>,
        recorder: Recorder,
        callback: oneshot::Sender<DandelionResult<(Vec<Option<CompositionSet>>, Recorder)>>,
    },
    CompositionRequest {
        composition: String,
        inputs: Vec<DispatcherInput>,
        is_cold: bool,
        recorder: Recorder,
        callback: oneshot::Sender<DandelionResult<(Vec<Option<CompositionSet>>, Recorder)>>,
    },
}

/// Recording setup
static TRACING_ARCHIVE: OnceLock<Archive> = OnceLock::new();

async fn dispatcher_loop(
    mut request_receiver: mpsc::Receiver<DispatcherCommand>,
    dispatcher: &'static Dispatcher,
) {
    while let Some(dispatcher_args) = request_receiver.recv().await {
        match dispatcher_args {
            DispatcherCommand::FunctionRequest {
                function_id,
                inputs,
                is_cold,
                recorder,
                mut callback,
            } => {
                debug!("Handling function request for function {}", function_id);
                let function_future =
                    dispatcher.queue_function_by_name(function_id, inputs, !is_cold, recorder);
                spawn(async {
                    select! {
                        function_output = function_future => {
                            // either get an ok, meaning the data was sent, or get the data back
                            // no need to handle ok, and nothing useful to do with data if we get it back
                            // drop it here to release resources
                            let _ = callback.send(function_output);
                        }
                        _ = callback.closed() => ()
                    }
                });
            }
            DispatcherCommand::CompositionRequest {
                composition,
                inputs,
                is_cold,
                recorder,
                mut callback,
            } => {
                debug!("Handling composition request");
                let future = dispatcher.queue_unregistered_composition(
                    composition,
                    inputs,
                    !is_cold,
                    recorder,
                );
                spawn(async {
                    select! {
                        output = future => {
                            let _ = callback.send(output);
                        }
                        _ = callback.closed() => ()
                    }
                });
            }
            DispatcherCommand::FunctionRegistration {
                name,
                engine_type,
                context_size,
                metadata,
                callback,
                path,
            } => {
                debug!("Handling function registration for {}", name);
                let insertion_res =
                    dispatcher.insert_function(name, engine_type, context_size, path, metadata);
                callback
                    .send(insertion_res)
                    .expect("Function registration callback failed!");
            }
            DispatcherCommand::CompositionRegistration {
                composition,
                callback,
            } => {
                debug!("Handling composition registration");
                let insertion_res = dispatcher.insert_compositions(composition);
                callback
                    .send(insertion_res)
                    .expect("Composition registration callback failed!");
            }
            DispatcherCommand::RemoteRegistration { callback } => {
                // get queue to poll on
                let workqueue = dispatcher.get_work_queue();
                callback
                    .send(Ok(workqueue))
                    .expect("Remote registration callback should not fail.");
            }
            DispatcherCommand::RemoteFunctionRequest {
                function_id,
                inputs,
                recorder,
                mut callback,
            } => {
                debug!(
                    "Handling remote function request for function_id={}",
                    function_id
                );
                let dispatcher_input = inputs
                    .into_iter()
                    .map(|input_option| {
                        if let Some(input_set) = input_option {
                            DispatcherInput::Set(input_set)
                        } else {
                            DispatcherInput::None
                        }
                    })
                    .collect();
                let function_future = dispatcher.queue_function_by_name(
                    function_id,
                    dispatcher_input,
                    false,
                    recorder,
                );
                spawn(async {
                    select! {
                        function_output = function_future => {
                            // either get an ok, meaning the data was sent, or get the data back
                            // no need to handle ok, and nothing useful to do with data if we get it back
                            // drop it here to release resources
                            let _ = callback.send(function_output);
                        }
                        _ = callback.closed() => ()
                    }
                });
            }
        };
    }
}

static CHANGE_WAKER: AtomicWaker = AtomicWaker::new();
struct ChangePoller {}
impl Stream for ChangePoller {
    type Item = ();

    // Required method
    fn poll_next(
        self: core::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        CHANGE_WAKER.register(cx.waker());
        let updated = UPDATED_COUNT.load(Ordering::Acquire);
        if updated {
            UPDATED_COUNT.store(false, Ordering::Release);
            Poll::Ready(Some(()))
        } else {
            Poll::Pending
        }
    }
}

// TODO make into array with all relevant engines
static IDLE_COUNT: AtomicUsize = AtomicUsize::new(0);
static UPDATED_COUNT: AtomicBool = AtomicBool::new(false);

fn add_idle_send() {
    IDLE_COUNT.fetch_add(1, Ordering::AcqRel);
    UPDATED_COUNT.store(true, Ordering::Release);
    CHANGE_WAKER.wake();
}

fn remove_idle_send() {
    IDLE_COUNT.fetch_sub(1, Ordering::AcqRel);
    UPDATED_COUNT.store(true, Ordering::Release);
    CHANGE_WAKER.wake();
}

// TODO generalize to multiple remotes
// TODO think about additional frontend request to add or remove a remote from the list
// TODO think if we may want to fuse client and server to make them bidirectional
async fn remote_queue_client(remote_url: String) {
    // array with currently idle engines
    use futures::StreamExt;
    let mut change_poller = ChangePoller {};
    while let Some(()) = change_poller.next().await {
        let idle_cores = IDLE_COUNT.load(Ordering::Acquire);
        println!("New number of idle cores: {}", idle_cores);
        if idle_cores > 0 {}
    }
}

async fn remote_queue_server(queue_port: u16) {
    // socket to listen to
    let addr: SocketAddr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();

    loop {
        // wait for new connection to arrive
        let accept_result = listener.accept().await;
        let socket = if accept_result.is_ok() {
            let (socket, _address) = accept_result.unwrap();
            remote_queue_handler(socket);
        } else {
            // TODO handle errors on incomming request
            continue;
        };
    }
}

fn main() -> () {
    let default_warn_level = if cfg!(debug_assertions) {
        "debug"
    } else {
        "warn"
    };

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(default_warn_level))
        .init();

    // check if there is a configuration file
    let config = dandelion_server::config::DandelionConfig::get_config();
    info!("Loaded configuration:\n{:?}", config);

    // create globally available path to folder for data
    let folder_path: &'static str = Box::leak(config.folder_path.clone().into_boxed_str());

    // Initilize metric collection
    match TRACING_ARCHIVE.set(Archive::init()) {
        Ok(_) => (),
        Err(_) => panic!("Failed to initialize tracing archive"),
    }

    // find available resources
    let num_phyiscal_cores = u8::try_from(num_cpus::get_physical()).unwrap();
    let num_virt_cores = u8::try_from(num_cpus::get()).unwrap();
    if num_phyiscal_cores != num_virt_cores {
        warn!(
            "Hyperthreading might be enabled detected {} logical and {} physical cores",
            num_virt_cores, num_phyiscal_cores
        );
    }

    let resource_conversion = |core_index| ComputeResource::CPU(core_index);

    let dispatcher_cores = config.get_dispatcher_cores();
    let frontend_cores = config.get_frontend_cores();
    let communication_cores = config
        .get_communication_cores()
        .into_iter()
        .map(|core| resource_conversion(core))
        .collect();
    let compute_cores = config
        .get_computation_cores()
        .into_iter()
        .map(|core| resource_conversion(core))
        .collect();

    println!("core allocation:");
    println!("frontend cores {:?}", frontend_cores);
    println!("dispatcher cores: {:?}", dispatcher_cores);
    println!("communication cores: {:?}", communication_cores);
    println!("compute cores: {:?}", compute_cores);

    // make multithreaded front end runtime
    // set up tokio runtime, need io in any case
    let frontent_core_num = frontend_cores.len();
    let mut frontend_cpuset = nix::sched::CpuSet::new();
    for cpu in frontend_cores {
        frontend_cpuset.set(usize::from(cpu)).unwrap();
    }
    let mut runtime_builder = Builder::new_multi_thread();
    runtime_builder.enable_io();
    runtime_builder.enable_time();
    runtime_builder.worker_threads(frontent_core_num);
    runtime_builder.on_thread_start(move || {
        nix::sched::sched_setaffinity(Pid::from_raw(0), &frontend_cpuset).unwrap()
    });
    runtime_builder.global_queue_interval(10);
    runtime_builder.event_interval(10);
    let runtime = runtime_builder.build().unwrap();

    let dispatcher_core_num = dispatcher_cores.len();
    let mut dispatcher_coreset = nix::sched::CpuSet::new();
    for cpu in dispatcher_cores {
        dispatcher_coreset.set(usize::from(cpu)).unwrap();
    }
    let dispatcher_runtime = Builder::new_multi_thread()
        .worker_threads(dispatcher_core_num)
        .on_thread_start(move || {
            nix::sched::sched_setaffinity(Pid::from_raw(0), &dispatcher_coreset).unwrap()
        })
        .build()
        .unwrap();
    let (dispatcher_sender, dispatcher_recevier) = mpsc::channel(1000);

    // set up dispatcher configuration basics
    let mut pool_map = BTreeMap::new();

    // insert engines for the currentyl selected compute engine type
    // todo add function to machine config to detect resources and auto generate this
    #[cfg(feature = "mmu")]
    let engine_type = EngineType::Process;
    #[cfg(feature = "kvm")]
    let engine_type = EngineType::Kvm;
    #[cfg(feature = "cheri")]
    let engine_type = EngineType::Cheri;
    #[cfg(any(feature = "cheri", feature = "mmu", feature = "kvm"))]
    pool_map.insert(engine_type, compute_cores);
    #[cfg(feature = "reqwest_io")]
    pool_map.insert(EngineType::Reqwest, communication_cores);
    let resource_pool = ResourcePool {
        engine_pool: futures::lock::Mutex::new(pool_map),
    };

    // get RAM size
    // TODO could be a configuration, open question on how to split between engines
    // or if we unify somehow and have one underlying pool
    let max_ram = read_to_string("/proc/meminfo")
        .unwrap()
        .lines()
        .find_map(|line| {
            line.strip_prefix("MemTotal:")
                .and_then(|line| line.strip_suffix("kB"))
                .and_then(|line| Some(line.trim().parse::<usize>()))
        })
        .unwrap()
        .unwrap()
        * 1024;

    let memory_pool = match config.test_mode {
        Some(dandelion_server::config::TestMode::NoEngine) => BTreeMap::new(),
        Some(_) | None => BTreeMap::from([
            #[cfg(feature = "cheri")]
            (
                DomainType::Cheri,
                MemoryResource::Anonymous { size: max_ram },
            ),
            #[cfg(feature = "kvm")]
            (DomainType::Kvm, MemoryResource::Anonymous { size: max_ram }),
            #[cfg(feature = "mmu")]
            (
                DomainType::Process,
                MemoryResource::Shared {
                    id: 0,
                    size: max_ram,
                },
            ),
        ]),
    };

    let work_queue = WorkQueue::init(add_idle_send, remove_idle_send);
    let dispatcher = Box::leak(Box::new(
        Dispatcher::init(resource_pool, memory_pool, work_queue)
            .expect("Should be able to start dispatcher"),
    ));
    // start dispatcher
    dispatcher_runtime.spawn(dispatcher_loop(dispatcher_recevier, dispatcher));

    // register preload functions
    let preload_func = config.get_preload_functions();
    if preload_func.len() > 0 {
        Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                for pf in preload_func.iter() {
                    let engine_type = match pf.engine_type_id.to_lowercase().as_str() {
                        #[cfg(feature = "mmu")]
                        "process" => EngineType::Process,
                        #[cfg(feature = "kvm")]
                        "kvm" => EngineType::Kvm,
                        #[cfg(feature = "cheri")]
                        "cheri" => EngineType::Cheri,
                        _ => {
                            error!(
                                "Failed to preload function {}: Unkown engine type string {}",
                                pf.name, pf.engine_type_id
                            );
                            continue;
                        }
                    };
                    let input_sets: Vec<(String, Option<CompositionSet>)> = pf
                        .metadata
                        .input_sets
                        .iter()
                        .map(|s| (s.clone(), None))
                        .collect();
                    let output_sets = pf.metadata.output_sets.clone();
                    let metadata = Metadata {
                        input_sets: input_sets,
                        output_sets: output_sets,
                    };
                    match dispatcher.insert_function(
                        pf.name.clone(),
                        engine_type,
                        pf.ctx_size,
                        pf.bin_path.clone(),
                        metadata,
                    ) {
                        Err(err) => warn!("Failed to preload function {}: {}", pf.name, err),
                        Ok(_) => info!("Inserted preload function {}", pf.name),
                    }
                }
            });
    }

    let _guard = runtime.enter();

    // TODO would be nice to just print server ready with all enabled features if that would be possible
    print!("Server start with features:");
    #[cfg(feature = "cheri")]
    print!(" cheri");
    #[cfg(feature = "mmu")]
    print!(" mmu");
    #[cfg(feature = "kvm")]
    print!(" kvm");
    #[cfg(feature = "reqwest_io")]
    print!(" request_io");
    #[cfg(feature = "timestamp")]
    print!(" timestamp");
    print!("\n");

    // listen for other nodes trying to poll from local work queue
    runtime.spawn(remote_queue_server(config.q_port));

    // start a thread to check if we should be checking remote queues
    if let Some(remote_url) = config.remote_queue_url {
        runtime.spawn(remote_queue_client(remote_url));
    }

    // Run this server for... forever... unless I receive a signal!
    runtime.block_on(frontend::service_loop(
        dispatcher_sender,
        folder_path,
        config.port,
    ));

    // clean up folder in tmp that is used for function storage
    let removal_error = std::fs::remove_dir_all(folder_path);
    if let Err(err) = removal_error {
        warn!("Removing function folder failed with: {}", err);
    }
    // clean up folder with shared files in case the context backed by shared files left some behind
    for shm_dir_entry in std::fs::read_dir("/dev/shm/").unwrap() {
        if let Ok(shm_file) = shm_dir_entry {
            if shm_file
                .file_name()
                .into_string()
                .unwrap()
                .starts_with("shm_")
            {
                warn!(
                    "Found left over shared memory file: {:?}",
                    shm_file.file_name()
                );
                if std::fs::remove_file(shm_file.path()).is_err() {
                    warn!("Failed to remove shared memory file {:?}", shm_file.path());
                }
            }
        }
    }
}

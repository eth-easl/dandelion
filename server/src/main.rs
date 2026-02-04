use core_affinity::{self, CoreId};
use dandelion_commons::{
    records::{Archive, Recorder},
    DandelionResult,
};
use dispatcher::{
    dispatcher::{Dispatcher, DispatcherInput},
    resource_pool::ResourcePool,
};
use log::{debug, error, info, warn};
use machine_interface::{
    composition::CompositionSet,
    function_driver::{ComputeResource, Metadata},
    machine_config::{DomainType, EngineType},
    memory_domain::MemoryResource,
};
use std::{
    collections::BTreeMap,
    fs::read_to_string,
    sync::{
        atomic::{AtomicUsize, Ordering},
        OnceLock,
    },
    time::Instant,
};
use tokio::{
    runtime::Builder,
    select, spawn,
    sync::{mpsc, oneshot},
};

mod frontend;

// TODO: move to config?
const FUNCTION_FOLDER_PATH: &str = "/tmp/dandelion_server";

pub enum DispatcherCommand {
    FunctionRequest {
        name: String,
        inputs: Vec<DispatcherInput>,
        is_cold: bool,
        start_time: Instant,
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
        host: String,
        port: u16,
        engines: Vec<(EngineType, u32)>,
        callback: oneshot::Sender<DandelionResult<()>>,
    },
    RemoteDeregistration {
        host: String,
        port: u16,
        callback: oneshot::Sender<DandelionResult<()>>,
    },
    RemoteFunctionRequest {
        function_id: String,
        inputs: Vec<Option<CompositionSet>>,
        start_time: Instant,
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
                name,
                inputs,
                is_cold,
                start_time,
                mut callback,
            } => {
                debug!("Handling function request for function {}", name);
                let function_future =
                    dispatcher.queue_function_by_name(name, inputs, is_cold, start_time);
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
            DispatcherCommand::RemoteRegistration {
                host,
                port,
                engines,
                callback,
            } => {
                debug!(
                    "Handling remote registration from host={}, port={}",
                    host, port
                );
            }
            DispatcherCommand::RemoteDeregistration {
                host,
                port,
                callback,
            } => {
                debug!(
                    "Handling remote deregistration from host={}, port={}",
                    host, port
                );
            }
            DispatcherCommand::RemoteFunctionRequest {
                function_id,
                inputs,
                start_time,
                callback,
            } => {
                debug!(
                    "Handling remote function request for function_id={}",
                    function_id
                );
            }
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
    let mut runtime_builder = Builder::new_multi_thread();
    runtime_builder.enable_io();
    runtime_builder.worker_threads(frontend_cores.len());
    runtime_builder.on_thread_start(move || {
        static ATOMIC_INDEX: AtomicUsize = AtomicUsize::new(0);
        let core_index = ATOMIC_INDEX.fetch_add(1, Ordering::SeqCst);
        if !core_affinity::set_for_current(CoreId {
            id: frontend_cores[core_index].into(),
        }) {
            return;
        }
        info!(
            "Frontend thread running on core {}",
            frontend_cores[core_index]
        );
    });
    runtime_builder.global_queue_interval(10);
    runtime_builder.event_interval(10);
    let runtime = runtime_builder.build().unwrap();

    let dispatcher_runtime = Builder::new_multi_thread()
        .worker_threads(dispatcher_cores.len())
        .on_thread_start(move || {
            static ATOMIC_INDEX: AtomicUsize = AtomicUsize::new(0);
            let core_index = ATOMIC_INDEX.fetch_add(1, Ordering::SeqCst);
            if !core_affinity::set_for_current(CoreId {
                id: dispatcher_cores[core_index].into(),
            }) {
                return;
            }
            info!(
                "Dispatcher thread running on core {}",
                dispatcher_cores[core_index]
            );
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

    let memory_pool = BTreeMap::from([
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
    ]);

    // Create an ARC pointer to the dispatcher for thread-safe access
    let dispatcher = Box::leak(Box::new(
        Dispatcher::init(resource_pool, memory_pool).expect("Should be able to start dispatcher"),
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

    // Run this server for... forever... unless I receive a signal!
    runtime.block_on(frontend::service_loop(dispatcher_sender, config.port));

    // clean up folder in tmp that is used for function storage
    let removal_error = std::fs::remove_dir_all(FUNCTION_FOLDER_PATH);
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

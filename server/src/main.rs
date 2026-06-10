use dandelion_server::config::{self, FuncMetadata, PreloadFunc};
use dispatcher::{dispatcher::Dispatcher, queue::WorkQueue, resource_pool::ResourcePool};
use log::{debug, error, info, warn};
use machine_interface::{
    composition::{
        get_remote_data_client, set_remote_data_client, AnyShardingMode, AnyShardingParams,
        LocalCompositionSet, RemoteData,
    },
    function_driver::{ComputeResource, Metadata},
    machine_config::{create_engine_resource_map, DomainType, EngineType},
    memory_domain::MemoryResource,
};
use multinode::{data::ExportRegistry, DispatcherCommand};
use nix::sched::CpuSet;
use std::{collections::BTreeMap, fs::read_to_string, sync::Arc};
use tokio::{runtime::Builder, select, spawn, sync::mpsc};

mod frontend;

async fn delete_service_loop(
    mut remote_data_deletion_receiver: mpsc::UnboundedReceiver<RemoteData>,
) {
    loop {
        if let Some(remote_data) = remote_data_deletion_receiver.recv().await {
            spawn(async move {
                match get_remote_data_client() {
                    Ok(client) => {
                        if let Err(err) = client.delete_remote_data(remote_data).await {
                            warn!("Failed to delete remote data: {}", err);
                        }
                    }
                    Err(err) => {
                        warn!("Failed to delete remote data: {}", err);
                    }
                };
            });
            continue;
        }
    }
}

async fn dispatcher_loop(
    mut request_receiver: mpsc::Receiver<DispatcherCommand>,
    dispatcher: &'static Dispatcher,
) {
    loop {
        let dispatcher_args = request_receiver.recv().await;
        let Some(dispatcher_args) = dispatcher_args else {
            break;
        };
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
            DispatcherCommand::RemoteFunctionRequest {
                function_id,
                inputs,
                recorder,
                callback,
                is_cold,
            } => {
                debug!(
                    "Handling remote function request for function_id={}",
                    function_id
                );
                let function_future =
                    dispatcher.queue_function(function_id, inputs, !is_cold, recorder.clone());
                spawn(async {
                    callback
                        .callback(function_future.await.map(|sets| (sets, recorder)))
                        .await;
                });
            }
        };
    }
}

async fn remote_queue_server(
    queue_port: u16,
    queue: WorkQueue,
    export_registry: ExportRegistry,
    remote_data_deletion_sender: mpsc::UnboundedSender<RemoteData>,
) {
    // socket to listen to
    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], queue_port));
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();

    loop {
        // wait for new connection to arrive
        let accept_result = listener.accept().await;
        if let Ok((socket, _address)) = accept_result {
            socket.set_nodelay(true).unwrap();
            spawn(multinode::client::remote_queue_server(
                socket,
                queue.clone(),
                export_registry.clone(),
                remote_data_deletion_sender.clone(),
            ));
        } else {
            // TODO handle errors on incomming request
            continue;
        };
    }
}

async fn remote_queue_client(
    remote_url: String,
    sender: mpsc::Sender<DispatcherCommand>,
    export_registry: ExportRegistry,
    queue: WorkQueue,
) {
    let connection = tokio::net::TcpStream::connect(remote_url).await.unwrap();
    connection.set_nodelay(true).unwrap();
    multinode::client::remote_queue_client(connection, sender, export_registry, queue).await;
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

    // set the reqwest engine concurrency limit if it is available
    #[cfg(feature = "reqwest_io")]
    let _ = machine_interface::function_driver::system_driver::reqwest::CONCURRENCY_LIMIT
        .set(config.io_concurrency);

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

    // set the max sys core number
    let max_sys_cores = config.get_max_sys_cores();
    machine_interface::async_runtime::MAX_SYS_CORES
        .set(max_sys_cores)
        .unwrap();
    let communication_cores = (0..max_sys_cores)
        .map(|core_id| resource_conversion(core_id as u8))
        .collect();
    // set the min sys core set
    let min_sys_cores = config.get_min_sys_cores();
    let mut core_set = CpuSet::new();
    for core in 0..min_sys_cores {
        core_set.set(core).unwrap();
    }
    machine_interface::async_runtime::MIN_SYS_CORESET
        .set(core_set)
        .unwrap();
    // get the async runtime which uses the values set above for initialization
    let system_runtime = &machine_interface::async_runtime::GLOBAL_RUNTIME;

    let compute_cores = config
        .get_computation_cores()
        .into_iter()
        .map(|core| resource_conversion(core))
        .collect();

    println!("core allocation:");
    println!("minimum system cores {}", min_sys_cores);
    println!("maximum sysmte cores {}", max_sys_cores);
    println!("compute cores: {:?}", compute_cores);

    // TODO get rid of this since the are on the same runtime now anyway
    let (dispatcher_sender, dispatcher_recevier) = mpsc::channel(1000);

    // set up dispatcher configuration basics
    let pool_map = create_engine_resource_map(compute_cores, communication_cores);
    let resource_pool = ResourcePool {
        engine_pool: futures::lock::Mutex::new(pool_map),
    };

    // get RAM size
    // TODO: open question on how to split between engines or if we unify somehow and have one
    // underlying pool.
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
        * 1024
        * config.virtual_max_ram_multiplier;

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

    let work_queue = WorkQueue::init();

    // define the any sharding mode
    info!("Using any sharding mode: {:?}", config.any_sharding_mode);
    let any_sharding_mode = match config.any_sharding_mode {
        config::AnyShardingMode::MaxSharding => AnyShardingMode::MaxSharding,
        config::AnyShardingMode::FixedSharding(n) => AnyShardingMode::FixedSharding(n),
        config::AnyShardingMode::AutoSharding(n) => {
            AnyShardingMode::AutoSharding(AnyShardingParams {
                sys_info: work_queue.system_info.clone(),
                offload_const: n,
            })
        }
    };

    let dispatcher = Box::leak(Box::new(
        Dispatcher::init(
            resource_pool,
            memory_pool,
            work_queue.clone(),
            any_sharding_mode,
        )
        .expect("Should be able to start dispatcher"),
    ));

    // start dispatcher
    system_runtime.spawn(dispatcher_loop(dispatcher_recevier, dispatcher));

    // register preload functions
    let (preload_functions, preload_compositions) = config.get_preload_functions();
    debug!(
        "Preloading {} functions and {} compositions",
        preload_functions.len(),
        preload_compositions.len()
    );
    if preload_functions.len() > 0 {
        Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(async {
                for pf in preload_functions.into_iter() {
                    let PreloadFunc {
                        name,
                        engine_type_id,
                        metadata:
                            FuncMetadata {
                                input_sets,
                                output_sets,
                                min_set_bytes,
                            },
                        ctx_size,
                        bin_path,
                    } = pf;
                    debug!("Inserting preload function: {}", name);
                    let engine_type = match engine_type_id.to_lowercase().as_str() {
                        #[cfg(feature = "mmu")]
                        "process" => EngineType::Process,
                        #[cfg(feature = "kvm")]
                        "kvm" => EngineType::Kvm,
                        #[cfg(feature = "cheri")]
                        "cheri" => EngineType::Cheri,
                        _ => {
                            error!(
                                "Failed to preload function {}: Unkown engine type string {}",
                                name, engine_type_id
                            );
                            continue;
                        }
                    };

                    let input_sets: Vec<(String, Option<LocalCompositionSet>)> =
                        input_sets.into_iter().map(|s| (s, None)).collect();
                    let metadata = Metadata {
                        input_sets,
                        output_sets,
                        min_set_bytes,
                    };
                    match dispatcher.insert_function(
                        name.clone(),
                        engine_type,
                        ctx_size,
                        bin_path.clone(),
                        metadata,
                    ) {
                        Err(err) => warn!("Failed to preload function {}: {}", name, err),
                        Ok(_) => info!("Inserted preload function {}", name),
                    }
                }
            });
    }
    if preload_compositions.len() > 0 {
        Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(async {
                for preload_composition in preload_compositions.into_iter() {
                    match dispatcher.insert_compositions(preload_composition) {
                        Err(err) => warn!("Failed to preload composition {}", err),
                        Ok(()) => info!("Inserted preload composition"),
                    }
                }
            });
    }

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

    if let Some(multinode_settings) =
        multinode::config::MultinodeConfig::load(config.multinode_config.as_deref())
    {
        let node_id = config.node_id;
        let export_registry = ExportRegistry::new(node_id);
        if multinode_settings.queue_server.node_id == node_id {
            let (remote_data_deletion_sender, remote_data_deletion_receiver) =
                mpsc::unbounded_channel();
            system_runtime.spawn(delete_service_loop(remote_data_deletion_receiver));

            let queue_server_port =
                multinode::config::queue_server_port(&multinode_settings.queue_server.url)
                    .unwrap_or_else(|err| panic!("Invalid queue server entry: {}", err));
            // listen for other nodes trying to poll from local work queue
            system_runtime.spawn(remote_queue_server(
                queue_server_port,
                work_queue,
                export_registry.clone(),
                remote_data_deletion_sender.clone(),
            ));
        } else {
            // start a thread to check if we should be checking remote queues
            system_runtime.spawn(remote_queue_client(
                multinode::config::tcp_address(&multinode_settings.queue_server.url),
                dispatcher_sender.clone(),
                export_registry.clone(),
                work_queue,
            ));
        }

        let data_server_urls = multinode_settings.data_server_urls();
        let data_server_address = data_server_urls
            .get(&node_id)
            .unwrap_or_else(|| panic!("No data server entry for node {}", node_id));
        let data_server_port = multinode::config::data_server_port(data_server_address)
            .unwrap_or_else(|err| {
                panic!("Invalid data server entry for node {}: {}", node_id, err)
            });
        system_runtime.spawn(multinode::data::service_loop(
            data_server_port,
            export_registry.clone(),
        ));
        set_remote_data_client(Arc::new(multinode::data::HttpRemoteDataClient::new(
            data_server_urls,
            export_registry,
        )));
    }

    // Run this server for... forever... unless I receive a signal!
    system_runtime.block_on(frontend::service_loop(
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

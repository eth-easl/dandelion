use core_affinity::{self, CoreId};
use dandelion_commons::{
    records::{Archive, ArchiveInit, RecordPoint, Recorder},
    DandelionResult,
};
use dandelion_server::DandelionBody;
use dispatcher::{
    composition::CompositionSet, dispatcher::Dispatcher, function_registry::Metadata,
    resource_pool::ResourcePool,
};
use http_body_util::BodyExt;
use hyper::{
    body::{Body, Incoming},
    service::service_fn,
    Request, Response,
};
use log::{debug, error, info, trace, warn};
use machine_interface::{
    function_driver::ComputeResource,
    machine_config::EngineType,
    memory_domain::{bytes_context::BytesContext, read_only::ReadOnlyContext},
    DataItem, DataSet, Position,
};
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    convert::Infallible,
    io::Write,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, OnceLock,
    },
};
use tokio::{
    net::TcpListener,
    runtime::Builder,
    select,
    signal::unix::SignalKind,
    spawn,
    sync::{mpsc, oneshot},
};

const FUNCTION_FOLDER_PATH: &str = "/tmp/dandelion_server";

enum DispatcherCommand {
    FunctionRequest {
        name: String,
        inputs: Vec<(usize, CompositionSet)>,
        is_cold: bool,
        recorder: Recorder,
        callback: oneshot::Sender<DandelionResult<BTreeMap<usize, CompositionSet>>>,
    },
    FunctionRegistration {
        name: String,
        engine_type: EngineType,
        context_size: usize,
        path: String,
        metadata: Metadata,
        callback: oneshot::Sender<DandelionResult<u64>>,
    },
    CompositionRegistration {
        composition: String,
        callback: oneshot::Sender<DandelionResult<()>>,
    },
}

async fn serve_request(
    is_cold: bool,
    req: Request<Incoming>,
    dispatcher: mpsc::Sender<DispatcherCommand>,
) -> Result<Response<DandelionBody>, Infallible> {
    debug!("Starting to serve request");
    let mut recorder = TRACING_ARCHIVE.get().unwrap().get_recorder().unwrap();
    let _ = recorder.record(RecordPoint::Arrival);

    // pull all frames from the network
    let mut incomming = req.into_body();
    let mut body_pin = std::pin::Pin::new(&mut incomming);
    let mut frame_data = Vec::new();
    let mut total_size = 0usize;
    loop {
        if let Some(frame_result) =
            futures::future::poll_fn(|cx| body_pin.as_mut().poll_frame(cx)).await
        {
            let data_frame = frame_result.unwrap().into_data().unwrap();
            total_size += data_frame.len();
            frame_data.push(data_frame);
        } else {
            if body_pin.is_end_stream() {
                break;
            } else {
                continue;
            }
        }
    }

    // from context from frame bytes
    let request_context_result = BytesContext::from_bytes_vec(frame_data, total_size).await;
    if request_context_result.is_err() {
        warn!("request parsing failed with: {:?}", request_context_result);
    }
    let (function_name, request_context) = request_context_result.unwrap();
    debug!("finshed creating request context");
    // TODO match set names to assign sets to composition sets
    // map sets in the order they are in the request
    let request_number = request_context.content.len();
    let request_arc = Arc::new(request_context);
    let mut inputs = vec![];
    for request_set in 0..request_number {
        trace!(
            "adding input set {} from request",
            request_arc.content[request_set].as_ref().unwrap().ident
        );
        inputs.push((
            request_set,
            CompositionSet::from((request_set, vec![request_arc.clone()])),
        ));
    }
    // want a 1 to 1 mapping of all outputs the functions gives as long as we don't add user input on what they want
    recorder
        .record(RecordPoint::QueueFunctionDispatcher)
        .unwrap();
    let (callback, output_recevier) = tokio::sync::oneshot::channel();
    dispatcher
        .send(DispatcherCommand::FunctionRequest {
            name: function_name,
            inputs,
            is_cold,
            recorder: recorder.get_sub_recorder().unwrap(),
            callback,
        })
        .await
        .unwrap();
    let function_output = output_recevier
        .await
        .unwrap()
        .expect("Should get result from function");
    let response_body = dandelion_server::DandelionBody::new(function_output);
    debug!("finshed creating response body");
    let response = Ok::<_, Infallible>(Response::new(response_body));
    debug!("finshed creating response");
    recorder.record(RecordPoint::EndService).unwrap();
    TRACING_ARCHIVE.get().unwrap().return_recorder(recorder);

    return response;
}

#[derive(Debug, Deserialize)]
struct RegisterFunction {
    name: String,
    context_size: u64,
    engine_type: String,
    binary: Vec<u8>,
    input_sets: Vec<(String, Option<Vec<(String, Vec<u8>)>>)>,
    output_sets: Vec<String>,
}

async fn register_function(
    req: Request<Incoming>,
    dispatcher: mpsc::Sender<DispatcherCommand>,
) -> Result<Response<DandelionBody>, Infallible> {
    let bytes = req
        .collect()
        .await
        .expect("Failed to extract body from function registration")
        .to_bytes();
    // find first line end character
    let request_map: RegisterFunction =
        bson::from_slice(&bytes).expect("Should be able to deserialize request");
    // write function to file
    std::fs::create_dir_all(FUNCTION_FOLDER_PATH).unwrap();
    let mut path_buff = PathBuf::from(FUNCTION_FOLDER_PATH);
    path_buff.push(request_map.name.clone());
    let mut function_file = std::fs::File::create(path_buff.clone())
        .expect("Failed to create file for registering function");
    function_file
        .write_all(&request_map.binary)
        .expect("Failed to write file with content for registering");
    let engine_type = match request_map.engine_type.as_str() {
        #[cfg(feature = "wasm")]
        "RWasm" => EngineType::RWasm,
        #[cfg(feature = "mmu")]
        "Process" => EngineType::Process,
        #[cfg(feature = "cheri")]
        "Cheri" => EngineType::Cheri,
        unkown => panic!("Unkown engine type string {}", unkown),
    };
    let input_sets = request_map
        .input_sets
        .into_iter()
        .map(|(name, data)| {
            if let Some(static_data) = data {
                let data_contexts = static_data
                    .into_iter()
                    .map(|(item_name, data_vec)| {
                        let item_size = data_vec.len();
                        let mut new_context =
                            ReadOnlyContext::new(data_vec.into_boxed_slice()).unwrap();
                        new_context.content.push(Some(DataSet {
                            ident: name.clone(),
                            buffers: vec![DataItem {
                                ident: item_name,
                                data: Position {
                                    offset: 0,
                                    size: item_size,
                                },
                                key: 0,
                            }],
                        }));
                        (Arc::new(new_context), 0usize..1usize)
                    })
                    .collect();
                let composition_set = CompositionSet {
                    set_index: 0,
                    context_list: data_contexts,
                };
                (name, Some(composition_set))
            } else {
                (name, None)
            }
        })
        .collect();
    let (callback, confirmation) = oneshot::channel();
    let metadata = Metadata {
        input_sets: Arc::new(input_sets),
        output_sets: Arc::new(request_map.output_sets),
    };
    dispatcher
        .send(DispatcherCommand::FunctionRegistration {
            name: request_map.name,
            engine_type,
            context_size: request_map.context_size as usize,
            path: path_buff.to_str().unwrap().to_string(),
            metadata,
            callback,
        })
        .await
        .unwrap();
    confirmation
        .await
        .unwrap()
        .expect("Should be able to insert function");
    return Ok::<_, Infallible>(Response::new(DandelionBody::from_vec(
        "Function registered".as_bytes().to_vec(),
    )));
}

#[derive(Debug, Deserialize)]
struct RegisterChain {
    composition: String,
}

async fn register_composition(
    req: Request<Incoming>,
    dispatcher: mpsc::Sender<DispatcherCommand>,
) -> Result<Response<DandelionBody>, Infallible> {
    let bytes = req
        .collect()
        .await
        .expect("Failed to extract body from function registration")
        .to_bytes();
    // find first line end character
    let request_map: RegisterChain =
        bson::from_slice(&bytes).expect("Should be able to deserialize request");
    let (callback, confirmation) = oneshot::channel();
    dispatcher
        .send(DispatcherCommand::CompositionRegistration {
            composition: request_map.composition,
            callback,
        })
        .await
        .unwrap();
    confirmation
        .await
        .unwrap()
        .expect("Should be able to insert composition");
    return Ok::<_, Infallible>(Response::new(DandelionBody::from_vec(
        "Function registered".as_bytes().to_vec(),
    )));
}

async fn serve_stats(_req: Request<Incoming>) -> Result<Response<DandelionBody>, Infallible> {
    let archive_ref = TRACING_ARCHIVE.get().unwrap();
    let response = Response::new(DandelionBody::from_vec(
        archive_ref.get_summary().into_bytes(),
    ));
    archive_ref.reset();
    return Ok::<_, Infallible>(response);
}

async fn service(
    req: Request<Incoming>,
    dispatcher: mpsc::Sender<DispatcherCommand>,
) -> Result<Response<DandelionBody>, Infallible> {
    let uri = req.uri().path();
    match uri {
        // TODO rename to cold func and hot func, remove matmul, compute, io
        "/register/function" => register_function(req, dispatcher).await,
        "/register/composition" => register_composition(req, dispatcher).await,
        "/cold/matmul"
        | "/cold/matmulstore"
        | "/cold/compute"
        | "/cold/io"
        | "/cold/chain_scaling"
        | "/cold/middleware_app"
        | "/cold/python_app" => serve_request(true, req, dispatcher).await,
        "/hot/matmul"
        | "/hot/matmulstore"
        | "/hot/compute"
        | "/hot/io"
        | "/hot/chain_scaling"
        | "/hot/middleware_app"
        | "/hot/python_app" => serve_request(false, req, dispatcher).await,
        "/stats" => serve_stats(req).await,
        other_uri => {
            trace!("Received request on {}", other_uri);
            Ok::<_, Infallible>(Response::new(DandelionBody::from_vec(
                format!("Hello, Wor\n").into_bytes(),
            )))
        }
    }
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
                recorder,
                mut callback,
            } => {
                let function_future =
                    dispatcher.queue_function_by_name(name, inputs, None, is_cold, recorder);
                spawn(async {
                    select! {
                        function_output = function_future => {
                            callback.send(function_output).unwrap();
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
                mut callback,
                path,
            } => {
                let insertion_future =
                    dispatcher.insert_func(name, engine_type, context_size, path, metadata);
                spawn(async {
                    select! {
                        result = insertion_future => {
                            callback.send(result).unwrap();
                        }
                        _ = callback.closed() => ()
                    }
                });
            }
            DispatcherCommand::CompositionRegistration {
                composition,
                mut callback,
            } => {
                let insertion_future = dispatcher.insert_compositions(composition);
                spawn(async {
                    select! {
                        result = insertion_future => {
                            callback.send(result).unwrap();
                        }
                        _ = callback.closed() => ()
                    }
                });
            }
        };
    }
}

async fn service_loop(request_sender: mpsc::Sender<DispatcherCommand>, port: u16) {
    // socket to listen to
    let addr: SocketAddr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr).await.unwrap();
    // signal handlers for gracefull shutdown
    let mut sigterm_stream = tokio::signal::unix::signal(SignalKind::terminate()).unwrap();
    let mut sigint_stream = tokio::signal::unix::signal(SignalKind::interrupt()).unwrap();
    let mut sigquit_stream = tokio::signal::unix::signal(SignalKind::quit()).unwrap();
    loop {
        tokio::select! {
            connection_pair = listener.accept() => {
                let (stream,_) = connection_pair.unwrap();
                let loop_dispatcher = request_sender.clone();
                let io = hyper_util::rt::TokioIo::new(stream);
                tokio::task::spawn(async move {
                    let service_dispatcher_ptr = loop_dispatcher.clone();
                    if let Err(err) = hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new())
                        .serve_connection_with_upgrades(
                            io,
                            service_fn(|req| service(req, service_dispatcher_ptr.clone())),
                        )
                        .await
                    {
                        error!("Request serving failed with error: {:?}", err);
                    }
                });
            }
            _ = sigterm_stream.recv() => return,
            _ = sigint_stream.recv() => return,
            _ = sigquit_stream.recv() => return,
        }
    }
}

fn main() -> () {
    // check if there is a configuration file
    let config = dandelion_server::config::DandelionConfig::get_config();

    println!("config: {:?}", config);

    let default_warn_level = if cfg!(debug_assertions) {
        "debug"
    } else {
        "warn"
    };

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(default_warn_level))
        .init();

    // Initilize metric collection
    match TRACING_ARCHIVE.set(Archive::init(ArchiveInit {
        #[cfg(feature = "timestamp")]
        timestamp_count: config.timestamp_count,
    })) {
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
    #[cfg(feature = "wasm")]
    let engine_type = EngineType::RWasm;
    #[cfg(feature = "mmu")]
    let engine_type = EngineType::Process;
    #[cfg(feature = "cheri")]
    let engine_type = EngineType::Cheri;
    #[cfg(any(feature = "cheri", feature = "wasm", feature = "mmu"))]
    pool_map.insert(engine_type, compute_cores);
    #[cfg(feature = "reqwest_io")]
    pool_map.insert(EngineType::Reqwest, communication_cores);
    let resource_pool = ResourcePool {
        engine_pool: futures::lock::Mutex::new(pool_map),
    };

    // Create an ARC pointer to the dispatcher for thread-safe access
    let dispatcher = Box::leak(Box::new(
        Dispatcher::init(resource_pool).expect("Should be able to start dispatcher"),
    ));
    // start dispatcher
    dispatcher_runtime.spawn(dispatcher_loop(dispatcher_recevier, dispatcher));

    let _guard = runtime.enter();

    // TODO would be nice to just print server ready with all enabled features if that would be possible
    print!("Server start with features:");
    #[cfg(feature = "cheri")]
    print!(" cheri");
    #[cfg(feature = "mmu")]
    print!(" mmu");
    #[cfg(feature = "wasm")]
    print!(" wasm");
    #[cfg(feature = "reqwest_io")]
    print!(" request_io");
    #[cfg(feature = "timestamp")]
    print!(" timestamp");
    print!("\n");

    // Run this server for... forever... unless I receive a signal!
    runtime.block_on(service_loop(dispatcher_sender, config.port));

    // clean up folder in tmp that is used for function storage
    std::fs::remove_dir_all(FUNCTION_FOLDER_PATH).unwrap();
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

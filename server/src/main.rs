use core_affinity::{self, CoreId};
use dandelion_commons::records::{Archive, ArchiveInit, RecordPoint, Recorder};
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
use log::{debug, error, info, warn};
use machine_interface::{
    function_driver::ComputeResource,
    machine_config::EngineType,
    memory_domain::{bytes_context::BytesContext, Context},
};
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    convert::Infallible,
    io::Write,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc, OnceLock,
    },
};
use tokio::{net::TcpListener, runtime::Builder, signal::unix::SignalKind};

const FUNCTION_FOLDER_PATH: &str = "/tmp/dandelion_server";

// TODO: integrate into serve request
// async fn run_mat_func(
//     dispatcher: Arc<Dispatcher>,
//     is_cold: bool,
//     function_name: String,
//     request: Context,
//     mut recorder: Recorder,
// ) -> DandelionBody {
//     // TODO match set names to assign sets to composition sets
//     let request_arc = Arc::new(request);
//     let inputs = vec![
//         (0, CompositionSet::from((0, vec![request_arc.clone()]))),
//         (1, CompositionSet::from((1, vec![request_arc.clone()]))),
//     ];
//     let outputs = vec![Some(0)];
//     recorder
//         .record(RecordPoint::QueueFunctionDispatcher)
//         .unwrap();
//     let result = dispatcher
//         .queue_function_by_name(function_name, inputs, None, is_cold, recorder)
//         .await
//         .expect("Should get result from function");

//     return dandelion_server::DandelionBody::new(result);
// }

async fn serve_request(
    is_cold: bool,
    req: Request<Incoming>,
    dispatcher: Arc<Dispatcher>,
) -> Result<Response<DandelionBody>, Infallible> {
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
        inputs.push((
            request_set,
            CompositionSet::from((request_set, vec![request_arc.clone()])),
        ));
    }
    // let inputs = vec![(0, CompositionSet::from((0, vec![(Arc::new(request))])))];
    // want a 1 to 1 mapping of all outputs the functions gives as long as we don't add user input on what they want
    recorder
        .record(RecordPoint::QueueFunctionDispatcher)
        .unwrap();
    let function_output = dispatcher
        .queue_function_by_name(
            function_name,
            inputs,
            None,
            is_cold,
            recorder.get_sub_recorder().unwrap(),
        )
        .await
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
}

async fn register_function(
    req: Request<Incoming>,
    dispatcher: Arc<Dispatcher>,
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
        #[cfg(feature = "gpu_thread")]
        "GpuThread" => EngineType::GpuThread,
        #[cfg(feature = "gpu_process")]
        "GpuProcess" => EngineType::GpuProcess,
        _ => panic!("Unkown engine type string"),
    };

    dispatcher
        .insert_func(
            request_map.name,
            engine_type,
            request_map.context_size as usize,
            path_buff.to_str().unwrap(),
            Metadata {
                // Comment to switch between matmul and inference workloads. TODO: stop hard coding
                input_sets: Arc::new(vec![
                    (String::from("A"), None),
                    (String::from("B"), None),
                    (String::from("cfg"), None),
                ]),
                // #[cfg(feature = "gpu")]
                // input_sets: Arc::new(vec![(String::from("A"), None), (String::from("cfg"), None)]),
                // #[cfg(not(feature = "gpu"))]
                // input_sets: Arc::new(vec![(String::from("A"), None)]),

                // output_sets: Arc::new(vec![String::from("B")]),
                output_sets: Arc::new(vec![String::from("D")]),
            },
        )
        .await
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
    dispatcher: Arc<Dispatcher>,
) -> Result<Response<DandelionBody>, Infallible> {
    let bytes = req
        .collect()
        .await
        .expect("Failed to extract body from function registration")
        .to_bytes();
    // find first line end character
    let request_map: RegisterChain =
        bson::from_slice(&bytes).expect("Should be able to deserialize request");
    // write function to file
    dispatcher
        .insert_compositions(request_map.composition)
        .await
        .expect("Should be able to insert function");
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
    dispatcher: Arc<Dispatcher>,
) -> Result<Response<DandelionBody>, Infallible> {
    let uri = req.uri().path();
    match uri {
        // TODO rename to cold func and hot func, remove matmul, compute, io
        "/register/function" => register_function(req, dispatcher).await,
        "/register/composition" => register_composition(req, dispatcher).await,
        "/cold/matmul" | "/cold/compute" | "/cold/io" | "/cold/inference" => {
            serve_request(true, req, dispatcher).await
        }
        "/hot/matmul" | "/hot/compute" | "/hot/io" | "/hot/inference" => {
            serve_request(false, req, dispatcher).await
        }
        "/stats" => serve_stats(req).await,
        _ => Ok::<_, Infallible>(Response::new(DandelionBody::from_vec(
            format!("Hello, Wor\n").into_bytes(),
        ))),
    }
}

/// Recording setup
static TRACING_ARCHIVE: OnceLock<Archive> = OnceLock::new();

async fn service_loop(dispacher: Arc<Dispatcher>) {
    // socket to listen to
    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    let listener = TcpListener::bind(addr).await.unwrap();
    // signal handlers for gracefull shutdown
    let mut sigterm_stream = tokio::signal::unix::signal(SignalKind::terminate()).unwrap();
    let mut sigint_stream = tokio::signal::unix::signal(SignalKind::interrupt()).unwrap();
    let mut sigquit_stream = tokio::signal::unix::signal(SignalKind::quit()).unwrap();
    loop {
        tokio::select! {
            connection_pair = listener.accept() => {
                let (stream,_) = connection_pair.unwrap();
                let loop_dispatcher = dispacher.clone();
                let io = hyper_util::rt::TokioIo::new(stream);
                tokio::task::spawn(async move {
                    let service_dispatcher_ptr = loop_dispatcher.clone();
                    if let Err(err) = hyper::server::conn::http1::Builder::new()
                        .serve_connection(
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
    let config = dandelion_server::config::get_config();

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
    let num_cores = config.total_cores;
    let num_phyiscal_cores = u8::try_from(num_cpus::get_physical()).unwrap();
    let num_virt_cores = u8::try_from(num_cpus::get()).unwrap();
    if num_phyiscal_cores != num_virt_cores {
        warn!(
            "Hyperthreading might be enabled detected {} logical and {} physical cores",
            num_virt_cores, num_phyiscal_cores
        );
    }
    // TODO: This calculation makes sense only for running matmul-128x128 workload on MMU engines
    let num_dispatcher_cores = config.dispatcher_cores;
    assert!(
        num_dispatcher_cores > 0 && num_dispatcher_cores < num_cores,
        "invalid dispatcher core number: {}",
        num_dispatcher_cores
    );
    // make multithreaded front end that only uses core 0
    // set up tokio runtime, need io in any case
    let mut runtime_builder = Builder::new_multi_thread();
    runtime_builder.enable_io();
    runtime_builder.worker_threads(num_dispatcher_cores.into());
    runtime_builder.on_thread_start(|| {
        static ATOMIC_ID: AtomicU8 = AtomicU8::new(0);
        let core_id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
        if !core_affinity::set_for_current(CoreId { id: core_id.into() }) {
            return;
        }
        info!("Dispatcher running on core {}", core_id);
    });
    runtime_builder.global_queue_interval(10);
    runtime_builder.event_interval(10);
    let runtime = runtime_builder.build().unwrap();

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
    #[cfg(feature = "gpu_thread")]
    let engine_type = EngineType::GpuThread;
    #[cfg(feature = "gpu_process")]
    let engine_type = EngineType::GpuProcess;
    #[cfg(any(feature = "cheri", feature = "wasm", feature = "mmu"))]
    pool_map.insert(
        engine_type,
        (num_dispatcher_cores + 1..num_cores)
            .map(|code_id| ComputeResource::CPU(code_id as u8))
            .collect(),
    );
    #[cfg(any(feature = "gpu_thread", feature = "gpu_process"))]
    {
        let gpu_count: u8 = 4; // TODO: don't hard code this
        pool_map.insert(
            engine_type,
            (num_dispatcher_cores..num_cores)
                .zip(0..gpu_count)
                .map(|(cpu_id, gpu_id)| ComputeResource::GPU(cpu_id as u8, gpu_id))
                .collect(),
        );
    }
    #[cfg(feature = "reqwest_io")]
    pool_map.insert(
        EngineType::Reqwest,
        (num_dispatcher_cores..num_dispatcher_cores + 1)
            .map(|core_id| ComputeResource::CPU(core_id as u8))
            .collect(),
    );
    let resource_pool = ResourcePool {
        engine_pool: futures::lock::Mutex::new(pool_map),
    };

    // Create an ARC pointer to the dispatcher for thread-safe access
    let dispatcher_ptr =
        Arc::new(Dispatcher::init(resource_pool).expect("Should be able to start dispatcher"));

    let _guard = runtime.enter();

    // TODO would be nice to just print server ready with all enabled features if that would be possible
    print!("Server start with features:");
    #[cfg(feature = "cheri")]
    print!(" cheri");
    #[cfg(feature = "mmu")]
    print!(" mmu");
    #[cfg(feature = "wasm")]
    print!(" wasm");
    #[cfg(feature = "gpu_thread")]
    print!(" gpu_thread");
    #[cfg(feature = "gpu_process")]
    print!(" gpu_process");
    #[cfg(feature = "reqwest_io")]
    print!(" request_io");
    #[cfg(feature = "timestamp")]
    print!(" timestamp");
    print!("\n");

    // Run this server for... forever... unless I receive a signal!
    runtime.block_on(service_loop(dispatcher_ptr));

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

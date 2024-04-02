use core_affinity::{self, CoreId};
use dandelion_commons::records::{Archive, RecordPoint, Recorder};
use dandelion_server::DandelionRequest;
use dispatcher::{
    composition::CompositionSet, dispatcher::Dispatcher, function_registry::Metadata,
    resource_pool::ResourcePool,
};
use http_body_util::{BodyExt, Full};
use hyper::{
    body::{Bytes, Incoming},
    service::service_fn,
    Request, Response, StatusCode,
};
use log::{error, info};
use machine_interface::{
    function_driver::ComputeResource,
    machine_config::EngineType,
    memory_domain::{mmap::MmapMemoryDomain, ContextTrait, MemoryDomain},
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
        atomic::{AtomicU8, Ordering},
        Arc, Once, OnceLock,
    },
};
use tokio::{net::TcpListener, runtime::Builder, signal::unix::SignalKind};

static INIT_MATRIX: Once = Once::new();
static mut DUMMY_MATRIX: Vec<i64> = Vec::new();
const FUNCTION_FOLDER_PATH: &str = "/tmp/dandelion_server";

async fn run_chain(
    dispatcher: Arc<Dispatcher>,
    is_cold: bool,
    function_name: String,
    get_uri: String,
    post_uri: String,
    mut recorder: Recorder,
) -> u64 {
    // TODO just have all the strings concatinated and create const context
    let domain = MmapMemoryDomain::init(machine_interface::memory_domain::MemoryResource::None)
        .expect("Should be able to get Mmap domain");
    let mut input_context = domain
        .acquire_context(128)
        .expect("Should be able to get malloc context");
    let get_request = format!("GET {} HTTP/1.1", get_uri);
    let post_request = format!("PUT {} HTTP/1.1", post_uri);
    let get_request_offset = input_context
        .get_free_space_and_write_slice(get_request.as_bytes())
        .expect("Should be able to write") as usize;
    let post_request_offset = input_context
        .get_free_space_and_write_slice(post_request.as_bytes())
        .expect("Should be able to write") as usize;
    input_context.content.push(Some(DataSet {
        ident: String::from("request"),
        buffers: vec![DataItem {
            ident: String::from("request"),
            data: Position {
                offset: get_request_offset,
                size: get_request.len(),
            },
            key: 0,
        }],
    }));
    input_context.content.push(Some(DataSet {
        ident: String::from("request"),
        buffers: vec![DataItem {
            ident: String::from("request"),
            data: Position {
                offset: post_request_offset,
                size: post_request.len(),
            },
            key: 0,
        }],
    }));
    let input_arc = Arc::new(input_context);
    let inputs = vec![
        (0, CompositionSet::from((0, vec![input_arc.clone()]))),
        (1, CompositionSet::from((1, vec![input_arc]))),
    ];
    let output_mapping = vec![Some(0), Some(1)];

    recorder
        .record(RecordPoint::QueueFunctionDispatcher)
        .unwrap();
    let result = dispatcher
        .queue_function_by_name(function_name, inputs, output_mapping, is_cold, recorder)
        .await
        .expect("Should get response from chain");
    assert_eq!(2, result.len());
    // check http post response
    let post_composition_set = result
        .get(&1)
        .expect("Should have composition set for post response");
    assert_eq!(1, post_composition_set.context_list.len());
    let post_context = &post_composition_set.context_list[0].0;
    assert_eq!(3, post_context.content.len());
    let post_set = post_context.content[0]
        .as_ref()
        .expect("Should have status set");
    assert_eq!(1, post_set.buffers.len());
    let post_status_position = post_set.buffers[0].data;
    let mut post_vec = Vec::<u8>::new();
    post_vec.resize(post_status_position.size, 0);
    post_context
        .read(post_status_position.offset, post_vec.as_mut_slice())
        .expect("Should be able to read post response");
    assert_eq!("HTTP/1.1 200 OK".as_bytes(), post_vec.as_slice());

    // check iteration result
    let result_compositon_set = result.get(&0).expect("Should have set 0");
    assert_eq!(1, result_compositon_set.context_list.len());
    let result_context = &result_compositon_set.context_list[0].0;
    assert_eq!(1, result_context.content.len());
    let result_set = result_context.content[0]
        .as_ref()
        .expect("Should contain a return number");
    assert_eq!(1, result_set.buffers.len());
    let result_position = result_set.buffers[0].data;

    let mut result_vec = vec![0u8; result_position.size];
    result_context
        .read(result_position.offset, result_vec.as_mut_slice())
        .expect("Should be able to read result");
    let checksum = u64::from_ne_bytes(result_vec[0..8].try_into().unwrap());

    return checksum;
}

async fn run_mat_func(
    dispatcher: Arc<Dispatcher>,
    is_cold: bool,
    request: DandelionRequest,
    mut recorder: Recorder,
) -> i64 {
    let (name, input_context) = dandelion_server::parse_request(request);

    let inputs = vec![(
        0,
        CompositionSet::from((0, vec![(Arc::new(input_context))])),
    )];
    let outputs = vec![Some(0)];
    recorder
        .record(RecordPoint::QueueFunctionDispatcher)
        .unwrap();
    let result = dispatcher
        .queue_function_by_name(name, inputs, outputs, is_cold, recorder)
        .await
        .expect("Should get result from function")
        .remove(&0)
        .expect("Should have composition set 0");

    return get_checksum(result);
}

// Given a result context, return the last element of the resulting matrix
fn get_checksum(composition_set: CompositionSet) -> i64 {
    // Determine offset of last matrix element
    assert_eq!(1, composition_set.context_list.len());
    let context = &composition_set.context_list[0].0;
    let output_dataset = context.content[0].as_ref().expect("Should contain matrix");
    let output_item = output_dataset
        .buffers
        .iter()
        .find(|buffer| buffer.key == 0)
        .expect("should find a buffer with the correct key")
        .data;
    let checksum_offset = output_item.offset + output_item.size - 8;

    // Read out the checksum
    let mut read_buffer: Vec<i64> = vec![0; 1];
    context
        .read(checksum_offset, &mut read_buffer)
        .expect("Context should contain matrix");

    return read_buffer[0];
}

async fn serve_request(
    is_cold: bool,
    req: Request<Incoming>,
    dispatcher: Arc<Dispatcher>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let mut recorder = TRACING_ARCHIVE.get().unwrap().get_recorder().unwrap();
    let _ = recorder.record(RecordPoint::Arrival);

    // Try to parse the request
    let request_buf = req
        .collect()
        .await
        .expect("Could not read request body")
        .to_bytes();
    let request: DandelionRequest =
        bson::from_slice(&request_buf).expect("Should be able to deserialize matrix request");

    let response_vec = run_mat_func(
        dispatcher,
        is_cold,
        request,
        recorder.get_sub_recorder().unwrap(),
    )
    .await
    .to_be_bytes()
    .to_vec();

    let response = Ok::<_, Infallible>(Response::new(response_vec.into()));

    recorder.record(RecordPoint::EndService).unwrap();
    TRACING_ARCHIVE.get().unwrap().return_recorder(recorder);

    return response;
}

#[derive(Deserialize)]
struct ChainRequest {
    name: String,
    get_uri: String,
    post_uri: String,
}

async fn serve_chain(
    is_cold: bool,
    req: Request<Incoming>,
    dispatcher: Arc<Dispatcher>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let mut recorder = TRACING_ARCHIVE.get().unwrap().get_recorder().unwrap();
    let _ = recorder.record(RecordPoint::Arrival);

    let request_buf = req
        .collect()
        .await
        .expect("Should be able to parse body")
        .to_bytes();
    let request_map: ChainRequest =
        bson::from_slice(&request_buf).expect("Should be able to deserialize matrix request");

    let response_vec = run_chain(
        dispatcher,
        is_cold,
        request_map.name,
        request_map.get_uri,
        request_map.post_uri,
        recorder.get_sub_recorder().unwrap(),
    )
    .await
    .to_be_bytes()
    .to_vec();

    let response = Ok::<_, Infallible>(Response::new(response_vec.into()));

    recorder.record(RecordPoint::EndService).unwrap();
    TRACING_ARCHIVE.get().unwrap().return_recorder(recorder);

    return response;
}

async fn serve_native(req: Request<Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
    // Try to parse the request
    let request_buf = req
        .collect()
        .await
        .expect("Could not read request body")
        .to_bytes();

    if request_buf.len() != 16 {
        let mut bad_request = Response::new(Full::new(String::from("").into()));
        *bad_request.status_mut() = StatusCode::BAD_REQUEST;
        return Ok::<_, Infallible>(bad_request);
    }

    let rows = i64::from_le_bytes(request_buf[0..8].try_into().unwrap()) as usize;
    let cols = i64::from_le_bytes(request_buf[8..16].try_into().unwrap()) as usize;

    let mat_size: usize = rows * cols;

    // Initialize matrix if necessary
    unsafe {
        INIT_MATRIX.call_once(|| {
            for i in 0..mat_size {
                DUMMY_MATRIX.push(i as i64 + 1)
            }
        });
    }

    let mut out_mat: Vec<i64> = vec![0; mat_size];
    for i in 0..rows {
        for j in 0..rows {
            for k in 0..cols {
                unsafe {
                    out_mat[i * rows + j] +=
                        DUMMY_MATRIX[i * cols + k] * DUMMY_MATRIX[j * cols + k];
                }
            }
        }
    }

    let checksum = out_mat[rows * cols - 1];

    Ok::<_, Infallible>(Response::new(checksum.to_be_bytes().to_vec().into()))
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
) -> Result<Response<Full<Bytes>>, Infallible> {
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
        _ => panic!("Unkown engine type string"),
    };
    dispatcher
        .insert_func(
            request_map.name,
            engine_type,
            request_map.context_size as usize,
            path_buff.to_str().unwrap(),
            Metadata {
                input_sets: Arc::new(vec![(String::from(""), None)]),
                output_sets: Arc::new(vec![String::from("")]),
            },
        )
        .await
        .expect("Should be able to insert function");
    return Ok::<_, Infallible>(Response::new("Function registered".into()));
}

#[derive(Debug, Deserialize)]
struct RegisterChain {
    composition: String,
}

async fn register_composition(
    req: Request<Incoming>,
    dispatcher: Arc<Dispatcher>,
) -> Result<Response<Full<Bytes>>, Infallible> {
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
    return Ok::<_, Infallible>(Response::new("Function registered".into()));
}

async fn serve_stats(_req: Request<Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
    let archive_ref = TRACING_ARCHIVE.get().unwrap();
    let response = Response::new(archive_ref.get_summary().into());
    archive_ref.reset();
    return Ok::<_, Infallible>(response);
}

async fn service(
    req: Request<Incoming>,
    dispatcher: Arc<Dispatcher>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let uri = req.uri().path();
    match uri {
        "/register/function" => register_function(req, dispatcher).await,
        "/register/composition" => register_composition(req, dispatcher).await,
        "/cold/matmul" => serve_request(true, req, dispatcher).await,
        "/hot/matmul" => serve_request(false, req, dispatcher).await,
        "/cold/compute" => serve_chain(true, req, dispatcher).await,
        "/hot/compute" => serve_chain(false, req, dispatcher).await,
        "/cold/io" => serve_chain(true, req, dispatcher).await,
        "/hot/io" => serve_chain(false, req, dispatcher).await,
        "/native" => serve_native(req).await,
        "/stats" => serve_stats(req).await,
        _ => Ok::<_, Infallible>(Response::new(
            // format!("Hello, World! You asked for: {}\n", uri).into(),
            format!("Hello, Wor\n").into(),
        )),
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
            _ = sigterm_stream.recv() => break,
            _ = sigint_stream.recv() => break,
            _ = sigquit_stream.recv() => break,
        }
    }
}

fn main() -> () {
    env_logger::init();

    // Initilize metric collection
    match TRACING_ARCHIVE.set(Archive::init()) {
        Ok(_) => (),
        Err(_) => panic!("Failed to initialize tracing archive"),
    }

    // find available resources
    let num_cores = std::env::var("NUM_TOTAL_CORES").map_or_else(
        |_e| u8::try_from(num_cpus::get_physical()).unwrap(),
        |n| n.parse::<u8>().unwrap(),
    );
    let num_virt_cores = u8::try_from(core_affinity::get_core_ids().unwrap().len()).unwrap();
    let threads_per_core = num_virt_cores / num_cores;
    // TODO: This calculation makes sense only for running matmul-128x128 workload on MMU engines
    let num_dispatcher_cores = std::env::var("NUM_DISP_CORES")
        .map_or_else(|_e| (num_cores + 13) / 14, |n| n.parse::<u8>().unwrap());
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
    #[cfg(any(feature = "cheri", feature = "wasm", feature = "mmu"))]
    pool_map.insert(
        engine_type,
        (num_dispatcher_cores * threads_per_core..num_virt_cores)
            .step_by(threads_per_core.into())
            .map(|code_id| ComputeResource::CPU(code_id))
            .collect(),
    );
    #[cfg(feature = "hyper_io")]
    pool_map.insert(
        EngineType::Hyper,
        (0..num_dispatcher_cores * threads_per_core)
            .step_by(threads_per_core.into())
            .map(|core_id| ComputeResource::CPU(core_id))
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
    print!("Server start with features: ");
    #[cfg(feature = "cheri")]
    print!("cheri, ");
    #[cfg(feature = "mmu")]
    print!("mmu, ");
    #[cfg(feature = "wasm")]
    print!("wasm, ");
    #[cfg(feature = "timestamp")]
    print!("timestamp, ");
    print!("\n");

    // Run this server for... forever... unless I receive a signal!
    runtime.block_on(service_loop(dispatcher_ptr));

    // clean up folder in tmp that is used for function storage
    std::fs::remove_dir_all(FUNCTION_FOLDER_PATH).unwrap();
}

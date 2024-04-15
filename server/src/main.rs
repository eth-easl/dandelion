use bytes::Buf;
use core_affinity::{self, CoreId};
use dispatcher::{
    composition::CompositionSet, dispatcher::Dispatcher, function_registry::Metadata,
    resource_pool::ResourcePool,
};
use futures::lock::Mutex;
use futures::stream::StreamExt;
use http::StatusCode;
use hyper::{
    service::{make_service_fn, service_fn},
    Body, Request, Response, Server,
};

use log::{error, info};
use machine_interface::{
    function_driver::ComputeResource,
    machine_config::EngineType,
    memory_domain::{
        mmap::MmapMemoryDomain, read_only::ReadOnlyContext, Context, ContextTrait, MemoryDomain,
    },
    DataItem, DataSet, Position,
};
use signal_hook::consts::signal::*;
use signal_hook_tokio::Signals;

use serde::Deserialize;
use std::{
    collections::BTreeMap,
    convert::Infallible,
    io::{self, Write},
    mem::size_of,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc, Once,
    },
};
use tokio::runtime::Builder;

static INIT_MATRIX: Once = Once::new();
static mut DUMMY_MATRIX: Vec<i64> = Vec::new();
const FUNCTION_FOLDER_PATH: &str = "/tmp/dandelion_server";

// Handles graceful shutdown
async fn handle_signals(mut signals: Signals) -> io::Result<()> {
    #[allow(clippy::never_loop)]
    while let Some(signal) = signals.next().await {
        match signal {
            SIGTERM | SIGINT | SIGQUIT => {
                return Ok(());
            }
            _ => unreachable!(),
        }
    }
    Ok(())
}

async fn run_chain(
    dispatcher: Arc<Dispatcher>,
    is_cold: bool,
    function_name: String,
    get_uri: String,
    post_uri: String,
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

    let result = dispatcher
        .queue_function_by_name(function_name, inputs, output_mapping, is_cold)
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
    rows: usize,
    cols: usize,
    function_name: String,
) -> i64 {
    let mat_size: usize = rows * cols;

    // Initialize matrix if necessary
    unsafe {
        INIT_MATRIX.call_once(|| {
            // TODO: add cols
            DUMMY_MATRIX.push(rows as i64);
            for i in 0..mat_size {
                DUMMY_MATRIX.push(i as i64 + 1)
            }
        });
    }

    let input_context = unsafe { add_matmul_inputs(&mut DUMMY_MATRIX) };

    let inputs = vec![(
        0,
        CompositionSet::from((0, vec![(Arc::new(input_context))])),
    )];
    let outputs = vec![Some(0)];
    let result: Result<BTreeMap<usize, CompositionSet>, dandelion_commons::DandelionError> =
        dispatcher
            .queue_function_by_name(function_name, inputs, outputs, is_cold)
            .await;

    let result_context = result
        .expect("should get result from function")
        .remove(&0)
        .expect("should have composition set 0");

    return get_checksum(result_context);
}

// Add the matrix multiplication inputs to the given context
fn add_matmul_inputs(matrix: &'static mut Vec<i64>) -> Context {
    // Allocate a new set entry
    let matrix_size = matrix.len() * size_of::<i64>();
    let mut context = ReadOnlyContext::new_static(matrix);
    context.content.resize_with(1, || None);
    let _ = context.occupy_space(0, matrix_size);

    if let Some(set) = &mut context.content[0] {
        set.buffers.push(DataItem {
            ident: String::from(""),
            data: Position {
                offset: 0,
                size: matrix_size,
            },
            key: 0,
        });
    } else {
        context.content[0] = Some(DataSet {
            ident: "".to_string(),
            buffers: vec![DataItem {
                ident: "".to_string(),
                data: Position {
                    offset: 0,
                    size: matrix_size,
                },
                key: 0,
            }],
        });
    }
    return context;
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

#[derive(Deserialize)]
struct MatrixRequest {
    name: String,
    rows: u64,
    cols: u64,
}

async fn serve_request(
    is_cold: bool,
    req: Request<Body>,
    dispatcher: Arc<Dispatcher>,
) -> Result<Response<Body>, Infallible> {
    // Try to parse the request
    let request_buf = hyper::body::to_bytes(req.into_body())
        .await
        .expect("Could not read request body");
    let request_map: MatrixRequest =
        bson::from_slice(&request_buf).expect("Should be able to deserialize matrix request");

    let response_vec: Vec<u8> = run_mat_func(
        dispatcher,
        is_cold,
        request_map.rows as usize,
        request_map.cols as usize,
        request_map.name,
    )
    .await
    .to_be_bytes()
    .to_vec();
    let response = Ok::<_, Infallible>(Response::new(response_vec.into()));
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
    req: Request<Body>,
    dispatcher: Arc<Dispatcher>,
) -> Result<Response<Body>, Infallible> {
    let request_buf = hyper::body::to_bytes(req.into_body())
        .await
        .expect("Should be able to parse body");
    let request_map: ChainRequest =
        bson::from_slice(&request_buf).expect("Should be able to deserialize matrix request");

    let response_vec = run_chain(
        dispatcher,
        is_cold,
        request_map.name,
        request_map.get_uri,
        request_map.post_uri,
    )
    .await
    .to_be_bytes()
    .to_vec();
    let response = Ok::<_, Infallible>(Response::new(response_vec.into()));
    return response;
}

async fn serve_native(_req: Request<Body>) -> Result<Response<Body>, Infallible> {
    // Try to parse the request
    let mut request_buf = hyper::body::to_bytes(_req.into_body())
        .await
        .expect("Could not read request body");

    if request_buf.len() != 16 {
        let mut bad_request = Response::new(Body::empty());
        *bad_request.status_mut() = StatusCode::BAD_REQUEST;
        return Ok::<_, Infallible>(bad_request);
    }

    let rows = request_buf.get_i64() as usize;
    let cols = request_buf.get_i64() as usize;

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
    req: Request<Body>,
    dispatcher: Arc<Dispatcher>,
) -> Result<Response<Body>, Infallible> {
    let bytes = hyper::body::to_bytes(req.into_body())
        .await
        .expect("Failed to extract body from function registration");
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
    req: Request<Body>,
    dispatcher: Arc<Dispatcher>,
) -> Result<Response<Body>, Infallible> {
    let bytes = hyper::body::to_bytes(req.into_body())
        .await
        .expect("Failed to extract body from function registration");
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

async fn serve_stats(
    _req: Request<Body>,
    dispatcher: Arc<Dispatcher>,
) -> Result<Response<Body>, Infallible> {
    let archive_guard = match dispatcher.archive.lock() {
        Ok(guard) => guard,
        Err(_) => {
            return Ok::<_, Infallible>(Response::new("Could not lock archive for stats".into()))
        }
    };
    return Ok::<_, Infallible>(Response::new(archive_guard.get_summary().into()));
}

async fn service(
    req: Request<Body>,
    dispatcher: Arc<Dispatcher>,
) -> Result<Response<Body>, Infallible> {
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
        "/stats" => serve_stats(req, dispatcher).await,
        _ => Ok::<_, Infallible>(Response::new(
            // format!("Hello, World! You asked for: {}\n", uri).into(),
            format!("Hello, Wor\n").into(),
        )),
    }
}

fn main() -> () {
    env_logger::init();

    // find available resources
    let num_cores = u8::try_from(core_affinity::get_core_ids().unwrap().len()).unwrap();
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
        (num_dispatcher_cores..num_cores)
            .map(|code_id| ComputeResource::CPU(code_id))
            .collect(),
    );
    #[cfg(feature = "hyper_io")]
    pool_map.insert(
        EngineType::Hyper,
        (0..num_dispatcher_cores)
            .map(|core_id| ComputeResource::CPU(core_id))
            .collect(),
    );
    let resource_pool = ResourcePool {
        engine_pool: Mutex::new(pool_map),
    };

    // Create an ARC pointer to the dispatcher for thread-safe access
    let dispatcher_ptr =
        Arc::new(Dispatcher::init(resource_pool).expect("Should be able to start dispatcher"));

    let _guard = runtime.enter();

    // ready http endpoint
    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    let make_svc = make_service_fn(move |_| {
        let new_dispatcher_ptr = dispatcher_ptr.clone();
        async move {
            Ok::<_, Infallible>(service_fn(move |req| {
                let service_dispatcher_ptr = new_dispatcher_ptr.clone();
                service(req, service_dispatcher_ptr)
            }))
        }
    });
    let server = Server::bind(&addr).serve(make_svc);

    let signals = Signals::new(&[SIGTERM, SIGINT, SIGQUIT]).unwrap();
    let graceful =
        server.with_graceful_shutdown(async move { handle_signals(signals).await.unwrap() });

    #[cfg(feature = "cheri")]
    println!("Hello, World (cheri)");
    #[cfg(feature = "mmu")]
    println!("Hello, World (mmu)");
    #[cfg(feature = "wasm")]
    println!("Hello, World (wasm)");
    #[cfg(not(any(feature = "cheri", feature = "mmu", feature = "wasm")))]
    println!("Hello, World (native)");
    // Run this server for... forever... unless I receive a signal!
    if let Err(e) = runtime.block_on(graceful) {
        error!("server error: {}", e);
    }

    // clean up folder in tmp that is used for function storage
    std::fs::remove_dir_all(FUNCTION_FOLDER_PATH).unwrap();
}

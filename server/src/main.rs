use core_affinity::{self, CoreId};
use dandelion_commons::{ContextTypeId, EngineTypeId};
use dispatcher::{
    dispatcher::Dispatcher,
    function_registry::FunctionRegistry,
    resource_pool::ResourcePool,
    dispatcher::CompositionSet,
};
use futures::lock::Mutex;
use hyper::{
    service::{make_service_fn, service_fn},
    Body, Request, Response, Server,
};
use http::{StatusCode};
use bytes::Buf;

#[cfg(feature = "cheri")]
use machine_interface::{
    function_driver::{compute_driver::cheri::CheriDriver, Driver},
    memory_domain::{cheri::CheriMemoryDomain, ContextTrait, MemoryDomain, Context},
    DataItem, DataSet, Position,
};

#[cfg(not(feature = "cheri"))]
use machine_interface::{
    memory_domain::{malloc::MallocMemoryDomain, ContextTrait, MemoryDomain, Context},
    DataItem, DataSet, Position,
};

use std::{collections::BTreeMap, convert::Infallible, net::SocketAddr, sync::Arc};
use tokio::runtime::Builder;

const HOT_ID: u64 = 0;
const COLD_ID: u64 = 1;

static mut DUMMY_MATRIX: Vec<i64> = Vec::new();

async fn run_mat_func(dispatcher: Arc<Dispatcher>, is_cold: bool, rows: usize, cols: usize) -> (i64) {

    let mut inputs = Vec::new();
    let mut input_context;
    let mat_size: usize = rows * cols;
    let total_size = 2 * mat_size * 8 + 8;

    // Initialize matrix if necessary
    // NOTE: Not exactly thread safe but works for now
    unsafe {
        if DUMMY_MATRIX.is_empty() {
            for i in 0..mat_size {
                DUMMY_MATRIX.push(i as i64 + 1)
            }
        }
    }

    #[cfg(feature = "cheri")]
    let domain = CheriMemoryDomain::init(Vec::new()).expect("Should be able to initialize domain");
    #[cfg(not(feature = "cheri"))]
    let domain = MallocMemoryDomain::init(Vec::new()).expect("Should be able to initialize domain");

    input_context = domain
        .acquire_context(total_size)
        .expect("Should always have space");

    let size_offset = input_context
        .get_free_space(8, 8)
        .expect("Should have space");

    // TODO: Add cols argument to function
    input_context
        .write(size_offset, &[rows])
        .expect("Should be able to write");
    input_context.content.push(Some(DataSet {
        ident: "".to_string(),
        buffers: vec![DataItem {
            ident: "".to_string(),
            data: Position {
                offset: size_offset,
                size: 8,
            },
            key: 0,
        }],
    }));

    let in_map_offset = input_context
        .get_free_space(mat_size * 8, 8)
        .expect("Should have space");
    unsafe {
        input_context
            .write(in_map_offset, &DUMMY_MATRIX)
            .expect("Should be able to write input matrix");
    }

    input_context.content.push(Some(DataSet {
        ident: "".to_string(),
        buffers: vec![DataItem {
            ident: "".to_string(),
            data: Position {
                offset: in_map_offset,
                size: mat_size * 8,
            },
            key: 0,
        }],
    }));
    let input_rc = Arc::new(input_context);
    inputs.push((
        0,
        CompositionSet {
            context_list: vec![input_rc.clone()],
            set_index: 0,
            sharding_mode: dispatcher::dispatcher::ShardingMode::NoSharding,
        },
    ));

    inputs.push((
        1,
        CompositionSet {
            set_index: 1,
            context_list: vec![input_rc.clone()],
            sharding_mode: dispatcher::dispatcher::ShardingMode::NoSharding,
        },
    ));

    let result_context: Context = dispatcher
        .queue_function(is_cold as u64, inputs, is_cold)
        .await
        .expect("Should get back context");

    return get_checksum(result_context);

}

// Given a result context, return the last element of the resulting matrix
fn get_checksum(context: machine_interface::memory_domain::Context) -> i64 {

    // Determine offset of last matrix element
    let dataset = context.content[0]
        .as_ref()
        .expect("Should contain matrix");
    let checksum_offset = dataset.buffers[0].data.offset
        + dataset.buffers[0].data.size - 8;

    // Read out the checksum
    let mut read_buffer: Vec<u8> = vec![0; 8];
    context
        .read(checksum_offset, &mut read_buffer)
        .expect("Context should contain matrix");
    let checksum = i64::from_le_bytes(read_buffer.try_into().unwrap());

    return checksum;
}

async fn serve_request(
    is_cold: bool,
    _req: Request<Body>,
    dispatcher: Arc<Dispatcher>,
) -> Result<Response<Body>, Infallible> {

    // Try to parse the request
    let mut request_buf = hyper::body::to_bytes(_req.into_body())
        .await
        .expect("Could not read request body");

    if request_buf.len() != 16 {
        let mut bad_request = Response::new(Body::empty());
        *bad_request.status_mut() = StatusCode::BAD_REQUEST;
        return Ok::<_, Infallible>(bad_request)
    }

    let rows = request_buf.get_i64() as usize;
    let cols = request_buf.get_i64() as usize;

    let response_vec: Vec<u8> = run_mat_func(dispatcher, is_cold, rows, cols)
        .await.to_be_bytes().to_vec();
    let response = Ok::<_, Infallible>(Response::new(response_vec.into()));
    return response
}

async fn serve_cold(
    _req: Request<Body>,
    dispatcher: Arc<Dispatcher>,
) -> Result<Response<Body>, Infallible> {

    return serve_request(true, _req, dispatcher).await
}

async fn serve_hot(
    _req: Request<Body>,
    dispatcher: Arc<Dispatcher>,
) -> Result<Response<Body>, Infallible> {
    return serve_request(false, _req, dispatcher).await
}

async fn serve_native(_req: Request<Body>) -> Result<Response<Body>, Infallible> {
    // Try to parse the request
    let mut request_buf = hyper::body::to_bytes(_req.into_body())
        .await
        .expect("Could not read request body");

    if request_buf.len() != 16 {
        let mut bad_request = Response::new(Body::empty());
        *bad_request.status_mut() = StatusCode::BAD_REQUEST;
        return Ok::<_, Infallible>(bad_request)
    }

    let rows = request_buf.get_i64() as usize;
    let cols = request_buf.get_i64() as usize;

    let mat_size: usize = rows * cols;

    // Initialize matrix if necessary
    // NOTE: Not exactly thread safe but works for now
    unsafe {
        if DUMMY_MATRIX.is_empty() {
            for i in 0..mat_size {
                DUMMY_MATRIX.push(i as i64 + 1)
            }
        }
    }

    let mut out_mat: Vec<i64> = vec![0; mat_size];
    for i in 0..rows {
        for j in 0..rows {
            for k in 0..cols {
                unsafe{
                out_mat[i * rows + j] += 
                    DUMMY_MATRIX[i * cols + k] * DUMMY_MATRIX[j * cols + k];
                
                }
            }
        }
    }

    let checksum = out_mat[rows * cols - 1];
    
    Ok::<_, Infallible>(Response::new(checksum.to_be_bytes().to_vec().into()))
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
        "/cold" => serve_cold(req, dispatcher).await,
        "/hot" => serve_hot(req, dispatcher).await,
        "/native" => serve_native(req).await,
        "/stats" => serve_stats(req, dispatcher).await,
        _ => Ok::<_, Infallible>(Response::new(
            // format!("Hello, World! You asked for: {}\n", uri).into(),
            format!("Hello, Wor\n").into(),
        )),
    }
}

fn main() -> () {

    // set up dispatcher configuration basics
    let mut domains = BTreeMap::new();
    let context_id: ContextTypeId = 0;
    let engine_id: EngineTypeId = 0;
    let mut type_map = BTreeMap::new();
    type_map.insert(engine_id, context_id);
    let mut pool_map = BTreeMap::new();
    pool_map.insert(0, vec![1, 2, 3]);
    let resource_pool = ResourcePool {
        engine_pool: Mutex::new(pool_map),
    };
    let mut registry;
    // insert specific configuration
    // TODO this won't work if both features are enabled
    #[cfg(feature = "cheri")]
    {
        let mut drivers = BTreeMap::new();
        domains.insert(
            context_id,
            CheriMemoryDomain::init(Vec::new()).expect("Should be able to initialize domain"),
        );
        let driver: Box<dyn Driver> = Box::new(CheriDriver {});
        drivers.insert(engine_id, driver);
        let mut drivers: BTreeMap<_, Box<dyn Driver>> = BTreeMap::new();
        drivers.insert(0, Box::new(CheriDriver {}));
        registry = FunctionRegistry::new(drivers);
        let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("../machine_interface/tests/data/test_elf_cheri_matmul");
        // add for hot function
        registry.add_local(
            HOT_ID,
            engine_id,
            path.to_str().unwrap(),
            vec![String::from(""), String::from("")],
            vec![String::from("")],
        );
        // add for cold function
        registry.add_local(
            COLD_ID,
            engine_id,
            path.to_str().unwrap(),
            vec![String::from(""), String::from("")],
            vec![String::from("")],
        );
    }
    #[cfg(not(feature = "cheri"))]
    {
        // TODO: Add non-cheri driver once implemented
        let loader_map = BTreeMap::new();
        registry = FunctionRegistry::new(loader_map);
    }

    let dispatcher = Arc::new(
        Dispatcher::init(domains, type_map, registry, resource_pool)
            .expect("Should be able to start dispatcher"),
    );

    // make multithreaded front end that only uses core 0
    // set up tokio runtime, need io in any case
    let mut runtime_builder = Builder::new_multi_thread();
    runtime_builder.enable_io();
    runtime_builder.on_thread_start(|| {
        core_affinity::set_for_current(CoreId { id: 0usize });
        println!("Hello from Tokio thread");
    });
    let runtime = runtime_builder.build().unwrap();
    let _guard = runtime.enter();

    // ready http endpoint
    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    let make_svc = make_service_fn(move |_| {
        let new_dispatcher = dispatcher.clone();
        async move {
            Ok::<_, Infallible>(service_fn(move |req| {
                let service_dispatcher = new_dispatcher.clone();
                service(req, service_dispatcher)
            }))
        }
    });
    let server = Server::bind(&addr).serve(make_svc);

    println!("Hello, World");
    // Run this server for... forever!
    if let Err(e) = runtime.block_on(server) {
        eprintln!("server error: {}", e);
    }
}

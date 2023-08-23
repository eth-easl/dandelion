use core_affinity::{self, CoreId};
use dandelion_commons::{ContextTypeId, EngineTypeId};
use dispatcher::{
    dispatcher::Dispatcher, function_registry::FunctionRegistry, resource_pool::ResourcePool,
};
use futures::lock::Mutex;
use hyper::{
    service::{make_service_fn, service_fn},
    Body, Request, Response, Server,
};

#[cfg(feature = "cheri")]
use machine_interface::{
    function_driver::{compute_driver::cheri::CheriDriver, Driver},
    memory_domain::{cheri::CheriMemoryDomain, ContextTrait, MemoryDomain},
    DataItem, DataSet, Position,
};
use std::{collections::BTreeMap, convert::Infallible, net::SocketAddr, sync::Arc};
use tokio::runtime::Builder;

const MAT_DIM: usize = 128;
const MAT_SIZE: usize = MAT_DIM * MAT_DIM * 8;
static mut IN_MAT: [u8; MAT_SIZE] = [0; MAT_SIZE];
static IN_SIZE: u64 = MAT_DIM as u64;
const HOT_ID: u64 = 0;
const COLD_ID: u64 = 1;

#[cfg(feature = "cheri")]
async fn run_mat_func(dispatcher: Arc<Dispatcher>, non_caching: bool) -> () {
    use dispatcher::dispatcher::CompositionSet;

    let mut inputs = Vec::new();
    let mut input_context;
    let total_size = 2 * MAT_SIZE + 8;
    let domain = CheriMemoryDomain::init(Vec::new()).expect("Should be able to initialize domain");
    input_context = domain
        .acquire_context(total_size)
        .expect("Should always have space");
    let size_offset = input_context
        .get_free_space(8, 8)
        .expect("Should have space");
    input_context
        .write(size_offset, &[IN_SIZE])
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
        .get_free_space(MAT_SIZE, 8)
        .expect("Should have space");
    unsafe {
        input_context
            .write(in_map_offset, &IN_MAT)
            .expect("Should be able to write input matrix");
    }
    input_context.content.push(Some(DataSet {
        ident: "".to_string(),
        buffers: vec![DataItem {
            ident: "".to_string(),
            data: Position {
                offset: in_map_offset,
                size: MAT_SIZE,
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

    let result_context = dispatcher
        .queue_function(COLD_ID, inputs, non_caching)
        .await
        .expect("Should get back context");
    domain
        .release_context(result_context)
        .expect("Should be able to release result");
    let input_context_unwrapped = match Arc::try_unwrap(input_rc) {
        Ok(val) => val,
        Err(_) => panic!("Should get context from Rc"),
    };
    domain
        .release_context(input_context_unwrapped)
        .expect("Should be able to release input");
}

async fn serve_cold(
    _req: Request<Body>,
    dispatcher: Arc<Dispatcher>,
) -> Result<Response<Body>, Infallible> {
    #[cfg(feature = "cheri")]
    run_mat_func(dispatcher, true).await;
    let answer = "Done: Cold\n";
    Ok::<_, Infallible>(Response::new(answer.into()))
}

async fn serve_hot(
    _req: Request<Body>,
    dispatcher: Arc<Dispatcher>,
) -> Result<Response<Body>, Infallible> {
    #[cfg(feature = "cheri")]
    run_mat_func(dispatcher, false).await;
    let answer = "Done: Hot \n";
    Ok::<_, Infallible>(Response::new(answer.into()))
}

async fn serve_native(_req: Request<Body>) -> Result<Response<Body>, Infallible> {
    let mut in_mat: [u64; MAT_DIM * MAT_DIM] = [0; MAT_DIM * MAT_DIM];
    let mut out_mat: [u64; MAT_DIM * MAT_DIM] = [0; MAT_DIM * MAT_DIM];
    for i in 0..in_mat.len() {
        in_mat[i] = i as u64 + 1;
    }
    for i in 0..MAT_DIM {
        for j in 0..MAT_DIM {
            for k in 0..MAT_DIM {
                out_mat[i * MAT_DIM + j] += in_mat[i * MAT_DIM + k] * in_mat[j * MAT_DIM + k];
            }
        }
    }
    Ok::<_, Infallible>(Response::new("Done: Natv\n".into()))
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
    println!("Server Hello");
    // set up input data
    unsafe {
        for i in 0..MAT_SIZE {
            if i % 8 == 0 {
                IN_MAT[i] = 2;
            }
        }
    }

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
    // Run this server for... forever!
    if let Err(e) = runtime.block_on(server) {
        eprintln!("server error: {}", e);
    }
}

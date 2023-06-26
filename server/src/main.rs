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
use machine_interface::function_lib::cheri::CheriDriver;
#[cfg(feature = "cheri")]
use machine_interface::{
    function_lib::{
        cheri::{CheriDriver, CheriLoader},
        Driver, Loader, LoaderFunction,
    },
    memory_domain::{cheri::CheriMemoryDomain, ContextTrait, MemoryDomain},
    DataItem, Position,
};
#[cfg(feature = "pagetable")]
use machine_interface::{
    function_lib::{
        pagetable::PagetableDriver,
        Driver,
    },
    memory_domain::{pagetable::PagetableMemoryDomain, ContextTrait, MemoryDomain},
    DataItem, Position,
};
use std::{collections::HashMap, convert::Infallible, net::SocketAddr, sync::Arc};
use tokio::runtime::Builder;
// use std::time::Instant;

const MAT_DIM: usize = 128;
const MAT_SIZE: usize = MAT_DIM * MAT_DIM * 8;
static mut IN_MAT: [u8; MAT_SIZE] = [0; MAT_SIZE];
static IN_SIZE: [u8; 8] = u64::to_ne_bytes(MAT_DIM as u64);
const HOT_ID: u64 = 0;
const COLD_ID: u64 = 1;

async fn run_mat_func(dispatcher: Arc<Dispatcher>, non_caching: bool) -> () {
    let mut inputs = Vec::new();
    let mut input_context;
    {
        let total_size = 2 * MAT_SIZE + 8;
        #[cfg(feature = "cheri")]
        let domain =
            CheriMemoryDomain::init(Vec::new()).expect("Should be able to initialize domain");
        #[cfg(feature = "pagetable")]
        let domain =
            PagetableMemoryDomain::init(Vec::new()).expect("Should be able to initialize domain");
        input_context = domain
            .acquire_context(total_size)
            .expect("Should always have space");
        let size_offset = input_context
            .get_free_space(8, 8)
            .expect("Should have space");
        input_context
            .write(size_offset, Vec::<u8>::from(IN_SIZE))
            .expect("Should be able to write");
        input_context.dynamic_data.insert(
            0,
            DataItem::Item(Position {
                offset: size_offset,
                size: 8,
            }),
        );
        let in_map_offset = input_context
            .get_free_space(MAT_SIZE, 8)
            .expect("Should have space");
        unsafe {
            input_context
                .write(in_map_offset, Vec::<u8>::from(IN_MAT))
                .expect("Should be able to write input matrix");
        }
        input_context.dynamic_data.insert(
            1,
            DataItem::Item(Position {
                offset: in_map_offset,
                size: MAT_SIZE,
            }),
        );
        let out_map_offset = input_context
            .get_free_space(MAT_SIZE, 8)
            .expect("Should have space");
        input_context.dynamic_data.insert(
            2,
            DataItem::Item(Position {
                offset: out_map_offset,
                size: MAT_SIZE,
            }),
        );
        inputs.push((
            &input_context,
            vec![
                (0usize, None, 0usize),
                (1usize, None, 1usize),
                (2usize, None, 2usize),
            ],
        ));
        let result_context = dispatcher
            .queue_function(COLD_ID, inputs, non_caching)
            .await
            .expect("Should get back context");
        domain
            .release_context(result_context)
            .expect("Should be able to release result");
        domain
            .release_context(input_context)
            .expect("Should be able to release input");
        // let item = match result_context.dynamic_data.get(&0) {
        //     Some(item) => item,
        //     None => {
        //         let answer = format!("Dispatcher no output item no 0\n");
        //         return;
        //     }
        // };
        // if let DataItem::Item(position) = item {
        //     println!("item size: {}", position.size);
        //     for i in 0..MAT_DIM * MAT_DIM {
        //         let value = u64::from_ne_bytes(
        //             result_context
        //                 .read(position.offset + i * 8, 8)
        //                 .expect("Should read")[0..8]
        //                 .try_into()
        //                 .expect("Should have right size"),
        //         );
        //         println!("Dispatcher Ok with result = {:?}\n", value);
        //     }
        // }
    }
}

async fn serve_cold(
    _req: Request<Body>,
    dispatcher: Arc<Dispatcher>,
) -> Result<Response<Body>, Infallible> {
    run_mat_func(dispatcher, true).await;
    let answer = "Done: Cold\n";
    Ok::<_, Infallible>(Response::new(answer.into()))
}

async fn serve_hot(
    _req: Request<Body>,
    dispatcher: Arc<Dispatcher>,
) -> Result<Response<Body>, Infallible> {
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

async fn service(
    req: Request<Body>,
    dispatcher: Arc<Dispatcher>,
) -> Result<Response<Body>, Infallible> {
    let uri = req.uri().path();
    match uri {
        "/cold" => serve_cold(req, dispatcher).await,
        "/hot" => serve_hot(req, dispatcher).await,
        "/native" => serve_native(req).await,
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
    let mut domains = HashMap::new();
    let context_id: ContextTypeId = 0;
    let engine_id: EngineTypeId = 0;
    let mut drivers = HashMap::new();
    let mut type_map = HashMap::new();
    type_map.insert(engine_id, context_id);
    let mut pool_map = HashMap::new();
    pool_map.insert(0, vec![1, 2, 3]);
    let resource_pool = ResourcePool {
        engine_pool: Mutex::new(pool_map),
    };
    let mut registry;
    // insert specific configuration
    // TODO this won't work if both features are enabled
    #[cfg(feature = "cheri")]
    {
        let mut drivers = HashMap::new();
        domains.insert(
            context_id,
            CheriMemoryDomain::init(Vec::new()).expect("Should be able to initialize domain"),
        );
        let driver: Box<dyn Driver> = Box::new(CheriDriver {});
        drivers.insert(engine_id, driver);
        let mut loader_map = HashMap::new();
        loader_map.insert(0, CheriLoader::parse_function as LoaderFunction);
        registry = FunctionRegistry::new(loader_map);
        let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("../machine_interface/tests/data/test_elf_aarch64c_matmul");
        // add for hot function
        registry.add_local(HOT_ID, engine_id, path.to_str().unwrap());
        // add for cold function
        registry.add_local(COLD_ID, engine_id, path.to_str().unwrap());
    }
    #[cfg(feature = "pagetable")]
    {
        domains.insert(
            context_id,
            PagetableMemoryDomain::init(Vec::new()).expect("Should be able to initialize domain"),
        );
        let driver: Box<dyn Driver> = Box::new(PagetableDriver {});
        drivers.insert(engine_id, driver);
        registry = FunctionRegistry::new(drivers);
        let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("../machine_interface/tests/data/test_elf_x86c_matmul");
        // add for hot function
        registry.add_local(HOT_ID, engine_id, path.to_str().unwrap());
        // add for cold function
        registry.add_local(COLD_ID, engine_id, path.to_str().unwrap());
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
    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
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

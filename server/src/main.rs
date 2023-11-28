use bytes::Buf;
use core_affinity::{self, CoreId};
use dandelion_commons::{ContextTypeId, EngineTypeId};
use dispatcher::{
    composition::{Composition, FunctionDependencies},
    dispatcher::{CompositionSet, Dispatcher},
    function_registry::FunctionRegistry,
    resource_pool::ResourcePool,
};
use futures::lock::Mutex;
use http::StatusCode;
use hyper::{
    service::{make_service_fn, service_fn},
    Body, Request, Response, Server,
};
use machine_interface::{
    function_driver::{
        system_driver::{get_system_function_input_sets, get_system_function_output_sets},
        SystemFunction,
    },
    memory_domain::{malloc::MallocMemoryDomain, read_only::ReadOnlyContext},
};

#[cfg(feature = "hyper_io")]
use machine_interface::function_driver::system_driver::hyper::HyperDriver;

#[cfg(feature = "cheri")]
use machine_interface::{
    function_driver::{compute_driver::cheri::CheriDriver, Driver},
    memory_domain::{cheri::CheriMemoryDomain, Context, ContextTrait, MemoryDomain},
    DataItem, DataSet, Position,
};

#[cfg(feature = "mmu")]
use machine_interface::{
    function_driver::{compute_driver::mmu::MmuDriver, Driver},
    memory_domain::{mmu::MmuMemoryDomain, Context, ContextTrait, MemoryDomain},
    DataItem, DataSet, Position,
};

#[cfg(not(any(feature = "cheri", feature = "mmu")))]
use machine_interface::{
    memory_domain::{Context, ContextTrait, MemoryDomain},
    DataItem, DataSet, Position,
};

use std::{
    collections::BTreeMap,
    convert::Infallible,
    mem::size_of,
    net::SocketAddr,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc, Once,
    },
};
use tokio::runtime::Builder;

const HOT_ID: u64 = 0;
const COLD_ID: u64 = 1;
const HTTP_ID: u64 = 2;
const BUSY_ID: u64 = 3;
const COMPOSITION_ID: u64 = 4;

static INIT_MATRIX: Once = Once::new();
static mut DUMMY_MATRIX: Vec<i64> = Vec::new();

async fn run_chain(dispatcher: Arc<Dispatcher>, get_uri: String, post_uri: String) -> u64 {
    let domain = MallocMemoryDomain::init(vec![]).expect("Should be able to get malloc domain");
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
        (
            0,
            CompositionSet {
                context_list: vec![input_arc.clone()],
                sharding_mode: dispatcher::dispatcher::ShardingMode::NoSharding,
                set_index: 0,
            },
        ),
        (
            5,
            CompositionSet {
                context_list: vec![input_arc],
                sharding_mode: dispatcher::dispatcher::ShardingMode::NoSharding,
                set_index: 1,
            },
        ),
    ];
    let output_mapping = vec![Some(0), Some(1)];

    let result = dispatcher
        .queue_function(COMPOSITION_ID, inputs, output_mapping, false)
        .await
        .expect("Should get response from chain");
    assert_eq!(2, result.len());
    // check http post response
    let post_composition_set = result
        .get(&1)
        .expect("Should have composition set for post response");
    assert_eq!(1, post_composition_set.context_list.len());
    let post_context = &post_composition_set.context_list[0];
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
    let result_context = &result_compositon_set.context_list[0];
    assert_eq!(1, result_context.content.len());
    let result_set = result_context.content[0]
        .as_ref()
        .expect("Should contain a return number");
    assert_eq!(1, result_set.buffers.len());
    let result_position = result_set.buffers[0].data;
    assert_eq!(8, result_position.size);
    let mut result_vec = Vec::<u64>::with_capacity(1);
    result_vec.resize(1, 0);
    result_context
        .read(result_position.offset, result_vec.as_mut_slice())
        .expect("Should be able to read result");
    return result_vec[0];
}

async fn run_mat_func(dispatcher: Arc<Dispatcher>, is_cold: bool, rows: usize, cols: usize) -> i64 {
    let mat_size: usize = rows * cols;
    // [rows] [input_matrix]
    let context_size: usize = (1 + mat_size) * 8;

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

    let mut input_context = unsafe { add_matmul_inputs(&mut DUMMY_MATRIX) };

    let inputs = vec![(
        0,
        CompositionSet {
            context_list: vec![(Arc::new(input_context))],
            sharding_mode: dispatcher::dispatcher::ShardingMode::NoSharding,
            set_index: 0,
        },
    )];
    let outputs = vec![Some(0)];
    let result = dispatcher
        .queue_function(is_cold as u64, inputs, outputs, is_cold)
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
    context.occupy_space(0, matrix_size);

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
    let context = &composition_set.context_list[0];
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
    req: Request<Body>,
    dispatcher: Arc<Dispatcher>,
) -> Result<Response<Body>, Infallible> {
    // Try to parse the request
    let mut request_buf = hyper::body::to_bytes(req.into_body())
        .await
        .expect("Could not read request body");

    if request_buf.len() != 16 {
        let mut bad_request = Response::new(Body::empty());
        *bad_request.status_mut() = StatusCode::BAD_REQUEST;
        return Ok::<_, Infallible>(bad_request);
    }

    let rows = request_buf.get_i64() as usize;
    let cols = request_buf.get_i64() as usize;

    let response_vec: Vec<u8> = run_mat_func(dispatcher, is_cold, rows, cols)
        .await
        .to_be_bytes()
        .to_vec();
    let response = Ok::<_, Infallible>(Response::new(response_vec.into()));
    return response;
}

async fn serve_chain(
    req: Request<Body>,
    dispatcher: Arc<Dispatcher>,
) -> Result<Response<Body>, Infallible> {
    let request_buf = hyper::body::to_bytes(req.into_body())
        .await
        .expect("Should be able to parse body");

    let request_str = std::str::from_utf8(&request_buf).unwrap();
    let uris: Vec<&str> = request_str.split("::").collect();
    let get_uri = uris[0].to_string();
    let post_uri = uris[1].to_string();

    let response_vec = run_chain(dispatcher, get_uri, post_uri)
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
        "/cold/matmul" => serve_request(true, req, dispatcher).await,
        "/hot/matmul" => serve_request(false, req, dispatcher).await,
        "/cold/compute" => serve_chain(req, dispatcher).await,
        "/hot/compute" => serve_chain(req, dispatcher).await,
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
    const COMPUTE_DOMAIN: ContextTypeId = 0;
    const COMPUTE_ENGINE: EngineTypeId = 0;
    const SYS_CONTEXT: ContextTypeId = 1;
    const SYS_ENGINE: EngineTypeId = 1;
    let mut type_map = BTreeMap::new();
    type_map.insert(COMPUTE_ENGINE, COMPUTE_DOMAIN);
    type_map.insert(SYS_ENGINE, SYS_CONTEXT);
    let mut pool_map = BTreeMap::new();
    pool_map.insert(COMPUTE_ENGINE, (1..=3).collect());
    pool_map.insert(SYS_ENGINE, (0..128).collect());
    let resource_pool = ResourcePool {
        engine_pool: Mutex::new(pool_map),
    };
    domains.insert(
        SYS_CONTEXT,
        MallocMemoryDomain::init(Vec::new()).expect("Should be able to initialize malloc domain"),
    );
    let mut registry;
    // insert specific configuration
    #[cfg(all(feature = "cheri", feature = "mmu"))]
    std::compile_error!("Should only have one feature out of mmu or cheri");
    #[cfg(all(any(feature = "cheri", feature = "mmu"), feature = "hyper_io"))]
    {
        let mut drivers = BTreeMap::new();
        let mut mmm_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let mut busy_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let driver;
        #[cfg(feature = "cheri")]
        {
            domains.insert(
                COMPUTE_DOMAIN,
                CheriMemoryDomain::init(Vec::new()).expect("Should be able to initialize domain"),
            );
            driver = Box::new(CheriDriver {}) as Box<dyn Driver>;
            mmm_path.push("../machine_interface/tests/data/test_elf_cheri_matmul");
            busy_path.push("../machine_interface/tests/data/test_elf_cheri_busy");
        }
        #[cfg(feature = "mmu")]
        {
            domains.insert(
                COMPUTE_DOMAIN,
                MmuMemoryDomain::init(Vec::new()).expect("Should be able to initialize domain"),
            );
            driver = Box::new(MmuDriver {}) as Box<dyn Driver>;
            mmm_path.push(format!(
                "../machine_interface/tests/data/test_elf_mmu_{}_matmul",
                std::env::consts::ARCH
            ));
            busy_path.push(format!(
                "../machine_interface/tests/data/test_elf_mmu_{}_busy",
                std::env::consts::ARCH
            ));
        }
        let system_driver = Box::new(HyperDriver {});
        drivers.insert(COMPUTE_ENGINE, driver);
        drivers.insert(SYS_ENGINE, system_driver);
        registry = FunctionRegistry::new(drivers);
        // add for hot function
        registry.add_local(
            HOT_ID,
            COMPUTE_ENGINE,
            mmm_path.to_str().unwrap(),
            vec![String::from("")],
            vec![String::from("")],
        );
        // add for cold function
        registry.add_local(
            COLD_ID,
            COMPUTE_ENGINE,
            mmm_path.to_str().unwrap(),
            vec![String::from("")],
            vec![String::from("")],
        );
        // add for cold function
        registry.add_local(
            BUSY_ID,
            COMPUTE_ENGINE,
            busy_path.to_str().unwrap(),
            vec![String::from("")],
            vec![String::from("")],
        );
        // add http system function
        // first try only download and spin, TODO: add upload
        registry
            .add_system(HTTP_ID, SYS_ENGINE)
            .expect("Should be able to add system function");
        let composition = Composition {
            dependencies: vec![
                FunctionDependencies {
                    function: HTTP_ID,
                    input_set_ids: vec![Some(0), None, None],
                    output_set_ids: vec![Some(1), Some(2), Some(3)],
                },
                FunctionDependencies {
                    function: BUSY_ID,
                    input_set_ids: vec![Some(3)],
                    output_set_ids: vec![Some(4)],
                },
                FunctionDependencies {
                    function: HTTP_ID,
                    input_set_ids: vec![Some(5), None, Some(4)],
                    output_set_ids: vec![Some(6)],
                },
            ],
        };
        let output_set_map = BTreeMap::from([(4, 0), (6, 1)]);
        let input_sets = get_system_function_input_sets(SystemFunction::HTTP);
        let output_sets = get_system_function_output_sets(SystemFunction::HTTP);
        registry.add_composition(
            COMPOSITION_ID,
            composition,
            input_sets,
            output_sets,
            output_set_map,
        );
    }
    #[cfg(not(all(any(feature = "cheri", feature = "mmu"), feature = "hyper_io")))]
    {
        let loader_map = BTreeMap::new();
        registry = FunctionRegistry::new(loader_map);
    }

    // Create an ARC pointer to the dispatcher for thread-safe access
    let dispatcher_ptr = Arc::new(
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
        let new_dispatcher_ptr = dispatcher_ptr.clone();
        async move {
            Ok::<_, Infallible>(service_fn(move |req| {
                let service_dispatcher_ptr = new_dispatcher_ptr.clone();
                service(req, service_dispatcher_ptr)
            }))
        }
    });
    let server = Server::bind(&addr).serve(make_svc);

    #[cfg(feature = "cheri")]
    println!("Hello, World (cheri)");
    #[cfg(feature = "mmu")]
    println!("Hello, World (mmu)");
    #[cfg(not(any(feature = "cheri", feature = "mmu")))]
    println!("Hello, World (native)");
    // Run this server for... forever!
    if let Err(e) = runtime.block_on(server) {
        eprintln!("server error: {}", e);
    }
}

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
use log::{error, info};
use machine_interface::{
    function_driver::{
        system_driver::{get_system_function_input_sets, get_system_function_output_sets},
        ComputeResource, SystemFunction,
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

#[cfg(feature = "wasm")]
use machine_interface::{
    function_driver::{compute_driver::wasm::WasmDriver, Driver},
    memory_domain::{wasm::WasmMemoryDomain, Context, ContextTrait, MemoryDomain},
    DataItem, DataSet, Position,
};

#[cfg(not(any(feature = "cheri", feature = "mmu", feature = "wasm")))]
use machine_interface::{
    memory_domain::{Context, ContextTrait, MemoryDomain},
    DataItem, DataSet, Position,
};

use std::{
    collections::BTreeMap,
    convert::Infallible,
    mem::size_of,
    net::SocketAddr,
    path::PathBuf,
    process::Command,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc, Once,
    },
    time::Duration,
};
use tokio::runtime::Builder;

const MMM_ID: u64 = 0;
const HTTP_ID: u64 = 2;
const BUSY_ID: u64 = 3;
const COMPOSITION_ID: u64 = 4;

// can support 10000 RPS for 2 mins
const NUM_COLD: u64 = 0x100000;
const MMM_COLD_ID_BASE: u64 = 0x1000000;
const BUSY_COLD_ID_BASE: u64 = 0x2000000;
const COMPOSITION_COLD_ID_BASE: u64 = 0x3000000;

static INIT_MATRIX: Once = Once::new();
static mut DUMMY_MATRIX: Vec<i64> = Vec::new();

async fn run_chain(
    dispatcher: Arc<Dispatcher>,
    is_cold: bool,
    get_uri: String,
    post_uri: String,
) -> u64 {
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

    let counter = dispatcher.counter.fetch_add(1, Ordering::Relaxed);
    let function_id = if !is_cold {
        COMPOSITION_ID
    } else {
        COMPOSITION_COLD_ID_BASE + counter % NUM_COLD
    };
    let result = dispatcher
        .queue_function(function_id, inputs, output_mapping, false)
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

    let mut result_vec = vec![0u8; result_position.size];
    result_context
        .read(result_position.offset, result_vec.as_mut_slice())
        .expect("Should be able to read result");
    let checksum = u64::from_ne_bytes(result_vec[0..8].try_into().unwrap());

    return checksum;
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
    let counter = dispatcher.counter.fetch_add(1, Ordering::Relaxed);
    let function_id = if !is_cold {
        MMM_ID
    } else {
        MMM_COLD_ID_BASE + counter % NUM_COLD
    };
    let result = dispatcher
        .queue_function(function_id, inputs, outputs, is_cold)
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
    is_cold: bool,
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

    let response_vec = run_chain(dispatcher, is_cold, get_uri, post_uri)
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
    // println!("Got request for {}", uri);
    match uri {
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

fn drop_page_caches() {
    let output = Command::new("sh")
        .arg("-c")
        .arg("echo 1 | sudo tee /proc/sys/vm/drop_caches")
        .output()
        .expect("Should be able to drop page caches");
    assert!(output.status.success());
}

fn no_page_cache_for(path: PathBuf) -> bool {
    let output = Command::new("fincore")
        .args(["-n", "-r", "-o", "pages", path.to_str().unwrap()])
        .output()
        .expect("Should be able to get page numbers");
    assert!(output.status.success());
    return output.stdout[0] == '0' as u8;
}

fn add_cold_functions(
    registry: &mut FunctionRegistry,
    engine_id: EngineTypeId,
    path: &PathBuf,
    cold_id_base: u64,
) -> PathBuf {
    let tmp_dir = std::path::Path::new("/tmp").join(path.file_name().unwrap());
    std::fs::create_dir_all(&tmp_dir).unwrap();
    for i in 0..NUM_COLD {
        let tmp_path = tmp_dir.join(i.to_string());
        if !tmp_path.exists() {
            std::fs::copy(path, &tmp_path).unwrap();
        }
        registry.add_local(
            cold_id_base + i,
            engine_id,
            tmp_path.to_str().unwrap(),
            vec![String::from("")],
            vec![String::from("")],
        );
    }
    return tmp_dir;
}

fn main() -> () {
    env_logger::init();
    // set up dispatcher configuration basics
    let mut domains = BTreeMap::new();
    const COMPUTE_DOMAIN: ContextTypeId = 0;
    const COMPUTE_ENGINE: EngineTypeId = 0;
    const SYS_CONTEXT: ContextTypeId = 1;
    const SYS_ENGINE: EngineTypeId = 1;
    let mut type_map = BTreeMap::new();
    type_map.insert(COMPUTE_ENGINE, COMPUTE_DOMAIN);
    type_map.insert(SYS_ENGINE, SYS_CONTEXT);
    let num_cores = u8::try_from(core_affinity::get_core_ids().unwrap().len()).unwrap();
    // TODO: This calculation makes sense only for running matmul-128x128 workload on MMU engines
    let num_dispatcher_cores = std::env::var("NUM_DISP_CORES")
        .map_or_else(|_e| (num_cores + 13) / 14, |n| n.parse::<u8>().unwrap());
    assert!(
        num_dispatcher_cores > 0 && num_dispatcher_cores < num_cores,
        "invalid dispatcher core number: {}",
        num_dispatcher_cores
    );
    let mut pool_map = BTreeMap::new();
  
    pool_map.insert(
        COMPUTE_ENGINE,
        (num_dispatcher_cores..num_cores)
            .map(|code_id| ComputeResource::CPU(code_id))
            .collect(),
    );
    pool_map.insert(
        SYS_ENGINE,
        (0..num_dispatcher_cores)
            .map(|core_id| ComputeResource::CPU(core_id))
            .collect(),
    );
    let resource_pool = ResourcePool {
        engine_pool: Mutex::new(pool_map),
    };
    domains.insert(
        SYS_CONTEXT,
        MallocMemoryDomain::init(Vec::new()).expect("Should be able to initialize malloc domain"),
    );
    let mut registry;
    // insert specific configuration
    #[cfg(all(feature = "cheri", feature = "mmu", feature = "wasm"))]
    std::compile_error!("Should only have one feature out of mmu or cheri or wasm");
    #[cfg(all(any(feature = "cheri", feature = "mmu", feature = "wasm"), feature = "hyper_io"))]
    {
        let mut drivers = BTreeMap::new();
        let mut mmm_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let mut busy_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
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
        #[cfg(feature = "wasm")]
        {
            domains.insert(
                COMPUTE_DOMAIN,
                WasmMemoryDomain::init(Vec::new()).expect("Should be able to initialize domain"),
            );
            driver = Box::new(WasmDriver {}) as Box<dyn Driver>;
            mmm_path.push(format!(
                "../machine_interface/tests/data/test_sysld_wasm_{}_matmul",
                std::env::consts::ARCH
            ));
            busy_path.push(format!(
                "../machine_interface/tests/data/test_sysld_wasm_{}_busy",
                std::env::consts::ARCH
            ));
        }
        let system_driver = Box::new(HyperDriver {});
        drivers.insert(COMPUTE_ENGINE, driver);
        drivers.insert(SYS_ENGINE, system_driver);
        registry = FunctionRegistry::new(drivers);
        // add for mmm hot function
        registry.add_local(
            MMM_ID,
            COMPUTE_ENGINE,
            mmm_path.to_str().unwrap(),
            vec![String::from("")],
            vec![String::from("")],
        );
        // add for mmm cold functions
        let mmm_cold_dir =
            add_cold_functions(&mut registry, COMPUTE_ENGINE, &mmm_path, MMM_COLD_ID_BASE);
        // add for busy hot functions
        registry.add_local(
            BUSY_ID,
            COMPUTE_ENGINE,
            busy_path.to_str().unwrap(),
            vec![String::from("")],
            vec![String::from("")],
        );
        // add for busy cold functions
        let busy_cold_dir =
            add_cold_functions(&mut registry, COMPUTE_ENGINE, &busy_path, BUSY_COLD_ID_BASE);
        // add http system function
        // first try only download and spin, TODO: add upload
        registry
            .add_system(HTTP_ID, SYS_ENGINE)
            .expect("Should be able to add system function");
        // add composition using hot busy function
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
        // add compositions using cold busy functions
        for i in 0..NUM_COLD {
            let composition = Composition {
                dependencies: vec![
                    FunctionDependencies {
                        function: HTTP_ID,
                        input_set_ids: vec![Some(0), None, None],
                        output_set_ids: vec![Some(1), Some(2), Some(3)],
                    },
                    FunctionDependencies {
                        function: BUSY_COLD_ID_BASE + i,
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
                COMPOSITION_COLD_ID_BASE + i,
                composition,
                input_sets,
                output_sets,
                output_set_map,
            );
        }
        // drop page caches to ensure cold functions are loaded from disk
        loop {
            info!("Waiting for page cache to be clean");
            drop_page_caches();
            std::thread::sleep(Duration::from_secs(10));
            // check if page caches are actually dropped
            let mut no_page_cache_for_all = true;
            for i in (0..NUM_COLD).step_by(1000) {
                let mmm_cold_path = mmm_cold_dir.join(i.to_string());
                let busy_cold_path = busy_cold_dir.join(i.to_string());
                if !no_page_cache_for(mmm_cold_path) || !no_page_cache_for(busy_cold_path) {
                    no_page_cache_for_all = false;
                    break;
                }
            }
            if no_page_cache_for_all {
                break;
            }
        }
    }
    #[cfg(not(all(any(feature = "cheri", feature = "mmu", feature = "wasm"), feature = "hyper_io")))]
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
    #[cfg(feature = "wasm")]
    println!("Hello, World (wasm)");
    #[cfg(not(any(feature = "cheri", feature = "mmu", feature = "wasm")))]
    println!("Hello, World (native)");
    // Run this server for... forever!
    if let Err(e) = runtime.block_on(server) {
        error!("server error: {}", e);
    }
}

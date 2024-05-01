use std::fs;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use dandelion_commons::records::{Archive, RecordPoint, Recorder};
use libc::c_void;
use machine_interface::function_driver::compute_driver::gpu::hip;
use machine_interface::function_driver::thread_utils::EngineLoop;
use machine_interface::promise::Promise;
use machine_interface::{
    function_driver::{
        compute_driver::gpu::{GpuDriver, GpuLoop},
        test_queue::TestQueue,
        ComputeResource, Driver, EngineArguments, FunctionArguments,
    },
    memory_domain::{mmu::MmuMemoryDomain, MemoryDomain, MemoryResource},
    DataItem, DataSet, Position,
};

const ROWS_ARR: [usize; 3] = [1usize, 128, 1024];

fn context_allocation(c: &mut Criterion) {
    let mut group = c.benchmark_group("context_allocation");

    if !core_affinity::set_for_current(core_affinity::CoreId { id: 3 }) {
        log::error!("core received core id that could not be set");
        panic!("Couldn't set core affinity");
    }

    let filename = "/home/smithj/dandelion/machine_interface/hip_interface/matmul_para.json";
    let dom_init = MemoryResource::None;
    let driver: Box<dyn Driver> = Box::new(GpuDriver {});
    let domain = MmuMemoryDomain::init(dom_init).expect("Should have initialized domain");
    let function = driver
        .parse_function(filename.to_string(), &domain)
        .expect("Should be able to parse function");
    for rows in ROWS_ARR {
        group.bench_with_input(format!("{} x {} output", rows, rows), &rows, |b, &rows| {
            b.iter_custom(|iters| {
                let mut total = std::time::Duration::new(0, 0);
                for _ in 0..iters {
                    let mut function_context = function
                        // 64MiB context should be enough to hold everything
                        .load(&domain, 1 << 30)
                        .expect("Should be able to load function");

                    let begin = std::time::Instant::now();
                    function_context
                        .get_free_space(8 + rows * rows * 8, 8)
                        .expect("Getting space should succeed");
                    let duration = begin.elapsed();
                    total += duration;
                }
                total
            })
        });
    }
}

fn engine_loop_run_matmul(c: &mut Criterion) {
    let core_id = 3u8;
    let mut group = c.benchmark_group("matmul_benchmark");
    group.measurement_time(std::time::Duration::new(30, 0));

    // mostly taken from compute_driver_tests
    let filename = "/home/smithj/dandelion/machine_interface/hip_interface/matmul_para.json";
    let dom_init = MemoryResource::None;
    let driver: Box<dyn Driver> = Box::new(GpuDriver {});
    let domain = MmuMemoryDomain::init(dom_init).expect("Should have initialized domain");
    let function = driver
        .parse_function(filename.to_string(), &domain)
        .expect("Should be able to parse function");
    let mut engine_loop =
        GpuLoop::init(ComputeResource::GPU(core_id, 0)).expect("Should be able to init engineloop");
    // set core affinity
    if !core_affinity::set_for_current(core_affinity::CoreId { id: core_id.into() }) {
        log::error!("core received core id that could not be set");
        panic!("Couldn't set core affinity");
    }

    for rows in ROWS_ARR {
        // each elem in the matrix is 8 bytes
        group.throughput(Throughput::Bytes((rows * rows * 8) as u64));

        group.bench_with_input(
            BenchmarkId::new(format!("{} x {}", rows, rows), rows),
            &rows,
            |b, &rows| {
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::new(0, 0);
                    for _ in 0..iters {
                        let config = function.config.clone();
                        let mut function_context = function
                            // 64MiB context should be enough to hold everything
                            .load(&domain, 1 << 30)
                            .expect("Should be able to load function");
                        let mut mat_vec = Vec::<i64>::new();
                        mat_vec.push(rows as i64);
                        for i in 0..(rows * rows) {
                            mat_vec.push(i as i64);
                        }
                        let in_mat_offset = function_context
                            .get_free_space_and_write_slice(&mat_vec)
                            .expect("Should have space");
                        function_context.content.push(Some(DataSet {
                            ident: "A".to_string(),
                            buffers: vec![DataItem {
                                ident: "".to_string(),
                                data: Position {
                                    offset: in_mat_offset as usize,
                                    size: mat_vec.len() * 8,
                                },
                                key: 0,
                            }],
                        }));
                        let output_sets = Arc::new(vec![String::from("B")]);

                        let begin = std::time::Instant::now();
                        engine_loop
                            .run(config, function_context, output_sets)
                            .unwrap();
                        let duration = begin.elapsed();
                        total += duration;
                    }
                    total
                });
            },
        );
    }
}

fn get_inference_inputs() -> (Vec<f32>, Vec<f32>) {
    let mut a: Vec<f32> = Vec::with_capacity(224 * 224 + 2);
    let mut b: Vec<f32> = Vec::with_capacity(5 * 5 + 2);

    a.push(224.0);
    a.push(224.0);
    for i in 0..224 * 224 {
        a.push(i as f32);
    }

    b.push(5.0);
    b.push(5.0);
    for i in 0..5 * 5 {
        b.push(i as f32);
    }

    (a, b)
}

fn inference_benchmark_latency(c: &mut Criterion) {
    let filename = "/home/smithj/dandelion/machine_interface/hip_interface/inference.json";
    let dom_init = MemoryResource::None;
    let driver: Box<dyn Driver> = Box::new(GpuDriver {});
    let drv_init = ComputeResource::GPU(7, 0);
    let queue = Box::new(TestQueue::new());
    let domain = MmuMemoryDomain::init(dom_init).expect("Should have initialized domain");
    let function = driver
        .parse_function(filename.to_string(), &domain)
        .expect("Should be able to parse function");
    driver
        .start_engine(drv_init, queue.clone())
        .expect("Should be able to start engine");

    c.bench_function("Engine Latency", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::new(0, 0);
            for _ in 0..iters {
                let mut function_context = function
                    .load(&domain, 1 << 25)
                    .expect("Should be able to load function");
                let config = function.config.clone();
                let d_size: usize = 112 * 112 * 4 + 8; //side_len * side_len * sizeof(float) + [[size convention at start]]
                let a_grid_dim: usize = (224 + 31) / 32;
                let d_grid_dim: usize = (112 + 31) / 32;
                let (a_matrix, b_matrix) = get_inference_inputs();
                let cfg_offset = function_context
                    .get_free_space_and_write_slice(&[d_size, a_grid_dim, d_grid_dim])
                    .expect("Should have space for cfg");
                let a_offset = function_context
                    .get_free_space_and_write_slice(&a_matrix)
                    .expect("Should have space for A");
                let b_offset = function_context
                    .get_free_space_and_write_slice(&b_matrix)
                    .expect("Should have space for B");

                function_context.content.append(&mut vec![
                    Some(DataSet {
                        ident: "cfg".to_string(),
                        buffers: vec![DataItem {
                            ident: "".to_string(),
                            data: Position {
                                offset: cfg_offset as usize,
                                size: 3 * 8,
                            },
                            key: 0,
                        }],
                    }),
                    Some(DataSet {
                        ident: "A".to_string(),
                        buffers: vec![DataItem {
                            ident: "".to_string(),
                            data: Position {
                                offset: a_offset as usize,
                                size: a_matrix.len() * 4,
                            },
                            key: 0,
                        }],
                    }),
                    Some(DataSet {
                        ident: "B".to_string(),
                        buffers: vec![DataItem {
                            ident: "".to_string(),
                            data: Position {
                                offset: b_offset as usize,
                                size: b_matrix.len() * 4,
                            },
                            key: 0,
                        }],
                    }),
                ]);
                let archive = Arc::new(Mutex::new(Archive::new()));
                let recorder = Recorder::new(archive, RecordPoint::TransferEnd);

                let begin = std::time::Instant::now();
                let promise = queue.enqueu(EngineArguments::FunctionArguments(FunctionArguments {
                    config,
                    context: function_context,
                    output_sets: Arc::new(vec![String::from("D")]),
                    recorder,
                }));
                let _ = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap()
                    .block_on(promise)
                    .expect("Engine should run ok with basic function");
                let duration = begin.elapsed();
                total += duration;
            }
            total
        })
    });
}

fn module_load_helper(iters: u64, path: &str) -> Duration {
    let mut total = Duration::new(0, 0);

    for _ in 0..iters {
        let begin = std::time::Instant::now();
        let module = hip::module_load(path).unwrap();
        let duration = begin.elapsed();

        drop(module);
        total += duration
    }
    total
}

fn module_load_data_helper(iters: u64, path: &str) -> Duration {
    let mut total = Duration::new(0, 0);

    let file = fs::read(path).unwrap();
    let image = file.as_ptr() as *const c_void;

    for _ in 0..iters {
        let begin = std::time::Instant::now();
        let module = hip::module_load_data(image).unwrap();
        let duration = begin.elapsed();

        drop(module);
        total += duration
    }
    total
}

fn loading(c: &mut Criterion) {
    let mut group = c.benchmark_group("Loading");

    // hipModuleLoad
    group.bench_with_input(
        BenchmarkId::new("hipModuleLoad", 34),
        "/home/smithj/dandelion/machine_interface/hip_interface/matmul.hsaco",
        |b, path| b.iter_custom(|iters| module_load_helper(iters, path)),
    );

    group.bench_with_input(
        BenchmarkId::new("hipModuleLoad", 61),
        "/home/smithj/dandelion/machine_interface/hip_interface/module.hsaco",
        |b, path| b.iter_custom(|iters| module_load_helper(iters, path)),
    );

    group.bench_with_input(
        BenchmarkId::new("hipModuleLoad", 87),
        "/home/smithj/dandelion/machine_interface/hip_interface/combined.hsaco",
        |b, path| b.iter_custom(|iters| module_load_helper(iters, path)),
    );

    group.bench_with_input(
        BenchmarkId::new("hipModuleLoad", 121),
        "/home/smithj/dandelion/machine_interface/hip_interface/bigger.hsaco",
        |b, path| b.iter_custom(|iters| module_load_helper(iters, path)),
    );

    // hipModuleLoadData
    group.bench_with_input(
        BenchmarkId::new("hipModuleLoadData", 34),
        "/home/smithj/dandelion/machine_interface/hip_interface/matmul.hsaco",
        |b, path| b.iter_custom(|iters| module_load_data_helper(iters, path)),
    );

    group.bench_with_input(
        BenchmarkId::new("hipModuleLoadData", 61),
        "/home/smithj/dandelion/machine_interface/hip_interface/module.hsaco",
        |b, path| b.iter_custom(|iters| module_load_data_helper(iters, path)),
    );

    group.bench_with_input(
        BenchmarkId::new("hipModuleLoadData", 87),
        "/home/smithj/dandelion/machine_interface/hip_interface/combined.hsaco",
        |b, path| b.iter_custom(|iters| module_load_data_helper(iters, path)),
    );

    group.bench_with_input(
        BenchmarkId::new("hipModuleLoadData", 121),
        "/home/smithj/dandelion/machine_interface/hip_interface/bigger.hsaco",
        |b, path| b.iter_custom(|iters| module_load_data_helper(iters, path)),
    );

    group.finish();
}

criterion_group!(benches, inference_benchmark_latency);

#[cfg(feature = "gpu")]
criterion_main!(benches);

#[cfg(not(feature = "gpu"))]
fn main() {}

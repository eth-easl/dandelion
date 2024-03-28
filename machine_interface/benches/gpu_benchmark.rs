use std::sync::{Arc, Mutex};

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use dandelion_commons::records::{Archive, RecordPoint, Recorder};
use machine_interface::function_driver::thread_utils::EngineLoop;
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

fn matmul_benchmark(c: &mut Criterion) {
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
    let mut engine_loop = GpuLoop::init(core_id).expect("Should be able to init engineloop");
    // set core affinity
    if !core_affinity::set_for_current(core_affinity::CoreId { id: core_id.into() }) {
        log::error!("core received core id that could not be set");
        panic!("Couldn't set core affinity");
    }

    for rows in ROWS_ARR {
        // each elem in the matrix is 8 bytes
        group.throughput(Throughput::Bytes((rows * rows * 8) as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{} x {}", rows, rows)),
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

criterion_group!(benches, matmul_benchmark);

#[cfg(feature = "gpu")]
criterion_main!(benches);

#[cfg(not(feature = "gpu"))]
fn main() {}

#[cfg(feature = "pagetable")]
mod pagetable_bench {
    use criterion::{criterion_group, BenchmarkId, Criterion};
    use dandelion_commons::records::{Archive, RecordPoint, Recorder};
    use machine_interface::{
        function_driver::{
            compute_driver::pagetable::PagetableDriver, util::load_static, Driver, Function,
        },
        memory_domain::{pagetable::PagetableMemoryDomain, MemoryDomain},
        DataItem, DataSet, Position,
    };
    use std::sync::{Arc, Mutex};

    fn context_benchmark(c: &mut Criterion) {
        let domain =
            PagetableMemoryDomain::init(Vec::<u8>::new()).expect("Should be able to initialize");
        let mut group = c.benchmark_group("pagetable context aquire and release");
        static KB: usize = 1024;
        for size in [128 * KB, 2 * KB * KB].iter() {
            group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, i| {
                b.iter(|| {
                    let _context = domain
                        .acquire_context(*i)
                        .expect("Should be able to allocate");
                })
            });
        }
        group.finish();
    }

    // TODO add extend to more benchmarks that check more detailed
    // TODO change to be instantiable with type to make it easier to collect for other engines

    fn matmul_sequential_benchmark(c: &mut Criterion) {
        const MAT_SIZE: usize = 128;

        let mut domain =
            PagetableMemoryDomain::init(Vec::<u8>::new()).expect("Should be able to initialize");
        let driver = PagetableDriver {};
        let mut engine = driver
            .start_engine(vec![1])
            .expect("Should be able to get one engine");
        let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push(format!(
            "tests/data/test_elf_pagetable_{}_matmul",
            std::env::consts::ARCH
        ));
        let mut elf_file = std::fs::File::open(path).expect("Should have found test file");
        let mut elf_buffer = Vec::<u8>::new();
        use std::io::Read;
        elf_file
            .read_to_end(&mut elf_buffer)
            .expect("Should be able to read entire file");
        let Function {
            requirements,
            context: mut static_context,
            config,
        } = driver
            .parse_function(elf_buffer, &mut domain)
            .expect("Should success at parsing");
        c.bench_function("matmul", |b| {
            b.iter(|| {
                let mut function_context =
                    load_static(&mut domain, &mut static_context, &requirements)
                        .expect("Should be able to configure function context");

                let input_size = 8 * MAT_SIZE * MAT_SIZE + 8;
                let mut mat_vec = Vec::<i64>::new();
                mat_vec.push(MAT_SIZE as i64);
                for i in 0..(MAT_SIZE * MAT_SIZE) {
                    mat_vec.push(i as i64);
                }
                let in_mat_offset = function_context
                    .get_free_space_and_write_slice(&mat_vec)
                    .expect("Write should go through");
                function_context.content.push(Some(DataSet {
                    ident: "".to_string(),
                    buffers: vec![DataItem {
                        ident: "".to_string(),
                        data: Position {
                            offset: in_mat_offset as usize,
                            size: input_size,
                        },
                        key: 0,
                    }],
                }));
                let archive = Arc::new(Mutex::new(Archive::new()));
                let recorder = Recorder::new(archive, RecordPoint::TransferEnd);
                let (result, _result_context) = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap()
                    .block_on(engine.run(
                        &config,
                        function_context,
                        &vec![String::from("")],
                        recorder,
                    ));
                if result.is_err() {
                    panic!("returned error result");
                }
            });
        });
    }

    criterion_group!(benches, context_benchmark, matmul_sequential_benchmark);
}

use criterion::criterion_main;

#[cfg(feature = "pagetable")]
criterion_main!(self::pagetable_bench::benches);

#[cfg(not(feature = "pagetable"))]
fn main() {}

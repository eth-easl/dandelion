#![cfg(feature = "cheri")]
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use machine_interface::{
    function_lib::{
        cheri::{CheriDriver, CheriLoader},
        util::load_static,
        Driver, Loader, Function,
    },
    memory_domain::{cheri::CheriMemoryDomain, ContextTrait, MemoryDomain},
    DataItem, Position,
};

fn context_benchmark(c: &mut Criterion) {
    let mut domain =
        CheriMemoryDomain::init(Vec::<u8>::new()).expect("Should be able to initialize");
    let mut group = c.benchmark_group("cheri context aquire and release");
    static KB: usize = 1024;
    for size in [128 * KB, 2 * KB * KB].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, i| {
            b.iter(|| {
                let context = domain
                    .acquire_context(*i)
                    .expect("Should be able to allocate");
                domain
                    .release_context(context)
                    .expect("Should be able to deallocate");
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
        CheriMemoryDomain::init(Vec::<u8>::new()).expect("Should be able to initialize");
    let driver = CheriDriver { };
    let mut engine = driver.start_engine(vec![1]).expect("Should be able to get one engine");
    let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("tests/data/test_elf_aarch64c_matmul");
    let mut elf_file = std::fs::File::open(path).expect("Should have found test file");
    let mut elf_buffer = Vec::<u8>::new();
    use std::io::Read;
    elf_file
        .read_to_end(&mut elf_buffer)
        .expect("Should be able to read entire file");
    let Function { requirements, context: mut static_context, config } =
        CheriLoader::parse_function(elf_buffer, &mut domain).expect("Should success at parsing");
    c.bench_function("matmul", |b| {
        b.iter(|| {
            let mut function_context = load_static(&mut domain, &mut static_context, &requirements)
                .expect("Should be able to configure function context");
            // add inputs
            let in_size_offset = function_context
                .get_free_space(8, 8)
                .expect("Should have space for single i64");
            function_context
                .write(in_size_offset, i64::to_ne_bytes(MAT_SIZE as i64).to_vec())
                .expect("Write should go through");
            function_context.dynamic_data.insert(
                0,
                DataItem::Item(Position {
                    offset: in_size_offset,
                    size: 8,
                }),
            );
            let input_size = 8 * MAT_SIZE * MAT_SIZE;
            let in_mat_offset = function_context
                .get_free_space(input_size, 8)
                .expect("Should have space for single i64");
            let mut mat_vec = Vec::<u8>::new();
            for i in 0..(MAT_SIZE * MAT_SIZE) {
                mat_vec.append(&mut i64::to_ne_bytes(i as i64).to_vec());
            }
            function_context
                .write(in_mat_offset, mat_vec)
                .expect("Write should go through");
            function_context.dynamic_data.insert(
                1,
                DataItem::Item(Position {
                    offset: in_mat_offset,
                    size: input_size,
                }),
            );
            let out_mat_offset = function_context
                .get_free_space(input_size, 8)
                .expect("Should have space for single i64");
            function_context.dynamic_data.insert(
                2,
                DataItem::Item(Position {
                    offset: out_mat_offset,
                    size: input_size,
                }),
            );
            let (_result, result_context) = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(engine.run(&config, function_context));
            domain.release_context(result_context);
        })
    });
}

async fn run_matmul() {}

criterion_group!(benches, context_benchmark, matmul_sequential_benchmark);
criterion_main!(benches);

use crate::{
    function_driver::{
        compute_driver::{
            compute_driver_tests::compute_driver_tests::prepare_engine_and_function,
            gpu::gpu_tests::{get_driver, Archive, ArchiveInit, RecordPoint},
        },
        test_queue::TestQueue,
        Arc, ComputeResource, Driver, FunctionConfig, WorkToDo,
    },
    memory_domain::{gpu::GpuMemoryDomain, Context, ContextTrait, MemoryResource},
};

pub fn setup_test(filename: &str) -> (Context, FunctionConfig, Box<TestQueue>) {
    let dom_init = MemoryResource::Shared {
        id: 0,
        size: (1 << 32), // TODO : choose a good value for the context size
    };
    let driver: Box<dyn Driver> = get_driver();
    let drv_init = vec![ComputeResource::GPU(7, 0, 2)];

    prepare_engine_and_function::<GpuMemoryDomain>(filename, dom_init, &driver, drv_init)
}

pub fn execute_test(
    function_context: Context,
    config: FunctionConfig,
    queue: Box<TestQueue>,
    output_name: &str,
) -> Context {
    let archive = Box::leak(Box::new(Archive::init(ArchiveInit {
        #[cfg(feature = "timestamp")]
        timestamp_count: 1000,
    })));
    let mut recorder = archive.get_recorder().unwrap();
    recorder
        .record(RecordPoint::TransferEnd)
        .expect("Should have properly initialized recorder state");
    let promise = queue.enqueu(WorkToDo::FunctionArguments {
        config,
        context: function_context,
        output_sets: Arc::new(vec![String::from(output_name)]),
        recorder: recorder.get_sub_recorder().unwrap(),
    });
    queue.enqueu(WorkToDo::Shutdown());
    let result_context = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(promise)
        .expect("Engine should run ok with basic function")
        .get_context();
    recorder
        .record(RecordPoint::FutureReturn)
        .expect("Should have properly advanced recorder state");
    result_context
}

pub fn get_result(result_context: Context, output_size: usize, asserts: bool) -> Vec<f32> {
    let output_item = result_context.content[0]
        .as_ref()
        .expect("Set should be present");
    let position = output_item.buffers[0].data;
    if asserts {
        assert_eq!(output_size, position.size, "Checking for size of output");
    } else {
        println!(
            "Expected output size: {output_size}\t\tActual size: {0}",
            position.size
        );
    }
    let mut read_buffer = vec![0f32; position.size / 4];
    result_context
        .context
        .read(position.offset, &mut read_buffer)
        .expect("Should succeed in reading");
    read_buffer
}

pub fn compare_result(expected: Vec<f32>, read_buffer: Vec<f32>, asserts: bool) {
    const DELTA: f32 = 0.003;
    for (should, is) in expected.iter().zip(read_buffer.iter()) {
        let abs_diff = (should - is).abs();
        let mut ratio = should / is;
        if ratio.is_nan() {
            ratio = 1.0;
        }
        let diff_ratio = ratio - 1.0;
        let abs_diff_ratio = diff_ratio.abs();
        if asserts {
            assert!(
                abs_diff_ratio <= DELTA,
                "Checking final result: {should} - {is}"
            );
        } else {
            println!("{should:10.3}\t{is:10.3}\t{abs_diff:10.3}\t{ratio:10.3}");
        }
    }
    println!("Correct result!");
}

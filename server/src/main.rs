use machine_interface::{
    function_lib::{
        cheri::{CheriDriver, CheriLoader},
        util::load_static,
        Driver, Engine, Loader,
    },
    memory_domain::{cheri::CheriMemoryDomain, transefer_memory, ContextTrait, MemoryDomain},
    DataItem, DataItemType, Position,
};
use std::time::Instant;

const MAT_SIZE: usize = 128;
const REPETITIONS: usize = 1000;

async fn run_loop(engine_id: u8) {
    println!("hello from run_loop {}", engine_id);
    // set up hardware
    let mut domain = CheriMemoryDomain::init(Vec::<u8>::new())
        .expect("Should be able to have single cheri domain");
    let mut driver = CheriDriver::new(vec![engine_id]).expect("Should be able to set up engine");
    let mut engine = driver
        .start_engine()
        .expect("Should be able to start engine");

    // load elf file
    let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("../machine_interface/tests/data/test_elf_aarch64c_matmul");
    let mut elf_file = std::fs::File::open(path).expect("Should have found test file");
    let mut elf_buffer = Vec::<u8>::new();
    use std::io::Read;
    let _file_size = elf_file
        .read_to_end(&mut elf_buffer)
        .expect("Should be able to read entire file");
    let (req_list, mut static_context, config) =
        CheriLoader::parse_function(elf_buffer, &mut domain)
            .expect("Should load mat mul executable");

    // main load loop
    let mut loop_start: [Instant; REPETITIONS] = [Instant::now(); REPETITIONS];
    let mut static_load_end: [Instant; REPETITIONS] = [Instant::now(); REPETITIONS];
    let mut function_start: [Instant; REPETITIONS] = [Instant::now(); REPETITIONS];
    let mut function_end: [Instant; REPETITIONS] = [Instant::now(); REPETITIONS];
    let mut loop_end: [Instant; REPETITIONS] = [Instant::now(); REPETITIONS];
    let mut mat_vec_original = Vec::<u8>::new();
    for i in 0..(MAT_SIZE * MAT_SIZE) {
        mat_vec_original.append(&mut i64::to_ne_bytes(i as i64).to_vec());
    }
    let mut mat_context = domain
        .acquire_context(MAT_SIZE * MAT_SIZE * 8)
        .expect("Should have memory for matrix");
    mat_context
        .write(0, mat_vec_original)
        .expect("Should have space matrix");
    for i in 0..REPETITIONS {
        loop_start[i] = Instant::now();
        // set up context and fill in static requirements
        let function_context_result = load_static(&mut domain, &mut static_context, &req_list);
        static_load_end[i] = Instant::now();
        let mut function_context = match function_context_result {
            Ok(c) => c,
            Err(err) => panic!("Expect static loading to succeed, failed with {:?}", err),
        };
        // add inputs
        let in_size_offset = function_context
            .get_free_space(8, 8)
            .expect("Should have space for single i64");
        function_context
            .write(in_size_offset, i64::to_ne_bytes(MAT_SIZE as i64).to_vec())
            .expect("Write should go through");
        function_context.dynamic_data.push(DataItem {
            index: 0,
            item_type: DataItemType::Item(Position {
                offset: in_size_offset,
                size: 8,
            }),
        });
        let in_mat_offset = function_context
            .get_free_space(8 * MAT_SIZE * MAT_SIZE, 8)
            .expect("Should have space matrix");
        transefer_memory(
            &mut function_context,
            &mut mat_context,
            in_mat_offset,
            0,
            MAT_SIZE * MAT_SIZE * 8,
            false,
        )
        .expect("Should transfer matrix");
        function_context.dynamic_data.push(DataItem {
            index: 1,
            item_type: DataItemType::Item(Position {
                offset: in_mat_offset,
                size: MAT_SIZE * MAT_SIZE * 8,
            }),
        });
        let out_mat_offset = function_context
            .get_free_space(8 * MAT_SIZE * MAT_SIZE, 8)
            .expect("Should have space for matrix");
        function_context.dynamic_data.push(DataItem {
            index: 2,
            item_type: DataItemType::Item(Position {
                offset: out_mat_offset,
                size: MAT_SIZE * MAT_SIZE * 8,
            }),
        });
        function_start[i] = Instant::now();
        let (result, result_context) = engine.run(&config, function_context).await;
        function_end[i] = Instant::now();
        result.expect("Engine should run ok with basic function");
        domain
            .release_context(result_context)
            .expect("Should release context");
        loop_end[i] = Instant::now();
    }

    domain
        .release_context(static_context)
        .expect("Should release context");
    driver
        .stop_engine(engine)
        .expect("Should be able to stop engine");
    // compute timings
    let mut static_context_average = 0;
    let mut input_setup_average = 0;
    let mut run_time_average = 0;
    let mut clean_up_average = 0;
    for i in 0..REPETITIONS {
        static_context_average += static_load_end[i].duration_since(loop_start[i]).as_micros();
        input_setup_average += function_start[i]
            .duration_since(static_load_end[i])
            .as_micros();
        run_time_average += function_end[i]
            .duration_since(function_start[i])
            .as_micros();
        clean_up_average += loop_end[i].duration_since(function_end[i]).as_micros();
    }
    static_context_average /= REPETITIONS as u128;
    input_setup_average /= REPETITIONS as u128;
    run_time_average /= REPETITIONS as u128;
    clean_up_average /= REPETITIONS as u128;
    println!("average static context setup: {}us", static_context_average);
    println!("average input setup {}us", input_setup_average);
    println!("average run time {}us", run_time_average);
    println!("average clean up {}us", clean_up_average);
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> () {
    println!("Server Hello");

    let local = tokio::task::LocalSet::new();
    local.spawn_local(run_loop(1));
    // local.spawn_local(run_loop(2));
    // local.spawn_local(run_loop(3));
    local.await;
}

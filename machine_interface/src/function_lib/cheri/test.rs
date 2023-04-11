use std::vec;

use crate::{
    function_lib::{Driver, Engine, FunctionConfig, Navigator},
    memory_domain::{transefer_memory, Context, ContextTrait, MemoryDomain},
    DataItem, DataItemType, DataRequirementList, Position,
};

use super::{CheriDriver, CheriNavigator};
use crate::memory_domain::cheri::CheriMemoryDomain;
use crate::memory_domain::malloc::MallocMemoryDomain;

// basic navigator test
#[test]
#[should_panic]
fn test_navigator_empty() {
    // load elf file
    let elf_file = Vec::<u8>::new();
    let malloc_domain = MallocMemoryDomain {};
    CheriNavigator::parse_function(elf_file, &malloc_domain)
        .expect("Empty string should return error");
}

fn read_file(name: &str, expected_size: usize) -> Vec<u8> {
    // load elf file
    let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("tests/data");
    path.push(name);
    let mut elf_file = std::fs::File::open(path).expect("Should have found test file");
    let mut elf_buffer = Vec::<u8>::new();
    use std::io::Read;
    assert_eq!(
        expected_size,
        elf_file
            .read_to_end(&mut elf_buffer)
            .expect("Should be able to read entire file")
    );
    return elf_buffer;
}

#[test]
fn test_navigator_basic() {
    let elf_buffer = read_file("test_elf_aarch64c_basic", 3240);
    let malloc_domain = MallocMemoryDomain {};
    let (req_list, context, config) = CheriNavigator::parse_function(elf_buffer, &malloc_domain)
        .expect("Should correctly parse elf file");
    // check requirement list
    let expected_requirements = vec![
        Position {
            offset: 0x200000,
            size: 0x354,
        },
        Position {
            offset: 0x210354,
            size: 0x2a4,
        },
        Position {
            offset: 0x2205f8,
            size: 0x10,
        },
    ];
    assert_eq!(
        0x40_0000, req_list.size,
        "Missmatch in expected default context size"
    );
    let expected_sizes = vec![0x354, 0x2a4, 0x0];
    assert_eq!(
        expected_requirements.len(),
        req_list.static_requirements.len(),
        "Requirements list lengths don't match"
    );
    for (index, (expected, actual)) in req_list
        .static_requirements
        .iter()
        .zip(expected_requirements.iter())
        .enumerate()
    {
        assert_eq!(
            expected.size, actual.size,
            "Static requirement size missmatch for index: {}",
            index
        );
        assert_eq!(
            expected.offset, actual.offset,
            "Static requirement offset missmatch for index: {}",
            index
        );
    }
    // check layout
    let mut expected_offset = 0;
    let layout = &context.static_data;
    for (index, position) in layout.into_iter().enumerate() {
        assert_eq!(
            expected_offset, position.offset,
            "Offset missmatch for item {}",
            index
        );
        assert_eq!(
            expected_sizes[index], position.size,
            "Size missmatch for item {}",
            index
        );
        expected_offset += expected_sizes[index];
    }
    // checks for config
    let function_config = match config {
        FunctionConfig::ElfConfig(conf_struct) => conf_struct,
        _ => panic!("Non elf FunctionConfig from cheri navigator"),
    };
    assert_eq!(
        (0x200198, 0x8),
        function_config.input_root,
        "Input root offset or size missmatch"
    );
    assert_eq!(
        (0x220600, 0x8),
        function_config.output_root,
        "Output root offset or size missmatch"
    );
    assert_eq!(
        (0x200190, 0x4),
        function_config.input_number,
        "Input number offset or size missmatch"
    );
    assert_eq!(
        (0x2205f8, 0x4),
        function_config.output_number,
        "Output number offset or size missmatch"
    );
    assert_eq!(
        (0x2001a0, 0x4),
        function_config.max_output_number,
        "Max output number offset or size missmatch"
    );
    assert_eq!(
        (0x2001b0, 0x10),
        function_config.return_offset,
        "Return offset missmatch"
    );
    assert_eq!(
        0x210354, function_config.entry_point,
        "Entry point missmatch"
    );
}

// TODO driver tests

fn load_static(
    mut function_context: Context,
    mut static_context: Context,
    requirement_list: DataRequirementList,
) -> Context {
    let layout = static_context.static_data.to_vec();
    assert_eq!(
        layout.len(),
        requirement_list.static_requirements.len(),
        "Expected layout and static requirements to have same length"
    );
    let static_pairs = layout
        .iter()
        .zip(requirement_list.static_requirements.iter());
    for (position, requirement) in static_pairs {
        assert!(
            requirement.size >= position.size,
            "Expect requirement size to be larger or equal"
        );
        let result;
        (result, function_context, static_context) = transefer_memory(
            function_context,
            static_context,
            requirement.offset,
            position.offset,
            position.size,
            false,
        );
        assert_eq!(Ok(()), result, "Transfer should succeed");
        function_context.static_data.push(Position {
            offset: requirement.offset,
            size: requirement.size,
        });
    }
    function_context
}

#[test]
fn test_engine_minimal() {
    // load elf file
    let elf_buffer = read_file("test_elf_aarch64c_basic", 3240);
    let domain = CheriMemoryDomain::init(Vec::<u8>::new())
        .expect("Should have initialized new cheri domain");
    let (req_list, static_context, config) = CheriNavigator::parse_function(elf_buffer, &domain)
        .expect("Empty string should return error");

    let driver = CheriDriver::new(Vec::<u8>::new()).expect("Should be able to create driver");
    let engine = driver
        .start_engine(0)
        .expect("Should be able to start engine 0");
    // set up context
    let mut function_context = domain
        .acquire_context(req_list.size)
        .expect("Should be able to acquire context");
    // fill in static requirements
    function_context = load_static(function_context, static_context, req_list);
    let (result, _) = engine.run(config, function_context);
    result.expect("Engine should run ok with basic function");
}

#[test]
fn test_engine_matmul_1() {
    // load elf file
    let elf_buffer = read_file("test_elf_aarch64c_matmul", 3600);
    let domain = CheriMemoryDomain::init(Vec::<u8>::new())
        .expect("Should have initialized new cheri domain");
    let (req_list, static_context, config) = CheriNavigator::parse_function(elf_buffer, &domain)
        .expect("Empty string should return error");

    let driver = CheriDriver::new(Vec::<u8>::new()).expect("Should be able to create driver");
    let engine = driver
        .start_engine(0)
        .expect("Should be able to start engine 0");
    // set up context
    let mut function_context = domain
        .acquire_context(req_list.size)
        .expect("Should be able to acquire context");
    // fill in static requirements
    function_context = load_static(function_context, static_context, req_list);
    // add inputs
    let in_mat_offset = function_context
        .get_free_space(8, 8)
        .expect("Should have space for single i64");
    function_context
        .write(in_mat_offset, i64::to_ne_bytes(2).to_vec())
        .expect("Write should go through");
    function_context.dynamic_data.push(DataItem {
        index: 1,
        item_type: DataItemType::Item(Position {
            offset: in_mat_offset,
            size: 8,
        }),
    });
    let out_mat_offset = function_context
        .get_free_space(8, 8)
        .expect("Should have space for single i64");
    function_context.dynamic_data.push(DataItem {
        index: 2,
        item_type: DataItemType::Item(Position {
            offset: out_mat_offset,
            size: 8,
        }),
    });
    let (result, mut result_context) = engine.run(config, function_context);
    result.expect("Engine should run ok with basic function");
    // check that result is 4
    assert_eq!(1, result_context.dynamic_data.len());
    let output_item = &result_context.dynamic_data[0];
    assert_eq!(0, output_item.index);
    let position = match &output_item.item_type {
        DataItemType::Item(pos) => pos,
        DataItemType::Set(_) => panic!("Output type should not be set"),
    };
    assert_eq!(8, position.size, "Checking for size of output");
    let raw_output = result_context
        .context
        .read(position.offset, position.size, false)
        .expect("Should succeed in reading");
    let converted_output = u64::from_ne_bytes(
        raw_output[0..8]
            .try_into()
            .expect("Should have correct length"),
    );
    assert_eq!(4, converted_output);
}

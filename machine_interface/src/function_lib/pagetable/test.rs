use std::vec;

use crate::{
    function_lib::{util::load_static, Driver, Engine, FunctionConfig, Loader},
    memory_domain::{ContextTrait, MemoryDomain},
    DataItem, DataItemType, HardwareError, Position,
};

use super::{PagetableDriver, PagetableLoader};
use crate::memory_domain::pagetable::PagetableMemoryDomain;

// basic loader test
#[test]
#[should_panic]
fn test_loader_empty() {
    // load elf file
    let elf_file = Vec::<u8>::new();
    let mut malloc_domain = PagetableMemoryDomain {};
    PagetableLoader::parse_function(elf_file, &mut malloc_domain)
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
fn test_loader_basic() {
    let elf_buffer = read_file("test_elf_x86c_basic", 13952);
    let mut malloc_domain = PagetableMemoryDomain {};
    let (req_list, context, config) = PagetableLoader::parse_function(elf_buffer, &mut malloc_domain)
        .expect("Should correctly parse elf file");
    // check requirement list
    let expected_requirements = vec![
        Position {
            offset: 0x400000,
            size: 0x254,
        },
        Position {
            offset: 0x401000,
            size: 0x197,
        },
        Position {
            offset: 0x402000,
            size: 0x178,
        },
        Position {
            offset: 0x404000,
            size: 0x20,
        },
    ];
    assert_eq!(
        0x80_0000, req_list.size,
        "Missmatch in expected default context size"
    );
    let expected_sizes = vec![0x254, 0x197, 0x178, 0x10];
    assert_eq!(
        expected_requirements.len(),
        req_list.static_requirements.len(),
        "Requirements list lengths don't match"
    );
    for (index, (actual, expected)) in req_list
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
        _ => panic!("Non elf FunctionConfig from pagetable loader"),
    };
    assert_eq!(
        (0x404000, 0x8),
        function_config.input_root,
        "Input root offset or size missmatch"
    );
    assert_eq!(
        (0x404010, 0x8),
        function_config.output_root,
        "Output root offset or size missmatch"
    );
    assert_eq!(
        (0x404008, 0x4),
        function_config.input_number,
        "Input number offset or size missmatch"
    );
    assert_eq!(
        (0x404018, 0x4),
        function_config.output_number,
        "Output number offset or size missmatch"
    );
    assert_eq!(
        (0x40400c, 0x4),
        function_config.max_output_number,
        "Max output number offset or size missmatch"
    );
    // assert_eq!(
    //     (0x2001b0, 0x10),
    //     function_config.return_offset,
    //     "Return offset missmatch"
    // );
    assert_eq!(
        0x401000, function_config.entry_point,
        "Entry point missmatch"
    );
}

#[test]
fn test_driver() {
    let mut no_engine_driver =
        PagetableDriver::new(Vec::<u8>::new()).expect("Should be able to create driver");
    let no_engine = no_engine_driver.start_engine();
    match no_engine {
        Ok(_) => panic!("Should not be able to get engine"),
        Err(err) => assert_eq!(HardwareError::NoEngineAvailable, err),
    }
    drop(no_engine_driver);
    let wrong_engine_driver = PagetableDriver::new(vec![255]);
    match wrong_engine_driver {
        Ok(driver) => {
            drop(driver);
            panic!("Should not be able to get engine")
        }
        Err(err) => assert_eq!(HardwareError::MalformedConfig, err),
    }
    let mut some_engines_driver =
        PagetableDriver::new(vec![0]).expect("Should be able to create driver");
    for i in 0..2 {
        let engine = some_engines_driver.start_engine();
        if i < 1 {
            engine.expect("Should be able to get engine");
        } else {
            match engine {
                Ok(_) => panic!("Should not be able to get more engines than expected"),
                Err(err) => assert_eq!(HardwareError::NoEngineAvailable, err),
            }
        };
    }
}

#[test]
fn test_engine_minimal() {
    // load elf file
    let elf_buffer = read_file("test_elf_x86c_basic", 13952);
    let mut domain = PagetableMemoryDomain::init(Vec::<u8>::new())
        .expect("Should have initialized new pagetable domain");
    let (req_list, mut static_context, config) =
        PagetableLoader::parse_function(elf_buffer, &mut domain)
            .expect("Empty string should return error");

    let mut driver = PagetableDriver::new(vec![0]).expect("Should be able to create driver");
    let mut engine = driver
        .start_engine()
        .expect("Should be able to start engine");
    // set up context and fill in static requirements
    let function_context_result = load_static(&mut domain, &mut static_context, &req_list);
    let function_context = match function_context_result {
        Ok(c) => c,
        Err(err) => panic!("Expect static loading to succeed, failed with {:?}", err),
    };
    let (result, function_context) = engine.run(&config, function_context);
    result.expect("Engine should run ok with basic function");
    domain
        .release_context(function_context)
        .expect("Should release context");
    domain
        .release_context(static_context)
        .expect("Should release context");
    driver
        .stop_engine(engine)
        .expect("Should be able to stop engine");
}

#[test]
fn test_engine_matmul_single() {
    // load elf file
    let elf_buffer = read_file("test_elf_x86c_matmul", 13952);
    let mut domain = PagetableMemoryDomain::init(Vec::<u8>::new())
        .expect("Should have initialized new pagetable domain");
    let (req_list, mut static_context, config) =
        PagetableLoader::parse_function(elf_buffer, &mut domain)
            .expect("Empty string should return error");

    let mut driver = PagetableDriver::new(vec![0]).expect("Should be able to create driver");
    let mut engine = driver
        .start_engine()
        .expect("Should be able to start engine");
    // set up context and fill in static requirements
    let function_context_result = load_static(&mut domain, &mut static_context, &req_list);
    let mut function_context = match function_context_result {
        Ok(c) => c,
        Err(err) => panic!("Expect static loading to succeed, failed with {:?}", err),
    };
    // add inputs
    let in_size_offset = function_context
        .get_free_space(8, 8)
        .expect("Should have space for single i64");
    function_context
        .write(in_size_offset, i64::to_ne_bytes(1).to_vec())
        .expect("Write should go through");
    function_context.dynamic_data.push(DataItem {
        index: 0,
        item_type: DataItemType::Item(Position {
            offset: in_size_offset,
            size: 8,
        }),
    });
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
    let (result, mut result_context) = engine.run(&config, function_context);
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
    let converted_output = i64::from_ne_bytes(
        raw_output[0..8]
            .try_into()
            .expect("Should have correct length"),
    );
    assert_eq!(4, converted_output);
    domain
        .release_context(result_context)
        .expect("Should release context");
    domain
        .release_context(static_context)
        .expect("Should release context");
    driver
        .stop_engine(engine)
        .expect("Should be able to stop engine");
}

const LOWER_SIZE_BOUND: usize = 2;
const UPPER_SIZE_BOUND: usize = 16;

fn get_expected_mat(size: usize) -> Vec<i64> {
    let mut in_mat_vec = Vec::<i64>::new();
    for i in 0..(size * size) {
        in_mat_vec.push(i as i64);
    }
    let mut out_mat_vec = vec![0 as i64; size * size];
    for i in 0..size {
        for j in 0..size {
            for k in 0..size {
                out_mat_vec[i * size + j] += in_mat_vec[i * size + k] * in_mat_vec[j * size + k];
            }
        }
    }
    return out_mat_vec;
}

#[test]
fn test_engine_matmul_size_sweep() {
    // load elf file
    let elf_buffer = read_file("test_elf_x86c_matmul", 13952);
    let mut domain = PagetableMemoryDomain::init(Vec::<u8>::new())
        .expect("Should have initialized new cheri domain");
    let (req_list, mut static_context, config) =
        PagetableLoader::parse_function(elf_buffer, &mut domain)
            .expect("Empty string should return error");

    let mut driver = PagetableDriver::new(vec![0]).expect("Should be able to create driver");
    let mut engine = driver
        .start_engine()
        .expect("Should be able to start engine 0");
    for mat_size in LOWER_SIZE_BOUND..UPPER_SIZE_BOUND {
        // set up context and fill in static requirements
        let function_context_result = load_static(&mut domain, &mut static_context, &req_list);
        let mut function_context = match function_context_result {
            Ok(c) => c,
            Err(err) => panic!("Expect static loading to succeed, failed with {:?}", err),
        };
        // add inputs
        let in_size_offset = function_context
            .get_free_space(8, 8)
            .expect("Should have space for single i64");
        function_context
            .write(in_size_offset, i64::to_ne_bytes(mat_size as i64).to_vec())
            .expect("Write should go through");
        function_context.dynamic_data.push(DataItem {
            index: 0,
            item_type: DataItemType::Item(Position {
                offset: in_size_offset,
                size: 8,
            }),
        });
        let input_size = 8 * mat_size * mat_size;
        let in_mat_offset = function_context
            .get_free_space(input_size, 8)
            .expect("Should have space for single i64");
        let mut mat_vec = Vec::<u8>::new();
        for i in 0..(mat_size * mat_size) {
            mat_vec.append(&mut i64::to_ne_bytes(i as i64).to_vec());
        }
        function_context
            .write(in_mat_offset, mat_vec)
            .expect("Write should go through");
        function_context.dynamic_data.push(DataItem {
            index: 1,
            item_type: DataItemType::Item(Position {
                offset: in_mat_offset,
                size: input_size,
            }),
        });
        let out_mat_offset = function_context
            .get_free_space(input_size, 8)
            .expect("Should have space for single i64");
        function_context.dynamic_data.push(DataItem {
            index: 2,
            item_type: DataItemType::Item(Position {
                offset: out_mat_offset,
                size: input_size,
            }),
        });
        let (result, mut result_context) = engine.run(&config, function_context);
        result.expect("Engine should run ok with basic function");
        // check that result is 4
        assert_eq!(1, result_context.dynamic_data.len());
        let output_item = &result_context.dynamic_data[0];
        assert_eq!(0, output_item.index);
        let position = match &output_item.item_type {
            DataItemType::Item(pos) => pos,
            DataItemType::Set(_) => panic!("Output type should not be set"),
        };
        assert_eq!(input_size, position.size, "Checking for size of output");
        let raw_output = result_context
            .context
            .read(position.offset, position.size, false)
            .expect("Should succeed in reading");
        let expected = get_expected_mat(mat_size);
        for (index, chunk) in raw_output.chunks_exact(8).enumerate() {
            let value = i64::from_ne_bytes(chunk.try_into().expect("Should have correct length"));
            assert_eq!(expected[index], value);
        }
        domain
            .release_context(result_context)
            .expect("Should release context");
    }
    domain
        .release_context(static_context)
        .expect("Should release context");
    driver
        .stop_engine(engine)
        .expect("Should be able to stop engine");
}

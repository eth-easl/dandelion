use std::vec;

use crate::{
    function_lib::{
        cheri::{CheriDriver, CheriLoader},
        util::load_static,
        Driver, FunctionConfig, Loader,
    },
    memory_domain::{
        cheri::CheriMemoryDomain, malloc::MallocMemoryDomain, ContextTrait, MemoryDomain,
    },
    DataItem, DataSet, Position,
};
use dandelion_commons::DandelionError;

// basic loader test
#[test]
#[should_panic]
fn test_loader_empty() {
    // load elf file
    let elf_file = Vec::<u8>::new();
    let mut malloc_domain =
        MallocMemoryDomain::init(Vec::new()).expect("Should be able to get malloc domain");
    CheriLoader::parse_function(elf_file, &mut malloc_domain)
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
    let file_length = elf_file
        .read_to_end(&mut elf_buffer)
        .expect("Should be able to read entire file");
    if expected_size != 0 {
        assert_eq!(expected_size, file_length);
    }
    return elf_buffer;
}

#[test]
fn test_loader_basic() {
    let elf_buffer = read_file("test_elf_aarch64c_basic", 6400);
    let mut malloc_domain =
        MallocMemoryDomain::init(Vec::new()).expect("Should be able to get malloc domain");
    let (req_list, context, config) =
        CheriLoader::parse_function(elf_buffer, &mut malloc_domain).expect("Parsing should work");
    // check requirement list to be list of programm header info for after load
    // meaning addresses and sizes in virtual address space
    let expected_requirements = vec![
        Position {
            offset: 0x200000,
            size: 0x49c,
        },
        Position {
            offset: 0x21049c,
            size: 0xac4,
        },
        Position {
            offset: 0x220f60,
            size: 0x70,
        },
    ];
    assert_eq!(
        0x800_0000, req_list.size,
        "Missmatch in expected default context size"
    );
    // actual sizes in file
    let expected_sizes = vec![0x49c, 0xac4, 0x0];
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
    assert_eq!(1, context.content.len());
    let layout = &context.content[0].buffers;
    for (index, item) in layout.into_iter().enumerate() {
        assert_eq!(
            expected_offset, item.data.offset,
            "Offset missmatch for item {}",
            index
        );
        assert_eq!(
            expected_sizes[index], item.data.size,
            "Size missmatch for item {}",
            index
        );
        expected_offset += expected_sizes[index];
    }
    // checks for config
    let function_config = match config {
        FunctionConfig::ElfConfig(conf_struct) => conf_struct,
        _ => panic!("Non elf FunctionConfig from cheri loader"),
    };
    assert_eq!(
        (0x220f78),
        function_config.system_data_offset,
        "System data offset missmatch"
    );
    assert_eq!(
        (0x220fc0, 0x10),
        function_config.return_offset,
        "Return offset missmatch"
    );
    assert_eq!(
        0x21049c, function_config.entry_point,
        "Entry point missmatch"
    );
}

#[test]
fn test_driver() {
    let no_resource_engine = CheriDriver::start_engine(Vec::<u8>::new());
    match no_resource_engine {
        Ok(_) => panic!("Should not be able to get engine"),
        Err(err) => assert_eq!(DandelionError::ConfigMissmatch, err),
    }
    let wrong_resource_engine = CheriDriver::start_engine(vec![4]);
    match wrong_resource_engine {
        Ok(_) => panic!("Should not be able to get engine"),
        Err(err) => assert_eq!(DandelionError::MalformedConfig, err),
    }

    for i in 0..4 {
        let engine = CheriDriver::start_engine(vec![i]);
        engine.expect("Should be able to get engine");
    }
}

#[test]
fn test_engine_minimal() {
    // load elf file
    let elf_buffer = read_file("test_elf_aarch64c_basic", 6400);
    let mut domain = CheriMemoryDomain::init(Vec::<u8>::new())
        .expect("Should have initialized new cheri domain");
    let (req_list, static_context, config) =
        CheriLoader::parse_function(elf_buffer, &mut domain).expect("Parsing should work");

    let mut engine = CheriDriver::start_engine(vec![1]).expect("Should be able to start engine");

    // set up context and fill in static requirements
    let function_context_result = load_static(&mut domain, &static_context, &req_list);
    let function_context = match function_context_result {
        Ok(c) => c,
        Err(err) => panic!("Expect static loading to succeed, failed with {:?}", err),
    };
    let (result, function_context) = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(engine.run(&config, function_context, vec![]));
    result.expect("Engine should run ok with basic function");
    domain
        .release_context(function_context)
        .expect("Should release context");
    domain
        .release_context(static_context)
        .expect("Should release context");
}

#[test]
fn test_engine_matmul_single() {
    // load elf file
    let elf_buffer = read_file("test_elf_aarch64c_matmul", 0);
    let mut domain = CheriMemoryDomain::init(Vec::<u8>::new())
        .expect("Should have initialized new cheri domain");
    let (req_list, mut static_context, config) =
        CheriLoader::parse_function(elf_buffer, &mut domain).expect("Parsing should work");

    let mut engine = CheriDriver::start_engine(vec![1]).expect("Should be able to start engine");

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
    function_context.content.push(DataSet {
        ident: "".to_string(),
        buffers: vec![DataItem {
            ident: "".to_string(),
            data: Position {
                offset: in_size_offset,
                size: 8,
            },
        }],
    });
    let in_mat_offset = function_context
        .get_free_space(8, 8)
        .expect("Should have space for single i64");
    function_context
        .write(in_mat_offset, i64::to_ne_bytes(2).to_vec())
        .expect("Write should go through");
    function_context.content.push(DataSet {
        ident: "".to_string(),
        buffers: vec![DataItem {
            ident: "".to_string(),
            data: Position {
                offset: in_mat_offset,
                size: 8,
            },
        }],
    });
    let (result, result_context) = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(engine.run(&config, function_context, vec!["".to_string()]));
    result.expect("Engine should run ok with basic function");
    // check that result is 4
    assert_eq!(1, result_context.content.len());
    let output_item = &result_context.content[0];
    assert_eq!(1, output_item.buffers.len());
    let position = output_item.buffers[0].data;
    assert_eq!(8, position.size, "Checking for size of output");
    let raw_output = result_context
        .context
        .read(position.offset, position.size)
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
    let elf_buffer = read_file("test_elf_aarch64c_matmul", 6896);
    let mut domain = CheriMemoryDomain::init(Vec::<u8>::new())
        .expect("Should have initialized new cheri domain");
    let (req_list, static_context, config) =
        CheriLoader::parse_function(elf_buffer, &mut domain).expect("Parsing should work");

    let mut engine = CheriDriver::start_engine(vec![1]).expect("Should be able to start engine");
    for mat_size in LOWER_SIZE_BOUND..UPPER_SIZE_BOUND {
        // set up context and fill in static requirements
        let function_context_result = load_static(&mut domain, &static_context, &req_list);
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
        function_context.content.push(DataSet {
            ident: "".to_string(),
            buffers: vec![DataItem {
                ident: "".to_string(),
                data: Position {
                    offset: in_size_offset,
                    size: 8,
                },
            }],
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
        function_context.content.push(DataSet {
            ident: "".to_string(),
            buffers: vec![DataItem {
                ident: "".to_string(),
                data: Position {
                    offset: in_mat_offset,
                    size: input_size,
                },
            }],
        });
        let (result, result_context) = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(engine.run(&config, function_context, vec!["".to_string()]));
        result.expect("Engine should run ok with basic function");
        // check that result is 4
        assert_eq!(1, result_context.content.len());
        let output_item = &result_context.content[0];
        assert_eq!(1, output_item.buffers.len());
        let position = output_item.buffers[0].data;
        assert_eq!(input_size, position.size, "Checking for size of output");
        let raw_output = result_context
            .context
            .read(position.offset, position.size)
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
}

#[test]
fn test_engine_stdio() {
    panic!("Stdio still needs fixing");
    // load elf file
    let elf_buffer = read_file("stdio", 0);
    let mut domain = CheriMemoryDomain::init(Vec::<u8>::new())
        .expect("Should have initialized new cheri domain");
    let (req_list, static_context, config) =
        CheriLoader::parse_function(elf_buffer, &mut domain).expect("Parsing should work");

    let mut engine = CheriDriver::start_engine(vec![1]).expect("Should be able to start engine");
    let function_context_result = load_static(&mut domain, &static_context, &req_list);
    let mut function_context = match function_context_result {
        Ok(c) => c,
        Err(err) => panic!("Expect static loading to succeed, failed with {:?}", err),
    };
    let stdin_content = "Test line \n line 2\n";
    let stdin_offset = function_context
        .get_free_space(stdin_content.len(), 8)
        .expect("Should have space");
    function_context
        .write(stdin_offset, stdin_content.as_bytes().to_vec())
        .expect("Write should go through");
    function_context.content.push(DataSet {
        ident: "stdio".to_string(),
        buffers: vec![DataItem {
            ident: "stdin".to_string(),
            data: Position {
                offset: stdin_offset,
                size: stdin_content.len(),
            },
        }],
    });
    println!("{:?}", function_context.content);
    let (result, result_context) = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(engine.run(&config, function_context, vec!["stdio".to_string()]));
    result.expect("Engine should run ok with basic function");
    // check there is exactly one set called stdio
    assert_eq!(1, result_context.content.len());
    let io_set = &result_context.content[0];
    assert_eq!("stdio", io_set.ident);
    assert_eq!(2, io_set.buffers.len());
    let mut stdout_vec = Vec::<u8>::new();
    let mut stderr_vec = Vec::<u8>::new();
    for item in &io_set.buffers {
        match item.ident.as_str() {
            "stdout" => {
                stdout_vec = result_context
                    .read(item.data.offset, item.data.size)
                    .expect("stdout read should succeed")
            }
            "stderr" => {
                stderr_vec = result_context
                    .read(item.data.offset, item.data.size)
                    .expect("stderr read should succeed")
            }
            _ => panic!("found item in stdio set that is neither out nor err"),
        }
    }
    let stdout_string = std::str::from_utf8(&stdout_vec).expect("should be string");
    let stderr_string = std::str::from_utf8(&stderr_vec).expect("should be string");
    let expected_stdout = format!(
        "Test string to stdout\nread {} characters from stdin\n{}",
        stdin_content.len(),
        stdin_content
    );
    assert_eq!(expected_stdout, stdout_string);
    assert_eq!("Test string to stderr\n", stderr_string);
}

#[test]
fn test_engine_fileio() {
    // load elf file
    let elf_buffer = read_file("test_elf_aarch64c_fileio", 2197048);
    let mut domain = CheriMemoryDomain::init(Vec::<u8>::new())
        .expect("Should have initialized new cheri domain");
    let (req_list, static_context, config) =
        CheriLoader::parse_function(elf_buffer, &mut domain).expect("Parsing should work");

    let mut engine = CheriDriver::start_engine(vec![1]).expect("Should be able to start engine");
    let function_context_result = load_static(&mut domain, &static_context, &req_list);
    let mut function_context = match function_context_result {
        Ok(c) => c,
        Err(err) => panic!("Expect static loading to succeed, failed with {:?}", err),
    };
    let in_file_content = "Test file 0\n line 2\n";
    let in_file_offset = function_context
        .get_free_space(in_file_content.len(), 8)
        .expect("Should have space");
    function_context
        .write(in_file_offset, in_file_content.as_bytes().to_vec())
        .expect("Write should go through");
    function_context.content.push(DataSet {
        ident: "in".to_string(),
        buffers: vec![DataItem {
            ident: "in_file".to_string(),
            data: Position {
                offset: in_file_offset,
                size: in_file_content.len(),
            },
        }],
    });
    let in_file1_content = "Test file 1 \n line 2\n";
    let in_file1_offset = function_context
        .get_free_space(in_file1_content.len(), 8)
        .expect("Should have space");
    function_context
        .write(in_file1_offset, in_file1_content.as_bytes().to_vec())
        .expect("Write should go through");
    let in_file2_content = "Test file 2 \n line 2\n";
    let in_file2_offset = function_context
        .get_free_space(in_file2_content.len(), 8)
        .expect("Should have space");
    function_context
        .write(in_file2_offset, in_file2_content.as_bytes().to_vec())
        .expect("Write should go through");
    let in_file3_content = "Test file 3 \n line 2\n";
    let in_file3_offset = function_context
        .get_free_space(in_file3_content.len(), 8)
        .expect("Should have space");
    function_context
        .write(in_file3_offset, in_file3_content.as_bytes().to_vec())
        .expect("Write should go through");
    let in_file4_content = "Test file 4 \n line 2\n";
    let in_file4_offset = function_context
        .get_free_space(in_file4_content.len(), 8)
        .expect("Should have space");
    function_context
        .write(in_file4_offset, in_file4_content.as_bytes().to_vec())
        .expect("Write should go through");
    function_context.content.push(DataSet {
        ident: "in_nested".to_string(),
        buffers: vec![
            DataItem {
                ident: "in_file".to_string(),
                data: Position {
                    offset: in_file1_offset,
                    size: in_file1_content.len(),
                },
            },
            DataItem {
                ident: "in_folder/in_file".to_string(),
                data: Position {
                    offset: in_file2_offset,
                    size: in_file2_content.len(),
                },
            },
            DataItem {
                ident: "in_folder/in_file_two".to_string(),
                data: Position {
                    offset: in_file3_offset,
                    size: in_file3_content.len(),
                },
            },
            DataItem {
                ident: "in_folder/in_folder_two/in_file".to_string(),
                data: Position {
                    offset: in_file4_offset,
                    size: in_file4_content.len(),
                },
            },
        ],
    });
    let (result, result_context) = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(engine.run(
            &config,
            function_context,
            vec![
                "stdio".to_string(),
                "out".to_string(),
                "out_nested".to_string(),
            ],
        ));
    result.expect("Engine should run ok with basic function");
    assert_eq!(3, result_context.content.len());
    // check out set
    assert_eq!(1, result_context.content[1].buffers.len());
    let item = &result_context.content[1].buffers[0];
    assert_eq!("out_file", item.ident);
    let content = result_context
        .read(item.data.offset, item.data.size)
        .expect("should be able to read");
    assert_eq!(
        in_file_content,
        std::str::from_utf8(&content).expect("output content should be string")
    );
    assert_eq!(4, result_context.content[2].buffers.len());
    for item in result_context.content[2].buffers.iter() {
        let content = result_context
            .read(item.data.offset, item.data.size)
            .expect("should be able to read");
        let content_string = std::str::from_utf8(&content).expect("content should be string");
        let expected_string = match item.ident.as_str() {
            "out_file" => in_file1_content,
            "out_folder/out_file" => in_file2_content,
            "out_folder/out_file_two" => in_file3_content,
            "out_folder/out_folder_two/out_file" => in_file4_content,
            _ => panic!("unexpeced output identifier {}", item.ident),
        };
        assert_eq!(expected_string, content_string);
    }
}

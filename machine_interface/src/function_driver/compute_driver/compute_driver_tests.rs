use crate::{
    function_driver::{util::load_static, Driver, Loader},
    memory_domain::{ContextTrait, MemoryDomain},
    DataItem, DataSet, Position,
};
use dandelion_commons::{
    records::{Archive, RecordPoint, Recorder},
    DandelionError,
};
use std::sync::{Arc, Mutex};

fn _read_file(name: &str, expected_size: usize) -> Vec<u8> {
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

fn _loader_empty<D: MemoryDomain, L: Loader>(dom_init: Vec<u8>) {
    // load elf file
    let elf_file = Vec::<u8>::new();
    let mut domain = D::init(dom_init).expect("Should be able to get domain");
    L::parse_function(elf_file, &mut domain).expect("Empty string should return error");
}

// fn test_loader_basic<D: MemoryDomain, L: Loader>(
//     dom_init: Vec<u8>,
//     file_name: &str,
//     file_size: usize,
// ) {
//     let elf_buffer = _read_file(file_name, filesize);
//     let mut domain = D::init(dom_init).expect("Should be able to get domain");
//     let (req_list, context, config) =
//         L::parse_function(elf_buffer, &mut domain).expect("Parsing should work");
//     // check requirement list to be list of programm header info for after load
//     // meaning addresses and sizes in virtual address space
//     let expected_requirements = vec![
//         Position {
//             offset: 0x200000,
//             size: 0x49c,
//         },
//         Position {
//             offset: 0x21049c,
//             size: 0xac4,
//         },
//         Position {
//             offset: 0x220f60,
//             size: 0x70,
//         },
//     ];
//     assert_eq!(
//         0x800_0000, req_list.size,
//         "Missmatch in expected default context size"
//     );
//     // actual sizes in file
//     let expected_sizes = vec![0x49c, 0xac4, 0x0];
//     assert_eq!(
//         expected_requirements.len(),
//         req_list.static_requirements.len(),
//         "Requirements list lengths don't match"
//     );
//     for (index, (expected, actual)) in req_list
//         .static_requirements
//         .iter()
//         .zip(expected_requirements.iter())
//         .enumerate()
//     {
//         assert_eq!(
//             expected.size, actual.size,
//             "Static requirement size missmatch for index: {}",
//             index
//         );
//         assert_eq!(
//             expected.offset, actual.offset,
//             "Static requirement offset missmatch for index: {}",
//             index
//         );
//     }
//     // check layout
//     let mut expected_offset = 0;
//     assert_eq!(1, context.content.len());
//     let layout = &context.content[0].buffers;
//     for (index, item) in layout.into_iter().enumerate() {
//         assert_eq!(
//             expected_offset, item.data.offset,
//             "Offset missmatch for item {}",
//             index
//         );
//         assert_eq!(
//             expected_sizes[index], item.data.size,
//             "Size missmatch for item {}",
//             index
//         );
//         expected_offset += expected_sizes[index];
//     }
//     // checks for config
//     let function_config = match config {
//         FunctionConfig::ElfConfig(conf_struct) => conf_struct,
//         _ => panic!("Non elf FunctionConfig from cheri loader"),
//     };
//     assert_eq!(
//         (0x220f78),
//         function_config.system_data_offset,
//         "System data offset missmatch"
//     );
//     assert_eq!(
//         (0x220fc0, 0x10),
//         function_config.return_offset,
//         "Return offset missmatch"
//     );
//     assert_eq!(
//         0x21049c, function_config.entry_point,
//         "Entry point missmatch"
//     );
// }

fn _driver<Drv: Driver>(init: Vec<u8>, wrong_init: Vec<u8>) {
    let no_resource_engine = Drv::start_engine(Vec::<u8>::new());
    match no_resource_engine {
        Ok(_) => panic!("Should not be able to get engine"),
        Err(err) => assert_eq!(DandelionError::ConfigMissmatch, err),
    }
    let wrong_resource_engine = Drv::start_engine(wrong_init);
    match wrong_resource_engine {
        Ok(_) => panic!("Should not be able to get engine"),
        Err(err) => assert_eq!(DandelionError::MalformedConfig, err),
    }

    for resource in init {
        let engine = Drv::start_engine(vec![resource]);
        engine.expect("Should be able to get engine");
    }
}

fn _engine_minimal<Dom: MemoryDomain, L: Loader, Drv: Driver>(
    filename: &str,
    dom_init: Vec<u8>,
    drv_init: Vec<u8>,
) {
    // load elf file
    let elf_buffer = _read_file(filename, 0);
    let mut domain = Dom::init(dom_init).expect("Should be able to initialized domain");
    let (req_list, static_context, config) =
        L::parse_function(elf_buffer, &mut domain).expect("Parsing should work");

    let mut engine = Drv::start_engine(vec![drv_init[0]]).expect("Should be able to start engine");

    // set up context and fill in static requirements
    let function_context_result = load_static(&mut domain, &static_context, &req_list);
    let function_context = match function_context_result {
        Ok(c) => c,
        Err(err) => panic!("Expect static loading to succeed, failed with {:?}", err),
    };
    let archive = Arc::new(Mutex::new(Archive::new()));
    let recorder = Recorder::new(archive, RecordPoint::TransferEnd);
    let (result, function_context) = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(engine.run(&config, function_context, &vec![], recorder.clone()));
    result.expect("Engine should run ok with basic function");
    domain
        .release_context(function_context)
        .expect("Should release context");
    domain
        .release_context(static_context)
        .expect("Should release context");
}

fn _engine_matmul_single<Dom: MemoryDomain, L: Loader, Drv: Driver>(
    filename: &str,
    dom_init: Vec<u8>,
    drv_init: Vec<u8>,
) {
    // load elf file
    let elf_buffer = _read_file(filename, 0);
    let mut domain = Dom::init(dom_init).expect("Should have initialized new cheri domain");
    let (req_list, mut static_context, config) =
        L::parse_function(elf_buffer, &mut domain).expect("Parsing should work");

    let mut engine = Drv::start_engine(vec![drv_init[0]]).expect("Should be able to start engine");

    // set up context and fill in static requirements
    let function_context_result = load_static(&mut domain, &mut static_context, &req_list);
    let mut function_context = match function_context_result {
        Ok(c) => c,
        Err(err) => panic!("Expect static loading to succeed, failed with {:?}", err),
    };
    // add inputs
    let in_size_offset = function_context
        .get_free_space_and_write_slice(core::slice::from_ref(&1i64))
        .expect("Should have space for single i64");
    // function_context
    // .write(in_size_offset, i64::to_ne_bytes(1).to_vec())
    // .expect("Write should go through");
    function_context.content.push(Some(DataSet {
        ident: "".to_string(),
        buffers: vec![DataItem {
            ident: "".to_string(),
            data: Position {
                offset: in_size_offset as usize,
                size: 8,
            },
        }],
    }));
    let in_mat_offset = function_context
        .get_free_space_and_write_slice(core::slice::from_ref(&2i64))
        .expect("Should have space for single i64");
    // function_context
    // .write(in_mat_offset, i64::to_ne_bytes(2))
    // .expect("Write should go through");
    function_context.content.push(Some(DataSet {
        ident: "".to_string(),
        buffers: vec![DataItem {
            ident: "".to_string(),
            data: Position {
                offset: in_mat_offset as usize,
                size: 8,
            },
        }],
    }));
    let archive = Arc::new(Mutex::new(Archive::new()));
    let mut recorder = Recorder::new(archive, RecordPoint::TransferEnd);
    let (result, result_context) = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(engine.run(
            &config,
            function_context,
            &vec!["".to_string()],
            recorder.clone(),
        ));
    result.expect("Engine should run ok with basic function");
    recorder
        .record(RecordPoint::FutureReturn)
        .expect("Should have properly advanced recorder state");
    // check that result is 4
    assert_eq!(1, result_context.content.len());
    let output_item = result_context.content[0]
        .as_ref()
        .expect("Set should be present");
    assert_eq!(1, output_item.buffers.len());
    let position = output_item.buffers[0].data;
    assert_eq!(8, position.size, "Checking for size of output");
    let mut read_buffer = vec![0i64; position.size / 8];
    result_context
        .context
        .read(position.offset, &mut read_buffer)
        .expect("Should succeed in reading");
    assert_eq!(4, read_buffer[0]);
    domain
        .release_context(result_context)
        .expect("Should release context");
    domain
        .release_context(static_context)
        .expect("Should release context");
}

fn _get_expected_mat(size: usize) -> Vec<i64> {
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

fn _engine_matmul_size_sweep<Dom: MemoryDomain, L: Loader, Drv: Driver>(
    filename: &str,
    dom_init: Vec<u8>,
    drv_init: Vec<u8>,
) {
    const LOWER_SIZE_BOUND: usize = 2;
    const UPPER_SIZE_BOUND: usize = 16;
    // load elf file
    let elf_buffer = _read_file(filename, 0);
    let mut domain = Dom::init(dom_init).expect("Should have initialized new cheri domain");
    let (req_list, static_context, config) =
        L::parse_function(elf_buffer, &mut domain).expect("Parsing should work");

    let mut engine = Drv::start_engine(vec![drv_init[0]]).expect("Should be able to start engine");
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
            .write(in_size_offset, &i64::to_ne_bytes(mat_size as i64))
            .expect("Write should go through");
        function_context.content.push(Some(DataSet {
            ident: "".to_string(),
            buffers: vec![DataItem {
                ident: "".to_string(),
                data: Position {
                    offset: in_size_offset,
                    size: 8,
                },
            }],
        }));
        let input_size = 8 * mat_size * mat_size;
        let in_mat_offset = function_context
            .get_free_space(input_size, 8)
            .expect("Should have space for single i64");
        let mut mat_vec = Vec::<i64>::new();
        for i in 0..(mat_size * mat_size) {
            mat_vec.push(i as i64);
        }
        function_context
            .write(in_mat_offset, &mut mat_vec)
            .expect("Write should go through");
        function_context.content.push(Some(DataSet {
            ident: "".to_string(),
            buffers: vec![DataItem {
                ident: "".to_string(),
                data: Position {
                    offset: in_mat_offset,
                    size: input_size,
                },
            }],
        }));
        let archive = Arc::new(Mutex::new(Archive::new()));
        let mut recorder = Recorder::new(archive.clone(), RecordPoint::TransferEnd);
        let (result, result_context) = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(engine.run(
                &config,
                function_context,
                &vec!["".to_string()],
                recorder.clone(),
            ));
        result.expect("Engine should run ok with basic function");
        recorder
            .record(RecordPoint::FutureReturn)
            .expect("Should have properly advanced recorder state");
        // check that result is 4
        assert_eq!(1, result_context.content.len());
        let output_item = &result_context.content[0]
            .as_ref()
            .expect("Set should be present");
        assert_eq!(1, output_item.buffers.len());
        let position = output_item.buffers[0].data;
        assert_eq!(input_size, position.size, "Checking for size of output");
        let mut output = vec![0i64; position.size / 8];
        result_context
            .context
            .read(position.offset, &mut output)
            .expect("Should succeed in reading");
        let expected = _get_expected_mat(mat_size);
        assert_eq!(expected.len(), output.len());
        for (should, is) in expected.iter().zip(output.iter()) {
            assert_eq!(should, is);
        }
        domain
            .release_context(result_context)
            .expect("Should release context");
    }
    domain
        .release_context(static_context)
        .expect("Should release context");
}

fn _engine_stdio<Dom: MemoryDomain, L: Loader, Drv: Driver>(
    filename: &str,
    dom_init: Vec<u8>,
    drv_init: Vec<u8>,
) {
    // load elf file
    let elf_buffer = _read_file(filename, 0);
    let mut domain = Dom::init(dom_init).expect("Should have initialized new cheri domain");
    let (req_list, static_context, config) =
        L::parse_function(elf_buffer, &mut domain).expect("Parsing should work");

    let mut engine = Drv::start_engine(vec![drv_init[0]]).expect("Should be able to start engine");
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
        .write(stdin_offset, stdin_content.as_bytes())
        .expect("Write should go through");
    function_context.content.push(Some(DataSet {
        ident: "stdio".to_string(),
        buffers: vec![DataItem {
            ident: "stdin".to_string(),
            data: Position {
                offset: stdin_offset,
                size: stdin_content.len(),
            },
        }],
    }));
    println!("{:?}", function_context.content);
    let archive = Arc::new(Mutex::new(Archive::new()));
    let mut recorder = Recorder::new(archive, RecordPoint::TransferEnd);
    let (result, result_context) = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(engine.run(
            &config,
            function_context,
            &vec!["stdio".to_string()],
            recorder.clone(),
        ));
    result.expect("Engine should run ok with basic function");
    recorder
        .record(RecordPoint::FutureReturn)
        .expect("Should have properly advanced recorder state");
    // check there is exactly one set called stdio
    assert_eq!(1, result_context.content.len());
    let io_set = &result_context.content[0]
        .as_ref()
        .expect("Set should be present");
    assert_eq!("stdio", io_set.ident);
    assert_eq!(2, io_set.buffers.len());
    let mut stdout_vec = Vec::<u8>::new();
    let mut stderr_vec = Vec::<u8>::new();
    for item in &io_set.buffers {
        match item.ident.as_str() {
            "stdout" => {
                stdout_vec = vec![0; item.data.size];
                result_context
                    .read(item.data.offset, &mut stdout_vec)
                    .expect("stdout read should succeed")
            }
            "stderr" => {
                stderr_vec = vec![0; item.data.size];
                result_context
                    .read(item.data.offset, &mut stderr_vec)
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

fn _engine_fileio<Dom: MemoryDomain, L: Loader, Drv: Driver>(
    filename: &str,
    dom_init: Vec<u8>,
    drv_init: Vec<u8>,
) {
    // load elf file
    let elf_buffer = _read_file(filename, 0);
    let mut domain = Dom::init(dom_init).expect("Should have initialized new cheri domain");
    let (req_list, static_context, config) =
        L::parse_function(elf_buffer, &mut domain).expect("Parsing should work");

    let mut engine = Drv::start_engine(vec![drv_init[0]]).expect("Should be able to start engine");
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
        .write(in_file_offset, in_file_content.as_bytes())
        .expect("Write should go through");
    function_context.content.push(Some(DataSet {
        ident: "in".to_string(),
        buffers: vec![DataItem {
            ident: "in_file".to_string(),
            data: Position {
                offset: in_file_offset,
                size: in_file_content.len(),
            },
        }],
    }));
    let in_file1_content = "Test file 1 \n line 2\n";
    let in_file1_offset = function_context
        .get_free_space(in_file1_content.len(), 8)
        .expect("Should have space");
    function_context
        .write(in_file1_offset, in_file1_content.as_bytes())
        .expect("Write should go through");
    let in_file2_content = "Test file 2 \n line 2\n";
    let in_file2_offset = function_context
        .get_free_space(in_file2_content.len(), 8)
        .expect("Should have space");
    function_context
        .write(in_file2_offset, in_file2_content.as_bytes())
        .expect("Write should go through");
    let in_file3_content = "Test file 3 \n line 2\n";
    let in_file3_offset = function_context
        .get_free_space(in_file3_content.len(), 8)
        .expect("Should have space");
    function_context
        .write(in_file3_offset, in_file3_content.as_bytes())
        .expect("Write should go through");
    let in_file4_content = "Test file 4 \n line 2\n";
    let in_file4_offset = function_context
        .get_free_space(in_file4_content.len(), 8)
        .expect("Should have space");
    function_context
        .write(in_file4_offset, in_file4_content.as_bytes())
        .expect("Write should go through");
    function_context.content.push(Some(DataSet {
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
    }));
    let archive = Arc::new(Mutex::new(Archive::new()));
    let mut recorder = Recorder::new(archive, RecordPoint::TransferEnd);
    let (result, result_context) = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(engine.run(
            &config,
            function_context,
            &vec![
                "stdio".to_string(),
                "out".to_string(),
                "out_nested".to_string(),
            ],
            recorder.clone(),
        ));
    result.expect("Engine should run ok with basic function");
    recorder
        .record(RecordPoint::FutureReturn)
        .expect("Should have properly advanced recorder state");
    assert_eq!(3, result_context.content.len());
    // check out set
    let set1 = result_context.content[1]
        .as_ref()
        .expect("Set should be present");
    assert_eq!(1, set1.buffers.len());
    let item = &set1.buffers[0];
    assert_eq!("out_file", item.ident);
    let mut read_buffer = vec![0; item.data.size];
    result_context
        .read(item.data.offset, &mut read_buffer)
        .expect("should be able to read");
    assert_eq!(
        in_file_content,
        std::str::from_utf8(&read_buffer).expect("output content should be string")
    );
    let output_set = result_context.content[2]
        .as_ref()
        .expect("Should have output set");
    assert_eq!(4, output_set.buffers.len());
    for item in output_set.buffers.iter() {
        let mut read_buffer = vec![0; item.data.size];
        result_context
            .read(item.data.offset, &mut read_buffer)
            .expect("should be able to read");
        let content_string = std::str::from_utf8(&read_buffer).expect("content should be string");
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

macro_rules! driverTests {
    ($name : ident; $domain : ty; $dom_init: expr; $driver : ty ; $drv_init : expr; $drv_init_wrong : expr; $loader : ty) => {
        use super::*;

        #[test]
        #[should_panic]
        fn test_loader_empty() {
            _loader_empty::<$domain, $loader>($dom_init);
        }

        #[test]
        fn test_driver() {
            _driver::<$driver>($drv_init, $drv_init_wrong);
        }

        #[test]
        fn test_engine_minimal() {
            let name = format!("test_elf_{}_basic", stringify!($name));
            _engine_minimal::<$domain, $loader, $driver>(&name, $dom_init, $drv_init);
        }

        #[test]
        fn test_engine_matmul_single() {
            let name = format!("test_elf_{}_matmul", stringify!($name));
            _engine_matmul_single::<$domain, $loader, $driver>(&name, $dom_init, $drv_init);
        }

        #[test]
        fn test_engine_matmul_size_sweep() {
            let name = format!("test_elf_{}_matmul", stringify!($name));
            _engine_matmul_size_sweep::<$domain, $loader, $driver>(&name, $dom_init, $drv_init);
        }

        #[test]
        #[ignore]
        fn test_engine_stdio() {
            let name = format!("test_elf_{}_stdio", stringify!($name));
            _engine_stdio::<$domain, $loader, $driver>(&name, $dom_init, $drv_init);
        }

        #[test]
        fn test_engine_fileio() {
            let name = format!("test_elf_{}_fileio", stringify!($name));
            _engine_fileio::<$domain, $loader, $driver>(&name, $dom_init, $drv_init)
        }
    };
}

#[cfg(feature = "cheri")]
mod cheri {
    use crate::function_driver::compute_driver::cheri::CheriDriver as cheriDriver;
    use crate::function_driver::compute_driver::cheri::CheriLoader as cheriLoader;
    use crate::memory_domain::cheri::CheriMemoryDomain as cheriDomain;
    driverTests!(cheri; cheriDomain; Vec::new(); cheriDriver; vec![1,2,3]; vec![4]; cheriLoader);
}

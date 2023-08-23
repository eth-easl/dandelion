#[cfg(all(test, any(feature = "cheri")))]
mod compute_driver_tests {
    use crate::{
        function_driver::{util::load_static, Driver, Engine, Function, FunctionConfig},
        memory_domain::{Context, ContextTrait, MemoryDomain},
        DataItem, DataSet, Position,
    };
    use dandelion_commons::{
        records::{Archive, RecordPoint, Recorder},
        DandelionError,
    };
    use std::sync::{Arc, Mutex};

    fn read_file(name: &str) -> Vec<u8> {
        // load elf file
        let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("tests/data");
        path.push(name);
        let mut elf_file = std::fs::File::open(path).expect("Should have found test file");
        let mut elf_buffer = Vec::<u8>::new();
        use std::io::Read;
        let _ = elf_file
            .read_to_end(&mut elf_buffer)
            .expect("Should be able to read entire file");
        return elf_buffer;
    }

    fn loader_empty<Dom: MemoryDomain>(dom_init: Vec<u8>, driver: Box<dyn Driver>) {
        // load elf file
        let elf_file = Vec::<u8>::new();
        let mut domain = Dom::init(dom_init).expect("Should be able to get domain");
        driver
            .parse_function(elf_file, &mut domain)
            .expect("Empty string should return error");
    }

    fn driver(driver: Box<dyn Driver>, init: Vec<u8>, wrong_init: Vec<u8>) {
        let no_resource_engine = driver.start_engine(Vec::<u8>::new());
        match no_resource_engine {
            Ok(_) => panic!("Should not be able to get engine"),
            Err(err) => assert_eq!(DandelionError::ConfigMissmatch, err),
        }

        let wrong_resource_engine = driver.start_engine(wrong_init);
        match wrong_resource_engine {
            Ok(_) => panic!("Should not be able to get engine"),
            Err(err) => assert_eq!(DandelionError::MalformedConfig, err),
        }

        for resource in init {
            let engine = driver.start_engine(vec![resource]);
            engine.expect("Should be able to get engine");
        }
    }

    fn prepare_engine_and_function<Dom: MemoryDomain>(
        filename: &str,
        dom_init: Vec<u8>,
        driver: &Box<dyn Driver>,
        drv_init: Vec<u8>,
    ) -> (Box<dyn Engine>, Context, FunctionConfig) {
        let elf_buffer = read_file(filename);
        let mut domain = Dom::init(dom_init).expect("Should have initialized domain");
        let Function {
            requirements,
            context,
            config,
        } = driver
            .parse_function(elf_buffer, &mut domain)
            .expect("Should be able to parse function");
        let engine = driver
            .start_engine(vec![drv_init[0]])
            .expect("Should be able to start engine");
        let function_context = load_static(&mut domain, &context, &requirements)
            .expect("Should be able to load function");
        return (engine, function_context, config);
    }

    fn engine_minimal<Dom: MemoryDomain>(
        filename: &str,
        dom_init: Vec<u8>,
        driver: Box<dyn Driver>,
        drv_init: Vec<u8>,
    ) {
        let (mut engine, function_context, config) =
            prepare_engine_and_function::<Dom>(filename, dom_init, &driver, drv_init);
        let archive = Arc::new(Mutex::new(Archive::new()));
        let recorder = Recorder::new(archive, RecordPoint::TransferEnd);
        let (result, _function_context) = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(engine.run(&config, function_context, &vec![], recorder.clone()));
        result.expect("Engine should run ok with basic function");
    }

    fn engine_matmul_single<Dom: MemoryDomain>(
        filename: &str,
        dom_init: Vec<u8>,
        driver: Box<dyn Driver>,
        drv_init: Vec<u8>,
    ) {
        let (mut engine, mut function_context, config) =
            prepare_engine_and_function::<Dom>(filename, dom_init, &driver, drv_init);
        // add inputs
        let in_size_offset = function_context
            .get_free_space_and_write_slice(&[1i64, 2i64])
            .expect("Should have space for single i64");
        function_context.content.push(Some(DataSet {
            ident: "".to_string(),
            buffers: vec![DataItem {
                ident: "".to_string(),
                data: Position {
                    offset: in_size_offset as usize,
                    size: 16,
                },
                key: 0,
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
        assert_eq!(16, position.size, "Checking for size of output");
        let mut read_buffer = vec![0i64; position.size / 8];
        result_context
            .context
            .read(position.offset, &mut read_buffer)
            .expect("Should succeed in reading");
        assert_eq!(1, read_buffer[0]);
        assert_eq!(4, read_buffer[1]);
    }

    fn get_expected_mat(size: usize) -> Vec<i64> {
        let mut in_mat_vec = Vec::<i64>::new();
        for i in 0..(size * size) {
            in_mat_vec.push(i as i64);
        }
        let mut out_mat_vec = vec![0i64; size * size];
        for i in 0..size {
            for j in 0..size {
                for k in 0..size {
                    out_mat_vec[i * size + j] +=
                        in_mat_vec[i * size + k] * in_mat_vec[j * size + k];
                }
            }
        }
        return out_mat_vec;
    }

    fn engine_matmul_size_sweep<Dom: MemoryDomain>(
        filename: &str,
        dom_init: Vec<u8>,
        driver: Box<dyn Driver>,
        drv_init: Vec<u8>,
    ) {
        const LOWER_SIZE_BOUND: usize = 2;
        const UPPER_SIZE_BOUND: usize = 16;
        for mat_size in LOWER_SIZE_BOUND..UPPER_SIZE_BOUND {
            let (mut engine, mut function_context, config) = prepare_engine_and_function::<Dom>(
                filename,
                dom_init.clone(),
                &driver,
                drv_init.clone(),
            );
            // add inputs
            let mut mat_vec = Vec::<i64>::new();
            mat_vec.push(mat_size as i64);
            for i in 0..(mat_size * mat_size) {
                mat_vec.push(i as i64);
            }
            let in_mat_offset = function_context
                .get_free_space_and_write_slice(&mat_vec)
                .expect("Should have space") as usize;
            function_context.content.push(Some(DataSet {
                ident: "".to_string(),
                buffers: vec![DataItem {
                    ident: "".to_string(),
                    data: Position {
                        offset: in_mat_offset,
                        size: mat_vec.len() * core::mem::size_of::<i64>(),
                    },
                    key: 0,
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
            assert_eq!(1, result_context.content.len());
            let output_item = &result_context.content[0]
                .as_ref()
                .expect("Set should be present");
            assert_eq!(1, output_item.buffers.len());
            let position = output_item.buffers[0].data;
            assert_eq!(
                (mat_size * mat_size + 1) * 8,
                position.size,
                "Checking for size of output"
            );
            let mut output = vec![0i64; position.size / 8];
            result_context
                .context
                .read(position.offset, &mut output)
                .expect("Should succeed in reading");
            let expected = self::get_expected_mat(mat_size);
            assert_eq!(mat_size as i64, output[0]);
            for (should, is) in expected.iter().zip(output[1..].iter()) {
                assert_eq!(should, is);
            }
        }
    }

    fn engine_stdio<Dom: MemoryDomain>(
        filename: &str,
        dom_init: Vec<u8>,
        driver: Box<dyn Driver>,
        drv_init: Vec<u8>,
    ) {
        let (mut engine, mut function_context, config) =
            prepare_engine_and_function::<Dom>(filename, dom_init, &driver, drv_init);
        let stdin_content = "Test line \n line 2\n";
        let stdin_offset = function_context
            .get_free_space_and_write_slice(stdin_content.as_bytes())
            .expect("Should have space") as usize;
        function_context.content.push(Some(DataSet {
            ident: "stdio".to_string(),
            buffers: vec![DataItem {
                ident: "stdin".to_string(),
                data: Position {
                    offset: stdin_offset,
                    size: stdin_content.len(),
                },
                key: 0,
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

    fn engine_fileio<Dom: MemoryDomain>(
        filename: &str,
        dom_init: Vec<u8>,
        driver: Box<dyn Driver>,
        drv_init: Vec<u8>,
    ) {
        let (mut engine, mut function_context, config) =
            prepare_engine_and_function::<Dom>(filename, dom_init, &driver, drv_init);
        let in_file_content = "Test file 0\n line 2\n";
        let in_file_offset = function_context
            .get_free_space_and_write_slice(in_file_content.as_bytes())
            .expect("Should have space") as usize;
        function_context.content.push(Some(DataSet {
            ident: "in".to_string(),
            buffers: vec![DataItem {
                ident: "in_file".to_string(),
                data: Position {
                    offset: in_file_offset,
                    size: in_file_content.len(),
                },
                key: 0,
            }],
        }));
        let in_file1_content = "Test file 1 \n line 2\n";
        let in_file1_offset = function_context
            .get_free_space_and_write_slice(in_file1_content.as_bytes())
            .expect("Should have space") as usize;
        let in_file2_content = "Test file 2 \n line 2\n";
        let in_file2_offset = function_context
            .get_free_space_and_write_slice(in_file2_content.as_bytes())
            .expect("Should have space") as usize;
        let in_file3_content = "Test file 3 \n line 2\n";
        let in_file3_offset = function_context
            .get_free_space_and_write_slice(in_file3_content.as_bytes())
            .expect("Should have space") as usize;
        let in_file4_content = "Test file 4 \n line 2\n";
        let in_file4_offset = function_context
            .get_free_space_and_write_slice(in_file4_content.as_bytes())
            .expect("Should have space") as usize;
        function_context.content.push(Some(DataSet {
            ident: "in_nested".to_string(),
            buffers: vec![
                DataItem {
                    ident: "in_file".to_string(),
                    data: Position {
                        offset: in_file1_offset,
                        size: in_file1_content.len(),
                    },
                    key: 0,
                },
                DataItem {
                    ident: "in_folder/in_file".to_string(),
                    data: Position {
                        offset: in_file2_offset,
                        size: in_file2_content.len(),
                    },
                    key: 0,
                },
                DataItem {
                    ident: "in_folder/in_file_two".to_string(),
                    data: Position {
                        offset: in_file3_offset,
                        size: in_file3_content.len(),
                    },
                    key: 0,
                },
                DataItem {
                    ident: "in_folder/in_folder_two/in_file".to_string(),
                    data: Position {
                        offset: in_file4_offset,
                        size: in_file4_content.len(),
                    },
                    key: 0,
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
            let content_string =
                std::str::from_utf8(&read_buffer).expect("content should be string");
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
        ($name : ident; $domain : ty; $dom_init: expr; $driver : expr ; $drv_init : expr; $drv_init_wrong : expr) => {
            #[test]
            #[should_panic]
            fn test_loader_empty() {
                let driver = Box::new($driver);
                super::loader_empty::<$domain>($dom_init, driver);
            }

            #[test]
            fn test_driver() {
                let driver = Box::new($driver);
                super::driver(driver, $drv_init, $drv_init_wrong);
            }

            #[test]
            fn test_engine_minimal() {
                let name = format!("test_elf_{}_basic", stringify!($name));
                let driver = Box::new($driver);
                super::engine_minimal::<$domain>(&name, $dom_init, driver, $drv_init);
            }

            #[test]
            fn test_engine_matmul_single() {
                let name = format!("test_elf_{}_matmul", stringify!($name));
                let driver = Box::new($driver);
                super::engine_matmul_single::<$domain>(&name, $dom_init, driver, $drv_init);
            }

            #[test]
            fn test_engine_matmul_size_sweep() {
                let name = format!("test_elf_{}_matmul", stringify!($name));
                let driver = Box::new($driver);
                super::engine_matmul_size_sweep::<$domain>(&name, $dom_init, driver, $drv_init);
            }

            #[test]
            #[ignore]
            fn test_engine_stdio() {
                let name = format!("test_elf_{}_stdio", stringify!($name));
                let driver = Box::new($driver);
                super::engine_stdio::<$domain>(&name, $dom_init, driver, $drv_init);
            }

            #[test]
            fn test_engine_fileio() {
                let name = format!("test_elf_{}_fileio", stringify!($name));
                let driver = Box::new($driver);
                super::engine_fileio::<$domain>(&name, $dom_init, driver, $drv_init)
            }
        };
    }

    #[cfg(feature = "cheri")]
    mod cheri {
        use crate::function_driver::compute_driver::cheri::CheriDriver;
        use crate::memory_domain::cheri::CheriMemoryDomain;
        driverTests!(cheri; CheriMemoryDomain; Vec::new(); CheriDriver {}; vec![1,2,3]; vec![4]);
    }
}

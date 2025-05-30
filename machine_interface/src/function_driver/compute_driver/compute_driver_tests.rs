#[cfg(all(test, any(feature = "cheri", feature = "mmu", feature = "wasm")))]
mod compute_driver_tests {
    use crate::{
        function_driver::{
            test_queue::TestQueue, ComputeResource, Driver, FunctionConfig, WorkToDo,
        },
        memory_domain::{Context, ContextState, ContextTrait, MemoryDomain, MemoryResource},
        DataItem, DataSet, Position,
    };
    use core::panic;
    use dandelion_commons::{
        records::{Archive, ArchiveInit, RecordPoint},
        DandelionError,
    };
    use std::sync::Arc;

    fn loader_empty<Dom: MemoryDomain>(dom_init: MemoryResource, driver: Box<dyn Driver>) {
        // load elf file
        let elf_path = String::new();
        let domain = Box::leak(Dom::init(dom_init).expect("Should be able to get domain"));
        driver
            .parse_function(elf_path, domain)
            .expect("Empty string should return error");
    }

    fn driver(
        driver: Box<dyn Driver>,
        init: Vec<ComputeResource>,
        wrong_init: Vec<ComputeResource>,
    ) {
        for wronge_resource in wrong_init {
            let queue_box = Box::new(TestQueue::new());
            let wrong_resource_engine = driver.start_engine(wronge_resource, queue_box);
            match wrong_resource_engine {
                Ok(_) => panic!("Should not be able to get engine"),
                Err(err) => assert_eq!(DandelionError::EngineResourceError, err),
            }
        }

        for resource in init {
            let queue_box = Box::new(TestQueue::new());
            let engine = driver.start_engine(resource, queue_box);
            engine.expect("Should be able to get engine");
        }
    }

    fn prepare_engine_and_function<Dom: MemoryDomain>(
        filename: &str,
        dom_init: MemoryResource,
        driver: &Box<dyn Driver>,
        drv_init: Vec<ComputeResource>,
    ) -> (Context, FunctionConfig, Box<TestQueue>) {
        let queue = Box::new(TestQueue::new());
        let domain = Box::leak(Dom::init(dom_init).expect("Should have initialized domain"));
        let function = driver
            .parse_function(filename.to_string(), domain)
            .expect("Should be able to parse function");
        driver
            .start_engine(drv_init[0], queue.clone())
            .expect("Should be able to start engine");
        let function_context = function
            .load(domain, 0x802_0000)
            .expect("Should be able to load function");
        return (function_context, function.config, queue);
    }

    //VICTOR added
    fn fpga_create_function_contexts<Dom: MemoryDomain>(
        driver: &Box<dyn Driver>,
        num: usize,
    ) -> (Vec<Context>, FunctionConfig, Box<TestQueue>) {
        //let driver: Box<dyn Driver> = Box::new(FpgaDriver {});
        let filename = "whatever";
        let dom_init = MemoryResource::None;
        let drv_init = vec![ComputeResource::CPU(1)];

        let queue = Box::new(TestQueue::new());
        let domain = Box::leak(Dom::init(dom_init).expect("Should have initialized domain"));
        let function = driver
            .parse_function(filename.to_string(), domain)
            .expect("Should be able to parse function");
        driver
            .start_engine(drv_init[0], queue.clone())
            .expect("Should be able to start engine");

        let mut results: Vec<Context> = Vec::new();

        for i in 0..num {
            let mut function_context = function
                .load(domain, 1024 * 1024 * 8) //8MB
                .expect("Should be able to load function");

            let input_example: [i16; 900] = std::array::from_fn(|j| j as i16);
            let bitstream_id: [u16; 1] = [i.try_into().expect("asdfd")];
            let bitstream_id_offset = function_context
                .get_free_space_and_write_slice(&bitstream_id)
                .expect("should have space for bitstream id");
            let data_offset = function_context
                .get_free_space_and_write_slice(&input_example)
                .expect("Should have space for a little data");
            println!("got data offset {:?}", data_offset);

            function_context.content.push(Some(DataSet {
                ident: "inputset".to_string(),
                buffers: vec![
                    DataItem {
                        ident: "bitstream_id".to_string(),
                        data: Position {
                            offset: bitstream_id_offset as usize,
                            size: 2,
                        },
                        key: 0,
                    },
                    DataItem {
                        ident: "inputitem".to_string(),
                        data: Position {
                            offset: data_offset as usize,
                            size: 2 * input_example.len(),
                        },
                        key: 1,
                    },
                ],
            }));
            results.push(function_context);
        }

        return (results, function.config, queue);
    }

    fn engine_minimal<Dom: MemoryDomain>(
        filename: &str,
        dom_init: MemoryResource,
        driver: Box<dyn Driver>,
        drv_init: Vec<ComputeResource>,
    ) {
        let (function_context, config, queue) =
            prepare_engine_and_function::<Dom>(filename, dom_init, &driver, drv_init);
        let archive = Box::leak(Box::new(Archive::init(ArchiveInit {
            #[cfg(feature = "timestamp")]
            timestamp_count: 1000,
        })));
        let recorder = archive.get_recorder().unwrap();
        let promise = queue.enqueu(WorkToDo::FunctionArguments {
            config: config,
            context: function_context,
            output_sets: Arc::new(Vec::new()),
            recorder,
        });
        let _ = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(promise)
            .expect("Engine should run ok with basic function");
    }

    fn engine_matmul_single<Dom: MemoryDomain>(
        filename: &str,
        dom_init: MemoryResource,
        driver: Box<dyn Driver>,
        drv_init: Vec<ComputeResource>,
    ) {
        let (mut function_context, config, queue) =
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
            output_sets: Arc::new(vec![String::from("")]),
            recorder: recorder.get_sub_recorder().unwrap(),
        });
        let result_context = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(promise)
            .expect("Engine should run ok with basic function")
            .get_context();
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
        dom_init: MemoryResource,
        driver: Box<dyn Driver>,
        drv_init: Vec<ComputeResource>,
    ) {
        const LOWER_SIZE_BOUND: usize = 2;
        const UPPER_SIZE_BOUND: usize = 16;
        for mat_size in LOWER_SIZE_BOUND..UPPER_SIZE_BOUND {
            let (mut function_context, config, queue) = prepare_engine_and_function::<Dom>(
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
                output_sets: Arc::new(vec![String::from("")]),
                recorder: recorder.get_sub_recorder().unwrap(),
            });
            let result_context = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(promise)
                .expect("Engine should run ok with basic function")
                .get_context();
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
        dom_init: MemoryResource,
        driver: Box<dyn Driver>,
        drv_init: Vec<ComputeResource>,
    ) {
        let (mut function_context, config, queue) =
            prepare_engine_and_function::<Dom>(filename, dom_init, &driver, drv_init);
        let stdin_content = "Test line \n line 2\n";
        let stdin_offset = function_context
            .get_free_space_and_write_slice(stdin_content.as_bytes())
            .expect("Should have space") as usize;
        let argv_content = "stdio\0flag0\0flag1\0";
        let argv_offset = function_context
            .get_free_space_and_write_slice(argv_content.as_bytes())
            .expect("Should have space") as usize;
        let env_content = "HOME=test_home\0";
        let env_offset = function_context
            .get_free_space_and_write_slice(env_content.as_bytes())
            .expect("Should have space") as usize;

        function_context.content.push(Some(DataSet {
            ident: "stdio".to_string(),
            buffers: vec![
                DataItem {
                    ident: "stdin".to_string(),
                    data: Position {
                        offset: stdin_offset,
                        size: stdin_content.len(),
                    },
                    key: 0,
                },
                DataItem {
                    ident: "argv".to_string(),
                    data: Position {
                        offset: argv_offset,
                        size: argv_content.len(),
                    },
                    key: 0,
                },
                DataItem {
                    ident: "environ".to_string(),
                    data: Position {
                        offset: env_offset,
                        size: env_content.len(),
                    },
                    key: 0,
                },
            ],
        }));
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
            output_sets: Arc::new(vec![String::from("stdio")]),
            recorder: recorder.get_sub_recorder().unwrap(),
        });
        let result_context = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(promise)
            .expect("Engine should run ok with basic function")
            .get_context();
        recorder
            .record(RecordPoint::FutureReturn)
            .expect("Should have properly advanced recorder state");
        // check the function exited with exit code 0
        match result_context.state {
            ContextState::InPreparation => panic!("context still in preparation, never evaluated "),
            ContextState::Run(exit_status) => assert_eq!(0, exit_status),
        }
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
                unexpected_name => panic!(
                    "found unexpected item named {} in stdio set",
                    unexpected_name
                ),
            }
        }
        let stdout_string = std::str::from_utf8(&stdout_vec).expect("should be string");
        let stderr_string = std::str::from_utf8(&stderr_vec).expect("should be string");
        let expected_stdout = format!(
            "Test string to stdout\n\
            read {} characters from stdin\n\
            {}argument 0 is stdio\n\
            argument 1 is flag0\n\
            argument 2 is flag1\n\
            environmental variable HOME is test_home\n",
            stdin_content.len(),
            stdin_content
        );
        assert_eq!(expected_stdout, stdout_string);
        assert_eq!("Test string to stderr\n", stderr_string);
    }

    fn engine_fileio<Dom: MemoryDomain>(
        filename: &str,
        dom_init: MemoryResource,
        driver: Box<dyn Driver>,
        drv_init: Vec<ComputeResource>,
    ) {
        let (mut function_context, config, queue) =
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
        let archive = Box::leak(Box::new(Archive::init(ArchiveInit {
            #[cfg(feature = "timestamp")]
            timestamp_count: 1000,
        })));
        let mut recorder = archive.get_recorder().unwrap();
        recorder
            .record(RecordPoint::TransferEnd)
            .expect("Should have properly initialized recorder state");
        let promise = queue.enqueu(WorkToDo::FunctionArguments {
            config: config,
            context: function_context,
            output_sets: Arc::new(vec![
                "stdio".to_string(),
                "out".to_string(),
                "out_nested".to_string(),
            ]),
            recorder: recorder.get_sub_recorder().unwrap(),
        });
        let result_context = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(promise)
            .expect("Engine should run ok with basic function")
            .get_context();
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
                let name = format!(
                    "{}/tests/data/test_{}_basic",
                    env!("CARGO_MANIFEST_DIR"),
                    stringify!($name)
                );
                let driver = Box::new($driver);
                super::engine_minimal::<$domain>(&name, $dom_init, driver, $drv_init);
            }

            #[test]
            fn test_engine_matmul_single() {
                let name = format!(
                    "{}/tests/data/test_{}_matmul",
                    env!("CARGO_MANIFEST_DIR"),
                    stringify!($name)
                );
                let driver = Box::new($driver);
                super::engine_matmul_single::<$domain>(&name, $dom_init, driver, $drv_init);
            }

            #[test]
            fn test_engine_matmul_size_sweep() {
                let name = format!(
                    "{}/tests/data/test_{}_matmul",
                    env!("CARGO_MANIFEST_DIR"),
                    stringify!($name)
                );
                let driver = Box::new($driver);
                super::engine_matmul_size_sweep::<$domain>(&name, $dom_init, driver, $drv_init);
            }

            #[test]
            #[cfg(not(feature = "wasm"))]
            fn test_engine_stdio() {
                let name = format!(
                    "{}/tests/data/test_{}_stdio",
                    env!("CARGO_MANIFEST_DIR"),
                    stringify!($name)
                );
                let driver = Box::new($driver);
                super::engine_stdio::<$domain>(&name, $dom_init, driver, $drv_init);
            }

            #[test]
            #[cfg(not(feature = "wasm"))]
            fn test_engine_fileio() {
                let name = format!(
                    "{}/tests/data/test_{}_fileio",
                    env!("CARGO_MANIFEST_DIR"),
                    stringify!($name)
                );
                let driver = Box::new($driver);
                super::engine_fileio::<$domain>(&name, $dom_init, driver, $drv_init)
            }
        };
    }

    #[cfg(feature = "cheri")]
    mod cheri {
        use crate::function_driver::{compute_driver::cheri::CheriDriver, ComputeResource};
        use crate::memory_domain::{cheri::CheriMemoryDomain, MemoryResource};
        driverTests!(elf_cheri; CheriMemoryDomain; MemoryResource::None; CheriDriver {};
        core_affinity::get_core_ids()
           .and_then(
                |core_vec|
                Some(core_vec
                    .into_iter()
                    .map(|id| ComputeResource::CPU(id.id as u8))
                    .collect())).expect("Should have at least one core");
        vec![
            ComputeResource::CPU(255),
            ComputeResource::GPU(0)
        ]);
    }

    #[cfg(feature = "mmu")]
    mod mmu {
        use crate::function_driver::{compute_driver::mmu::MmuDriver, ComputeResource};
        use crate::memory_domain::{mmu::MmuMemoryDomain, MemoryResource};
        #[cfg(target_arch = "x86_64")]
        driverTests!(elf_mmu_x86_64; MmuMemoryDomain; MemoryResource::None; MmuDriver {};
        core_affinity::get_core_ids()
           .and_then(
                |core_vec|
                Some(core_vec
                    .into_iter()
                    .map(|id| ComputeResource::CPU(id.id as u8))
                    .collect())).expect("Should have at least one core");
        vec![
            ComputeResource::CPU(255),
            ComputeResource::GPU(0)
        ]);
        #[cfg(target_arch = "aarch64")]
        driverTests!(elf_mmu_aarch64; MmuMemoryDomain; MemoryResource::None; MmuDriver {};
        core_affinity::get_core_ids()
            .and_then(
                |core_vec|
                Some(core_vec
                    .into_iter()
                    .map(|id| ComputeResource::CPU(id.id as u8))
                    .collect())).expect("Should have at least one core");
        vec![
            ComputeResource::CPU(255),
            ComputeResource::GPU(0),
        ]);
    }

    #[cfg(feature = "wasm")]
    mod wasm {
        use crate::function_driver::{compute_driver::wasm::WasmDriver, ComputeResource};
        use crate::memory_domain::{wasm::WasmMemoryDomain, MemoryResource};

        #[cfg(target_arch = "x86_64")]
        driverTests!(sysld_wasm_x86_64; WasmMemoryDomain; MemoryResource::None; WasmDriver {};
        core_affinity::get_core_ids()
            .and_then(
                |core_vec|
                Some(core_vec
                    .into_iter()
                    .map(|id| ComputeResource::CPU(id.id as u8))
                    .collect())).expect("Should have at least one core");
        vec![
            ComputeResource::CPU(255),
            ComputeResource::GPU(0),
        ]);

        #[cfg(target_arch = "aarch64")]
        driverTests!(sysld_wasm_aarch64; WasmMemoryDomain; MemoryResource::None; WasmDriver {};
        core_affinity::get_core_ids()
            .and_then(
                |core_vec|
                Some(core_vec
                    .into_iter()
                    .map(|id| ComputeResource::CPU(id.id as u8))
                    .collect())).expect("Should have at least one core");
        vec![
            ComputeResource::CPU(255),
            ComputeResource::GPU(0),
        ]);
    }

    #[cfg(feature = "fpga")]
    mod fpga {
        use super::engine_minimal;
        use super::prepare_engine_and_function;
        use crate::function_driver::compute_driver::compute_driver_tests::compute_driver_tests::fpga_create_function_contexts;
        use crate::function_driver::WorkDone;
        use crate::promise::Promise;
        use crate::{
            function_driver::{
                compute_driver::fpga, compute_driver::fpga::FpgaDriver, thread_utils::EngineLoop,
                ComputeResource, Driver, WorkToDo::FunctionArguments,
            },
            memory_domain::{mmap::MmapMemoryDomain, ContextState, ContextTrait, MemoryResource},
            DataItem, DataSet, Position,
        };
        use dandelion_commons::{
            records::{Archive, ArchiveInit, RecordPoint},
            DandelionError,
        };

        use std::sync::{Arc, Mutex};
        /*RUST_LOG=debug cargo test --package machine_interface --lib --features mmu --features fpga -- function_driver::compute_driver::compute_driver_tests::compute_driver_tests::fpga::temp_test --exact --show-output  */
        #[test]
        fn dummy_test_offline() {
            env_logger::init();
            let driver: Box<dyn Driver> = Box::new(FpgaDriver {});
            let filename = "dummy_input";
            let dom_init = MemoryResource::None;
            let drv_init = vec![ComputeResource::CPU(1)];

            //TODO VICTOR: replace this with just a prepare engine and a separate prepare function
            let (mut function_context, config, queue) =
                prepare_engine_and_function::<MmapMemoryDomain>(
                    filename, dom_init, &driver, drv_init,
                );
            let input_example: [i16; 5] = [2, 5, 5, 5, 5];
            let bitstream_id: [u16; 1] = [1];
            let bitstream_id_offset = function_context
                .get_free_space_and_write_slice(&bitstream_id)
                .expect("should have space for bitstream id");
            let data_offset = function_context
                .get_free_space_and_write_slice(&input_example)
                .expect("Should have space for a little data");
            println!("got data offset {:?}", data_offset);

            function_context.content.push(Some(DataSet {
                ident: "inputset".to_string(),
                buffers: vec![
                    DataItem {
                        ident: "bitstream_id".to_string(),
                        data: Position {
                            offset: bitstream_id_offset as usize,
                            size: 2,
                        },
                        key: 0,
                    },
                    DataItem {
                        ident: "inputitem".to_string(),
                        data: Position {
                            offset: data_offset as usize,
                            size: 4 * input_example.len(),
                        },
                        key: 1,
                    },
                ],
            }));
            let archive = Box::leak(Box::new(Archive::init(ArchiveInit {
                #[cfg(feature = "timestamp")]
                timestamp_count: 1000,
            })));

            let mut recorder = archive.get_recorder().unwrap();
            let promise = queue.enqueu(crate::function_driver::WorkToDo::FunctionArguments {
                config,
                context: function_context,
                output_sets: Arc::new(Vec::new()),
                recorder: recorder.get_sub_recorder().unwrap(),
            });

            let workdone = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(promise)
                .expect("Engine should run ok with basic function");

            if let WorkDone::Context(context) = workdone {
                if let Some(dataset) = &context.content[0] {
                    let item = &dataset.buffers[0];
                    let offset = item.data.offset;
                    let size = item.data.size;
                    assert!(size % 4 == 0);
                    let mut result: Vec<i16> = vec![0; size / 4];
                    println!("{size:?}");
                    context
                        .read(offset, &mut result)
                        .expect("couldn't read from context");
                    println!("got result: ");
                    for el in result {
                        println!("{el:?}");
                    }
                } else {
                    eprintln!("couldn't get content in test");
                    panic!()
                }
            }

            let shutdown_promise = queue.enqueu(crate::function_driver::WorkToDo::Shutdown());
            let shutdown_workdone = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(shutdown_promise)
                .expect("Engine should run ok with basic function");
            if let WorkDone::Resources(_resources) = shutdown_workdone {
                println!("successfully shut down! :D");
            }

            recorder
                .record(RecordPoint::FutureReturn)
                .expect("Should have properly advanced recorder state");
        }

        #[test]
        fn mini_test_online() {
            env_logger::init();
            let driver: Box<dyn Driver> = Box::new(FpgaDriver {});
            let filename = "mini_test";
            let dom_init = MemoryResource::None;
            let drv_init = vec![ComputeResource::CPU(1)];

            //TODO VICTOR: replace this with just a prepare engine and a separate prepare function
            let (mut function_context, config, queue) =
                prepare_engine_and_function::<MmapMemoryDomain>(
                    filename, dom_init, &driver, drv_init,
                );
            let input_example: [i16; 5] = [2, 5, 5, 5, 5];
            let bitstream_id: [u16; 1] = [1];
            let bitstream_id_offset = function_context
                .get_free_space_and_write_slice(&bitstream_id)
                .expect("should have space for bitstream id");
            let data_offset = function_context
                .get_free_space_and_write_slice(&input_example)
                .expect("Should have space for a little data");
            println!("got data offset {:?}", data_offset);

            function_context.content.push(Some(DataSet {
                ident: "inputset".to_string(),
                buffers: vec![
                    DataItem {
                        ident: "bitstream_id".to_string(),
                        data: Position {
                            offset: bitstream_id_offset as usize,
                            size: 2,
                        },
                        key: 0,
                    },
                    DataItem {
                        ident: "inputitem".to_string(),
                        data: Position {
                            offset: data_offset as usize,
                            size: 2 * input_example.len(),
                        },
                        key: 1,
                    },
                ],
            }));
            let archive = Box::leak(Box::new(Archive::init(ArchiveInit {
                #[cfg(feature = "timestamp")]
                timestamp_count: 1000,
            })));

            let mut recorder = archive.get_recorder().unwrap();
            let promise = queue.enqueu(crate::function_driver::WorkToDo::FunctionArguments {
                config,
                context: function_context,
                output_sets: Arc::new(Vec::new()),
                recorder: recorder.get_sub_recorder().unwrap(),
            });

            let workdone = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(promise)
                .expect("Engine should run ok with basic function");

            if let WorkDone::Context(context) = workdone {
                if let Some(dataset) = &context.content[0] {
                    let item = &dataset.buffers[0];
                    let offset = item.data.offset;
                    let size = item.data.size;
                    assert!(size % 2 == 0);
                    let mut result: Vec<i16> = vec![0; size / 4];
                    println!("{size:?}");
                    context
                        .read(offset, &mut result)
                        .expect("couldn't read from context");
                    println!("got result: ");
                    for el in result {
                        println!("{el:?}");
                    }
                } else {
                    eprintln!("couldn't get content in test");
                    panic!()
                }
            }

            let shutdown_promise = queue.enqueu(crate::function_driver::WorkToDo::Shutdown());
            let shutdown_workdone = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(shutdown_promise)
                .expect("Engine should run ok with basic function");
            if let WorkDone::Resources(_resources) = shutdown_workdone {
                println!("successfully shut down! :D");
            }

            recorder
                .record(RecordPoint::FutureReturn)
                .expect("Should have properly advanced recorder state");
        }

        #[test]
        fn multipacket_invocation_online() {
            env_logger::init();
            let driver: Box<dyn Driver> = Box::new(FpgaDriver {});
            let filename = "mini_test";
            let dom_init = MemoryResource::None;
            let drv_init = vec![ComputeResource::CPU(1)];

            //TODO VICTOR: replace this with just a prepare engine and a separate prepare function
            let (mut function_context, config, queue) =
                prepare_engine_and_function::<MmapMemoryDomain>(
                    filename, dom_init, &driver, drv_init,
                );
            let input_example: [i16; 900] = std::array::from_fn(|i| i as i16);
            let bitstream_id: [u16; 1] = [1];
            let bitstream_id_offset = function_context
                .get_free_space_and_write_slice(&bitstream_id)
                .expect("should have space for bitstream id");
            let data_offset = function_context
                .get_free_space_and_write_slice(&input_example)
                .expect("Should have space for a little data");
            println!("got data offset {:?}", data_offset);

            function_context.content.push(Some(DataSet {
                ident: "inputset".to_string(),
                buffers: vec![
                    DataItem {
                        ident: "bitstream_id".to_string(),
                        data: Position {
                            offset: bitstream_id_offset as usize,
                            size: 2,
                        },
                        key: 0,
                    },
                    DataItem {
                        ident: "inputitem".to_string(),
                        data: Position {
                            offset: data_offset as usize,
                            size: 2 * input_example.len(),
                        },
                        key: 1,
                    },
                ],
            }));
            let archive = Box::leak(Box::new(Archive::init(ArchiveInit {
                #[cfg(feature = "timestamp")]
                timestamp_count: 1000,
            })));

            let mut recorder = archive.get_recorder().unwrap();
            let promise = queue.enqueu(crate::function_driver::WorkToDo::FunctionArguments {
                config,
                context: function_context,
                output_sets: Arc::new(Vec::new()),
                recorder: recorder.get_sub_recorder().unwrap(),
            });

            let workdone = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(promise)
                .expect("Engine should run ok with basic function");

            if let WorkDone::Context(context) = workdone {
                if let Some(dataset) = &context.content[0] {
                    let item = &dataset.buffers[0];
                    let offset = item.data.offset;
                    let size = item.data.size;
                    assert!(size % 2 == 0);
                    let mut result: Vec<i16> = vec![0; size / 2];
                    println!("{size:?}");
                    context
                        .read(offset, &mut result)
                        .expect("couldn't read from context");
                    println!("got result: ");
                    for el in result {
                        println!("{el:?}");
                    }
                } else {
                    eprintln!("couldn't get content in test");
                    panic!()
                }
            }

            let shutdown_promise = queue.enqueu(crate::function_driver::WorkToDo::Shutdown());
            let shutdown_workdone = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(shutdown_promise)
                .expect("Engine should run ok with basic function");
            if let WorkDone::Resources(_resources) = shutdown_workdone {
                println!("successfully shut down! :D");
            }

            recorder
                .record(RecordPoint::FutureReturn)
                .expect("Should have properly advanced recorder state");
        }

        #[test]
        fn multi_packet_multi_invocation_online() {
            env_logger::init();
            let driver: Box<dyn Driver> = Box::new(FpgaDriver {});
            //TODO VICTOR: replace this with just a prepare engine and a separate prepare function
            let (mut function_contexts, config, queue) =
                fpga_create_function_contexts::<MmapMemoryDomain>(&driver, 8);

            let archive = Box::leak(Box::new(Archive::init(ArchiveInit {
                #[cfg(feature = "timestamp")]
                timestamp_count: 1000,
            })));

            let mut recorder = archive.get_recorder().unwrap();
            let mut promises: Vec<Promise> = Vec::new();
            let context_len = function_contexts.len();
            for i in 0..context_len {
                let promise = queue.enqueu(crate::function_driver::WorkToDo::FunctionArguments {
                    config: config.clone(),
                    context: function_contexts.pop().expect("idk"),
                    output_sets: Arc::new(Vec::new()),
                    recorder: recorder.get_sub_recorder().unwrap(),
                });
                promises.push(promise);
            }

            let mut workdone_vec: Vec<WorkDone> = Vec::new();

            for i in 0..context_len {
                let workdone = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap()
                    .block_on(promises.pop().expect("idk"))
                    .expect("Engine should run ok with basic function");
                workdone_vec.push(workdone);
            }

            for i in 0..context_len {
                if let WorkDone::Context(context) = workdone_vec.pop().expect("idk") {
                    if let Some(dataset) = &context.content[0] {
                        let item = &dataset.buffers[0];
                        let offset = item.data.offset;
                        let size = item.data.size;
                        assert!(size % 2 == 0);
                        let mut result: Vec<i16> = vec![0; size / 2];
                        println!("size: {size:?}");
                        context
                            .read(offset, &mut result)
                            .expect("couldn't read from context");
                        println!("got result: ");
                        for el in result {
                            print!("{el:?}, ");
                        }
                    } else {
                        eprintln!("couldn't get content in test");
                        panic!()
                    }
                }
            }

            let shutdown_promise = queue.enqueu(crate::function_driver::WorkToDo::Shutdown());
            let shutdown_workdone = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(shutdown_promise)
                .expect("Engine should run ok with basic function");
            if let WorkDone::Resources(_resources) = shutdown_workdone {
                println!("successfully shut down! :D");
            }

            recorder
                .record(RecordPoint::FutureReturn)
                .expect("Should have properly advanced recorder state");
        }
    }
}

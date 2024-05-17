#[cfg(all(
    test,
    any(feature = "cheri", feature = "mmu", feature = "wasm", feature = "gpu")
))]
#[allow(clippy::module_inception)]
mod compute_driver_tests {
    use crate::{
        function_driver::{
            test_queue::TestQueue, ComputeResource, Driver, FunctionArguments, FunctionConfig,
            WorkToDo,
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
        let promise = queue.enqueu(WorkToDo::FunctionArguments(FunctionArguments {
            config: config,
            context: function_context,
            output_sets: Arc::new(Vec::new()),
            recorder,
        }));
        queue.enqueu(WorkToDo::Shutdown());
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
        let promise = queue.enqueu(WorkToDo::FunctionArguments(FunctionArguments {
            config,
            context: function_context,
            output_sets: Arc::new(vec![String::from("")]),
            recorder: recorder.get_sub_recorder().unwrap(),
        }));
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
            let promise = queue.enqueu(WorkToDo::FunctionArguments(FunctionArguments {
                config,
                context: function_context,
                output_sets: Arc::new(vec![String::from("")]),
                recorder: recorder.get_sub_recorder().unwrap(),
            }));
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
        let promise = queue.enqueu(WorkToDo::FunctionArguments(FunctionArguments {
            config,
            context: function_context,
            output_sets: Arc::new(vec![String::from("stdio")]),
            recorder: recorder.get_sub_recorder().unwrap(),
        }));
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
        let promise = queue.enqueu(WorkToDo::FunctionArguments(FunctionArguments {
            config: config,
            context: function_context,
            output_sets: Arc::new(vec![
                "stdio".to_string(),
                "out".to_string(),
                "out_nested".to_string(),
            ]),
            recorder: recorder.get_sub_recorder().unwrap(),
        }));
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
            ComputeResource::GPU(0, 0)
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
            ComputeResource::GPU(0, 0),
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

    #[cfg(feature = "gpu")]
    mod gpu {

        use std::{
            cmp::max,
            os::raw::c_void,
            ptr::null,
            sync::{Arc, Mutex},
        };

        use dandelion_commons::records::{Archive, ArchiveInit, RecordPoint, Recorder};
        use futures::SinkExt;

        use crate::{
            function_driver::{
                compute_driver::{
                    compute_driver_tests::compute_driver_tests::{
                        get_expected_mat, prepare_engine_and_function,
                    },
                    gpu::{
                        dummy_run,
                        hip::{self, DEFAULT_STREAM},
                        GpuLoop, GpuProcessDriver, GpuThreadDriver,
                    },
                },
                load_utils::load_u8_from_file,
                thread_utils::EngineLoop,
                ComputeResource, Driver, FunctionArguments, WorkToDo,
            },
            memory_domain::{mmu::MmuMemoryDomain, ContextTrait, MemoryResource},
            DataItem, DataSet, Position,
        };

        use super::engine_minimal;

        // To force tests to run sequentially as we might otherwise run out of GPU memory
        lazy_static::lazy_static! {
            static ref GPU_LOCK: Mutex<()> = Mutex::new(());
        }

        fn get_driver() -> Box<dyn Driver> {
            #[cfg(all(feature = "gpu_process", feature = "gpu_thread"))]
            panic!("gpu_process and gpu_thread enabled simultaneously");

            #[cfg(not(any(feature = "gpu_process", feature = "gpu_thread")))]
            panic!("Neither gpu_process nor gpu_thread enabled");

            #[cfg(feature = "gpu_process")]
            return Box::new(GpuProcessDriver {});

            #[cfg(feature = "gpu_thread")]
            return Box::new(GpuThreadDriver {});
        }

        #[ignore = "pollutes stdout"]
        #[test]
        fn module_load_data_test() {
            let _lock = GPU_LOCK.lock().unwrap();
            let file = load_u8_from_file(
                "/home/smithj/dandelion/machine_interface/hip_interface/module.hsaco".into(),
            )
            .unwrap();

            let image = file.as_ptr() as *const c_void;

            let module = hip::module_load_data(image).unwrap();

            let func = hip::module_get_function(&module, "hello_world").unwrap();

            let args: [*const c_void; 0] = [];

            assert!(hip::module_launch_kernel(
                &func,
                1,
                1,
                1,
                1,
                1,
                1,
                0,
                DEFAULT_STREAM,
                args.as_ptr(),
                null()
            )
            .is_ok())
        }

        #[ignore = "pollutes stdout"]
        #[test]
        fn run_dummy_gpu_payload() {
            let _lock = GPU_LOCK.lock().unwrap();
            let mut runner = GpuLoop::init(ComputeResource::GPU(7, 0)).unwrap();
            assert!(dummy_run(&mut runner).is_ok());
        }

        #[test]
        fn minimal() {
            let _lock = GPU_LOCK.lock().unwrap();
            let driver: Box<dyn Driver> = get_driver();
            engine_minimal::<MmuMemoryDomain>(
                "/home/smithj/dandelion/machine_interface/hip_interface/minimal.json",
                MemoryResource::None,
                driver,
                vec![ComputeResource::GPU(7, 0)],
            );
        }

        #[test]
        fn basic_input_output() {
            let _lock = GPU_LOCK.lock().unwrap();
            let driver: Box<dyn Driver> = get_driver();
            let (mut function_context, config, queue) =
                prepare_engine_and_function::<MmuMemoryDomain>(
                    "/home/smithj/dandelion/machine_interface/hip_interface/basic_io.json",
                    MemoryResource::None,
                    &driver,
                    vec![ComputeResource::GPU(7, 0)],
                );
            // add inputs
            let in_size_offset = function_context
                .get_free_space_and_write_slice(&[12345i64])
                .expect("Should have space for single i64");
            function_context.content.push(Some(DataSet {
                ident: "A".to_string(),
                buffers: vec![DataItem {
                    ident: "".to_string(),
                    data: Position {
                        offset: in_size_offset as usize,
                        size: 8,
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
            let promise = queue.enqueu(WorkToDo::FunctionArguments(FunctionArguments {
                config,
                context: function_context,
                output_sets: Arc::new(vec!["A".into()]),
                recorder: recorder.get_sub_recorder().unwrap(),
            }));
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
            assert_eq!(1, result_context.content.len());
            let output_item = result_context.content[0]
                .as_ref()
                .expect("Set should be present");
            eprintln!("{:?}", output_item);
            assert_eq!(1, output_item.buffers.len());
            let position = output_item.buffers[0].data;
            assert_eq!(8, position.size, "Checking for size of output");
            let mut read_buffer = vec![0i64; position.size / 8];
            result_context
                .context
                .read(position.offset, &mut read_buffer)
                .expect("Should succeed in reading");
            assert_eq!(98765, read_buffer[0]);
        }

        #[test]
        fn engine_matmul_3x3_loop() {
            let _lock = GPU_LOCK.lock().unwrap();
            let filename =
                "/home/smithj/dandelion/machine_interface/hip_interface/matmul_loop.json";
            let dom_init = MemoryResource::None;
            let driver: Box<dyn Driver> = get_driver();
            let drv_init = vec![ComputeResource::GPU(7, 0)];
            let (mut function_context, config, queue) =
                prepare_engine_and_function::<MmuMemoryDomain>(
                    filename, dom_init, &driver, drv_init,
                );
            // add inputs, split over two buffers to test concatenating them in GPU memory
            let in_size_offset = function_context
                .get_free_space_and_write_slice(&[3i64])
                .expect("Should have space");
            let offset2 = function_context
                .get_free_space_and_write_slice(&[
                    0i64, 1i64, 2i64, 3i64, 4i64, 5i64, 6i64, 7i64, 8i64,
                ])
                .expect("Should have space");
            function_context.content.push(Some(DataSet {
                ident: "A".to_string(),
                buffers: vec![
                    DataItem {
                        ident: "".to_string(),
                        data: Position {
                            offset: in_size_offset as usize,
                            size: 8,
                        },
                        key: 0,
                    },
                    DataItem {
                        ident: "".to_string(),
                        data: Position {
                            offset: offset2 as usize,
                            size: 72,
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
            let promise = queue.enqueu(WorkToDo::FunctionArguments(FunctionArguments {
                config,
                context: function_context,
                output_sets: Arc::new(vec![String::from("B")]),
                recorder: recorder.get_sub_recorder().unwrap(),
            }));
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
            assert_eq!(1, result_context.content.len());
            let output_item = result_context.content[0]
                .as_ref()
                .expect("Set should be present");
            assert_eq!(1, output_item.buffers.len());
            let position = output_item.buffers[0].data;
            assert_eq!(80, position.size, "Checking for size of output");
            let mut read_buffer = vec![0i64; position.size / 8];
            result_context
                .context
                .read(position.offset, &mut read_buffer)
                .expect("Should succeed in reading");
            assert_eq!(3, read_buffer[0]);
            let expected = self::get_expected_mat(3);
            assert_eq!(3i64, read_buffer[0]);
            for (should, is) in expected.iter().zip(read_buffer[1..].iter()) {
                assert_eq!(should, is);
            }
        }

        #[test]
        fn engine_matmul_size_sweep_parallel() {
            let _lock = GPU_LOCK.lock().unwrap();
            let filename =
                "/home/smithj/dandelion/machine_interface/hip_interface/matmul_para.json";
            let dom_init = MemoryResource::None;
            let driver: Box<dyn Driver> = get_driver();
            let drv_init = vec![ComputeResource::GPU(7, 0)];
            const LOWER_SIZE_BOUND: usize = 2;
            const UPPER_SIZE_BOUND: usize = 16;
            for mat_size in LOWER_SIZE_BOUND..UPPER_SIZE_BOUND {
                let (mut function_context, config, queue) =
                    prepare_engine_and_function::<MmuMemoryDomain>(
                        filename,
                        dom_init,
                        &driver,
                        drv_init.clone(),
                    );
                // add inputs, split over two buffers to test concatenating them in GPU memory
                let in_size_offset = function_context
                    .get_free_space_and_write_slice(&[mat_size as i64])
                    .expect("Should have space");

                let mut mat_vec = Vec::<i64>::new();
                for i in 0..(mat_size * mat_size) {
                    mat_vec.push(i as i64);
                }
                let in_mat_offset = function_context
                    .get_free_space_and_write_slice(&mat_vec)
                    .expect("Should have space") as usize;
                function_context.content.push(Some(DataSet {
                    ident: "A".to_string(),
                    buffers: vec![
                        DataItem {
                            ident: "".to_string(),
                            data: Position {
                                offset: in_size_offset as usize,
                                size: 8,
                            },
                            key: 0,
                        },
                        DataItem {
                            ident: "".to_string(),
                            data: Position {
                                offset: in_mat_offset as usize,
                                size: mat_vec.len() * 8,
                            },
                            key: 0,
                        },
                    ],
                }));
                let cfg_offset = function_context
                    .get_free_space_and_write_slice(&[(mat_size + 31) / 32])
                    .expect("Should be able to write cfg");
                function_context.content.push(Some(DataSet {
                    ident: "cfg".to_string(),
                    buffers: vec![DataItem {
                        ident: "".to_string(),
                        data: Position {
                            offset: cfg_offset as usize,
                            size: 8,
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
                let promise = queue.enqueu(WorkToDo::FunctionArguments(FunctionArguments {
                    config,
                    context: function_context,
                    output_sets: Arc::new(vec![String::from("B")]),
                    recorder: recorder.get_sub_recorder().unwrap(),
                }));
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

        fn get_inference_inputs() -> (Vec<f32>, Vec<f32>) {
            let mut a: Vec<f32> = Vec::with_capacity(224 * 224 + 2);
            let mut b: Vec<f32> = Vec::with_capacity(5 * 5 + 2);

            a.push(224.0);
            a.push(224.0);
            for i in 0..224 * 224 {
                a.push(i as f32);
            }

            b.push(5.0);
            b.push(5.0);
            for i in 0..5 * 5 {
                b.push(i as f32);
            }

            (a, b)
        }

        fn get_expected_inference_output() -> Vec<f32> {
            let (a, b) = get_inference_inputs();
            let mut c = vec![0f32; a.len()];
            let mut d = vec![0f32; c.len() / 2 + 1];

            // convolution
            c[0] = 224f32;
            c[1] = 224f32;
            for i in 0..224 {
                for j in 0..224 {
                    let mut sum = 0f32;
                    for k in -2..=2i32 {
                        for l in -2..=2i32 {
                            let cur_row = i - k;
                            let cur_col = j - l;

                            let filter_row = 2 + k;
                            let filter_col = 2 + l;

                            if (0..224).contains(&cur_row) && (0..224).contains(&cur_col) {
                                let cur_row = cur_row as usize; // There has to be a better way to do this...
                                let cur_col = cur_col as usize;
                                let filter_row = filter_row as usize;
                                let filter_col = filter_col as usize;
                                sum += a[2 + cur_row * 224 + cur_col]
                                    * b[2 + filter_row * 5 + filter_col];
                            }
                        }
                    }
                    let i = i as usize;
                    let j = j as usize;
                    c[2 + i * 224 + j] = sum;
                }
            }

            // relu
            for i in 0..224 * 224 {
                c[2 + i] = c[2 + i].max(0f32);
            }

            // max pooling
            d[0] = 112f32;
            d[1] = 112f32;
            for i in 0..112 {
                for j in 0..112 {
                    let mut max = f32::NEG_INFINITY;
                    let base_row = i * 2;
                    let base_col = j * 2;
                    for k in 0..2 {
                        for l in 0..2 {
                            max = max.max(c[2 + (base_row + k) * 224 + (base_col + l)]);
                        }
                    }
                    d[2 + i * 112 + j] = max;
                }
            }

            d
        }

        #[test]
        fn inference_benchmark_function() {
            let _lock = GPU_LOCK.lock().unwrap();
            let filename = "/home/smithj/dandelion/machine_interface/hip_interface/inference.json";
            let dom_init = MemoryResource::None;
            let driver: Box<dyn Driver> = get_driver();
            let drv_init = vec![ComputeResource::GPU(7, 0)];
            let (mut function_context, config, queue) =
                prepare_engine_and_function::<MmuMemoryDomain>(
                    filename, dom_init, &driver, drv_init,
                );
            let d_size: usize = 112 * 112 * 4 + 8; //side_len * side_len * sizeof(float) + [[size convention at start]]
            let a_grid_dim: usize = (224 + 31) / 32;
            let d_grid_dim: usize = (112 + 31) / 32;
            let (a_matrix, b_matrix) = get_inference_inputs();
            let cfg_offset = function_context
                .get_free_space_and_write_slice(&[d_size, a_grid_dim, d_grid_dim])
                .expect("Should have space for cfg");
            let a_offset = function_context
                .get_free_space_and_write_slice(&a_matrix)
                .expect("Should have space for A");
            let b_offset = function_context
                .get_free_space_and_write_slice(&b_matrix)
                .expect("Should have space for B");

            function_context.content.append(&mut vec![
                Some(DataSet {
                    ident: "cfg".to_string(),
                    buffers: vec![DataItem {
                        ident: "".to_string(),
                        data: Position {
                            offset: cfg_offset as usize,
                            size: 3 * 8,
                        },
                        key: 0,
                    }],
                }),
                Some(DataSet {
                    ident: "A".to_string(),
                    buffers: vec![DataItem {
                        ident: "".to_string(),
                        data: Position {
                            offset: a_offset as usize,
                            size: a_matrix.len() * 4,
                        },
                        key: 0,
                    }],
                }),
                Some(DataSet {
                    ident: "B".to_string(),
                    buffers: vec![DataItem {
                        ident: "".to_string(),
                        data: Position {
                            offset: b_offset as usize,
                            size: b_matrix.len() * 4,
                        },
                        key: 0,
                    }],
                }),
            ]);
            let archive = Box::leak(Box::new(Archive::init(ArchiveInit {
                #[cfg(feature = "timestamp")]
                timestamp_count: 1000,
            })));

            let mut recorder = archive.get_recorder().unwrap();
            recorder
                .record(RecordPoint::TransferEnd)
                .expect("Should have properly initialized recorder state");
            let promise = queue.enqueu(WorkToDo::FunctionArguments(FunctionArguments {
                config,
                context: function_context,
                output_sets: Arc::new(vec![String::from("D")]),
                recorder: recorder.get_sub_recorder().unwrap(),
            }));
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
            assert_eq!(1, result_context.content.len());
            let output_item = result_context.content[0]
                .as_ref()
                .expect("Set should be present");
            assert_eq!(1, output_item.buffers.len());
            let position = output_item.buffers[0].data;
            assert_eq!(
                112 * 112 * 4 + 8,
                position.size,
                "Checking for size of output"
            );
            let mut read_buffer = vec![0f32; position.size / 4];
            result_context
                .context
                .read(position.offset, &mut read_buffer)
                .expect("Should succeed in reading");
            let expected = get_expected_inference_output();
            for (idx, (should, is)) in expected.iter().zip(read_buffer[..].iter()).enumerate() {
                assert_eq!(should, is, "Comparing args at {}", idx);
            }
        }
    }
}

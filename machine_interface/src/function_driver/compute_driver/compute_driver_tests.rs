#[cfg(all(test, any(feature = "cheri", feature = "mmu", feature = "kvm")))]
mod compute_driver_tests {
    use crate::{
        composition::CompositionSet,
        function_driver::{
            functions::FunctionAlternative, test_queue::TestQueue, ComputeResource, Metadata,
            WorkToDo,
        },
        machine_config::EngineType,
        memory_domain::{
            read_only::ReadOnlyContext, test_resource::get_resource, ContextTrait, MemoryDomain,
            MemoryResource,
        },
        DataItem, DataSet, Position,
    };
    use core::panic;
    use dandelion_commons::{records::Recorder, DandelionError, FunctionId};
    use std::{sync::Arc, time::Instant};

    const DEFAULT_CONTEXT_SIZE: usize = 0x800_0000; // 128MiB

    #[inline]
    fn zero_id() -> FunctionId {
        Arc::new(0.to_string())
    }

    fn loader_empty<Dom: MemoryDomain>(dom_init: MemoryResource, engine_type: EngineType) {
        // load elf file
        let elf_path = String::new();
        let domain = Dom::init(get_resource(dom_init)).expect("Should be able to get domain");
        engine_type
            .parse_function(elf_path, &domain)
            .expect("Empty string should return error");
    }

    fn driver(
        engine_type: EngineType,
        init: Vec<ComputeResource>,
        wrong_init: Vec<ComputeResource>,
    ) {
        for wronge_resource in wrong_init {
            let wrong_resource_engine = engine_type.start_engine(wronge_resource, TestQueue::new());
            match wrong_resource_engine {
                Ok(_) => panic!("Should not be able to get engine"),
                Err(err) => assert_eq!(DandelionError::EngineResourceError, err),
            }
        }

        for resource in init {
            let engine = engine_type.start_engine(resource, TestQueue::new());
            engine.expect("Should be able to get engine");
        }
    }

    fn prepare_engine_and_domain<Dom: MemoryDomain>(
        dom_init: MemoryResource,
        engine_type: EngineType,
        drv_init: Vec<ComputeResource>,
    ) -> (Arc<Box<dyn MemoryDomain>>, TestQueue) {
        let queue = TestQueue::new();
        let domain =
            Arc::new(Dom::init(get_resource(dom_init)).expect("Should have initialized domain"));
        engine_type
            .start_engine(drv_init[0], queue.clone())
            .expect("Should be able to start engine");
        return (domain, queue);
    }

    fn engine_minimal<Dom: MemoryDomain>(
        filename: &str,
        dom_init: MemoryResource,
        engine_type: EngineType,
        drv_init: Vec<ComputeResource>,
    ) {
        let (domain, queue) = prepare_engine_and_domain::<Dom>(dom_init, engine_type, drv_init);

        let metadata = Arc::new(Metadata {
            input_sets: vec![],
            output_sets: vec![],
        });

        let recorder = Recorder::new(Arc::new(0.to_string()), Instant::now());
        let function_alternatives = vec![Arc::new(FunctionAlternative::new_unloaded(
            engine_type,
            DEFAULT_CONTEXT_SIZE,
            filename.to_string(),
            domain,
        ))];
        let promise = queue.enqueu(WorkToDo::FunctionArguments {
            function_id: Arc::new(String::new()),
            function_alternatives,
            input_sets: vec![],
            metadata,
            caching: false,
            recorder,
        });
        let _ = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(promise)
            .expect("Engine should run ok with basic function");
    }

    fn engine_caching<Dom: MemoryDomain>(
        filename: &str,
        dom_init: MemoryResource,
        engine_type: EngineType,
        drv_init: Vec<ComputeResource>,
    ) {
        let (domain, queue) = prepare_engine_and_domain::<Dom>(dom_init, engine_type, drv_init);

        let function = Arc::new(
            engine_type
                .parse_function(filename.to_string(), &domain)
                .expect("Should be able to parse function"),
        );

        let metadata = Arc::new(Metadata {
            input_sets: vec![],
            output_sets: vec![],
        });

        let recorder = Recorder::new(Arc::new(0.to_string()), Instant::now());
        let function_alternatives = vec![Arc::new(FunctionAlternative::new_loaded(
            engine_type,
            DEFAULT_CONTEXT_SIZE,
            String::new(), // do not use actual path, to ensure it fails if cached function is not used
            domain,
            function,
        ))];
        let promise = queue.enqueu(WorkToDo::FunctionArguments {
            function_id: Arc::new(String::new()),
            function_alternatives,
            input_sets: vec![],
            metadata,
            caching: true,
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
        engine_type: EngineType,
        drv_init: Vec<ComputeResource>,
    ) {
        let (domain, queue) = prepare_engine_and_domain::<Dom>(dom_init, engine_type, drv_init);
        // add inputs
        let in_data = vec![1i64, 2i64];
        let mut input_context = ReadOnlyContext::new(in_data.into_boxed_slice()).unwrap();
        input_context.content.push(Some(DataSet {
            ident: "".to_string(),
            buffers: vec![DataItem {
                ident: "".to_string(),
                data: Position {
                    offset: 0,
                    size: 16,
                },
                key: 0,
            }],
        }));
        let context_arc = Arc::new(input_context);
        let input_sets = vec![Some(CompositionSet::from((0, vec![context_arc])))];

        let metadata = Arc::new(Metadata {
            input_sets: vec![("".to_string(), None)],
            output_sets: vec!["".to_string()],
        });

        let recorder = Recorder::new(zero_id(), Instant::now());
        let function_alternatives = vec![Arc::new(FunctionAlternative::new_unloaded(
            engine_type,
            DEFAULT_CONTEXT_SIZE,
            filename.to_string(),
            domain,
        ))];
        let promise = queue.enqueu(WorkToDo::FunctionArguments {
            function_id: Arc::new(String::new()),
            function_alternatives,
            input_sets,
            metadata,
            caching: false,
            recorder,
        });
        let result_context = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(promise)
            .expect("Engine should run ok with basic function")
            .get_context();
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
        engine_type: EngineType,
        drv_init: Vec<ComputeResource>,
    ) {
        const LOWER_SIZE_BOUND: usize = 2;
        const UPPER_SIZE_BOUND: usize = 16;
        for mat_size in LOWER_SIZE_BOUND..UPPER_SIZE_BOUND {
            let (domain, queue) =
                prepare_engine_and_domain::<Dom>(dom_init.clone(), engine_type, drv_init.clone());
            // add inputs
            let mut mat_vec = Vec::<i64>::new();
            mat_vec.push(mat_size as i64);
            for i in 0..(mat_size * mat_size) {
                mat_vec.push(i as i64);
            }
            let item_size = mat_vec.len() * core::mem::size_of::<i64>();
            let mut input_context = ReadOnlyContext::new(mat_vec.into_boxed_slice()).unwrap();
            input_context.content = vec![Some(DataSet {
                ident: "".to_string(),
                buffers: vec![DataItem {
                    ident: "".to_string(),
                    data: Position {
                        offset: 0,
                        size: item_size,
                    },
                    key: 0,
                }],
            })];
            let context_arc = Arc::new(input_context);
            let input_sets = vec![Some(CompositionSet::from((0, vec![context_arc])))];

            let metadata = Arc::new(Metadata {
                input_sets: vec![("".to_string(), None)],
                output_sets: vec!["".to_string()],
            });

            let recorder = Recorder::new(zero_id(), Instant::now());
            let function_alternatives = vec![Arc::new(FunctionAlternative::new_unloaded(
                engine_type,
                DEFAULT_CONTEXT_SIZE,
                filename.to_string(),
                domain,
            ))];
            let promise = queue.enqueu(WorkToDo::FunctionArguments {
                function_id: Arc::new(String::new()),
                function_alternatives,
                input_sets,
                metadata,
                caching: false,
                recorder,
            });
            let result_context = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(promise)
                .expect("Engine should run ok with basic function")
                .get_context();
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
        engine_type: EngineType,
        drv_init: Vec<ComputeResource>,
    ) {
        let (domain, queue) = prepare_engine_and_domain::<Dom>(dom_init, engine_type, drv_init);

        let mut in_data = String::new();
        let mut content = Vec::new();
        let stdin_content = "Test line \n line 2\n";
        let stdin_offset = in_data.len();
        in_data.push_str(stdin_content);
        let argv_content = "stdio flag0 flag1";
        let argv_offset = in_data.len();
        in_data.push_str(argv_content);
        let env_content = "HOME=test_home\0";
        let env_offset = in_data.len();
        in_data.push_str(env_content);

        content.push(Some(DataSet {
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

        let mut input_context =
            ReadOnlyContext::new(unsafe { in_data.as_mut_vec() }.clone().into_boxed_slice())
                .unwrap();
        input_context.content = content;
        let context_arc = Arc::new(input_context);
        let input_sets = vec![Some(CompositionSet::from((0, vec![context_arc])))];

        let metadata = Arc::new(Metadata {
            input_sets: vec![("stdio".to_string(), None)],
            output_sets: vec!["stdio".to_string()],
        });

        let recorder = Recorder::new(zero_id(), Instant::now());
        let function_alternatives = vec![Arc::new(FunctionAlternative::new_unloaded(
            engine_type,
            DEFAULT_CONTEXT_SIZE,
            filename.to_string(),
            domain,
        ))];
        let promise = queue.enqueu(WorkToDo::FunctionArguments {
            function_id: Arc::new(String::new()),
            function_alternatives,
            input_sets,
            metadata,
            caching: false,
            recorder,
        });
        let result_context = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(promise)
            .expect("Engine should run ok with basic function")
            .get_context();

        // check the function exited with exit code 0
        use crate::memory_domain::ContextState;
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
        engine_type: EngineType,
        drv_init: Vec<ComputeResource>,
    ) {
        let (domain, queue) = prepare_engine_and_domain::<Dom>(dom_init, engine_type, drv_init);
        let mut in_data = String::new();
        let mut content = Vec::new();
        let in_file_content = "Test file 0\n line 2\n";
        content.push(Some(DataSet {
            ident: "in".to_string(),
            buffers: vec![DataItem {
                ident: "in_file".to_string(),
                data: Position {
                    offset: in_data.len(),
                    size: in_file_content.len(),
                },
                key: 0,
            }],
        }));
        in_data.push_str(in_file_content);

        let in_file1_content = "Test file 1 \n line 2\n";
        let in_file1_offset = in_data.len();
        in_data.push_str(in_file1_content);
        let in_file2_content = "Test file 2 \n line 2\n";
        let in_file2_offset = in_data.len();
        in_data.push_str(in_file2_content);
        let in_file3_content = "Test file 3 \n line 2\n";
        let in_file3_offset = in_data.len();
        in_data.push_str(in_file3_content);
        let in_file4_content = "Test file 4 \n line 2\n";
        let in_file4_offset = in_data.len();
        in_data.push_str(in_file4_content);

        content.push(Some(DataSet {
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

        let mut input_context =
            ReadOnlyContext::new(unsafe { in_data.as_mut_vec() }.clone().into_boxed_slice())
                .unwrap();
        input_context.content = content;
        let context_arc = Arc::new(input_context);
        let input_sets = vec![
            Some(CompositionSet::from((0, vec![context_arc.clone()]))),
            Some(CompositionSet::from((1, vec![context_arc]))),
        ];

        let metadata = Arc::new(Metadata {
            input_sets: vec![("in".to_string(), None), ("in_nested".to_string(), None)],
            output_sets: vec![
                "stdio".to_string(),
                "out".to_string(),
                "out_nested".to_string(),
            ],
        });

        let recorder = Recorder::new(zero_id(), Instant::now());
        let function_alternatives = vec![Arc::new(FunctionAlternative::new_unloaded(
            engine_type,
            DEFAULT_CONTEXT_SIZE,
            filename.to_string(),
            domain,
        ))];
        let promise = queue.enqueu(WorkToDo::FunctionArguments {
            function_id: Arc::new(String::new()),
            function_alternatives,
            input_sets,
            metadata,
            caching: false,
            recorder,
        });
        let result_context = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(promise)
            .expect("Engine should run ok with basic function")
            .get_context();
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
        ($name : ident; $domain : ty; $dom_init: expr; $engine_type : expr ; $drv_init : expr; $drv_init_wrong : expr) => {
            #[test_log::test]
            #[should_panic]
            fn test_loader_empty() {
                super::loader_empty::<$domain>($dom_init, $engine_type);
            }

            #[test_log::test]
            fn test_driver() {
                super::driver($engine_type, $drv_init, $drv_init_wrong);
            }

            #[test_log::test]
            fn test_engine_minimal() {
                let name = format!(
                    "{}/tests/data/test_{}_basic",
                    env!("CARGO_MANIFEST_DIR"),
                    stringify!($name)
                );
                super::engine_minimal::<$domain>(&name, $dom_init, $engine_type, $drv_init);
            }

            #[test_log::test]
            fn test_engine_caching() {
                let name = format!(
                    "{}/tests/data/test_{}_basic",
                    env!("CARGO_MANIFEST_DIR"),
                    stringify!($name)
                );
                super::engine_caching::<$domain>(&name, $dom_init, $engine_type, $drv_init);
            }

            #[test_log::test]
            fn test_engine_matmul_single() {
                let name = format!(
                    "{}/tests/data/test_{}_matmul",
                    env!("CARGO_MANIFEST_DIR"),
                    stringify!($name)
                );
                super::engine_matmul_single::<$domain>(&name, $dom_init, $engine_type, $drv_init);
            }

            #[test_log::test]
            fn test_engine_matmul_size_sweep() {
                let name = format!(
                    "{}/tests/data/test_{}_matmul",
                    env!("CARGO_MANIFEST_DIR"),
                    stringify!($name)
                );
                super::engine_matmul_size_sweep::<$domain>(
                    &name,
                    $dom_init,
                    $engine_type,
                    $drv_init,
                );
            }

            #[test_log::test]
            fn test_engine_stdio() {
                let name = format!(
                    "{}/tests/data/test_{}_stdio",
                    env!("CARGO_MANIFEST_DIR"),
                    stringify!($name)
                );
                super::engine_stdio::<$domain>(&name, $dom_init, $engine_type, $drv_init);
            }

            #[test_log::test]
            fn test_engine_fileio() {
                let name = format!(
                    "{}/tests/data/test_{}_fileio",
                    env!("CARGO_MANIFEST_DIR"),
                    stringify!($name)
                );
                super::engine_fileio::<$domain>(&name, $dom_init, $engine_type, $drv_init)
            }
        };
    }

    #[cfg(feature = "cheri")]
    mod cheri {
        use crate::{
            function_driver::ComputeResource,
            machine_config::EngineType,
            memory_domain::{cheri::CheriMemoryDomain, MemoryResource},
        };
        driverTests!(elf_cheri; CheriMemoryDomain; MemoryResource::Anonymous { size: (1<<30) }; EngineType::Cheri;
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
        use crate::{
            function_driver::ComputeResource,
            machine_config::EngineType,
            memory_domain::{mmu::MmuMemoryDomain, MemoryResource},
        };
        #[cfg(target_arch = "x86_64")]
        driverTests!(elf_mmu_x86_64; MmuMemoryDomain; MemoryResource::Shared { id: 0, size: (1<<30) }; EngineType::Process;
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
        driverTests!(elf_mmu_aarch64; MmuMemoryDomain; MemoryResource::Shared { id: 0, size: (1<<30) }; EngineType::Process;
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

    #[cfg(feature = "kvm")]
    mod kvm {
        use crate::{
            function_driver::ComputeResource,
            machine_config::EngineType,
            memory_domain::{kvm::KvmMemoryDomain, MemoryResource},
        };
        #[cfg(target_arch = "x86_64")]
        driverTests!(elf_kvm_x86_64; KvmMemoryDomain; MemoryResource::Anonymous { size: (1<<30) }; EngineType::Kvm;
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
        driverTests!(elf_kvm_aarch64; KvmMemoryDomain; MemoryResource::Anonymous { size: (1<<30) }; EngineType::Kvm;
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
}

#[cfg(all(test, any(feature = "cheri", feature = "mmu", feature = "wasm")))]
mod dispatcher_tests {
    use dandelion_commons::{ContextTypeId, EngineTypeId};
    use dispatcher::{
        composition::{Composition, FunctionDependencies},
        dispatcher::{CompositionSet, Dispatcher, ShardingMode},
        function_registry::FunctionRegistry,
        resource_pool::ResourcePool,
    };
    use futures::lock::Mutex;
    use machine_interface::{
        function_driver::{ComputeResource, Driver},
        memory_domain::{Context, ContextTrait, MemoryDomain},
        DataItem, DataSet, Position,
    };
    use std::{
        collections::{BTreeMap, BTreeSet},
        mem::size_of,
        sync::Arc,
    };

    fn setup_dispatcher<Dom: MemoryDomain>(
        domain_arg: Vec<u8>,
        name: &str,
        in_set_names: Vec<String>,
        out_set_names: Vec<String>,
        driver: Box<dyn Driver>,
        engine_resource: Vec<ComputeResource>,
    ) -> Dispatcher {
        let mut domains = BTreeMap::new();
        let context_id: ContextTypeId = 0;
        domains.insert(
            context_id,
            Dom::init(domain_arg).expect("Should be able to initialize domain"),
        );
        let engine_id: EngineTypeId = 0;

        let mut drivers = BTreeMap::<EngineTypeId, Box<dyn Driver>>::new();
        drivers.insert(engine_id, driver);
        let mut type_map = BTreeMap::new();
        type_map.insert(engine_id, context_id);
        let mut registry = FunctionRegistry::new(drivers);
        let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.pop();
        path.push("machine_interface/tests/data");
        path.push(name);
        let path_string = path.to_str().expect("Path should be string");
        registry.add_local(0, engine_id, path_string, in_set_names, out_set_names);
        let mut pool_map = BTreeMap::new();
        pool_map.insert(engine_id, engine_resource);
        let resource_pool = ResourcePool {
            engine_pool: Mutex::new(pool_map),
        };
        return Dispatcher::init(domains, type_map, registry, resource_pool)
            .expect("Should have initialized dispatcher");
    }

    fn single_domain_and_engine_basic<Domain: MemoryDomain>(
        domain_arg: Vec<u8>,
        relative_path: &str,
        driver: Box<dyn Driver>,
        engine_resource: Vec<ComputeResource>,
    ) {
        let dispatcher = setup_dispatcher::<Domain>(
            domain_arg,
            relative_path,
            vec![],
            vec![],
            driver,
            engine_resource,
        );
        let result = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(dispatcher.queue_function(0, Vec::new(), Vec::new(), false));
        match result {
            Ok(_) => (),
            Err(err) => panic!("Failed with: {:?}", err),
        }
    }

    fn add_matmul_matrix(
        context: &mut Context,
        set: usize,
        key: u32,
        matrix_dim: u64,
        mut matrix: Vec<u64>,
    ) {
        // check the sets are not already full and ensure they exist
        if context.content.len() <= set {
            context.content.resize_with(set + 1, || None);
        }

        assert_eq!(matrix_dim * matrix_dim, matrix.len() as u64);

        matrix.insert(0, matrix_dim);
        let mat_offset = context
            .get_free_space_and_write_slice(&matrix)
            .expect("Should have space") as usize;
        if let Some(set) = &mut context.content[set] {
            set.buffers.push(DataItem {
                ident: String::from(""),
                data: Position {
                    offset: mat_offset,
                    size: matrix.len() * size_of::<i64>(),
                },
                key: key,
            });
        } else {
            context.content[set] = Some(DataSet {
                ident: "".to_string(),
                buffers: vec![DataItem {
                    ident: "".to_string(),
                    data: Position {
                        offset: mat_offset,
                        size: matrix.len() * size_of::<i64>(),
                    },
                    key: key,
                }],
            });
        }
    }

    fn add_matmac_matrix(
        context: &mut Context,
        set: usize,
        rows: u64,
        cols: u64,
        mut matrix: Vec<u64>,
    ) {
        // check the sets are not already full and ensure they exist
        if context.content.len() <= set {
            context.content.resize_with(set + 1, || None);
        }
        if context.content[set].is_some() || context.content[set].is_some() {
            panic!("trying to add matrix where there is already set");
        }

        assert_eq!(rows * cols, matrix.len() as u64);

        matrix.insert(0, rows);

        let in_mat_offset = context
            .get_free_space_and_write_slice(&matrix)
            .expect("Should have space") as usize;
        context.content[set] = Some(DataSet {
            ident: "".to_string(),
            buffers: vec![DataItem {
                ident: "".to_string(),
                data: Position {
                    offset: in_mat_offset,
                    size: matrix.len() * 8,
                },
                key: 0,
            }],
        });
    }

    fn check_matrix(context: &Context, set_id: usize, key: u32, rows: u64, expected: Vec<u64>) {
        assert!(context.content.len() >= set_id);
        let out_mat_set = context.content[set_id].as_ref().expect("Should have set");
        assert_eq!(
            1,
            out_mat_set
                .buffers
                .iter()
                .filter(|buffer| buffer.key == key)
                .count()
        );
        let out_mat_position = out_mat_set
            .buffers
            .iter()
            .find(|buffer| buffer.key == key)
            .expect("should find a buffer with the correct key");
        let mut out_mat = Vec::<u64>::new();
        assert_eq!((expected.len() + 1) * 8, out_mat_position.data.size);
        out_mat.resize(expected.len() + 1, 0);
        context
            .read(out_mat_position.data.offset, &mut out_mat)
            .expect("Should read output matrix");
        assert_eq!(rows, out_mat[0]);
        for i in 0..expected.len() {
            assert_eq!(expected[i], out_mat[1 + i]);
        }
    }

    fn single_domain_and_engine_matmul<Domain: MemoryDomain>(
        domain_arg: Vec<u8>,
        relative_path: &str,
        driver: Box<dyn Driver>,
        engine_resource: Vec<ComputeResource>,
    ) {
        let dispatcher = setup_dispatcher::<Domain>(
            Vec::new(),
            relative_path,
            vec![String::from("")],
            vec![String::from("")],
            driver,
            engine_resource,
        );
        const CONTEXT_SIZE: usize = 5 * 8;
        let mut in_context = Domain::init(domain_arg)
            .expect("Should be able to init domain")
            .acquire_context(CONTEXT_SIZE)
            .expect("Should get input matrix context");
        add_matmul_matrix(&mut in_context, 0, 0, 2, vec![1, 2, 3, 4]);

        let inputs = vec![(
            0,
            CompositionSet {
                context_list: vec![(Arc::new(in_context))],
                sharding_mode: ShardingMode::NoSharding,
                set_index: 0,
            },
        )];
        let outputs = vec![Some(0)];
        let result = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(dispatcher.queue_function(0, inputs, outputs, false));
        let out_sets = match result {
            Ok(context) => context,
            Err(err) => panic!("Failed with: {:?}", err),
        };
        assert_eq!(1, out_sets.len());
        let out_set = out_sets.get(&0).expect("Should have set 0");
        assert_eq!(1, out_set.context_list.len());
        let out_context = &out_set.context_list[0];
        assert_eq!(1, out_context.content.len());
        check_matrix(&out_context, 0, 0, 2, vec![5, 11, 11, 25])
    }

    fn composition_single_matmul<Domain: MemoryDomain>(
        domain_arg: Vec<u8>,
        relative_path: &str,
        driver: Box<dyn Driver>,
        engine_resource: Vec<ComputeResource>,
    ) {
        let dispatcher = setup_dispatcher::<Domain>(
            Vec::new(),
            relative_path,
            vec![String::from("")],
            vec![String::from("")],
            driver,
            engine_resource,
        );
        const CONTEXT_SIZE: usize = 9 * 8;
        let mut in_context = Domain::init(domain_arg)
            .expect("Should be able to init domain")
            .acquire_context(CONTEXT_SIZE)
            .expect("Should get input matrix context");
        add_matmul_matrix(&mut in_context, 0, 0, 2, vec![1, 2, 3, 4]);

        let composition = Composition {
            dependencies: vec![FunctionDependencies {
                function: 0,
                input_set_ids: vec![Some(0)],
                output_set_ids: vec![Some(1)],
            }],
        };
        let inputs = BTreeMap::from([(
            0,
            CompositionSet {
                set_index: 0,
                context_list: vec![Arc::new(in_context)],
                sharding_mode: ShardingMode::NoSharding,
            },
        )]);
        let outputs = BTreeMap::from([(1, 0)]);
        let result = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(dispatcher.queue_composition(composition, inputs, outputs, false));
        let mut out_contexts = match result {
            Ok(context) => context,
            Err(err) => panic!("Failed with: {:?}", err),
        };
        assert_eq!(1, out_contexts.len());
        let mut out_context_list = out_contexts.remove(&0).expect("Should have set 0");

        assert_eq!(1, out_context_list.context_list.len());
        let out_context = out_context_list.context_list.remove(0);
        assert_eq!(1, out_context.content.len());
        let out_mat_set = out_context.content[0].as_ref().expect("Should have set");
        assert_eq!(1, out_mat_set.buffers.len());
        check_matrix(&out_context, 0, 0, 2, vec![5, 11, 11, 25])
    }

    fn composition_parallel_matmul<Domain: MemoryDomain>(
        domain_arg: Vec<u8>,
        relative_path: &str,
        driver: Box<dyn Driver>,
        engine_resource: Vec<ComputeResource>,
    ) {
        let dispatcher = setup_dispatcher::<Domain>(
            Vec::new(),
            relative_path,
            vec![String::from("")],
            vec![String::from("")],
            driver,
            engine_resource,
        );
        // need space for the input matrix of 2x2 uint64_t as well as a output matrix of the same size
        // and an uint64_t size that gives the column / row size (which is 2)
        const CONTEXT_SIZE: usize = 18 * 8;
        let mut in_context = Domain::init(domain_arg)
            .expect("Should be able to init domain")
            .acquire_context(CONTEXT_SIZE)
            .expect("Should get input matrix context");
        add_matmul_matrix(&mut in_context, 0, 0, 2, vec![1, 2, 3, 4]);
        add_matmul_matrix(&mut in_context, 0, 1, 2, vec![1, 2, 3, 4]);

        let composition = Composition {
            dependencies: vec![FunctionDependencies {
                function: 0,
                input_set_ids: vec![Some(0)],
                output_set_ids: vec![Some(1)],
            }],
        };
        let inputs = BTreeMap::from([(
            0,
            CompositionSet {
                set_index: 0,
                context_list: vec![Arc::new(in_context)],
                sharding_mode: ShardingMode::KeySharding(BTreeSet::from([0, 1])),
            },
        )]);
        let outputs = BTreeMap::from([(1, 0)]);
        let result = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(dispatcher.queue_composition(composition, inputs, outputs, false));
        let mut out_vec = match result {
            Ok(v) => v,
            Err(err) => panic!("Failed with: {:?}", err),
        };
        assert_eq!(1, out_vec.len());
        let out_set = out_vec.remove(&0).expect("Should have set 0");
        assert_eq!(2, out_set.context_list.len());
        // check for each shard:
        for matrix_context in out_set.context_list {
            if let Some(matrix_set) = &matrix_context.content[0] {
                assert_eq!(1, matrix_set.buffers.len());
                let matrix_buffer = &matrix_set.buffers[0];
                assert!(matrix_buffer.key == 1 || matrix_buffer.key == 0);
                check_matrix(
                    &matrix_context,
                    0,
                    matrix_buffer.key,
                    2,
                    vec![5, 11, 11, 25],
                );
            }
        }
    }

    fn composition_chain_matmul<Domain: MemoryDomain>(
        domain_arg: Vec<u8>,
        relative_path: &str,
        driver: Box<dyn Driver>,
        engine_resource: Vec<ComputeResource>,
    ) {
        let dispatcher = setup_dispatcher::<Domain>(
            Vec::new(),
            relative_path,
            vec![String::from("")],
            vec![String::from("")],
            driver,
            engine_resource,
        );
        const CONTEXT_SIZE: usize = 9 * 8;
        let mut in_context = Domain::init(domain_arg)
            .expect("Should be able to init domain")
            .acquire_context(CONTEXT_SIZE)
            .expect("Should get input matrix context");
        add_matmul_matrix(&mut in_context, 0, 1, 2, vec![1, 2, 3, 4]);

        let composition = Composition {
            dependencies: vec![
                FunctionDependencies {
                    function: 0,
                    input_set_ids: vec![Some(0)],
                    output_set_ids: vec![Some(1)],
                },
                FunctionDependencies {
                    function: 0,
                    input_set_ids: vec![Some(1)],
                    output_set_ids: vec![Some(2)],
                },
            ],
        };
        let inputs = BTreeMap::from([(
            0,
            CompositionSet {
                set_index: 0,
                sharding_mode: ShardingMode::NoSharding,
                context_list: vec![Arc::new(in_context)],
            },
        )]);
        let output_contexts = BTreeMap::from([(2, 0)]);
        let result = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(dispatcher.queue_composition(composition, inputs, output_contexts, false));
        let out_contexts = match result {
            Ok(context) => context,
            Err(err) => panic!("Failed with: {:?}", err),
        };
        assert_eq!(1, out_contexts.len());
        let out_composition_set = out_contexts.get(&0).expect("Should have set 0");
        assert_eq!(1, out_composition_set.context_list.len());
        let out_context = &out_composition_set.context_list[0];
        assert_eq!(1, out_context.content.len());
        check_matrix(&out_context, 0, 0, 2, vec![146, 330, 330, 746]);
    }

    fn composition_diamond_matmac<Domain: MemoryDomain>(
        domain_arg: Vec<u8>,
        relative_path: &str,
        driver: Box<dyn Driver>,
        engine_resource: Vec<ComputeResource>,
    ) {
        let dispatcher = self::setup_dispatcher::<Domain>(
            Vec::new(),
            relative_path,
            vec![String::from(""), String::from(""), String::from("")],
            vec![String::from("")],
            driver,
            engine_resource,
        );
        const CONTEXT_SIZE: usize = 12 * 8;
        let mut in_context = Domain::init(domain_arg)
            .expect("Should be able to init domain")
            .acquire_context(CONTEXT_SIZE)
            .expect("Should get input matrix context");
        // A = [7]
        add_matmac_matrix(&mut in_context, 0, 1, 1, vec![7]);
        // B = [1,2,3,5]
        add_matmac_matrix(&mut in_context, 1, 1, 4, vec![1, 2, 3, 5]);
        // B^T
        add_matmac_matrix(&mut in_context, 2, 4, 1, vec![1, 2, 3, 5]);

        let composition = Composition {
            dependencies: vec![
                // C = A*B
                FunctionDependencies {
                    function: 0,
                    input_set_ids: vec![Some(0), Some(1), None],
                    output_set_ids: vec![Some(3)],
                },
                // D = B^T*A
                FunctionDependencies {
                    function: 0,
                    input_set_ids: vec![Some(2), Some(0), None],
                    output_set_ids: vec![Some(4)],
                },
                // E = B + C
                FunctionDependencies {
                    function: 0,
                    input_set_ids: vec![None, Some(1), Some(3)],
                    output_set_ids: vec![Some(5)],
                },
                // G = D * C
                FunctionDependencies {
                    function: 0,
                    input_set_ids: vec![Some(4), Some(3), None],
                    output_set_ids: vec![Some(6)],
                },
                // Result = D*E + G
                FunctionDependencies {
                    function: 0,
                    input_set_ids: vec![Some(4), Some(5), Some(6)],
                    output_set_ids: vec![Some(7)],
                },
            ],
        };
        let context_arc = Arc::new(in_context);
        let inputs = BTreeMap::from([
            (
                0,
                CompositionSet {
                    set_index: 0,
                    sharding_mode: ShardingMode::NoSharding,
                    context_list: vec![context_arc.clone()],
                },
            ),
            (
                1,
                CompositionSet {
                    set_index: 1,
                    sharding_mode: ShardingMode::NoSharding,
                    context_list: vec![context_arc.clone()],
                },
            ),
            (
                2,
                CompositionSet {
                    set_index: 2,
                    sharding_mode: ShardingMode::NoSharding,
                    context_list: vec![context_arc.clone()],
                },
            ),
        ]);
        let output_contexts = BTreeMap::from([(7, 0)]);
        let result = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(dispatcher.queue_composition(composition, inputs, output_contexts, false));
        let out_contexts = match result {
            Ok(context) => context,
            Err(err) => panic!("Failed with: {:?}", err),
        };
        assert_eq!(1, out_contexts.len());
        let out_composition_set = out_contexts.get(&0).expect("Should have set 0");
        assert_eq!(1, out_composition_set.context_list.len());
        let out_context = &out_composition_set.context_list[0];
        assert_eq!(1, out_context.content.len());
        check_matrix(
            out_context,
            0,
            0,
            4,
            vec![
                105, 210, 315, 525, 210, 420, 630, 1050, 315, 630, 945, 1575, 525, 1050, 1575, 2625,
            ],
        );
    }

    macro_rules! dispatcherTests {
        ($name: ident; $domain : ty; $init: expr; $driver : expr; $engine_resource: expr) => {
            #[test]
            fn test_single_domain_and_engine_basic() {
                let driver = Box::new($driver);
                let name = format!("test_{}_basic", stringify!($name));
                super::single_domain_and_engine_basic::<$domain>(
                    $init,
                    &name,
                    driver,
                    $engine_resource,
                )
            }
            #[test]
            fn test_single_domain_and_engine_matmul() {
                let driver = Box::new($driver);
                let name = format!("test_{}_matmul", stringify!($name));
                super::single_domain_and_engine_matmul::<$domain>(
                    $init,
                    &name,
                    driver,
                    $engine_resource,
                )
            }
            #[test]
            fn test_composition_single_matmul() {
                let driver = Box::new($driver);
                let name = format!("test_{}_matmul", stringify!($name));
                super::composition_single_matmul::<$domain>($init, &name, driver, $engine_resource)
            }

            #[test]
            fn test_composition_parallel() {
                let driver = Box::new($driver);
                let name = format!("test_{}_matmul", stringify!($name));
                super::composition_parallel_matmul::<$domain>(
                    $init,
                    &name,
                    driver,
                    $engine_resource,
                )
            }

            #[test]
            fn test_composition_chain() {
                let driver = Box::new($driver);
                let name = format!("test_{}_matmul", stringify!($name));
                super::composition_chain_matmul::<$domain>($init, &name, driver, $engine_resource)
            }

            #[test]
            fn test_composition_diamond() {
                let name = format!("test_{}_matmac", stringify!($name));
                let driver = Box::new($driver);
                super::composition_diamond_matmac::<$domain>($init, &name, driver, $engine_resource)
            }
        };
    }

    #[cfg(feature = "cheri")]
    mod cheri {
        use machine_interface::{
            function_driver::{compute_driver::cheri::CheriDriver, ComputeResource},
            memory_domain::cheri::CheriMemoryDomain,
        };
        dispatcherTests!(elf_cheri; CheriMemoryDomain; Vec::new(); CheriDriver {}; vec![ComputeResource::CPU(1)]);
    }

    #[cfg(feature = "mmu")]
    mod mmu {
        use machine_interface::{
            function_driver::{compute_driver::mmu::MmuDriver, ComputeResource},
            memory_domain::mmu::MmuMemoryDomain,
        };
        #[cfg(target_arch = "x86_64")]
        dispatcherTests!(elf_mmu_x86_64; MmuMemoryDomain; Vec::new(); MmuDriver {}; vec![ComputeResource::CPU(1)]);
        #[cfg(target_arch = "aarch64")]
        dispatcherTests!(elf_mmu_aarch64; MmuMemoryDomain; Vec::new(); MmuDriver {}; vec![ComputeResource::CPU(1)]);
    }

    #[cfg(feature = "wasm")]
    mod wasm {
        use machine_interface::{
            function_driver::{compute_driver::wasm::WasmDriver, ComputeResource},
            memory_domain::wasm::WasmMemoryDomain,
        };
        dispatcherTests!(sysld_wasm; WasmMemoryDomain; Vec::new(); WasmDriver {}; vec![ComputeResource::CPU(1)]);
        #[cfg(target_arch = "aarch64")]
        dispatcherTests!(sysld_wasm_aarch64; WasmMemoryDomain; Vec::new(); WasmDriver {}; vec![ComputeResource::CPU(1)]);
    }
}

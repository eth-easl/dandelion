#[cfg(all(test, any(feature = "cheri", feature = "mmu", feature = "kvm")))]
mod dispatcher_tests {
    mod function_tests;
    mod registry_tests;

    mod combination_tests;
    mod meta_function_tests;

    use composition::AnyShardingMode;
    use dandelion_commons::{records::Recorder, DandelionResult, FunctionId};
    use dispatcher::{dispatcher::Dispatcher, queue::WorkQueue, resource_pool::ResourcePool};
    use machine_interface::{
        function_driver::ComputeResource,
        machine_config::{DomainType, EngineType},
    };
    use memory::{
        context::{ContextTrait, MemoryResource},
        data::Metadata,
        DataItem, DataSet,
    };
    use std::{collections::BTreeMap, sync::Arc, time::Instant};

    const DEFAULT_CONTEXT_SIZE: usize = 0x800_0000; // 128MiB

    #[inline]
    fn zero_id() -> FunctionId {
        Arc::new(0.to_string())
    }

    /// Absolute path of the test binary with the given name.
    fn test_binary_path(name: &str) -> String {
        let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.pop();
        path.push("machine_interface/tests/data");
        path.push(name);
        path.to_str().expect("Path should be string").to_string()
    }

    fn setup_dispatcher(
        name: &str,
        in_set_names: Vec<(String, Option<DataSet>)>,
        out_set_names: Vec<String>,
        engine_type: EngineType,
        engine_resource: Vec<ComputeResource>,
        memory_resource: (DomainType, MemoryResource),
    ) -> (Dispatcher, FunctionId) {
        setup_dispatcher_with_meta_depth(
            name,
            in_set_names,
            out_set_names,
            engine_type,
            engine_resource,
            memory_resource,
            None,
        )
    }

    /// Like `setup_dispatcher`, with the given maximum metafunction nesting depth.
    fn setup_dispatcher_with_meta_depth(
        name: &str,
        in_set_names: Vec<(String, Option<DataSet>)>,
        out_set_names: Vec<String>,
        engine_type: EngineType,
        engine_resource: Vec<ComputeResource>,
        memory_resource: (DomainType, MemoryResource),
        max_meta_depth: Option<usize>,
    ) -> (Dispatcher, FunctionId) {
        let path_string = test_binary_path(name);
        let metadata = Metadata {
            input_sets: in_set_names,
            output_sets: out_set_names,
            min_set_bytes: vec![],
        };
        let mut pool_map = BTreeMap::new();
        pool_map.insert(engine_type, engine_resource);

        // insert a system engine if available
        pool_map.insert(EngineType::System, vec![ComputeResource::CPU(0)]);

        let resource_pool = ResourcePool {
            engine_pool: futures::lock::Mutex::new(pool_map),
        };
        let memory_resources = BTreeMap::from_iter(vec![memory_resource]);
        let work_queue = WorkQueue::init();
        let dispatcher = Dispatcher::init(
            resource_pool,
            memory_resources,
            work_queue,
            AnyShardingMode::MaxSharding,
            max_meta_depth,
        )
        .expect("Should have initialized dispatcher");
        let function_id = Arc::new(String::from("test_function"));
        dispatcher
            .insert_function(
                (*function_id).clone(),
                engine_type,
                DEFAULT_CONTEXT_SIZE,
                path_string,
                metadata,
            )
            .expect("Should be able to insert function in new dispatcher");
        (dispatcher, function_id)
    }

    /// Parses `composition` (which must contain exactly one composition) and runs it to completion
    /// on the given inputs, returning the (resolved) output sets.
    fn run_composition(
        dispatcher: &Dispatcher,
        composition: &str,
        inputs: Vec<DataSet>,
    ) -> Vec<Arc<Vec<DataItem>>> {
        match try_run_composition(dispatcher, composition, inputs) {
            Ok(out_sets) => out_sets,
            Err(err) => panic!("Failed with: {:?}", err),
        }
    }

    /// Like `run_composition`, but returns the error instead of panicking.
    fn try_run_composition(
        dispatcher: &Dispatcher,
        composition: &str,
        inputs: Vec<DataSet>,
    ) -> DandelionResult<Vec<Arc<Vec<DataItem>>>> {
        let recorder = Recorder::new(zero_id(), Instant::now());
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(dispatcher.queue_unregistered_composition(
                composition.to_string(),
                inputs,
                false,
                recorder,
            ))
            .map(|(out_sets, _)| out_sets)
    }

    fn check_matrix(item: &DataItem, rows: u64, expected: Vec<u64>) {
        let local = item
            .local_ref()
            .expect("Should not get non local item data");
        let context = &local.ctx;
        let out_mat_position = local.pos;
        let mut out_mat = Vec::<u64>::new();
        assert_eq!((expected.len() + 1) * 8, out_mat_position.size);
        out_mat.resize(expected.len() + 1, 0);
        context
            .read(out_mat_position.offset, &mut out_mat)
            .expect("Should read output matrix");
        assert_eq!(rows, out_mat[0]);
        let mut found_error = false;
        for i in 0..expected.len() {
            if expected[i] != out_mat[1 + i] {
                println!(
                    "expected {}, actual {}, at index {}, one before {:?}, one after {:?}",
                    expected[i],
                    out_mat[1 + i],
                    i,
                    out_mat.get(i),
                    out_mat.get(2 + i)
                );
                found_error = true;
            }
        }
        assert!(
            !found_error,
            "There was an error in the matrix, check stdout for logs"
        );
    }

    macro_rules! dispatcherTests {
        ($name: ident; $init : expr; $engine_type : expr; $engine_resource: expr) => {
            use crate::dispatcher_tests::{
                function_tests::{
                    composition_chain_large_matmac, composition_chain_matmul,
                    composition_diamond_matmac, composition_optional, composition_parallel_matmul,
                    composition_single_matmul, single_domain_and_engine_basic,
                    single_domain_and_engine_matmul,
                },
                registry_tests::{multiple_input_fixed, single_input_fixed},
            };

            #[test_log::test]
            fn test_single_domain_and_engine_basic() {
                let name = format!("test_{}_basic", stringify!($name));
                single_domain_and_engine_basic($init, &name, $engine_type, $engine_resource)
            }

            #[test_log::test]
            fn test_single_domain_and_engine_matmul() {
                let name = format!("test_{}_matmul", stringify!($name));
                single_domain_and_engine_matmul($init, &name, $engine_type, $engine_resource)
            }

            #[test_log::test]
            fn test_composition_single_matmul() {
                let name = format!("test_{}_matmul", stringify!($name));
                composition_single_matmul($init, &name, $engine_type, $engine_resource)
            }

            #[test_log::test]
            fn test_composition_optional() {
                let name = format!("test_{}_basic", stringify!($name));
                composition_optional($init, &name, $engine_type, $engine_resource)
            }

            #[test_log::test]
            fn test_composition_parallel() {
                let name = format!("test_{}_matmul", stringify!($name));
                composition_parallel_matmul($init, &name, $engine_type, $engine_resource)
            }

            #[test_log::test]
            fn test_composition_chain() {
                let name = format!("test_{}_matmul", stringify!($name));
                composition_chain_matmul($init, &name, $engine_type, $engine_resource)
            }

            #[test_log::test]
            fn test_composition_diamond() {
                let name = format!("test_{}_matmac", stringify!($name));
                composition_diamond_matmac($init, &name, $engine_type, $engine_resource)
            }

            #[test_log::test]
            fn test_composition_chain_large_matmac() {
                let name = format!("test_{}_matmac", stringify!($name));
                composition_chain_large_matmac($init, &name, $engine_type, $engine_resource)
            }

            #[test_log::test]
            fn test_single_input_fixed() {
                let name = format!("test_{}_matmac", stringify!($name));
                single_input_fixed($init, &name, $engine_type, $engine_resource)
            }

            #[test_log::test]
            fn test_multiple_input_fixed() {
                let name = format!("test_{}_matmac", stringify!($name));
                multiple_input_fixed($init, &name, $engine_type, $engine_resource)
            }

            #[test_log::test]
            fn test_fetch_compute() {
                use crate::dispatcher_tests::combination_tests::fetch_compute;
                let name = format!("test_{}_matmul", stringify!($name));
                fetch_compute($init, &name, $engine_type, $engine_resource)
            }
        };
    }

    /// Metafunction tests, separate from `dispatcherTests` as they need the planner test binary,
    /// which is not available for every engine (e.g. cheri).
    macro_rules! metaFunctionTests {
        ($name: ident; $init : expr; $engine_type : expr; $engine_resource: expr) => {
            mod meta_function {
                use super::*;
                use crate::dispatcher_tests::meta_function_tests::*;

                #[test_log::test]
                fn test_meta_function_runs_plan() {
                    meta_function_runs_plan(
                        $init,
                        stringify!($name),
                        $engine_type,
                        $engine_resource,
                    )
                }

                #[test_log::test]
                fn test_meta_function_disabled_at_depth_zero() {
                    meta_function_disabled_at_depth_zero(
                        $init,
                        stringify!($name),
                        $engine_type,
                        $engine_resource,
                    )
                }

                #[test_log::test]
                fn test_nested_meta_function_within_limit_runs() {
                    nested_meta_function_within_limit_runs(
                        $init,
                        stringify!($name),
                        $engine_type,
                        $engine_resource,
                    )
                }

                #[test_log::test]
                fn test_nested_meta_function_beyond_limit_fails() {
                    nested_meta_function_beyond_limit_fails(
                        $init,
                        stringify!($name),
                        $engine_type,
                        $engine_resource,
                    )
                }

                #[test_log::test]
                fn test_registered_composition_does_not_reset_depth() {
                    registered_composition_does_not_reset_depth(
                        $init,
                        stringify!($name),
                        $engine_type,
                        $engine_resource,
                    )
                }

                #[test_log::test]
                fn test_invalid_plan_fails_application() {
                    invalid_plan_fails_application(
                        $init,
                        stringify!($name),
                        $engine_type,
                        $engine_resource,
                    )
                }
            }
        };
    }

    #[cfg(feature = "cheri")]
    mod cheri {
        use machine_interface::{
            function_driver::ComputeResource,
            machine_config::{DomainType, EngineType},
        };
        use memory::context::MemoryResource;
        dispatcherTests!(elf_cheri; (DomainType::Cheri, MemoryResource::Anonymous { size: (1<<30) }); EngineType::Cheri; vec![ComputeResource::CPU(1)]);
    }

    #[cfg(feature = "mmu")]
    mod mmu {
        use machine_interface::{
            function_driver::ComputeResource,
            machine_config::{DomainType, EngineType},
        };
        use memory::context::MemoryResource;
        #[cfg(target_arch = "x86_64")]
        dispatcherTests!(elf_mmu_x86_64; (DomainType::Process ,MemoryResource::Shared { size: (1<<30) }); EngineType::Process; vec![ComputeResource::CPU(1)]);
        #[cfg(target_arch = "x86_64")]
        metaFunctionTests!(elf_mmu_x86_64; (DomainType::Process ,MemoryResource::Shared { size: (1<<30) }); EngineType::Process; vec![ComputeResource::CPU(1)]);
        #[cfg(target_arch = "aarch64")]
        dispatcherTests!(elf_mmu_aarch64; (DomainType::Process, MemoryResource::Shared { size: (1<<30) }); EngineType::Process; vec![ComputeResource::CPU(1)]);
        #[cfg(target_arch = "aarch64")]
        metaFunctionTests!(elf_mmu_aarch64; (DomainType::Process, MemoryResource::Shared { size: (1<<30) }); EngineType::Process; vec![ComputeResource::CPU(1)]);
    }

    #[cfg(feature = "kvm")]
    mod kvm {
        use machine_interface::{
            function_driver::ComputeResource,
            machine_config::{DomainType, EngineType},
        };
        use memory::context::MemoryResource;
        #[cfg(target_arch = "x86_64")]
        dispatcherTests!(elf_kvm_x86_64; (DomainType::Kvm, MemoryResource::Anonymous { size: (1<<30) }); EngineType::Kvm; vec![ComputeResource::CPU(1)]);
        #[cfg(target_arch = "x86_64")]
        metaFunctionTests!(elf_kvm_x86_64; (DomainType::Kvm, MemoryResource::Anonymous { size: (1<<30) }); EngineType::Kvm; vec![ComputeResource::CPU(1)]);
        #[cfg(target_arch = "aarch64")]
        dispatcherTests!(elf_kvm_aarch64; (DomainType::Kvm, MemoryResource::Anonymous { size: (1<<30) }); EngineType::Kvm; vec![ComputeResource::CPU(1)]);
        #[cfg(target_arch = "aarch64")]
        metaFunctionTests!(elf_kvm_aarch64; (DomainType::Kvm, MemoryResource::Anonymous { size: (1<<30) }); EngineType::Kvm; vec![ComputeResource::CPU(1)]);
    }
}

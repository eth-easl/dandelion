#[cfg(all(
    test,
    any(feature = "cheri", feature = "mmu", feature = "kvm", feature = "wasm")
))]
mod dispatcher_tests {
    mod function_tests;
    mod registry_tests;

    use dandelion_commons::FunctionId;
    use dispatcher::{
        composition::CompositionSet, dispatcher::Dispatcher, function_registry::Metadata,
        resource_pool::ResourcePool,
    };
    use machine_interface::{
        function_driver::ComputeResource,
        machine_config::{DomainType, EngineType},
        memory_domain::{Context, ContextTrait, MemoryDomain, MemoryResource},
    };
    use std::{collections::BTreeMap, sync::Arc};

    // using 0x802_0000 because that is what WASM specifies
    const DEFAULT_CONTEXT_SIZE: usize = 0x802_0000; // 128MiB

    fn setup_dispatcher<Dom: MemoryDomain>(
        name: &str,
        in_set_names: Vec<(String, Option<CompositionSet>)>,
        out_set_names: Vec<String>,
        engine_type: EngineType,
        engine_resource: Vec<ComputeResource>,
        memory_resource: (DomainType, MemoryResource),
    ) -> (Dispatcher, FunctionId) {
        let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.pop();
        path.push("machine_interface/tests/data");
        path.push(name);
        let path_string = path.to_str().expect("Path should be string").to_string();
        let metadata = Metadata {
            input_sets: Arc::new(in_set_names),
            output_sets: Arc::new(out_set_names),
        };
        let mut pool_map = BTreeMap::new();
        pool_map.insert(engine_type, engine_resource);
        let resource_pool = ResourcePool {
            engine_pool: futures::lock::Mutex::new(pool_map),
        };
        let memory_resources = vec![memory_resource]
            .into_iter()
            .map(|(dom, resource)| {
                (
                    dom,
                    machine_interface::memory_domain::test_resource::get_resource(resource),
                )
            })
            .collect();
        let dispatcher = Dispatcher::init(resource_pool, memory_resources)
            .expect("Should have initialized dispatcher");
        let function_id = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(dispatcher.insert_func(
                String::from("test_function"),
                engine_type,
                DEFAULT_CONTEXT_SIZE,
                path_string,
                metadata,
            ))
            .expect("Should be able to insert function in new dispatcher");
        return (dispatcher, function_id);
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

    macro_rules! dispatcherTests {
        ($name: ident; $domain : ty; $init : expr; $engine_type : expr; $engine_resource: expr) => {
            use crate::dispatcher_tests::{
                function_tests::{
                    composition_chain_matmul, composition_diamond_matmac, composition_optional,
                    composition_parallel_matmul, composition_single_matmul,
                    single_domain_and_engine_basic, single_domain_and_engine_matmul,
                },
                registry_tests::{multiple_input_fixed, single_input_fixed},
            };

            #[test_log::test]
            fn test_single_domain_and_engine_basic() {
                let name = format!("test_{}_basic", stringify!($name));
                single_domain_and_engine_basic::<$domain>(
                    $init,
                    &name,
                    $engine_type,
                    $engine_resource,
                )
            }

            #[test_log::test]
            fn test_single_domain_and_engine_matmul() {
                let name = format!("test_{}_matmul", stringify!($name));
                single_domain_and_engine_matmul::<$domain>(
                    $init,
                    &name,
                    $engine_type,
                    $engine_resource,
                )
            }

            #[test_log::test]
            fn test_composition_single_matmul() {
                let name = format!("test_{}_matmul", stringify!($name));
                composition_single_matmul::<$domain>($init, &name, $engine_type, $engine_resource)
            }

            #[test_log::test]
            fn test_composition_optional() {
                let name = format!("test_{}_basic", stringify!($name));
                composition_optional::<$domain>($init, &name, $engine_type, $engine_resource)
            }

            #[test_log::test]
            fn test_composition_parallel() {
                let name = format!("test_{}_matmul", stringify!($name));
                composition_parallel_matmul::<$domain>($init, &name, $engine_type, $engine_resource)
            }

            #[test_log::test]
            fn test_composition_chain() {
                let name = format!("test_{}_matmul", stringify!($name));
                composition_chain_matmul::<$domain>($init, &name, $engine_type, $engine_resource)
            }

            #[test_log::test]
            fn test_composition_diamond() {
                let name = format!("test_{}_matmac", stringify!($name));
                composition_diamond_matmac::<$domain>($init, &name, $engine_type, $engine_resource)
            }

            #[test_log::test]
            fn test_single_input_fixed() {
                let name = format!("test_{}_matmac", stringify!($name));
                single_input_fixed::<$domain>($init, &name, $engine_type, $engine_resource)
            }

            #[test_log::test]
            fn test_multiple_input_fixed() {
                let name = format!("test_{}_matmac", stringify!($name));
                multiple_input_fixed::<$domain>($init, &name, $engine_type, $engine_resource)
            }
        };
    }

    #[cfg(feature = "cheri")]
    mod cheri {
        use machine_interface::{
            function_driver::ComputeResource,
            machine_config::{DomainType, EngineType},
            memory_domain::{cheri::CheriMemoryDomain, MemoryResource},
        };
        dispatcherTests!(elf_cheri; CheriMemoryDomain; (DomainType::Cheri, MemoryResource::Anonymous { size: (1<<30) }); EngineType::Cheri; vec![ComputeResource::CPU(1)]);
    }

    #[cfg(feature = "mmu")]
    mod mmu {
        use machine_interface::{
            function_driver::ComputeResource,
            machine_config::{DomainType, EngineType},
            memory_domain::{mmu::MmuMemoryDomain, MemoryResource},
        };
        #[cfg(target_arch = "x86_64")]
        dispatcherTests!(elf_mmu_x86_64; MmuMemoryDomain; (DomainType::Process ,MemoryResource::Shared { id: 0, size: (1<<30) }); EngineType::Process; vec![ComputeResource::CPU(1)]);
        #[cfg(target_arch = "aarch64")]
        dispatcherTests!(elf_mmu_aarch64; MmuMemoryDomain; (DomainType::Process, MemoryResource::Shared { id: 0, size: (1<<30) }); EngineType::Process; vec![ComputeResource::CPU(1)]);
    }

    #[cfg(feature = "kvm")]
    mod kvm {
        use machine_interface::{
            function_driver::ComputeResource,
            machine_config::{DomainType, EngineType},
            memory_domain::{mmap::MmapMemoryDomain, MemoryResource},
        };
        #[cfg(target_arch = "x86_64")]
        dispatcherTests!(elf_kvm_x86_64; MmapMemoryDomain; (DomainType::Mmap, MemoryResource::Anonymous { size: (1<<30) }); EngineType::Kvm; vec![ComputeResource::CPU(1)]);
        #[cfg(target_arch = "aarch64")]
        dispatcherTests!(elf_kvm_aarch64; MmapMemoryDomain; (DomainType::Mmap, MemoryResource::Anonymous { size: (1<<30) }); EngineType::Kvm; vec![ComputeResource::CPU(1)]);
    }

    #[cfg(feature = "wasm")]
    mod wasm {
        use machine_interface::{
            function_driver::ComputeResource,
            machine_config::{DomainType, EngineType},
            memory_domain::{wasm::WasmMemoryDomain, MemoryResource},
        };

        #[cfg(target_arch = "x86_64")]
        dispatcherTests!(sysld_wasm_x86_64; WasmMemoryDomain; (DomainType::RWasm, MemoryResource::Anonymous { size: (1<<30) }); EngineType::RWasm; vec![ComputeResource::CPU(1)]);

        #[cfg(target_arch = "aarch64")]
        dispatcherTests!(sysld_wasm_aarch64; WasmMemoryDomain; (DomainType::RWasm, MemoryResource::Anonymous { size: (1<<30) }); EngineType::RWasm; vec![ComputeResource::CPU(1)]);
    }
}

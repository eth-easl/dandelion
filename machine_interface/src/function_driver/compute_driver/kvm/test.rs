use crate::{
    function_driver::{
        compute_driver::compute_driver_tests::{prepare_engine_and_domain, zero_id},
        functions::FunctionAlternative,
        ComputeResource, Metadata, WorkToDo,
    },
    Position,
};
use dandelion_commons::DandelionError;
use memory::{
    context::{
        kvm::KvmMemoryDomain, read_only::ReadOnlyContext, ContextDataItem, ContextDataSet,
        ContextTrait,
    },
    DataSet,
};
use std::sync::Arc;

const DEFAULT_CONTEXT_SIZE: usize = 5 * 1024 * 1024;

#[test_log::test]
fn check_memory_leak_oom() {
    #[cfg(target_arch = "x86_64")]
    let arch_string = "x86_64";
    #[cfg(target_arch = "aarch64")]
    let arch_string = "aarch64";

    let filename = format!(
        "{}/tests/data/test_elf_kvm_{}_matmul",
        env!("CARGO_MANIFEST_DIR"),
        arch_string
    );

    let engine_type = crate::machine_config::EngineType::Kvm;

    let (domain, queue) = prepare_engine_and_domain::<KvmMemoryDomain>(
        memory::context::MemoryResource::Anonymous {
            size: DEFAULT_CONTEXT_SIZE,
        },
        engine_type,
        vec![ComputeResource::CPU(0)],
    );

    unsafe {
        std::hint::assert_unchecked(
            std::mem::align_of_val(domain.as_ref()) == std::mem::align_of::<KvmMemoryDomain>(),
        )
    };
    let ptr = Arc::into_raw(domain.clone());
    let kvm_domain = unsafe { Arc::from_raw(ptr.cast::<Box<KvmMemoryDomain>>()) };

    let mut result_contexts = Vec::new();
    // repeat to run more functions until memory runs out
    loop {
        // add inputs
        let in_size = 512u64;
        let mat_size = (in_size * in_size) as usize + 1;
        let mat_bytes = mat_size * size_of::<u64>();
        let mut in_data = Vec::with_capacity(mat_size);
        in_data.push(in_size);
        in_data.resize(mat_size, 1);
        let mut input_context = ReadOnlyContext::from_boxed(in_data.into_boxed_slice()).unwrap();
        input_context.content.push(Some(ContextDataSet {
            items: vec![ContextDataItem {
                ident: "".to_string(),
                data: Position {
                    offset: 0,
                    size: mat_bytes,
                },
                key: 0,
            }],
            total_size: mat_bytes,
        }));
        let input_sets = DataSet::from_context(input_context);

        let metadata = Arc::new(Metadata {
            input_sets: vec![("".to_string(), None)],
            output_sets: vec!["".to_string()],
            min_set_bytes: vec![],
        });

        let recorder =
            dandelion_commons::trace::Recorder::new(zero_id(), 1, std::time::Instant::now());
        let function_alternatives = vec![Arc::new(FunctionAlternative::new_unloaded(
            engine_type,
            DEFAULT_CONTEXT_SIZE,
            filename.to_string(),
            domain.clone(),
        ))];
        let promise_oom = queue.enqueu(WorkToDo::FunctionArguments {
            function_id: Arc::new(String::new()),
            function_alternatives,
            input_sets,
            metadata,
            caching: false,
            recorder,
        });
        match tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(promise_oom)
        {
            Err(err) => match err.error {
                DandelionError::OutOfMemory => break,
                err => panic!("Should not have {:?}", err),
            },
            Ok(result) => result_contexts.push(result.get_data_items()),
        }
    }

    // release all the memory
    drop(result_contexts);
    // check all memory is back
    kvm_domain.check_memory();
}

#[test_log::test]
fn check_memory_leak_success() {
    #[cfg(target_arch = "x86_64")]
    let arch_string = "x86_64";
    #[cfg(target_arch = "aarch64")]
    let arch_string = "aarch64";

    let filename = format!(
        "{}/tests/data/test_elf_kvm_{}_matmul",
        env!("CARGO_MANIFEST_DIR"),
        arch_string
    );

    let engine_type = crate::machine_config::EngineType::Kvm;

    let (domain, queue) = prepare_engine_and_domain::<KvmMemoryDomain>(
        memory::context::MemoryResource::Anonymous {
            size: DEFAULT_CONTEXT_SIZE,
        },
        engine_type,
        vec![ComputeResource::CPU(0)],
    );

    unsafe {
        std::hint::assert_unchecked(
            std::mem::align_of_val(domain.as_ref()) == std::mem::align_of::<KvmMemoryDomain>(),
        )
    };
    let ptr = Arc::into_raw(domain.clone());
    let kvm_domain = unsafe { Arc::from_raw(ptr.cast::<Box<KvmMemoryDomain>>()) };

    // add inputs
    let in_size = 512u64;
    let mat_size = (in_size * in_size) as usize + 1;
    let mat_bytes = mat_size * size_of::<u64>();
    let mut in_data = Vec::with_capacity(mat_size);
    in_data.push(in_size);
    in_data.resize(mat_size, 1);
    let mut input_context = ReadOnlyContext::from_boxed(in_data.into_boxed_slice()).unwrap();
    input_context.content.push(Some(ContextDataSet {
        items: vec![ContextDataItem {
            ident: "".to_string(),
            data: Position {
                offset: 0,
                size: mat_bytes,
            },
            key: 0,
        }],
        total_size: mat_bytes,
    }));
    let input_sets = DataSet::from_context(input_context);

    let metadata = Arc::new(Metadata {
        input_sets: vec![("".to_string(), None)],
        output_sets: vec!["".to_string()],
        min_set_bytes: vec![],
    });

    let recorder = dandelion_commons::trace::Recorder::new(zero_id(), 1, std::time::Instant::now());
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

    let result_sets = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(promise)
        .expect("Engine should run ok with basic function")
        .get_data_items();

    // check that result is 4
    assert_eq!(1, result_sets.len());
    let result_set = &result_sets[0];
    assert_eq!(1, result_set.len());
    let output_item = result_set[0].local_ref().unwrap();
    assert_eq!(
        mat_bytes, output_item.pos.size,
        "Checking for size of output"
    );
    let mut read_buffer = vec![0u64; output_item.pos.size / 8];
    output_item
        .ctx
        .read(output_item.pos.offset, &mut read_buffer)
        .expect("Should succeed in reading");
    assert_eq!(in_size, read_buffer[0]);
    assert_eq!(in_size, read_buffer[1]);
    drop(result_sets);
    kvm_domain.check_memory();
}

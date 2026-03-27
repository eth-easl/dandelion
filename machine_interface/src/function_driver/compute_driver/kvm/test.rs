use crate::{
    function_driver::{
        compute_driver::compute_driver_tests::compute_driver_tests::{
            get_expected_mat, prepare_engine_and_domain, zero_id, DEFAULT_CONTEXT_SIZE,
        },
        functions::FunctionAlternative,
        CompositionSet, ComputeResource, Metadata, WorkToDo,
    },
    machine_config::EngineType,
    memory_domain::{
        bytes_context::BytesContext, kvm::KvmMemoryDomain, Context, ContextTrait, ContextType,
        MemoryResource,
    },
    DataItem, DataSet, Position,
};
use bytes::BytesMut;
use dandelion_commons::records::Recorder;
use std::{collections::BTreeMap, sync::Arc, time::Instant};

#[test_log::test]
fn zero_copy_from_bytes() {
    // create bytes context with data in it
    let mat_size = 32;
    let item_size = (mat_size * mat_size + 1) * core::mem::size_of::<i64>();
    let mut bytes = BytesMut::new();
    bytes.extend_from_slice(&i64::to_ne_bytes(mat_size as i64));
    for i in 0..(mat_size * mat_size) {
        bytes.extend_from_slice(&i64::to_ne_bytes(i as i64));
    }
    let mut input_context = Context::new(
        ContextType::Bytes(Box::new(BytesContext {
            frames: BTreeMap::from([(0, bytes.freeze())]),
        })),
        item_size,
    );
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

    #[cfg(target_arch = "aarch64")]
    let arch = "aarch64";
    #[cfg(target_arch = "x86_64")]
    let arch = "x86_64";
    let filename = format!(
        "{}/tests/data/test_elf_kvm_{}_matmul",
        env!("CARGO_MANIFEST_DIR"),
        arch
    );

    let memory_resource = 1usize << 30;
    let driver = EngineType::Kvm.get_driver();
    let (domain, queue) = prepare_engine_and_domain::<KvmMemoryDomain>(
        MemoryResource::Anonymous {
            size: memory_resource,
        },
        driver,
        vec![ComputeResource::CPU(0)],
    );

    let recorder = Recorder::new(zero_id(), Instant::now());
    let function_alternatives = vec![Arc::new(FunctionAlternative::new_unloaded(
        crate::machine_config::EngineType::Kvm,
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
    let expected = get_expected_mat(mat_size);
    assert_eq!(mat_size as i64, output[0]);
    for (should, is) in expected.iter().zip(output[1..].iter()) {
        assert_eq!(should, is);
    }
}

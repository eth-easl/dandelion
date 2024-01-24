use crate::{
    function_driver::{compute_driver::cheri::CheriDriver, Driver, Function, FunctionConfig},
    memory_domain::{malloc::MallocMemoryDomain, MemoryDomain},
    Position,
};

#[test]
fn test_loader_basic() {
    let elf_path = format!(
        "{}/tests/data/test_elf_cheri_basic",
        env!("CARGO_MANIFEST_DIR")
    );
    let mut malloc_domain =
        MallocMemoryDomain::init(Vec::new()).expect("Should be able to get malloc domain");
    let driver = CheriDriver {};
    let Function {
        requirements,
        context,
        config,
    } = driver
        .parse_function(elf_path, &mut malloc_domain)
        .expect("Parsing should work");
    // check requirement list to be list of programm header info for after load
    // meaning addresses and sizes in virtual address space
    let expected_requirements = vec![
        Position {
            offset: 0x00000,
            size: 0x1c8,
        },
        Position {
            offset: 0x101c8,
            size: 0xbb4,
        },
        Position {
            offset: 0x20d80,
            size: 0x18,
        },
        Position {
            offset: 0x30da0,
            size: 0x70,
        },
    ];
    // actual sizes in file
    let expected_sizes = vec![0x1c8, 0xbb4, 0x18, 0x0];
    assert_eq!(
        expected_requirements.len(),
        requirements.static_requirements.len(),
        "Requirements list lengths don't match"
    );
    for (index, (expected, actual)) in requirements
        .static_requirements
        .iter()
        .zip(expected_requirements.iter())
        .enumerate()
    {
        assert_eq!(
            expected.size, actual.size,
            "Static requirement size missmatch for index: {}",
            index
        );
        assert_eq!(
            expected.offset, actual.offset,
            "Static requirement offset missmatch for index: {}",
            index
        );
    }
    // check layout
    let mut expected_offset = 0;
    assert_eq!(1, context.content.len());
    let layout = &context.content[0]
        .as_ref()
        .expect("Set should be present")
        .buffers;
    for (index, item) in layout.into_iter().enumerate() {
        assert_eq!(
            expected_offset, item.data.offset,
            "Offset missmatch for item {}",
            index
        );
        assert_eq!(
            expected_sizes[index], item.data.size,
            "Size missmatch for item {}",
            index
        );
        expected_offset += expected_sizes[index];
    }
    // checks for config
    let function_config = match config {
        FunctionConfig::ElfConfig(conf_struct) => conf_struct,
        _ => panic!("Non elf FunctionConfig from cheri loader"),
    };
    assert_eq!(
        (0x30db8),
        function_config.system_data_offset,
        "System data offset missmatch"
    );
    assert_eq!(
        (0x30e00, 0x10),
        function_config.return_offset,
        "Return offset missmatch"
    );
    assert_eq!(
        0x101c8, function_config.entry_point,
        "Entry point missmatch"
    );
}

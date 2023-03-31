use std::vec;

use crate::{
    function_lib::{FunctionConfig, Navigator},
    DataItemType, DataRequirement, OffsetOrAlignment,
};

use super::CheriNavigator;
use crate::memory_domain::malloc::MallocMemoryDomain;

// basic navigator test
#[test]
#[should_panic]
fn test_navigator_empty() {
    // load elf file
    let elf_file = Vec::<u8>::new();
    let malloc_domain = MallocMemoryDomain {};
    CheriNavigator::parse_function(elf_file, &malloc_domain)
        .expect("Empty string should return error");
}

#[test]
fn test_navigator_basic() {
    // load elf file
    let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("tests/data/test_elf_le_64");
    let mut elf_file = std::fs::File::open(path).expect("Should have found test file");
    let mut elf_buffer = Vec::<u8>::new();
    use std::io::Read;
    assert_eq!(
        10400,
        elf_file
            .read_to_end(&mut elf_buffer)
            .expect("Should be able to read entire file")
    );
    let malloc_domain = MallocMemoryDomain {};
    let (req_list, _context, layout, config) =
        CheriNavigator::parse_function(elf_buffer, &malloc_domain)
            .expect("Empty string should return error");
    // check requirement list
    let expected_requirements = vec![
        DataRequirement {
            id: 0,
            req_type: crate::RequirementType::StaticData,
            position: Some(OffsetOrAlignment::Offset(0x400000)),
            size: Some(crate::SizeRequirement::Range(0x244, 0x244)),
        },
        DataRequirement {
            id: 1,
            req_type: crate::RequirementType::StaticData,
            position: Some(OffsetOrAlignment::Offset(0x401000)),
            size: Some(crate::SizeRequirement::Range(0x325, 0x325)),
        },
        DataRequirement {
            id: 2,
            req_type: crate::RequirementType::StaticData,
            position: Some(OffsetOrAlignment::Offset(0x402000)),
            size: Some(crate::SizeRequirement::Range(0x184, 0x184)),
        },
        DataRequirement {
            id: 3,
            req_type: crate::RequirementType::StaticData,
            position: Some(OffsetOrAlignment::Offset(0x404000)),
            size: Some(crate::SizeRequirement::Range(0x10, 0x10)),
        },
    ];
    assert_eq!(
        expected_requirements.len(),
        req_list.requirements.len(),
        "Requirements list lengths don't match"
    );
    for (id, (expected, actual)) in req_list
        .requirements
        .iter()
        .zip(expected_requirements.iter())
        .enumerate()
    {
        assert_eq!(expected, actual, "Requirements idx {} not matching.", id);
    }
    // check layout
    let expected_sizes = vec![0x244, 0x325, 0x184, 0x0];
    let mut expected_offset = 0;
    for (index, item) in layout.into_iter().enumerate() {
        assert_eq!(index, item.index as usize);
        let position = match item.item_type {
            DataItemType::Item(position) => position,
            _ => panic!("Layout returns non item data items to describe static context"),
        };
        assert_eq!(
            expected_offset, position.offset,
            "Offset missmatch for item {}",
            index
        );
        assert_eq!(
            expected_sizes[index], position.size,
            "Size missmatch for item {}",
            index
        );
        expected_offset += expected_sizes[index];
    }
    // checks for config
    let function_config = match config {
        FunctionConfig::ElfConfig(conf_struct) => conf_struct,
        _ => panic!("Non elf FunctionConfig from cheri navigator"),
    };
    assert_eq!(
        (0x402000, 0x8),
        function_config.input_root,
        "Input root offset or size missmatch"
    );
    assert_eq!(
        (0x404000, 0x8),
        function_config.output_root,
        "Output root offset or size missmatch"
    );
    assert_eq!(
        (0x402008, 0x4),
        function_config.input_number,
        "Input number offset or size missmatch"
    );
    assert_eq!(
        (0x404008, 0x4),
        function_config.output_number,
        "Output number offset or size missmatch"
    );
    assert_eq!(
        (0x40200c, 0x4),
        function_config.max_output_number,
        "Max output number offset or size missmatch"
    );
    assert_eq!(0x401000, function_config.entry_point);
}

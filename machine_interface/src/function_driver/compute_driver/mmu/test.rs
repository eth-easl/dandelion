use crate::{
    function_driver::{compute_driver::mmu::MmuDriver, Driver, Function, FunctionConfig},
    Position,
};

#[test_log::test]
fn test_loader_basic() {
    let elf_path = format!(
        "{}/tests/data/test_elf_mmu_{}_basic",
        env!("CARGO_MANIFEST_DIR"),
        std::env::consts::ARCH
    );
    let driver = MmuDriver {};
    let binary_data = std::fs::read(elf_path).unwrap();
    let Function {
        requirements,
        static_data: _,
        config,
    } = driver
        .parse_function(&binary_data)
        .expect("Should correctly parse elf file");
    // check requirement list
    #[cfg(target_arch = "x86_64")]
    let expected_requirements = vec![
        Position {
            offset: 0x10000,
            size: 0x62c,
        },
        Position {
            offset: 0x11630,
            size: 0x1908,
        },
        Position {
            offset: 0x13f38,
            size: 0xc8,
        },
        Position {
            offset: 0x14f38,
            size: 0x180,
        },
    ];
    #[cfg(target_arch = "aarch64")]
    let expected_requirements = vec![
        Position {
            offset: 0x10000,
            size: 0x1d1,
        },
        Position {
            offset: 0x201d4,
            size: 0x19bc,
        },
        Position {
            offset: 0x31b90,
            size: 0x470,
        },
        Position {
            offset: 0x41ba0,
            size: 0x180,
        },
    ];
    // actual sizes in file
    #[cfg(target_arch = "x86_64")]
    let expected_sizes = vec![0x62c, 0x1908, 0x0, 0x58];
    #[cfg(target_arch = "aarch64")]
    let expected_sizes = vec![0x1d1, 0x19bc, 0x10, 0x58];
    assert_eq!(
        expected_requirements.len(),
        requirements.static_requirements.len(),
        "Requirements list lengths don't match"
    );
    let mut expected_offset = 0;
    for (index, ((actual_requirement, actual_position), expected_requirement)) in requirements
        .static_requirements
        .iter()
        .zip(expected_requirements.iter())
        .enumerate()
    {
        assert_eq!(
            expected_requirement.size, actual_requirement.size,
            "Static requirement size missmatch for index: {}",
            index
        );
        assert_eq!(
            expected_requirement.offset, actual_requirement.offset,
            "Static requirement offset missmatch for index: {}",
            index
        );
        assert_eq!(
            expected_offset, actual_position.offset,
            "Offset missmatch for item {}",
            index
        );
        assert_eq!(
            expected_sizes[index], actual_position.size,
            "Size missmatch for item {}",
            index
        );
        expected_offset += expected_sizes[index];
    }

    // checks for config
    let function_config = match config {
        FunctionConfig::ElfConfig(conf_struct) => conf_struct,
        _ => panic!("Non elf FunctionConfig from mmu loader"),
    };
    #[cfg(target_arch = "x86_64")]
    assert_eq!(
        0x14f48, function_config.system_data_offset,
        "System data offset missmatch"
    );
    #[cfg(target_arch = "aarch64")]
    assert_eq!(
        0x41bb0, function_config.system_data_offset,
        "System data offset missmatch"
    );
    #[cfg(feature = "cheri")]
    assert_eq!(
        (0x0, 0x0),
        function_config.return_offset,
        "Return offset missmatch"
    );
    #[cfg(target_arch = "x86_64")]
    assert_eq!(
        0x11630, function_config.entry_point,
        "Entry point missmatch"
    );
    #[cfg(target_arch = "aarch64")]
    assert_eq!(
        0x201d4, function_config.entry_point,
        "Entry point missmatch"
    );
}

use crate::{
    function_driver::{
        compute_driver::pagetable::PagetableDriver, Driver, Function, FunctionConfig,
    },
    memory_domain::{pagetable::PagetableMemoryDomain, MemoryDomain},
    Position,
};

fn read_file(name: &str) -> Vec<u8> {
    // load elf file
    let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("tests/data");
    path.push(name);
    let mut elf_file = std::fs::File::open(path).expect("Should have found test file");
    let mut elf_buffer = Vec::<u8>::new();
    use std::io::Read;
    let _ = elf_file
        .read_to_end(&mut elf_buffer)
        .expect("Should be able to read entire file");
    return elf_buffer;
}

#[test]
fn test_loader_basic() {
    let elf_buffer = read_file(&format!(
        "test_elf_pagetable_{}_basic",
        std::env::consts::ARCH
    ));
    let driver = PagetableDriver {};
    let mut pagetable_domain =
        PagetableMemoryDomain::init(Vec::new()).expect("Should be able to get pagetable domain");
    let Function {
        requirements,
        context,
        config,
    } = driver
        .parse_function(elf_buffer, &mut pagetable_domain)
        .expect("Should correctly parse elf file");
    // check requirement list
    #[cfg(target_arch = "x86_64")]
    let expected_requirements = vec![
        Position {
            offset: 0x200000,
            size: 0x6bc,
        },
        Position {
            offset: 0x2016c0,
            size: 0xda8,
        },
        Position {
            offset: 0x203468,
            size: 0x10,
        },
        Position {
            offset: 0x204478,
            size: 0x60,
        },
    ];
    #[cfg(target_arch = "aarch64")]
    let expected_requirements = vec![
        Position {
            offset: 0x200000,
            size: 0x634,
        },
        Position {
            offset: 0x210634,
            size: 0xf40,
        },
        Position {
            offset: 0x221578,
            size: 0x10,
        },
        Position {
            offset: 0x231588,
            size: 0x60,
        },
    ];
    assert_eq!(
        0x800_0000, requirements.size,
        "Missmatch in expected default context size"
    );
    // actual sizes in file
    #[cfg(target_arch = "x86_64")]
    let expected_sizes = vec![0x6bc, 0xda8, 0x10, 0x0];
    #[cfg(target_arch = "aarch64")]
    let expected_sizes = vec![0x634, 0xf40, 0x10, 0x0];
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
        _ => panic!("Non elf FunctionConfig from pagetable loader"),
    };
    #[cfg(target_arch = "x86_64")]
    assert_eq!(
        0x204490, function_config.system_data_offset,
        "System data offset missmatch"
    );
    #[cfg(target_arch = "aarch64")]
    assert_eq!(
        0x2315A0, function_config.system_data_offset,
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
        0x2016c0, function_config.entry_point,
        "Entry point missmatch"
    );
    #[cfg(target_arch = "aarch64")]
    assert_eq!(
        0x210634, function_config.entry_point,
        "Entry point missmatch"
    );
}

use crate::{
    function_lib::{cheri::CheriLoader, FunctionConfig, Loader},
    memory_domain::{malloc::MallocMemoryDomain, MemoryDomain},
    Position,
};

fn read_file(name: &str, expected_size: usize) -> Vec<u8> {
    // load elf file
    let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("tests/data");
    path.push(name);
    let mut elf_file = std::fs::File::open(path).expect("Should have found test file");
    let mut elf_buffer = Vec::<u8>::new();
    use std::io::Read;
    let file_length = elf_file
        .read_to_end(&mut elf_buffer)
        .expect("Should be able to read entire file");
    if expected_size != 0 {
        assert_eq!(expected_size, file_length);
    }
    return elf_buffer;
}

#[test]
fn test_loader_basic() {
    let elf_buffer = read_file("test_elf_cheri_basic", 6400);
    let mut malloc_domain =
        MallocMemoryDomain::init(Vec::new()).expect("Should be able to get malloc domain");
    let (req_list, context, config) =
        CheriLoader::parse_function(elf_buffer, &mut malloc_domain).expect("Parsing should work");
    // check requirement list to be list of programm header info for after load
    // meaning addresses and sizes in virtual address space
    let expected_requirements = vec![
        Position {
            offset: 0x200000,
            size: 0x49c,
        },
        Position {
            offset: 0x21049c,
            size: 0xac4,
        },
        Position {
            offset: 0x220f60,
            size: 0x70,
        },
    ];
    assert_eq!(
        0x800_0000, req_list.size,
        "Missmatch in expected default context size"
    );
    // actual sizes in file
    let expected_sizes = vec![0x49c, 0xac4, 0x0];
    assert_eq!(
        expected_requirements.len(),
        req_list.static_requirements.len(),
        "Requirements list lengths don't match"
    );
    for (index, (expected, actual)) in req_list
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
        (0x220f78),
        function_config.system_data_offset,
        "System data offset missmatch"
    );
    assert_eq!(
        (0x220fc0, 0x10),
        function_config.return_offset,
        "Return offset missmatch"
    );
    assert_eq!(
        0x21049c, function_config.entry_point,
        "Entry point missmatch"
    );
}

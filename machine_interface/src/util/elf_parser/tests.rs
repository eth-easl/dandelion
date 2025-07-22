use std::io::Read;
use std::path::PathBuf;
use std::{fs::File, vec};

use super::ParsedElf;

fn load_file(name: &str, fsize: usize) -> Vec<u8> {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("tests/data");
    path.push(name);
    let mut elf_file = File::open(path).expect("Should have found test file");
    let mut elf_buffer = Vec::<u8>::new();
    assert_eq!(
        fsize,
        elf_file
            .read_to_end(&mut elf_buffer)
            .expect("Should be able to read entire file")
    );
    return elf_buffer;
}

#[test]
fn check_layout_le_64() -> () {
    let elf_buffer = load_file("test_elf_le_64", 10400);
    let parsed_elf = ParsedElf::new(&elf_buffer).expect("Should be able to create parsed elf");
    // let (requirements, items) = parsed_elf.get_layout_pair();
    let layout = parsed_elf.get_layout_pair();
    assert_eq!(4, layout.len());
    // checks on the requirements and binary positions
    let virt_offset_list = vec![0x400000, 0x401000, 0x402000, 0x404000];
    let virt_size_list = vec![0x244, 0x325, 0x184, 0x10];
    let file_offset_list = vec![0x0, 0x1000, 0x2000, 0x0];
    let file_size_list = vec![0x244, 0x325, 0x184, 0x0];
    for (index, (requirement, position)) in layout.iter().enumerate() {
        assert_eq!(virt_offset_list[index], requirement.offset);
        assert_eq!(virt_size_list[index], requirement.size);
        assert_eq!(file_offset_list[index], position.offset);
        assert_eq!(file_size_list[index], position.size);
    }
}

#[test]
#[should_panic]
fn check_find_symbol_failure() -> () {
    let elf_buffer = load_file("test_elf_le_64", 10400);
    let parsed_elf = ParsedElf::new(&elf_buffer).expect("Should be able to create parsed elf");
    parsed_elf
        .get_symbol_by_name(&elf_buffer, "test")
        .expect("Should fail as there is no such symbol");
}

#[test]
fn check_find_symbol_success() -> () {
    let elf_buffer = load_file("test_elf_le_64", 10400);
    let parsed_elf = ParsedElf::new(&elf_buffer).expect("Should be able to create parsed elf");
    let (symbol_offset, symbol_size) = parsed_elf
        .get_symbol_by_name(&elf_buffer, "main")
        .expect("Should have symbol main");
    assert_eq!(0x401000, symbol_offset);
    assert_eq!(0x16a, symbol_size);
    let (symbol_offset, symbol_size) = parsed_elf
        .get_symbol_by_name(&elf_buffer, "getInputPointer")
        .expect("Should have symbol getInputPointer");
    assert_eq!(0x4011a9, symbol_offset);
    assert_eq!(0x31, symbol_size);
}

#[test]
fn check_entry() -> () {
    let elf_buffer = load_file("test_elf_le_64", 10400);
    let parsed_elf = ParsedElf::new(&elf_buffer).expect("Should be able to create parsed elf");
    assert_eq!(0x401000, parsed_elf.get_entry_point());
}

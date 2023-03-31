use std::io::Read;
use std::path::PathBuf;
use std::{fs::File, vec};

use crate::{OffsetOrAlignment, RequirementType, SizeRequirement};

use super::ParsedElf;

fn load_file(name: &str, fsize: usize) -> Vec<u8> {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("src/util/elf_parser");
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
    let (requirements, items) = parsed_elf.get_layout_pair();
    assert_eq!(4, requirements.requirements.len());
    // checks on the requirements
    let virt_offset_list = vec![0x400000, 0x401000, 0x402000, 0x404000];
    let virt_size_list = vec![0x244, 0x325, 0x184, 0x10];
    for (index, requirement) in requirements.requirements.iter().enumerate() {
        assert_eq!(index as u32, requirement.id);
        assert_eq!(RequirementType::StaticData, requirement.req_type);
        assert_eq!(
            Some(OffsetOrAlignment::Offset(virt_offset_list[index])),
            requirement.position
        );
        assert_eq!(
            Some(SizeRequirement::Range(
                virt_size_list[index],
                virt_size_list[index]
            )),
            requirement.size
        );
    }
    // checks on the data items
    assert_eq!(4, items.len());
    let file_offset_list = vec![0x0, 0x1000, 0x2000, 0x0];
    let file_size_list = vec![0x244, 0x325, 0x184, 0x0];
    for (index, position) in items.iter().enumerate() {
        assert_eq!(file_offset_list[index], position.offset);
        assert_eq!(file_size_list[index], position.size);
    }
}

use crate::Position;
use dandelion_commons::{DandelionError, DandelionResult};

// TODO: replace this with infomation in the ELF header
pub const DEFAULT_ALIGNMENT: usize = 4096;

macro_rules! parser_code {
    ($name: ident; $in_type: ty; $out_type: ty; $parser: ident; $increment: literal) => {
        fn $name(slice: &Vec<u8>, counter: &mut usize) -> $out_type {
            let mut subslice: [u8; $increment] = [0; $increment];
            subslice.copy_from_slice(&slice[*counter..*counter + $increment]);
            let val = <$in_type>::$parser(subslice);
            *counter += $increment;
            return val as $out_type;
        }
    };
}

parser_code!(parse_half_le; u16; u16; from_le_bytes; 2);
parser_code!(parse_half_be; u16; u16; from_be_bytes; 2);
parser_code!(parse_word_le; u32; u32; from_le_bytes; 4);
parser_code!(parse_word_be; u32; u32; from_be_bytes; 4);
parser_code!(parse_offset_le_32; u32; u64; from_le_bytes; 4);
parser_code!(parse_offset_be_32; u32; u64; from_be_bytes; 4);
parser_code!(parse_offset_le_64; u64; u64; from_le_bytes; 8);
parser_code!(parse_offset_be_64; u64; u64; from_be_bytes; 8);

struct ParserFuncs {
    parse_half: fn(&Vec<u8>, &mut usize) -> u16,
    parse_word: fn(&Vec<u8>, &mut usize) -> u32,
    parse_offset: fn(&Vec<u8>, &mut usize) -> u64,
}

struct ElfEhdr {
    _e_type: u16,
    _e_machine: u16,
    _e_version: u32,
    e_entry: u64, // Addr
    e_phoff: u64, // Off
    e_shoff: u64, // Off
    _e_flags: u32,
    e_ehsize: u16,
    e_phentsize: u16,
    e_phnum: u16,
    e_shentsize: u16,
    e_shnum: u16,
    e_shstrndx: u16,
}

fn parse_ehdr(file: &Vec<u8>, pf: &ParserFuncs) -> ElfEhdr {
    let mut counter = 0x10;
    ElfEhdr {
        _e_type: (pf.parse_half)(file, &mut counter),
        _e_machine: (pf.parse_half)(file, &mut counter),
        _e_version: (pf.parse_word)(file, &mut counter),
        e_entry: (pf.parse_offset)(file, &mut counter),
        e_phoff: (pf.parse_offset)(file, &mut counter),
        e_shoff: (pf.parse_offset)(file, &mut counter),
        _e_flags: (pf.parse_word)(file, &mut counter),
        e_ehsize: (pf.parse_half)(file, &mut counter),
        e_phentsize: (pf.parse_half)(file, &mut counter),
        e_phnum: (pf.parse_half)(file, &mut counter),
        e_shentsize: (pf.parse_half)(file, &mut counter),
        e_shnum: (pf.parse_half)(file, &mut counter),
        e_shstrndx: (pf.parse_half)(file, &mut counter),
    }
}

struct ElfPhdr {
    p_type: u32,
    p_flags: u32,
    p_offset: u64,
    p_vaddr: u64,
    _p_paddr: u64,
    p_filesz: u64,
    p_memsz: u64,
    _p_align: u64,
}

fn parse_phdr_table(
    file: &Vec<u8>,
    pf: &ParserFuncs,
    ehdr: &ElfEhdr,
    is_32_bit: bool,
) -> DandelionResult<Vec<ElfPhdr>> {
    let mut phdr_table = Vec::<ElfPhdr>::new();
    let entries = ehdr.e_phnum;
    if (ehdr.e_phoff + (ehdr.e_phnum * ehdr.e_phentsize) as u64) as usize > file.len() {
        return Err(DandelionError::MalformedConfig);
    }
    for entry in 0..entries {
        let mut offset: usize = (ehdr.e_phoff + (entry * ehdr.e_phentsize) as u64) as usize;
        if is_32_bit {
            phdr_table.push(ElfPhdr {
                p_type: (pf.parse_word)(file, &mut offset),
                p_offset: (pf.parse_offset)(file, &mut offset),
                p_vaddr: (pf.parse_offset)(file, &mut offset),
                _p_paddr: (pf.parse_offset)(file, &mut offset),
                p_filesz: (pf.parse_offset)(file, &mut offset),
                p_memsz: (pf.parse_offset)(file, &mut offset),
                p_flags: (pf.parse_word)(file, &mut offset),
                _p_align: (pf.parse_offset)(file, &mut offset),
            })
        } else {
            phdr_table.push(ElfPhdr {
                p_type: (pf.parse_word)(file, &mut offset),
                p_flags: (pf.parse_word)(file, &mut offset),
                p_offset: (pf.parse_offset)(file, &mut offset),
                p_vaddr: (pf.parse_offset)(file, &mut offset),
                _p_paddr: (pf.parse_offset)(file, &mut offset),
                p_filesz: (pf.parse_offset)(file, &mut offset),
                p_memsz: (pf.parse_offset)(file, &mut offset),
                _p_align: (pf.parse_offset)(file, &mut offset),
            })
        }
    }
    return Ok(phdr_table);
}

struct ElfShdr {
    sh_name: u32,
    sh_type: u32,
    _sh_flags: u64,
    _sh_addr: u64,
    sh_offset: u64,
    sh_size: u64,
    _sh_link: u32,
    _sh_info: u32,
    _sh_addralign: u64,
    sh_entsize: u64,
}

fn parse_shrd_table(
    file: &Vec<u8>,
    pf: &ParserFuncs,
    ehdr: &ElfEhdr,
) -> DandelionResult<Vec<ElfShdr>> {
    let mut shdr_table = Vec::<ElfShdr>::new();
    let entries = ehdr.e_shnum;
    if (ehdr.e_shoff + (ehdr.e_shnum * ehdr.e_shentsize) as u64) as usize > file.len() {
        return Err(DandelionError::MalformedConfig);
    }
    for entry in 0..entries {
        let mut offset: usize = (ehdr.e_shoff + (entry * ehdr.e_shentsize) as u64) as usize;
        shdr_table.push(ElfShdr {
            sh_name: (pf.parse_word)(file, &mut offset),
            sh_type: (pf.parse_word)(file, &mut offset),
            _sh_flags: (pf.parse_offset)(file, &mut offset),
            _sh_addr: (pf.parse_offset)(file, &mut offset),
            sh_offset: (pf.parse_offset)(file, &mut offset),
            sh_size: (pf.parse_offset)(file, &mut offset),
            _sh_link: (pf.parse_word)(file, &mut offset),
            _sh_info: (pf.parse_word)(file, &mut offset),
            _sh_addralign: (pf.parse_offset)(file, &mut offset),
            sh_entsize: (pf.parse_offset)(file, &mut offset),
        })
    }
    return Ok(shdr_table);
}

#[derive(Debug)]
struct ElfSym {
    st_name: u32,
    st_value: u64,
    st_size: u64,
    _st_info: u8,
    _st_other: u8,
    _st_shndx: u16,
}

fn parse_symbol_table(
    file: &Vec<u8>,
    pf: &ParserFuncs,
    shdr_table: &Vec<ElfShdr>,
    is_32_bit: bool,
) -> DandelionResult<Vec<ElfSym>> {
    let mut symbol_table = Vec::<ElfSym>::new();
    let symbol_table_section_opt = shdr_table.iter().find(|x| x.sh_type == SHT_SYMTAB);
    let symbol_table_section = match symbol_table_section_opt {
        Some(section) => section,
        None => return Err(DandelionError::MalformedConfig),
    };
    let table_start = symbol_table_section.sh_offset as usize;
    let table_end = table_start + symbol_table_section.sh_size as usize;
    let entry_size = symbol_table_section.sh_entsize as usize;
    if entry_size == 0 {
        return Err(DandelionError::MalformedConfig);
    }
    let entries = symbol_table_section.sh_size as usize / entry_size;
    if entries * entry_size + table_start != table_end {
        return Err(DandelionError::MalformedConfig);
    }
    let parse_uchar = |value: &mut usize| {
        *value += 1;
        return file[*value - 1];
    };
    for entry in 0..entries {
        let mut counter = table_start + entry * entry_size;
        let counter_start = counter;
        if is_32_bit {
            symbol_table.push(ElfSym {
                st_name: (pf.parse_word)(file, &mut counter),
                st_value: (pf.parse_offset)(file, &mut counter),
                st_size: (pf.parse_offset)(file, &mut counter),
                _st_info: parse_uchar(&mut counter),
                _st_other: parse_uchar(&mut counter),
                _st_shndx: (pf.parse_half)(file, &mut counter),
            })
        } else {
            symbol_table.push(ElfSym {
                st_name: (pf.parse_word)(file, &mut counter),
                _st_info: parse_uchar(&mut counter),
                _st_other: parse_uchar(&mut counter),
                _st_shndx: (pf.parse_half)(file, &mut counter),
                st_value: (pf.parse_offset)(file, &mut counter),
                st_size: (pf.parse_offset)(file, &mut counter),
            })
        }
        if counter > counter_start + entry_size {
            return Err(DandelionError::MalformedConfig);
        }
    }
    return Ok(symbol_table);
}

pub struct ParsedElf {
    ehdr: ElfEhdr,
    program_header_table: Vec<ElfPhdr>,
    section_header_table: Vec<ElfShdr>,
    symbol_table: Vec<ElfSym>,
}

const SHT_SYMTAB: u32 = 0x2;
const SHT_STRTAB: u32 = 0x3;

impl ParsedElf {
    pub fn new(file: &Vec<u8>) -> DandelionResult<Self> {
        if file.len() < 6 {
            return Err(DandelionError::MalformedConfig);
        }
        // check magic number
        if file[0] != 0x7F || file[1] != 0x45 || file[2] != 0x4c || file[3] != 0x46 {
            return Err(DandelionError::MalformedConfig);
        }
        let is_32_bit = file[0x4] == 1;
        let little_endian = file[0x5] == 1;
        if (is_32_bit && file.len() < 0x34) || (!is_32_bit && file.len() < 0x40) {
            return Err(DandelionError::MalformedConfig);
        }
        let pf = match (little_endian, is_32_bit) {
            (true, true) => ParserFuncs {
                parse_half: parse_half_le,
                parse_word: parse_word_le,
                parse_offset: parse_offset_le_32,
            },
            (true, false) => ParserFuncs {
                parse_half: parse_half_le,
                parse_word: parse_word_le,
                parse_offset: parse_offset_le_64,
            },
            (false, true) => ParserFuncs {
                parse_half: parse_half_be,
                parse_word: parse_word_be,
                parse_offset: parse_offset_be_32,
            },
            (false, false) => ParserFuncs {
                parse_half: parse_half_be,
                parse_word: parse_word_be,
                parse_offset: parse_offset_be_64,
            },
        };
        let ehdr = parse_ehdr(&file, &pf);
        if (is_32_bit && ehdr.e_ehsize != 0x34) || (!is_32_bit && ehdr.e_ehsize != 0x40) {
            return Err(DandelionError::MalformedConfig);
        }
        let phdr_table = parse_phdr_table(&file, &pf, &ehdr, is_32_bit)?;
        let shdr_table = parse_shrd_table(&file, &pf, &ehdr)?;
        let sym_table = parse_symbol_table(file, &pf, &shdr_table, is_32_bit)?;
        return Ok(ParsedElf {
            ehdr: ehdr,
            program_header_table: phdr_table,
            section_header_table: shdr_table,
            symbol_table: sym_table,
        });
    }
    pub fn get_layout_pair(&self) -> (Vec<Position>, Vec<Position>) {
        let mut items = Vec::<Position>::new();
        let mut requirements = Vec::<Position>::new();
        // go through sections and find the ones that need to be loaded
        for program_header in &self.program_header_table {
            // check if section occupies memory during execution
            if program_header.p_type == 0x1 {
                items.push(Position {
                    offset: program_header.p_offset as usize,
                    size: program_header.p_filesz as usize,
                });
                requirements.push(Position {
                    offset: program_header.p_vaddr as usize,
                    size: program_header.p_memsz as usize,
                });
            }
        }

        return (requirements, items);
    }

    pub fn get_memory_protection_layout(&self) -> Vec<(u32, Position)> {
        let mut protection_requirement = Vec::<(u32, Position)>::new();
        for program_header in &self.program_header_table {
            // check if section occupies memory during execution
            if program_header.p_type == 0x1 {
                let mut start = program_header.p_vaddr as usize;
                let mut end = start + program_header.p_memsz as usize;
                if start % DEFAULT_ALIGNMENT != 0 {
                    start -= start % DEFAULT_ALIGNMENT;
                }
                if end % DEFAULT_ALIGNMENT != 0 {
                    end += DEFAULT_ALIGNMENT - end % DEFAULT_ALIGNMENT;
                }
                protection_requirement.push((
                    program_header.p_flags,
                    Position {
                        offset: start,
                        size: end - start,
                    },
                ));
            }
        }
        protection_requirement
    }

    pub fn get_symbol_by_name(
        &self,
        file: &Vec<u8>,
        name: &str,
    ) -> DandelionResult<(usize, usize)> {
        // find section header string table
        let section_name_entry = &self.section_header_table[self.ehdr.e_shstrndx as usize];
        let section_names_start = section_name_entry.sh_offset as usize;
        let section_names_end = section_names_start + section_name_entry.sh_size as usize;
        let section_name_string =
            match std::str::from_utf8(&file[section_names_start..section_names_end]) {
                Ok(str) => str,
                Err(_) => return Err(DandelionError::MalformedConfig),
            };
        let string_table_index = match section_name_string.find(".strtab") {
            Some(index) => index,
            None => return Err(DandelionError::MalformedConfig),
        };
        let string_table_section_opt = self
            .section_header_table
            .iter()
            .find(|x| x.sh_type == SHT_STRTAB && x.sh_name as usize == string_table_index);
        let string_table_section = match string_table_section_opt {
            Some(section) => section,
            None => return Err(DandelionError::MalformedConfig),
        };

        let string_table_start = string_table_section.sh_offset as usize;
        let string_table_end = string_table_start + string_table_section.sh_size as usize;
        let string_table_string =
            match std::str::from_utf8(&file[string_table_start..string_table_end]) {
                Ok(str) => str,
                Err(_) => return Err(DandelionError::MalformedConfig),
            };
        let string_table_index = string_table_string.find(name);
        let name_index = match string_table_index {
            Some(index) => index,
            None => return Err(DandelionError::UnknownSymbol),
        };
        let symbol = self
            .symbol_table
            .iter()
            .find(|sym| sym.st_name as usize == name_index);
        return match symbol {
            Some(sym) => return Ok((sym.st_value as usize, sym.st_size as usize)),
            None => Err(DandelionError::MalformedConfig),
        };
    }

    pub fn get_entry_point(&self) -> usize {
        return self.ehdr.e_entry as usize;
    }
}

#[cfg(test)]
mod tests;

use crate::function_lib::Navigator;

use super::CheriNavigator;
use crate::memory_domain::malloc::MallocMemoryDomain;

// basic navigator test
#[test]
fn test_navigator_basic() {
    // load elf file
    let elf_file = Vec::<u8>::new();
    let malloc_domain = MallocMemoryDomain {};
    CheriNavigator::parse_function(elf_file, &malloc_domain);
}

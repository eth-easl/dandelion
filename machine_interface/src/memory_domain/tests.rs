use std::vec;

use super::super::HardwareError;
use super::{ContextTrait, MemoryDomain};
// produces binary pattern 0b0101_01010 or 0x55
const BYTEPATTERN: u8 = 85;

fn init_domain<D: MemoryDomain>(arg: Vec<u8>) -> Box<D> {
    let init_result = D::init(arg);
    let domain = init_result.expect("memory domain should have been initialized");
    return domain;
}

fn write_single_oob_offset<D: MemoryDomain>(arg: Vec<u8>) {
    let domain: Box<D> = init_domain(arg);
    let mut context = domain
        .acquire_context(1)
        .expect("Single byte context should always be allocatable");
    assert_eq!(
        Err(HardwareError::InvalidWrite),
        context.write(1, vec![BYTEPATTERN])
    );
}

fn write_single_oob_size<D: MemoryDomain>(arg: Vec<u8>) {
    let domain: Box<D> = init_domain(arg);
    let mut context = domain
        .acquire_context(1)
        .expect("Single byte context should always be allocatable");
    assert_eq!(
        Err(HardwareError::InvalidWrite),
        context.write(0, vec![BYTEPATTERN, BYTEPATTERN])
    );
}

fn read_single_success<D: MemoryDomain>(arg: Vec<u8>) {
    let domain: Box<D> = init_domain(arg);
    let mut context = domain
        .acquire_context(1)
        .expect("Single byte context should always be allocatable");
    context
        .write(0, vec![BYTEPATTERN])
        .expect("Writing should succeed");
    let return_val = context
        .read(0, 1, false)
        .expect("context should return single value vector in range");
    assert_eq!(vec![BYTEPATTERN], return_val);
}

fn read_single_oob_offset<D: MemoryDomain>(arg: Vec<u8>) {
    let domain: Box<D> = init_domain(arg);
    let mut context = domain
        .acquire_context(1)
        .expect("Single byte context should always be allocatable");
    context
        .write(0, vec![BYTEPATTERN])
        .expect("Writing should succeed");
    assert_eq!(Err(HardwareError::InvalidRead), context.read(1, 1, false));
}

fn read_single_oob_size<D: MemoryDomain>(arg: Vec<u8>) {
    let domain: Box<D> = init_domain(arg);
    let mut context = domain
        .acquire_context(1)
        .expect("Single byte context should always be allocatable");
    context
        .write(0, vec![BYTEPATTERN])
        .expect("Writing should succeed");
    assert_eq!(Err(HardwareError::InvalidRead), context.read(0, 2, false));
}

fn read_single_sanitize<D: MemoryDomain>(arg: Vec<u8>) {
    let domain: Box<D> = init_domain(arg);
    let mut context = domain
        .acquire_context(1)
        .expect("Single byte context should always be allocatable");
    context
        .write(0, vec![BYTEPATTERN])
        .expect("Writing should succeed");
    let return_val = context
        .read(0, 1, true)
        .expect("context should return single value vector in range");
    assert_eq!(vec![BYTEPATTERN], return_val);
    let sanitized_val = context
        .read(0, 1, false)
        .expect("context should return single value vector in range");
    assert_eq!(vec![0], sanitized_val);
}

macro_rules! controllerReadWriteTests {
    ($name : ident ; $controller : ty ; $init : expr) => {
        mod $name {
            use super::*;
            #[test]
            fn test_write_single_oob_offset() {
                write_single_oob_offset::<$controller>($init);
            }
            #[test]
            fn test_write_single_oob_size() {
                write_single_oob_size::<$controller>($init);
            }
            #[test]
            fn test_read_single_success() {
                read_single_success::<$controller>($init);
            }
            #[test]
            fn test_read_single_oob_offset() {
                read_single_oob_offset::<$controller>($init);
            }
            #[test]
            fn test_read_single_oob_size() {
                read_single_oob_size::<$controller>($init);
            }
            #[test]
            fn test_read_single_sanitize() {
                read_single_sanitize::<$controller>($init);
            }
            // TODO longer writes and reads, as well as partial writes and reads
        }
    };
}
use super::malloc::MallocMemoryDomain as mallocType;
controllerReadWriteTests!(Malloc; mallocType; Vec::new());
use super::cheri::CheriMemoryDomain as cheriType;
controllerReadWriteTests!(Cheri; cheriType; Vec::new());

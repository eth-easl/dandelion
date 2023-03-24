use std::vec;

use super::super::HardwareError;
use super::{ContextTrait, MemoryDomain};
// produces binary pattern 0b0101_01010 or 0x55
const BYTEPATTERN: u8 = 85;

fn acquire_single<D: MemoryDomain>(arg: Vec<u8>) -> () {
    let init_result = D::init(arg);
    let domain = init_result.expect("should have initialized memory domain");
    let context_result = domain.acquire_context(1);
    match context_result {
        Ok(_) => assert!(true),
        Err(err) => assert!(false, "found {:?} when acquiring minimal context", err),
    }
}

fn acquire_too_much<D: MemoryDomain>(arg: Vec<u8>) -> () {
    let alloc_size = usize::MAX;
    let init_result = D::init(arg);
    let domain = init_result.expect("should have initialized memory domain");
    let context_result = domain.acquire_context(alloc_size);
    match context_result {
        Err(HardwareError::OutOfMemory) => assert!(true),
        Ok(_) => assert!(
            false,
            "Got okay for allocating context with size {}",
            alloc_size
        ),
        Err(err) => assert!(
            false,
            "Got wrong error for allocating context with size {}: {:?}",
            alloc_size, err
        ),
    }
}

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

macro_rules! domainTests {
    ($name : ident ; $domain : ty ; $init : expr) => {
        mod $name {
            use super::*;
            // domain tests
            #[test]
            fn test_aquire_single() {
                acquire_single::<$domain>($init);
            }
            #[test]
            fn test_aquire_too_much() {
                acquire_too_much::<$domain>($init);
            }
            // context tests
            #[test]
            fn test_write_single_oob_offset() {
                write_single_oob_offset::<$domain>($init);
            }
            #[test]
            fn test_write_single_oob_size() {
                write_single_oob_size::<$domain>($init);
            }
            #[test]
            fn test_read_single_success() {
                read_single_success::<$domain>($init);
            }
            #[test]
            fn test_read_single_oob_offset() {
                read_single_oob_offset::<$domain>($init);
            }
            #[test]
            fn test_read_single_oob_size() {
                read_single_oob_size::<$domain>($init);
            }
            #[test]
            fn test_read_single_sanitize() {
                read_single_sanitize::<$domain>($init);
            }
            // TODO longer writes and reads, as well as partial writes and reads
        }
    };
}
use super::malloc::MallocMemoryDomain as mallocType;
domainTests!(malloc; mallocType; Vec::new());
use super::cheri::CheriMemoryDomain as cheriType;
domainTests!(cheri; cheriType; Vec::new());

use std::vec;

use crate::memory_domain::{transefer_memory, transfer_data_set, ContextTrait, MemoryDomain};
use dandelion_commons::{DandelionError, DandelionResult};
// produces binary pattern 0b0101_01010 or 0x55
const BYTEPATTERN: u8 = 85;

fn acquire<D: MemoryDomain>(arg: Vec<u8>, acquisition_size: usize, expect_success: bool) -> () {
    let init_result = D::init(arg);
    let domain = init_result.expect("should have initialized memory domain");
    let context_result = domain.acquire_context(acquisition_size);
    match (expect_success, context_result) {
        (true, Ok(_)) | (false, Err(DandelionError::OutOfMemory)) => assert!(true),
        (false, Ok(_)) => assert!(
            false,
            "Got okay for allocating context with size {}",
            acquisition_size
        ),
        (_, Err(err)) => assert!(
            false,
            "Encountered unexpected error when acquireing context: {:?}",
            err
        ),
    }
}

fn init_domain<D: MemoryDomain>(arg: Vec<u8>) -> Box<dyn MemoryDomain> {
    let init_result = D::init(arg);
    let domain = init_result.expect("memory domain should have been initialized");
    return domain;
}

fn write<D: MemoryDomain>(
    arg: Vec<u8>,
    context_size: usize,
    offset: usize,
    size: usize,
    expect_success: bool,
) {
    let domain = init_domain::<D>(arg);
    let mut context = domain
        .acquire_context(context_size)
        .expect("Single byte context should always be allocatable");
    let write_error = context.write(offset, &vec![BYTEPATTERN; size]);
    match (expect_success, write_error) {
        (false, Err(DandelionError::InvalidWrite)) | (true, Ok(())) => (),
        (false, Ok(())) => panic!("Unexpected write success"),
        (_, Err(err)) => panic!("Unexpected write error: {:?}", err),
    }
}

fn read<D: MemoryDomain>(
    arg: Vec<u8>,
    context_size: usize,
    offset: usize,
    size: usize,
    expect_success: bool,
) {
    let domain = init_domain::<D>(arg);
    let mut context = domain
        .acquire_context(context_size)
        .expect("Context should always be allocatable");
    context
        .write(0, &vec![BYTEPATTERN; context_size])
        .expect("Writing should succeed");
    let mut read_buffer = vec![0; size];
    let read_error = context.read(offset, &mut read_buffer);
    match (expect_success, read_error) {
        (true, Ok(())) => assert_eq!(vec![BYTEPATTERN; size], read_buffer),
        (false, Ok(())) => panic!("Unexpected ok from read that should fail with context size: {}, read offset: {}, read size: {}", context_size, offset, size),
        (false, Err(DandelionError::InvalidRead)) => (),
        (_, Err(err)) => panic!("Unexpected error while reading: {:?}", err),
    }
}

fn transefer<D: MemoryDomain>(arg: Vec<u8>, size: usize) {
    let domain = init_domain::<D>(arg);
    let mut destination = domain
        .acquire_context(size)
        .expect("Context should be allocatable");
    let mut source = domain
        .acquire_context(size)
        .expect("Context should be allocatable");
    source
        .write(0, &vec![BYTEPATTERN; size])
        .expect("Writing should succeed");
    transefer_memory(&mut destination, &mut source, 0, 0, size)
        .expect("Should successfully transfer");
    let mut read_buffer = vec![0; size];
    destination
        .read(0, &mut read_buffer)
        .expect("Context should return single value vector in range");
    assert_eq!(vec![BYTEPATTERN; size], read_buffer);
}

fn transfer_item<D: MemoryDomain>(
    arg: Vec<u8>,
    context_size: usize,
    offset: usize,
    item_size: usize,
    source_index: usize,
    destination_index: usize,
    expect_result: DandelionResult<()>,
) {
    let domain = init_domain::<D>(arg);
    let mut destination = domain
        .acquire_context(context_size)
        .expect("Context should be allocatable");
    let mut source = domain
        .acquire_context(context_size)
        .expect("Context should be allocatable");
    source
        .write(offset, &vec![BYTEPATTERN; item_size])
        .expect("Writing should succeed");
    if source.content.len() <= source_index {
        source.content.resize_with(source_index + 1, || None);
    }
    source.content[source_index] = Some(crate::DataSet {
        ident: String::from(""),
        buffers: vec![crate::DataItem {
            ident: String::from(""),
            data: crate::Position {
                offset: offset,
                size: item_size,
            },
            key: 0,
        }],
    });
    let set_name = "";
    let transfer_error = transfer_data_set(
        &mut destination,
        &source,
        destination_index,
        8,
        &set_name,
        source_index,
    );
    assert_eq!(transfer_error, expect_result);
    if expect_result.is_err() {
        return;
    }
    // check transfer success
    assert!(destination_index < destination.content.len());
    let destination_item = destination.content[destination_index]
        .as_ref()
        .expect("Set should be present");
    assert_eq!("", destination_item.ident);
    assert_eq!(1, destination_item.buffers.len());
    assert_eq!("", destination_item.buffers[0].ident);
    assert_eq!(item_size, destination_item.buffers[0].data.size);
    let read_offset = destination_item.buffers[0].data.offset;
    let mut read_buffer = vec![0; item_size];
    destination
        .read(read_offset, &mut read_buffer)
        .expect("Context should be readable at item position");
    assert_eq!(vec![BYTEPATTERN; item_size], read_buffer);
}

// TODO make tests sweep ranges
macro_rules! domainTests {
    ($name : ident ; $domain : ty ; $init : expr) => {
        mod $name {
            use super::*;
            // domain tests
            #[test]
            fn test_aquire_success() {
                acquire::<$domain>($init, 1, true);
            }
            #[test]
            fn test_aquire_failure() {
                acquire::<$domain>($init, usize::MAX, false);
            }
            // context tests
            #[test]
            fn test_read_single_success() {
                read::<$domain>($init, 1, 0, 1, true);
            }
            #[test]
            fn test_read_single_oob_offset() {
                read::<$domain>($init, 1, 1, 1, false);
            }
            #[test]
            fn test_read_single_oob_size() {
                read::<$domain>($init, 1, 0, 2, false);
            }
            #[test]
            fn test_write_single_oob_offset() {
                write::<$domain>($init, 1, 1, 1, false);
            }
            #[test]
            fn test_write_single_oob_size() {
                write::<$domain>($init, 1, 0, 2, false);
            }
            #[test]
            fn test_transfer_single() {
                transefer::<$domain>($init, 1);
            }
            #[test]
            fn test_transfer_page() {
                transefer::<$domain>($init, 4096);
            }
            #[test]
            fn test_transfer_dataitem_item() {
                transfer_item::<$domain>($init, 4096, 128, 256, 1, 2, Ok(()));
            }
            // TODO
            // #[test]
            // fn test_transfer_dataitem_set() {
            //     transfer_set::<$domain>($init, 4096, 128, 256, 1, 2, Ok(()));
            // }
        }
    };
}
use super::malloc::MallocMemoryDomain as mallocType;
domainTests!(malloc; mallocType; Vec::new());
#[cfg(feature = "cheri")]
use super::cheri::CheriMemoryDomain as cheriType;
#[cfg(feature = "cheri")]
domainTests!(cheri; cheriType; Vec::new());
#[cfg(feature = "mmu")]
use super::mmu::MmuMemoryDomain as mmuType;
#[cfg(feature = "mmu")]
domainTests!(mmu; mmuType; Vec::new());
#[cfg(feature = "wasm")]
use super::wasm::WasmMemoryDomain as wasmType;
#[cfg(feature = "wasm")]
domainTests!(wasm; wasmType; Vec::new());
#[cfg(any(feature = "wasmtime-jit", feature = "wasmtime-precompiled"))]
use super::wasmtime::WasmtimeMemoryDomain as wasmtimeType;
#[cfg(any(feature = "wasmtime-jit", feature = "wasmtime-precompiled"))]
domainTests!(wasmtime; wasmtimeType; Vec::new());
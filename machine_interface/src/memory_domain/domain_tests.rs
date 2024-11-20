use std::vec;
use std::sync::Arc;

use crate::memory_domain::{
    transfer_data_set, transfer_memory, Context, ContextTrait, ContextType, MemoryDomain, MemoryResource
};
use log::debug;
use crate::memory_domain::system_domain::SystemMemoryDomain;
use dandelion_commons::{DandelionError, DandelionResult};
// produces binary pattern 0b0101_01010 or 0x55
const BYTEPATTERN: u8 = 85;

/// Test whether a context can be acquired, and panics if the success
/// does not match `expect_success`.
fn try_acquire<D: MemoryDomain>(
    arg: MemoryResource,
    acquisition_size: usize,
    expect_success: bool,
) {
    let init_result = D::init(arg);
    let domain = init_result.expect("should have initialized memory domain");
    let context_result = domain.acquire_context(acquisition_size);
    // let ctx = if let Ok(ctx) = context_result {ctx} else {panic!("")};
    // panic!("Tried acquiring context of size {}. Gotten size: {}", acquisition_size, ctx.size);
    match (expect_success, context_result) {
        (true, Ok(_))
        | (false, Err(DandelionError::OutOfMemory))
        | (false, Err(DandelionError::InvalidMemorySize))
        | (false, Err(DandelionError::MemoryAllocationError)) => assert!(true),
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

/// Acquire a context with a given size and return it. Will panic if the
/// context cannot be acquired.
fn acquire<D: MemoryDomain>(arg: MemoryResource, size: usize) -> Context {
    let domain = init_domain::<D>(arg);
    let context = domain
        .acquire_context(size)
        .expect("Context should be allocatable");
    return context;
}

fn init_domain<D: MemoryDomain>(arg: MemoryResource) -> Box<dyn MemoryDomain> {
    let init_result = D::init(arg);
    let domain = init_result.expect("memory domain should have been initialized");
    return domain;
}

fn write(ctx: &mut Context, offset: usize, size: usize, expect_success: bool) {
    let write_error = ctx.write(offset, &vec![BYTEPATTERN; size]);
    match (expect_success, write_error) {
        (false, Err(DandelionError::InvalidWrite)) | (true, Ok(())) => (),
        (false, Ok(())) => panic!("Unexpected write success"),
        (_, Err(err)) => panic!("Unexpected write error: {:?}", err),
    }
}

fn read(ctx: &mut Context, offset: usize, size: usize, expect_success: bool) {
    ctx.write(0, &vec![BYTEPATTERN; ctx.size])
        .expect("Writing should succeed");
    let mut read_buffer = vec![0; size];
    let read_error = ctx.read(offset, &mut read_buffer);
    match (expect_success, read_error) {
        (true, Ok(())) => assert_eq!(vec![BYTEPATTERN; size], read_buffer),
        (false, Ok(())) => panic!("Unexpected ok from read that should fail with context size: {}, read offset: {}, read size: {}", ctx.size, offset, size),
        (false, Err(DandelionError::InvalidRead)) => (),
        (_, Err(err)) => panic!("Unexpected error while reading: {:?}", err),
    }
}

fn read_system_context(system_ctx: &mut Context, base_ctx: Context, offset: usize, size: usize, expect_success: bool) {
    let _ = transfer_memory(system_ctx, Arc::from(base_ctx), 0, 0, system_ctx.size);

    let mut read_buffer = vec![0; size];
    let read_error = system_ctx.read(offset, &mut read_buffer);
    match (expect_success, read_error) {
        (true, Ok(())) => assert_eq!(vec![BYTEPATTERN; size], read_buffer),
        (false, Ok(())) => panic!("Unexpected ok from read that should fail with context size: {}, read offset: {}, read size: {}", system_ctx.size, offset, size),
        (false, Err(DandelionError::InvalidRead)) => (),
        (_, Err(err)) => panic!("Unexpected error while reading: {:?}", err),
    }
}

fn get_chunks(ctx: &mut Context, offset: usize, size: usize, expect_success: bool) {
    if expect_success {
        ctx.write(offset, &vec![BYTEPATTERN; size])
            .expect("Writing should succeed");
    }
    let mut total_read = 0usize;
    while total_read < size {
        let chunk_ref_result = ctx.get_chunk_ref(offset + total_read, size - total_read);
        match (expect_success, chunk_ref_result) {
            (true, Ok(chunk_ref)) => {
                assert_eq!(&vec![BYTEPATTERN; chunk_ref.len()], chunk_ref);
                total_read += chunk_ref.len()
            }
            (false, Ok(_)) => panic!("Unexpected ok from get_chunk_ref"),
            (false, Err(DandelionError::InvalidRead)) => return,
            (_, Err(err)) => panic!("Unexpected error from get_chunk_ref {:?}", err),
        }
    }
}

fn get_chunks_system_context(system_ctx: &mut Context, base_ctx: Context, offset: usize, size: usize, expect_success: bool) {
    let _ = transfer_memory(system_ctx, Arc::from(base_ctx), 0, 0, system_ctx.size);

    let mut total_read = 0usize;
    while total_read < size {
        let chunk_ref_result = system_ctx.get_chunk_ref(offset + total_read, size - total_read);
        match (expect_success, chunk_ref_result) {
            (true, Ok(chunk_ref)) => {
                assert_eq!(&vec![BYTEPATTERN; chunk_ref.len()], chunk_ref);
                total_read += chunk_ref.len()
            }
            (false, Ok(_)) => panic!("Unexpected ok from get_chunk_ref"),
            (false, Err(DandelionError::InvalidRead)) => return,
            (_, Err(err)) => panic!("Unexpected error from get_chunk_ref {:?}", err),
        }
    }
}

fn transfer(source: Box<Context>, destination: Box<Context>) {
    // If destination is a context, we assume that they are both 
    // initialised to the same size. This is because context may be larger
    // than their requested size

    let mut size = source.size;

    match &destination.context{
        ContextType::System(_) => {
            size = destination.size;
        }
        _ => {
            assert!(size == destination.size);
        }
    }

    let mut source_mut = *source;
    let mut destination_mut = *destination;
    source_mut
        .write(0, &vec![BYTEPATTERN; size])
        .expect("Writing should succeed");
    let source_ctxt = Arc::new(source_mut);
    transfer_memory(&mut destination_mut, source_ctxt, 0, 0, size).expect("Should successfully transfer");
    let mut read_buffer = vec![0; size];
    destination_mut
        .read(0, &mut read_buffer)
        .expect("Context should return single value vector in range");
    assert_eq!(vec![BYTEPATTERN; size], read_buffer);
}

fn transfer_item(
    source: Box<Context>,
    destination: Box<Context>,
    offset: usize,
    item_size: usize,
    source_index: usize,
    destination_index: usize,
    expect_result: DandelionResult<()>,
) {
    let mut source_mut = *source;
    let mut destination_mut = *destination;
    source_mut
        .write(offset, &vec![BYTEPATTERN; item_size])
        .expect("Writing should succeed");
    if source_mut.content.len() <= source_index {
        source_mut.content.resize_with(source_index + 1, || None);
    }
    source_mut.content[source_index] = Some(crate::DataSet {
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
        &mut destination_mut,
        Arc::new(source_mut),
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
    assert!(destination_index < destination_mut.content.len());
    let destination_item = destination_mut.content[destination_index]
        .as_ref()
        .expect("Set should be present");
    assert_eq!("", destination_item.ident);
    assert_eq!(1, destination_item.buffers.len());
    assert_eq!("", destination_item.buffers[0].ident);
    assert_eq!(item_size, destination_item.buffers[0].data.size);
    let read_offset = destination_item.buffers[0].data.offset;
    let mut read_buffer = vec![0; item_size];
    destination_mut
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
                try_acquire::<$domain>($init, 1, true);
            }
            #[test]
            fn test_aquire_failure() {
                try_acquire::<$domain>($init, usize::MAX, false);
            }
            // context tests
            #[test]
            fn test_read_single_success() {
                let mut ctx = acquire::<$domain>($init, 1);
                read(&mut ctx, 0, 1, true);
            }
            #[test]
            fn test_read_large_success() {
                let mut ctx = acquire::<$domain>($init, 12288);
                read(&mut ctx, 2048, 8192, true);
            }
            #[test]
            fn test_read_single_oob_offset() {
                let mut ctx = acquire::<$domain>($init, 1);
                let offset = ctx.size;
                read(&mut ctx, offset, 1, false);
            }
            #[test]
            fn test_read_single_oob_size() {
                let mut ctx = acquire::<$domain>($init, 1);
                let size = ctx.size + 1;
                read(&mut ctx, 0, size, false);
            }
            #[test]
            fn test_chunk_ref_single_success() {
                let mut ctx = acquire::<$domain>($init, 1);
                get_chunks(&mut ctx, 0, 1, true);
            }
            #[test]
            fn test_chunk_ref_single_oob_offset() {
                let mut ctx = acquire::<$domain>($init, 1);
                let size = ctx.size + 1;
                get_chunks(&mut ctx, size, 1, false);
            }
            #[test]
            fn test_chunk_ref_single_oob_size() {
                let mut ctx = acquire::<$domain>($init, 1);
                let size = ctx.size + 1;
                get_chunks(&mut ctx, 0, size, false);
            }
            #[test]
            fn test_chunk_ref_large_success() {
                let mut ctx = acquire::<$domain>($init, 12288);
                get_chunks(&mut ctx, 2048, 8192, true);
            }
            #[test]
            fn test_write_single_oob_offset() {
                let mut ctx = acquire::<$domain>($init, 1);
                let offset = ctx.size;
                write(&mut ctx, offset, 1, false);
            }
            #[test]
            fn test_write_single_oob_size() {
                let mut ctx = acquire::<$domain>($init, 1);
                let size = ctx.size + 1;
                write(&mut ctx, 0, size, false);
            }
            #[test]
            fn test_transfer_single() {
                let source = Box::new(acquire::<$domain>($init, 1));
                let destination = Box::new(acquire::<$domain>($init, 1));
                transfer(source, destination);
            }
            #[test]
            fn test_transfer_page() {
                let source = Box::new(acquire::<$domain>($init, 4096));
                let destination = Box::new(acquire::<$domain>($init, 4096));
                transfer(source, destination);
            }
            #[test]
            fn test_transfer_dataitem_item() {
                let source = Box::new(acquire::<$domain>($init, 4096));
                let destination = Box::new(acquire::<$domain>($init, 4096));
                transfer_item(source, destination, 0, 128, 1, 2, Ok(()));
            }
            
            // TODO
            // #[test]
            // fn test_transfer_dataitem_set() {
            //     transfer_set::<$domain>($init, 4096, 128, 256, 1, 2, Ok(()));
            // }
        }
    };
}

// #[cfg(feature = "wasm")]
// use super::wasm::WasmMemoryDomain as wasmType;
// #[cfg(feature = "wasm")]
// #[test]
// fn testing_transfer_system_context(){
//     let source = Box::new(acquire::<wasmType>(MemoryResource::None, 4096));
//     let destination = Box::new(acquire::<super::system_domain::SystemMemoryDomain>(MemoryResource::None, 4096));
//     transfer_item(source, destination, 0, 128, 1, 2, Ok(()));
// }

macro_rules! systemsDomainTests {
    ($name : ident ; $domain : ty ; $init : expr) => {
        mod $name {
            use super::*;
            
            #[test]
            fn test_aquire_success() {
                try_acquire::<$domain>($init, 1, true);
            }
            #[test]
            fn test_aquire_failure() {
                try_acquire::<$domain>($init, usize::MAX, false);
            }
            #[test]
            fn testing_transfer_system_context(){
                let source = Box::new(acquire::<$domain>($init, 4096));
                let destination = Box::new(acquire::<SystemMemoryDomain>($init, 4096));
                transfer_item(source, destination, 0, 128, 1, 2, Ok(()));
            }
            #[test]
            fn test_small_transfer_success() {
                let source = Box::new(acquire::<$domain>($init, 1));
                let destination = Box::new(acquire::<SystemMemoryDomain>($init, 1));
                transfer(source, destination)
            }
            #[test]
            fn test_page_transfer_success() {
                let size = 4096;
                let source = Box::new(acquire::<$domain>($init, size));
                let destination = Box::new(acquire::<SystemMemoryDomain>($init, size));
                transfer(source, destination)
            }
            #[test]
            fn test_large_transfer_success() {
                let size = 12288;
                let source = Box::new(acquire::<$domain>($init, size));
                let destination = Box::new(acquire::<SystemMemoryDomain>($init, size));
                transfer(source, destination)
            }
            #[test]
            fn test_read() {
                let size = 1024;
                let mut source = acquire::<$domain>($init, size);
                let mut destination = acquire::<SystemMemoryDomain>($init, size);
                source.write(0, &vec![BYTEPATTERN; size])
                    .expect("Writing should succeed");
                read_system_context(&mut destination, source, 512, 256, true);
            }
            #[test]
            fn test_read_large() {
                let size = 65536;
                let mut source = acquire::<$domain>($init, size);
                let mut destination = acquire::<SystemMemoryDomain>($init, size);
                source.write(0, &vec![BYTEPATTERN; size])
                    .expect("Writing should succeed");
                read_system_context(&mut destination, source, 1024, 8192, true);
            }

            #[test]
            fn test_read_single_oob_offset() {
                let size = 1;
                let mut source = acquire::<$domain>($init, size);
                let mut destination = acquire::<SystemMemoryDomain>($init, size);
                source.write(0, &vec![BYTEPATTERN; size])
                    .expect("Writing should succeed");
                read_system_context(&mut destination, source, size, 1, false);
            }
            #[test]
            fn test_read_single_oob_size() {
                let size = 1;
                let mut source = acquire::<$domain>($init, size);
                let mut destination = acquire::<SystemMemoryDomain>($init, size);
                source.write(0, &vec![BYTEPATTERN; size])
                    .expect("Writing should succeed");
                read_system_context(&mut destination, source, 0, size + 1, false);
            }
            #[test]
            fn test_chunk_ref_single_success() {
                let size = 1;
                let mut source = acquire::<$domain>($init, size);
                let mut destination = acquire::<SystemMemoryDomain>($init, size);
                let expected_success = true;
                source.write(0, &vec![BYTEPATTERN; size])
                    .expect("Writing should succeed");
                get_chunks_system_context(&mut destination, source, 0, 1, true);
            }
            #[test]
            fn test_chunk_ref_single_oob_offset() {
                let size = 1;
                let mut source = acquire::<$domain>($init, size);
                let mut destination = acquire::<SystemMemoryDomain>($init, size);
                let expected_success = false;
                source.write(0, &vec![BYTEPATTERN; size])
                    .expect("Writing should succeed");
                get_chunks_system_context(&mut destination, source, size, 1, false);
            }
            #[test]
            fn test_chunk_ref_single_oob_size() {
                let size = 1;
                let mut source = acquire::<$domain>($init, size);
                let mut destination = acquire::<SystemMemoryDomain>($init, size);
                let expected_success = false;
                source.write(0, &vec![BYTEPATTERN; size])
                    .expect("Writing should succeed");
                get_chunks_system_context(&mut destination, source, 0, size + 1, false);
            }
            #[test]
            fn test_chunk_ref_large_success() {
                let size = 12288;
                let mut source = acquire::<$domain>($init, size);
                let mut destination = acquire::<SystemMemoryDomain>($init, size);
                let expected_success = true;
                source.write(0, &vec![BYTEPATTERN; size])
                    .expect("Writing should succeed");
                get_chunks_system_context(&mut destination, source, 2048, 8192, expected_success);
            }
            #[test]
            fn test_fragmented_items_get_chunk_success(){
                // Here we test how the get_chunk function handles fractured memory
                let size = 1024;
                let mut source1 = acquire::<$domain>($init, size);
                let mut source2 = acquire::<$domain>($init, size);
                let mut system_ctx = acquire::<SystemMemoryDomain>($init, size);
                let expected_success = true;
                source1.write(0, &vec![BYTEPATTERN; size])
                    .expect("Writing should succeed");
                source2.write(0, &vec![BYTEPATTERN+1; size])
                    .expect("Writing should succeed");

                let _ = transfer_memory(&mut system_ctx, Arc::from(source1), 0, 0, 128);
                let _ = transfer_memory(&mut system_ctx, Arc::from(source2), 128, 0, 128);

                let mut total_read = 0usize;
                let chunk_ref_result = system_ctx.get_chunk_ref(0, 256);
                match chunk_ref_result {
                    Ok(chunk_ref) => {
                        assert_eq!(&vec![BYTEPATTERN; 128], chunk_ref);
                    }
                    Err(DandelionError::InvalidRead) => panic!("Invalid read"),
                    Err(err) => panic!("Unexpected error from get_chunk_ref {:?}", err),
                }

            }
        }
    };
}

use super::malloc::MallocMemoryDomain as mallocType;
domainTests!(malloc; mallocType; MemoryResource::None);

use super::mmap::MmapMemoryDomain as mmapType;
domainTests!(mmap; mmapType; MemoryResource::None);

#[cfg(feature = "cheri")]
use super::cheri::CheriMemoryDomain as cheriType;
#[cfg(feature = "cheri")]
domainTests!(cheri; cheriType; MemoryResource::None);
#[cfg(feature = "cheri")]
systemsDomainTests!(cheriSystem; cheriType; MemoryResource::None);

#[cfg(feature = "mmu")]
use super::mmu::MmuMemoryDomain as mmuType;
#[cfg(feature = "mmu")]
domainTests!(mmu; mmuType; MemoryResource::None);
#[cfg(feature = "mmu")]
systemsDomainTests!(mmuSystem; mmuType; MemoryResource::None);

#[cfg(feature = "wasm")]
use super::wasm::WasmMemoryDomain as wasmType;
#[cfg(feature = "wasm")]
domainTests!(wasm; wasmType; MemoryResource::None);
#[cfg(feature = "wasm")]
systemsDomainTests!(wasmSystem; wasmType; MemoryResource::None);

// use super::system_domain::SystemMemoryDomain as systemType;
// domainTests!(system_domain; systemType; MemoryResource::None);

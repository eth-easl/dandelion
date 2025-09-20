use crate::memory_domain::{
    test_resource::get_resource, transfer_data_item, transfer_memory, Context, ContextTrait,
    ContextType, MemoryDomain, MemoryResource,
};
use dandelion_commons::{DandelionError, DandelionResult, DomainError};
use std::sync::Arc;

#[cfg(any(feature = "cheri", feature = "kvm", feature = "mmu", feature = "wasm"))]
use crate::memory_domain::system_domain::SystemMemoryDomain;

// produces binary pattern 0b0101_01010 or 0x55
const BYTEPATTERN: u8 = 85;

/// Test whether a context can be acquired, and panics if the success
/// does not match `expect_success`.
fn try_acquire<D: MemoryDomain>(
    arg: MemoryResource,
    acquisition_size: usize,
    expect_success: bool,
) {
    let resource = get_resource(arg);
    let init_result = D::init(resource);
    let domain = init_result.expect("should have initialized memory domain");
    let context_result = domain.acquire_context(acquisition_size);
    match (expect_success, context_result) {
        (true, Ok(_))
        | (false, Err(DandelionError::OutOfMemory))
        | (false, Err(DandelionError::DomainError(DomainError::InvalidMemorySize)))
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
    let resource = get_resource(arg);
    let domain = init_domain::<D>(resource);
    let context = domain
        .acquire_context(size)
        .expect("Context should be allocatable");
    return context;
}

fn init_domain<D: MemoryDomain>(arg: MemoryResource) -> Box<dyn MemoryDomain> {
    let resource = get_resource(arg);
    let init_result = D::init(resource);
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

#[cfg(any(feature = "cheri", feature = "kvm", feature = "mmu", feature = "wasm"))]
fn read_system_context(
    system_ctx: &mut Context,
    base_ctx: Context,
    offset: usize,
    size: usize,
    expect_success: bool,
) {
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

#[cfg(any(feature = "cheri", feature = "kvm", feature = "mmu", feature = "wasm"))]
fn get_chunks_system_context(
    system_ctx: &mut Context,
    base_ctx: Context,
    offset: usize,
    size: usize,
    expect_success: bool,
) {
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

    match &destination.context {
        ContextType::System(_) => {
            // If destination is a systems context, we could have transfer from
            // different type of contexts, which may have different sizes for
            // the same initialisation parameter.
            // Thus, the assertion does not have to hold, even for correct initialisation
            size = destination.size;
        }
        _ => {
            assert!(size == destination.size);
        }
    }

    let mut source_context = *source;
    let mut destination_context = *destination;
    source_context
        .write(0, &vec![BYTEPATTERN; size])
        .expect("Writing should succeed");
    let source_ctxt_arc = Arc::new(source_context);
    transfer_memory(&mut destination_context, source_ctxt_arc, 0, 0, size)
        .expect("Should successfully transfer");
    let mut read_buffer = vec![0; size];
    destination_context
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
    let mut source_context = *source;
    let mut destination_context = *destination;
    source_context
        .write(offset, &vec![BYTEPATTERN; item_size])
        .expect("Writing should succeed");
    if source_context.content.len() <= source_index {
        source_context
            .content
            .resize_with(source_index + 1, || None);
    }
    source_context.content[source_index] = Some(crate::DataSet {
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
    destination_context.content.push(Some(crate::DataSet {
        ident: String::from(""),
        buffers: vec![],
    }));
    let transfer_error = transfer_data_item(
        &mut destination_context,
        Arc::new(source_context),
        destination_index,
        8,
        0,
        "",
        source_index,
        0,
    );
    assert_eq!(transfer_error, expect_result);
    if expect_result.is_err() {
        return;
    }
    // check transfer success
    assert!(destination_index < destination_context.content.len());
    let destination_item = destination_context.content[destination_index]
        .as_ref()
        .expect("Set should be present");
    assert_eq!("", destination_item.ident);
    assert_eq!(1, destination_item.buffers.len());
    assert_eq!("", destination_item.buffers[0].ident);
    assert_eq!(item_size, destination_item.buffers[0].data.size);
    let read_offset = destination_item.buffers[0].data.offset;
    let mut read_buffer = vec![0; item_size];
    destination_context
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
        }
    };
}

#[cfg(any(feature = "cheri", feature = "kvm", feature = "mmu", feature = "wasm"))]
macro_rules! systemsDomainTests {
    ($name : ident ; $domain : ty ; $init : expr) => {
        mod $name {
            use super::*;

            #[test]
            fn testing_transfer_system_context() {
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
            fn test_larger_transfer_success() {
                let size = 65536;
                let source = Box::new(acquire::<$domain>($init, size));
                let destination = Box::new(acquire::<SystemMemoryDomain>($init, size));
                transfer(source, destination)
            }
            #[test]
            fn test_read_single_oob_offset() {
                let size = 1;
                let mut source = acquire::<$domain>($init, size);
                let mut destination = acquire::<SystemMemoryDomain>($init, size);
                source
                    .write(0, &vec![BYTEPATTERN; size])
                    .expect("Writing should succeed");
                read_system_context(&mut destination, source, size, 1, false);
            }
            #[test]
            fn test_read_single_oob_size() {
                let size = 1;
                let mut source = acquire::<$domain>($init, size);
                let mut destination = acquire::<SystemMemoryDomain>($init, size);
                source
                    .write(0, &vec![BYTEPATTERN; size])
                    .expect("Writing should succeed");
                read_system_context(&mut destination, source, 0, size + 1, false);
            }
            #[test]
            fn test_chunk_ref_single_success() {
                let size = 1;
                let mut source = acquire::<$domain>($init, size);
                let mut destination = acquire::<SystemMemoryDomain>($init, size);
                source
                    .write(0, &vec![BYTEPATTERN; size])
                    .expect("Writing should succeed");
                get_chunks_system_context(&mut destination, source, 0, 1, true);
            }
            #[test]
            fn test_chunk_ref_single_oob_offset() {
                let size = 1;
                let mut source = acquire::<$domain>($init, size);
                let mut destination = acquire::<SystemMemoryDomain>($init, size);
                source
                    .write(0, &vec![BYTEPATTERN; size])
                    .expect("Writing should succeed");
                get_chunks_system_context(&mut destination, source, size, 1, false);
            }
            #[test]
            fn test_chunk_ref_single_oob_size() {
                let size = 1;
                let mut source = acquire::<$domain>($init, size);
                let mut destination = acquire::<SystemMemoryDomain>($init, size);
                source
                    .write(0, &vec![BYTEPATTERN; size])
                    .expect("Writing should succeed");
                get_chunks_system_context(&mut destination, source, 0, size + 1, false);
            }
            #[test]
            fn test_chunk_ref_large_success() {
                let size = 12288;
                let mut source = acquire::<$domain>($init, size);
                let mut destination = acquire::<SystemMemoryDomain>($init, size);
                source
                    .write(0, &vec![BYTEPATTERN; size])
                    .expect("Writing should succeed");
                get_chunks_system_context(&mut destination, source, 2048, 8192, true);
            }
            #[test]
            fn test_fragmented_items_get_chunk_success() {
                // Here we test how the get_chunk function handles fractured memory
                let size = 1024;
                let mut source1 = acquire::<$domain>($init, size);
                let mut source2 = acquire::<$domain>($init, size);
                let mut system_ctx = acquire::<SystemMemoryDomain>($init, size);
                source1
                    .write(0, &vec![BYTEPATTERN; size])
                    .expect("Writing should succeed");
                source2
                    .write(0, &vec![BYTEPATTERN + 1; size])
                    .expect("Writing should succeed");

                let _ = transfer_memory(&mut system_ctx, Arc::from(source1), 0, 0, 128);
                let _ = transfer_memory(&mut system_ctx, Arc::from(source2), 128, 0, 128);

                let chunk_ref_result = system_ctx.get_chunk_ref(0, 256);
                match chunk_ref_result {
                    Ok(chunk_ref) => {
                        assert_eq!(&vec![BYTEPATTERN; 128], chunk_ref);
                    }
                    Err(DandelionError::InvalidRead) => panic!("Invalid read"),
                    Err(err) => panic!("Unexpected error from get_chunk_ref {:?}", err),
                }
            }
            #[test]
            fn test_transfer_mulitple_bytes() {
                // Tests how transfers over multiple Bytes are handled
                let mut preamble = "Start\n".to_string();
                let preamble_bytes = bytes::Bytes::from(preamble.clone().into_bytes());
                let pre_len = preamble_bytes.len();
                let body = "body_body_body_body".to_string();
                let body_bytes = bytes::Bytes::from(body.clone().into_bytes());
                let body_len = body_bytes.len();
                let mut initial_ctx = acquire::<SystemMemoryDomain>($init, 128);
                match &mut initial_ctx.context {
                    ContextType::System(initial_ctx_) => {
                        crate::memory_domain::system_domain::system_context_write_from_bytes(
                            initial_ctx_,
                            preamble_bytes.clone(),
                            0,
                            pre_len,
                        );
                        crate::memory_domain::system_domain::system_context_write_from_bytes(
                            initial_ctx_,
                            body_bytes.clone(),
                            pre_len,
                            body_len,
                        );
                    }
                    _ => {
                        panic!("Error");
                    }
                }

                let mut second_ctx = acquire::<$domain>($init, 128);
                transfer_memory(
                    &mut second_ctx,
                    Arc::from(initial_ctx),
                    0,
                    0,
                    pre_len + body_len,
                )
                .expect("Transfer expected to be valid");
                let chunk_ref_result = second_ctx.get_chunk_ref(0, pre_len + body_len);

                fn bytes_to_string(input: &[u8]) -> Result<String, std::str::Utf8Error> {
                    std::str::from_utf8(input).map(|s| s.to_string())
                }
                let return_string = match bytes_to_string(chunk_ref_result.unwrap()) {
                    Ok(ret_str) => ret_str,
                    _ => {
                        panic!("Error");
                    }
                };
                preamble.push_str(&body);
                assert_eq!(preamble, return_string, "Not full string was returned");
            }
        }
    };
}

use super::malloc::MallocMemoryDomain as mallocType;
domainTests!(malloc; mallocType; MemoryResource::None);

#[cfg(feature = "cheri")]
use super::cheri::CheriMemoryDomain as cheriType;
#[cfg(feature = "cheri")]
domainTests!(cheri; cheriType; MemoryResource::Anonymous { size: (2<<22) });
#[cfg(feature = "cheri")]
systemsDomainTests!(cheri_system; cheriType; MemoryResource::Anonymous { size: (2<<22) });

#[cfg(feature = "kvm")]
use super::kvm::KvmMemoryDomain as kvmType;
#[cfg(feature = "kvm")]
domainTests!(kvm; kvmType; MemoryResource::Shared { id: 0, size: (2<<22) });
#[cfg(feature = "kvm")]
systemsDomainTests!(kvm_system; kvmType; MemoryResource::Shared { id: 0, size: (2<<22) });

#[cfg(feature = "mmu")]
use super::mmu::MmuMemoryDomain as mmuType;
#[cfg(feature = "mmu")]
domainTests!(mmu; mmuType; MemoryResource::Shared { id: 0, size: (2<<22) });
#[cfg(feature = "mmu")]
systemsDomainTests!(mmu_system; mmuType; MemoryResource::Shared {id: 0, size: (2<<22)});

#[cfg(feature = "wasm")]
use super::wasm::WasmMemoryDomain as wasmType;
#[cfg(feature = "wasm")]
domainTests!(wasm; wasmType; MemoryResource::Anonymous { size: (2<<22) });
#[cfg(feature = "wasm")]
systemsDomainTests!(wasm_system; wasmType; MemoryResource::Anonymous{size: (2<<22)});

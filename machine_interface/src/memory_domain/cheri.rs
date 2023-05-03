use libc::size_t;

// opaque type to allow type enforcement on pointer
#[repr(C)]
pub struct cheri_c_context {
    _data: [u8; 0],
    _marker: core::marker::PhantomData<(*mut u8, core::marker::PhantomPinned)>,
}

#[link(name = "cheri_lib")]
extern "C" {
    fn cheri_alloc(size: size_t) -> *const cheri_c_context;
    fn cheri_free(context: *const cheri_c_context, size: size_t) -> ();
    fn cheri_write_context(
        context: *const cheri_c_context,
        source_pointer: *const u8,
        context_offset: size_t,
        size: size_t,
    ) -> ();
    fn cheri_read_context(
        context: *const cheri_c_context,
        destination_pointer: *mut u8,
        context_offset: size_t,
        size: size_t,
    ) -> ();
    fn cheri_transfer_context(
        destination: *const cheri_c_context,
        source: *const cheri_c_context,
        destination_offset: size_t,
        source_offset: size_t,
        size: size_t,
    ) -> ();
}

use crate::memory_domain::{Context, ContextTrait, ContextType, MemoryDomain};
use dandelion_commons::{DandelionError, DandelionResult};
use std::collections::HashMap;

pub struct CheriContext {
    pub context: *const cheri_c_context,
    pub size: usize,
}
unsafe impl Send for CheriContext {}
unsafe impl Sync for CheriContext {}
// TODO implement drop

impl ContextTrait for CheriContext {
    fn write(&mut self, offset: usize, data: Vec<u8>) -> DandelionResult<()> {
        // perform size checks
        if data.len() + offset > self.size {
            return Err(DandelionError::InvalidWrite);
        }
        unsafe {
            cheri_write_context(self.context, data.as_ptr(), offset, data.len());
        }
        Ok(())
    }
    fn read(&self, offset: usize, read_size: usize) -> DandelionResult<Vec<u8>> {
        // perform size checks
        if read_size + offset > self.size {
            return Err(DandelionError::InvalidRead);
        }
        // try to allocate space for read values
        let mut result_vec = Vec::<u8>::new();
        if let Err(_) = result_vec.try_reserve(read_size) {
            return Err(DandelionError::OutOfMemory);
        }
        result_vec.resize(read_size, 0);
        unsafe { cheri_read_context(self.context, result_vec.as_mut_ptr(), offset, read_size) }
        Ok(result_vec)
    }
}

pub struct CheriMemoryDomain {}

impl MemoryDomain for CheriMemoryDomain {
    fn init(_config: Vec<u8>) -> DandelionResult<Box<dyn MemoryDomain>> {
        Ok(Box::new(CheriMemoryDomain {}))
    }
    fn acquire_context(&self, size: usize) -> DandelionResult<Context> {
        let mut new_context: Box<CheriContext> = Box::new(CheriContext {
            context: std::ptr::null_mut(),
            size: size,
        });
        unsafe {
            new_context.context = cheri_alloc(size);
        }
        if new_context.context.is_null() {
            return Err(DandelionError::OutOfMemory);
        }
        Ok(Context {
            context: ContextType::Cheri(new_context),
            dynamic_data: HashMap::new(),
            static_data: Vec::new(),
        })
    }
    fn release_context(&self, context: Context) -> DandelionResult<()> {
        match context.context {
            ContextType::Cheri(cheri_context) => {
                unsafe {
                    cheri_free(cheri_context.context, cheri_context.size);
                }
                Ok(())
            }
            _ => Err(DandelionError::ContextMissmatch),
        }
    }
}

pub fn cheri_transfer(
    destination: &mut CheriContext,
    source: &CheriContext,
    destination_offset: usize,
    source_offset: usize,
    size: usize,
) -> DandelionResult<()> {
    if source_offset + size > source.size {
        return Err(DandelionError::InvalidRead);
    }
    if destination_offset + size > destination.size {
        return Err(DandelionError::InvalidWrite);
    }
    unsafe {
        cheri_transfer_context(
            destination.context,
            source.context,
            destination_offset,
            source_offset,
            size,
        );
    }
    Ok(())
}

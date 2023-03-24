use std::u8;

use libc::size_t;

// opaque type to allow type enforcement on pointer
#[repr(C)]
struct cheri_c_context {
    _data: [u8; 0],
    _marker: core::marker::PhantomData<(*mut u8, core::marker::PhantomPinned)>,
}

#[link(name = "cheri_mem")]
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
        sanitize: i8,
    ) -> ();
}

use super::super::{HardwareError, HwResult};
use super::{Context, ContextTrait, MemoryDomain};

pub struct CheriContext {
    context: *const cheri_c_context,
    size: usize,
}
// TODO implement drop

impl ContextTrait for CheriContext {
    fn write(&mut self, offset: usize, data: Vec<u8>) -> HwResult<()> {
        // perform size checks
        if data.len() + offset > self.size {
            return Err(HardwareError::InvalidWrite);
        }
        unsafe {
            cheri_write_context(self.context, data.as_ptr(), offset, data.len());
        }
        Ok(())
    }
    fn read(&mut self, offset: usize, read_size: usize, sanitize: bool) -> HwResult<Vec<u8>> {
        // perform size checks
        if read_size + offset > self.size {
            return Err(HardwareError::InvalidRead);
        }
        // try to allocate space for read values
        let mut result_vec = Vec::<u8>::new();
        if let Err(_) = result_vec.try_reserve(read_size) {
            return Err(HardwareError::OutOfMemory);
        }
        result_vec.resize(read_size, 0);
        unsafe {
            cheri_read_context(
                self.context,
                result_vec.as_mut_ptr(),
                offset,
                read_size,
                sanitize as i8,
            )
        }
        Ok(result_vec)
    }
}

pub struct CheriMemoryDomain {}

impl MemoryDomain for CheriMemoryDomain {
    fn init(_config: Vec<u8>) -> HwResult<Box<Self>> {
        Ok(Box::new(CheriMemoryDomain {}))
    }
    fn acquire_context(&self, size: usize) -> HwResult<Context> {
        let mut new_context: Box<CheriContext> = Box::new(CheriContext {
            context: std::ptr::null_mut(),
            size: size,
        });
        unsafe {
            new_context.context = cheri_alloc(size);
        }
        println!("context pointer {}", new_context.context as usize);
        if new_context.context.is_null() {
            return Err(HardwareError::OutOfMemory);
        }
        Ok(Context::Cheri(new_context))
    }
    fn release_context(&self, context: Context) -> HwResult<()> {
        match context {
            Context::Cheri(context) => {
                unsafe {
                    cheri_free(context.context, context.size);
                }
                Ok(())
            }
            _ => Err(HardwareError::ContextMissmatch),
        }
    }
}

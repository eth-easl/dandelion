use std::u8;

use libc::{c_void, size_t};

// #[repr(C)]
// struct cheri_cap {
//     cap: [i8; 16],
// }

#[link(name = "cheri_mem")]
extern "C" {
    fn cheri_alloc(size: size_t) -> *mut c_void;
    fn cheri_free(context: *const c_void, size: size_t) -> ();
    fn cheri_write_context(
        context: *mut c_void,
        source_pointer: *const u8,
        context_offset: size_t,
        size: size_t,
    ) -> ();
    fn cheri_read_context(
        context: *mut c_void,
        destination_pointer: *mut u8,
        context_offset: size_t,
        size: size_t,
        sanitize: i8,
    ) -> ();
}

use super::super::{HardwareError, HwResult};
use super::{Context, ContextTrait, MemoryDomain};

pub struct CheriContext {
    context: *mut c_void,
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
    fn init(config: Vec<u8>) -> HwResult<Box<Self>> {
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
        if new_context.context == std::ptr::null_mut() {
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

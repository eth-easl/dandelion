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

pub struct CheriContext {
    pub context: *const cheri_c_context,
    pub size: usize,
}
unsafe impl Send for CheriContext {}
unsafe impl Sync for CheriContext {}
// TODO implement drop

impl ContextTrait for CheriContext {
    fn write<T>(&mut self, offset: usize, data: &[T]) -> DandelionResult<()> {
        // perform size checks
        let write_size = core::mem::size_of::<T>() * data.len();
        if write_size + offset > self.size {
            return Err(DandelionError::InvalidWrite);
        }
        unsafe {
            cheri_write_context(self.context, data.as_ptr() as *const u8, offset, write_size);
        }
        return Ok(());
    }
    fn read<T>(&self, offset: usize, read_buffer: &mut [T]) -> DandelionResult<()> {
        let read_size = read_buffer.len() * core::mem::size_of::<T>();
        // perform size checks
        if read_size + offset > self.size {
            return Err(DandelionError::InvalidRead);
        }
        unsafe {
            cheri_read_context(
                self.context,
                read_buffer.as_mut_ptr() as *mut u8,
                offset,
                read_size,
            )
        }
        Ok(())
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
        return Ok(Context::new(ContextType::Cheri(new_context), size));
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

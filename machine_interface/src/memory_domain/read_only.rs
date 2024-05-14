extern crate alloc;
use crate::memory_domain::{Context, ContextTrait};
use alloc::alloc::Layout;
use dandelion_commons::{DandelionError, DandelionResult};
use log::error;

#[derive(Debug)]
pub struct ReadOnlyContext {
    storage: &'static mut [u8],
    layout: Option<Layout>,
}

impl ContextTrait for ReadOnlyContext {
    fn write<T>(&mut self, _offset: usize, _data: &[T]) -> DandelionResult<()> {
        error!("Tried to write to read only context");
        return Err(DandelionError::InvalidWrite);
    }

    fn read<T>(&self, offset: usize, read_buffer: &mut [T]) -> DandelionResult<()> {
        if offset % core::mem::align_of::<T>() != 0 {
            return Err(DandelionError::ReadMisaligned);
        }

        let read_size = core::mem::size_of::<T>() * read_buffer.len();
        if offset + read_size > self.storage.len() {
            eprintln!(
                "Invalid in ROC: len {}, offset {}, read_size {}",
                self.storage.len(),
                offset,
                read_size
            );
            return Err(DandelionError::InvalidRead);
        }
        let byte_buffer = unsafe {
            core::slice::from_raw_parts_mut(
                read_buffer.as_ptr() as *mut u8,
                read_buffer.len() * core::mem::size_of::<T>(),
            )
        };
        byte_buffer.copy_from_slice(&self.storage[offset..offset + read_size]);
        return Ok(());
    }

    fn get_chunk_ref(&self, offset: usize, length: usize) -> DandelionResult<&[u8]> {
        if offset + length > self.storage.len() {
            return Err(DandelionError::InvalidRead);
        }
        return Ok(&self.storage[offset..offset + length]);
    }
}

impl ReadOnlyContext {
    pub fn new<T>(reference: Box<[T]>) -> DandelionResult<Context> {
        let ref_len = core::mem::size_of::<T>() * reference.len();
        let layout = core::alloc::Layout::from_size_align(ref_len, core::mem::align_of::<T>())
            .or(Err(DandelionError::ContextReadOnlyLayout))?;
        let new_ref = unsafe {
            core::slice::from_raw_parts_mut(Box::leak(reference).as_mut_ptr() as *mut u8, ref_len)
        };
        return Ok(Context::new(
            super::ContextType::ReadOnly(Box::new(ReadOnlyContext {
                storage: new_ref,
                layout: Some(layout),
            })),
            ref_len,
        ));
    }
    pub fn new_static<T>(reference: &'static mut [T]) -> Context {
        let ref_len = core::mem::size_of::<T>() * reference.len();
        let new_ref =
            unsafe { core::slice::from_raw_parts_mut(reference.as_mut_ptr() as *mut u8, ref_len) };
        return Context::new(
            super::ContextType::ReadOnly(Box::new(ReadOnlyContext {
                storage: new_ref,
                layout: None,
            })),
            ref_len,
        );
    }
}

impl Drop for ReadOnlyContext {
    fn drop(&mut self) {
        if let Some(layout) = self.layout {
            unsafe { alloc::alloc::dealloc(self.storage.as_mut_ptr(), layout) }
        }
    }
}

#[test]
fn read_test() {
    let expected_data = -0x123456789ABCDEFi64;
    let test_data = vec![expected_data];
    let read_context = ReadOnlyContext::new(test_data.into_boxed_slice())
        .expect("should be able to create allocation");
    let mut all_read_vec = Vec::<u8>::new();
    all_read_vec.resize(8, 0);
    read_context
        .read(0, &mut all_read_vec)
        .expect("read should succeed");
    assert_eq!(expected_data.to_ne_bytes(), all_read_vec.as_slice());
    let mut partial_read_vec = Vec::<u8>::new();
    partial_read_vec.resize(4, 0);
    read_context
        .read(2, &mut partial_read_vec)
        .expect("Partial read should succeed");
    assert_eq!(
        &expected_data.to_ne_bytes()[2..6],
        partial_read_vec.as_slice()
    );
}

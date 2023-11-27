use crate::memory_domain::{Context, ContextTrait};
use dandelion_commons::{DandelionError, DandelionResult};

#[derive(Debug)]
pub struct ReadOnlyContext {
    storage: &'static [u8],
}

impl ContextTrait for ReadOnlyContext {
    fn write<T>(&mut self, _offset: usize, _data: &[T]) -> DandelionResult<()> {
        println!("Tried to write to read only context");
        return Err(DandelionError::InvalidWrite);
    }

    fn read<T>(&self, offset: usize, read_buffer: &mut [T]) -> DandelionResult<()> {
        if offset % core::mem::align_of::<T>() != 0 {
            return Err(DandelionError::ReadMisaligned);
        }

        let read_size = core::mem::size_of::<T>() * read_buffer.len();
        if offset + read_size > self.storage.len() {
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
}

impl ReadOnlyContext {
    pub fn new<T>(reference: &[T]) -> Context {
        let ref_len = core::mem::size_of::<T>() * reference.len();
        let new_ref =
            unsafe { core::slice::from_raw_parts(reference.as_ptr() as *const u8, ref_len) };
        return Context::new(
            super::ContextType::ReadOnly(Box::new(ReadOnlyContext { storage: new_ref })),
            ref_len,
        );
    }
}

#[test]
fn read_test() {
    let test_data = -0x123456789ABCDEFi64;
    let read_context = ReadOnlyContext::new(&[test_data]);
    let mut all_read_vec = Vec::<u8>::new();
    all_read_vec.resize(8, 0);
    read_context
        .read(0, &mut all_read_vec)
        .expect("read should succeed");
    assert_eq!(test_data.to_ne_bytes(), all_read_vec.as_slice());
    let mut partial_read_vec = Vec::<u8>::new();
    partial_read_vec.resize(4, 0);
    read_context
        .read(2, &mut partial_read_vec)
        .expect("Partial read should succeed");
    assert_eq!(&test_data.to_ne_bytes()[2..6], partial_read_vec.as_slice());
}

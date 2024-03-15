use dandelion_commons::DandelionResult;

use super::ContextTrait;

// NOTE: might be removed in the future

#[derive(Debug)]
pub struct GpuContext {}

unsafe impl Send for GpuContext {}
unsafe impl Sync for GpuContext {}

impl ContextTrait for GpuContext {
    fn write<T>(&mut self, offset: usize, data: &[T]) -> DandelionResult<()> {
        todo!()
    }
    fn read<T>(&self, offset: usize, read_buffer: &mut [T]) -> DandelionResult<()> {
        todo!()
    }
}

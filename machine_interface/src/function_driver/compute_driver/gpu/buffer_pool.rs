use dandelion_commons::{DandelionError, DandelionResult};

use super::hip::{self, DevicePointer};

pub struct BufferPool {
    // (ptr, ptr in use)
    buffers: Vec<(DevicePointer, bool)>,
}

impl BufferPool {
    pub fn new(gpu_id: u8) -> Self {
        let buffers = vec![];
        Self { buffers }
    }

    // TODO: potentially coalesce / more sophisticated matching method (best fit)?
    pub fn find_buffer(&mut self, size: usize) -> DandelionResult<usize> {
        let idx = match self
            .buffers
            .iter()
            .position(|(dev_ptr, used)| !*used && dev_ptr.size >= size)
        {
            Some(idx) => {
                self.buffers[idx].1 = true;
                idx
            }
            None => {
                // TODO: maybe round here?
                let dev_ptr = hip::DevicePointer::try_new(size)?;
                self.buffers.push((dev_ptr, true));
                self.buffers.len() - 1
            }
        };
        self.buffers[idx].0.zero_out()?;
        Ok(idx)
    }

    pub fn get(&self, idx: usize) -> &DevicePointer {
        &self.buffers[idx].0
    }

    pub fn give_back(&mut self, idx: usize) {
        self.buffers[idx].1 = false;
    }
}

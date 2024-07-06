use dandelion_commons::{DandelionError, DandelionResult};
use log::debug;

use super::hip::{self, DeviceAllocation, DevicePointer};

#[allow(non_upper_case_globals)]
const Gi: usize = 1 << 30;

// REGION_SIZE * #engines_on_device should never exceed total VRAM capacity (64GiB for MI210)
const REGION_SIZE: usize = 15 * Gi;

#[derive(Debug)]
struct Buffer {
    offset: usize,
    length: usize,
}

impl Buffer {
    fn sentinel() -> Self {
        Self {
            offset: 0,
            length: 0,
        }
    }
}
pub struct BufferPool {
    // (ptr, ptr in use)
    allocation: DeviceAllocation,
    buffers: Vec<Buffer>,
}

impl BufferPool {
    pub fn try_new(gpu_id: u8) -> DandelionResult<Self> {
        hip::set_device(gpu_id)?;

        let mut allocation = hip::DeviceAllocation::try_new(REGION_SIZE)?;
        allocation.zero_out()?;
        // sentinel buffer to simplify logic
        let buffers = vec![Buffer::sentinel()];
        Ok(Self {
            allocation,
            buffers,
        })
    }

    pub fn alloc_buffer(&mut self, size: usize) -> DandelionResult<usize> {
        macro_rules! align {
            ($e: expr) => {
                ($e + 255) / 256 * 256
            };
        }
        // Round size to 256 bytes, which is the minimum that GPU allocators typically use. This might need to be changed
        let length = align!(size);

        let last = self
            .buffers
            .last()
            .expect("buffers should always hold sentinel");

        let offset = align!(last.offset + last.length);

        if offset + length > self.allocation.size {
            debug!(
                "Going to throw OutOfMemory, offset: {}, length: {}, self_alloc_size: {}",
                offset, length, self.allocation.size
            );
            return Err(DandelionError::OutOfMemory);
        }

        self.buffers.push(Buffer { offset, length });

        Ok(self.buffers.len() - 1)
    }

    pub fn get(&self, idx: usize) -> DandelionResult<DevicePointer> {
        let buffer = self.buffers.get(idx).ok_or(DandelionError::EngineError)?;
        Ok(DevicePointer {
            ptr: unsafe { self.allocation.ptr.byte_add(buffer.offset) },
        })
    }

    pub fn dealloc_all(&mut self) -> DandelionResult<()> {
        // Unwrap okay because buffers will always have at least sentinel
        let last = self.buffers.last().unwrap();
        self.allocation.zero_size(last.offset + last.length)?;
        self.buffers.clear();
        self.buffers.push(Buffer::sentinel());
        Ok(())
    }
}

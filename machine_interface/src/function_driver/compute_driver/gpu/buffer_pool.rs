use super::gpu_api::{self, DeviceAllocation, DevicePointer};
use dandelion_commons::{DandelionError, DandelionResult, FunctionId};
use log::debug;
use std::collections::HashMap;

#[derive(Debug)]
struct Buffer {
    offset: usize,
    size: usize,
}

#[derive(Debug)]
pub struct BufferPool {
    // (ptr, ptr in use)
    allocation: DeviceAllocation,
    buffers: HashMap<String, Buffer>,
    pub prev_function_id: FunctionId,
    last_offset_global: usize,
    last_offset_weights: usize,
    tmp_names: Vec<String>,
}

impl BufferPool {
    pub fn try_new(gpu_id: u8, region_size: usize) -> DandelionResult<Self> {
        gpu_api::set_device(gpu_id)?;

        let mut allocation = gpu_api::DeviceAllocation::try_new(region_size)?;
        allocation.zero_out()?;
        let buffers = HashMap::new();
        let last_offset_global = 0usize;
        let last_offset_weights = 0usize;
        let tmp_names = Vec::new();
        Ok(Self {
            allocation,
            buffers,
            prev_function_id: u64::MAX,
            last_offset_global,
            last_offset_weights,
            tmp_names,
        })
    }

    pub fn alloc_buffer(&mut self, name: &str, size: usize, is_weight: bool) -> DandelionResult<()> {
        let dev_ptr = self.last_offset_global;
        
        macro_rules! align {
            ($e: expr) => {
                ($e + 255) / 256 * 256
            };
        }
        // Round size to 256 bytes, which is the minimum that GPU allocators typically use. This might need to be changed
        let aligned_size = align!(size);

        if dev_ptr + aligned_size > self.allocation.size {
            debug!(
                "Going to throw OutOfMemory, offset: {}, aligned_size: {}, self_alloc_size: {}",
                dev_ptr, aligned_size, self.allocation.size
            );
            return Err(DandelionError::OutOfMemory);
        }

        self.last_offset_global = align!(self.last_offset_global + aligned_size);
        if is_weight {
            self.last_offset_weights = self.last_offset_global;
        } else {
            self.tmp_names.push(name.to_string());
        }

        self.buffers.insert(
            name.to_string(),
            Buffer { 
                offset: dev_ptr, 
                size: size,
            },
        );

        Ok(())
    }

    pub fn get_pointer(&self, name: &str) -> DandelionResult<DevicePointer> {
        let buffer = self.buffers.get(name).ok_or(DandelionError::EngineError)?;
        Ok(DevicePointer {
            ptr: unsafe { self.allocation.ptr.byte_add(buffer.offset) },
        })
    }

    pub fn get_size(&self, name: &str) -> DandelionResult<usize> {
        let buffer = self.buffers.get(name).ok_or(DandelionError::EngineError)?;
        Ok(buffer.size)
    }

    pub fn dealloc_tmp_buffers(&mut self) -> DandelionResult<()> {
        self.allocation.zero_from_to(self.last_offset_weights, self.last_offset_global)?;
        for name in &self.tmp_names {
            self.buffers.remove(name);
        }
        self.last_offset_global = self.last_offset_weights;
        self.tmp_names.clear();
        Ok(())
    }

    pub fn dealloc_all(&mut self) -> DandelionResult<()> {
        self.allocation.zero_size(self.last_offset_global)?;
        self.buffers.clear();
        self.last_offset_global = 0;
        self.last_offset_weights = 0;
        self.tmp_names.clear();
        Ok(())
    }
}

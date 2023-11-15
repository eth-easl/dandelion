use std::{rc::Rc, cell::RefCell, borrow::BorrowMut, sync::Arc, sync::Mutex, ops::{Deref, DerefMut}};

use crate::memory_domain::{Context, ContextTrait, ContextType, MemoryDomain};
use dandelion_commons::{DandelionError, DandelionResult};
use crate::interface::_32_bit::DandelionSystemData;
use crate::function_driver::compute_driver::wasm::WASM_MEM_ON_HEAP;

#[derive(Debug)]
pub struct WasmLayoutDescription {
    pub sdk_heap_base: usize,
    pub sdk_heap_size: usize,
    pub sdk_sysdata_offset: usize,
}

#[derive(Debug)]
pub struct WasmContext {
    pub base_offset: usize,
    pub layout: Option<WasmLayoutDescription>,
    pub data: Vec<u8>,
    pub sdk_sysdata: DandelionSystemData,
}

unsafe impl Send for WasmContext {}
unsafe impl Sync for WasmContext {}

impl ContextTrait for WasmContext {
    fn write<T>(&mut self, offset: usize, data: &[T]) -> DandelionResult<()> {

        if !WASM_MEM_ON_HEAP {

            // 1. the function driver is trying to write the DandelionSystemData struct
            //      - we check that the offset is correct and then copy the data into
            //        the system data struct (not into the sdk heap memory)

            if self.layout.as_ref().is_some_and(|l| l.sdk_sysdata_offset == offset) {
                // let layout = (&mut self.layout).as_mut().unwrap();

                if data.len() != 1 { return Err(DandelionError::InvalidWrite); }
                let data = &data[0];
                self.sdk_sysdata = unsafe {  // copy from data buffer
                    std::mem::transmute_copy::<T, DandelionSystemData>(data)
                };
                return Ok(());
            }

            // 2. the function driver is trying to copy binary sections
            //      - these will not be within the region [base_offset..sdk_heap_end]
            //      - we just ignore these writes
            
            else if offset < self.base_offset {
                return Ok(());
            }
            
            // 3. the function driver is trying to write to the sdk heap
            //      - we check that the offset is within the sdk heap region and then
            //        copy the data into the sdk heap memory
            
            else if offset >= self.base_offset {
                let write_size = data.len() * std::mem::size_of::<T>();
                let base = self.base_offset;
                if offset < base || offset + write_size > base + self.data.len() {
                    return Err(DandelionError::InvalidWrite);
                }
                let data_bytes: &[u8] = unsafe {
                    std::slice::from_raw_parts(data.as_ptr() as *const u8, write_size)
                };
                self.data[offset-base..offset-base+write_size].copy_from_slice(data_bytes);
                return Ok(());
            }

            return Err(DandelionError::InvalidWrite);

        } else {
            let write_size = data.len() * std::mem::size_of::<T>();
            let base = self.base_offset;

            if offset < base || offset + write_size > base + self.data.len() {
                return Err(DandelionError::InvalidWrite);
            }
            let data_bytes: &[u8] = unsafe {
                std::slice::from_raw_parts(data.as_ptr() as *const u8, write_size)
            };
            self.data[offset-base..offset-base+write_size].copy_from_slice(data_bytes);
            return Ok(());
        }
        
    }
    fn read<T>(&self, offset: usize, read_buffer: &mut [T]) -> DandelionResult<()> {

        let self_data = &self.data;
        let self_sdk_data = &self.sdk_sysdata;

        let base = self.base_offset;
        let read_size = read_buffer.len() * std::mem::size_of::<T>();

        if offset >= base && offset + read_size < base + self_data.len() {
            if offset < base || offset + read_size > base + self_data.len() {
                return Err(DandelionError::InvalidRead);
            }
            let data_bytes: &[u8] = &self_data[offset-base..offset-base+read_size];
            unsafe {
                (read_buffer.as_mut_ptr() as *mut u8).copy_from(data_bytes.as_ptr(), read_size)
            };
            return Ok(());
        }

        else if self.layout.as_ref().is_some_and(|l| l.sdk_sysdata_offset == offset) {
            // trying to read the DandelionSystemData struct
            if read_buffer.len() != 1 { return Err(DandelionError::InvalidRead); }
            read_buffer[0] = unsafe {
                std::mem::transmute_copy::<DandelionSystemData, T>(&self_sdk_data)
            };
            return Ok(());
        } 

        // turns out, if identifier lengths are 0 their positions is set to
        // 0 as well, but the interface still tries to read them
        else if offset == 0 {
            return Ok(());
        }
        
        else {
            return Err(DandelionError::InvalidRead);
        }
    }
}

impl WasmContext {
    pub fn new(size: usize, base_offset: usize) -> WasmContext {
        WasmContext {
            base_offset,
            layout: None,
            data:           vec![0; size],
            sdk_sysdata:    DandelionSystemData::default(),
        }
    }
    pub fn prepare_for_inputs(
        &mut self, 
        sdk_heap_base: usize, 
        sdk_heap_size: usize,
        sdk_sysdata_offset: usize
    ) {
        self.layout = Some(WasmLayoutDescription {
            sdk_heap_base,
            sdk_heap_size,
            sdk_sysdata_offset,
        });
    }
}

#[derive(Debug)]
pub struct WasmMemoryDomain {}

impl MemoryDomain for WasmMemoryDomain {
    fn init(_config: Vec<u8>) -> DandelionResult<Box<dyn MemoryDomain>> {
        Ok(Box::new(WasmMemoryDomain {}))
    }
    fn acquire_context(&self, size: usize, base_offset: usize) -> DandelionResult<Context> {
        Ok(Context::new(ContextType::Wasm(Box::new(WasmContext::new(size, base_offset))), size, base_offset))
    }
}

pub fn wasm_transfer(
    destination: &mut WasmContext,
    source: &WasmContext,
    destination_offset: usize,
    source_offset: usize,
    size: usize,
) -> DandelionResult<()> {
    // No need to copy anything before sdk heap region
    // This is a hack to make the elf loader work
    if source_offset == 0 { return Ok(()); };

    if source_offset < source.base_offset || size > destination.data.len() {
        return Err(DandelionError::InvalidRead);
    }
    destination.data[destination_offset..destination_offset + size]
        .copy_from_slice(&source.data[source_offset..source_offset + size]);
    Ok(())
}

use std::{fmt::{Debug, Formatter}, sync::Arc};

use crate::memory_domain::{Context, ContextTrait, ContextType, MemoryDomain};
use dandelion_commons::{DandelionError, DandelionResult};

use wasmtime::{Module, Store, Memory, Engine, MemoryType};

pub static WASM_PAGE_SIZE: usize = 64 * 1024;                       // 64KiB
pub static MAX_WASM_MEMORY_SIZE: usize = 4 * 1024 * 1024 * 1024;    // 4GiB

pub struct WasmtimeContext {
    pub store:  Option<Store<()>>,      // store initialized in load()
    pub memory: Option<Memory>,         // must be able to take out the memory during execution
    pub module: Option<Module>,         // module gets compiled in Dandelion engine
}

impl Debug for WasmtimeContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasmtimeContext")
            .field("store", &self.store)
            .field("module", &self.module)
            .field("engine", &"<Engine>")
            .field("memory", &self.memory)
            .finish()
    }
}

unsafe impl Send for WasmtimeContext {}
unsafe impl Sync for WasmtimeContext {}

impl ContextTrait for WasmtimeContext {
    fn write<T>(&mut self, offset: usize, data: &[T]) -> DandelionResult<()> {
        let buffer_byte_size = data.len() * std::mem::size_of::<T>();
        let buffer = unsafe {
            std::slice::from_raw_parts(data.as_ptr() as *const u8, buffer_byte_size)
        };
        self.memory.unwrap().write(
            self.store.as_mut().unwrap(), 
            offset, 
            buffer,
        ).map_err(|_| DandelionError::InvalidWrite)
    }
    fn read<T>(&self, offset: usize, read_buffer: &mut [T]) -> DandelionResult<()> {
        let buffer_byte_size = read_buffer.len() * std::mem::size_of::<T>();
        let buffer = unsafe {
            std::slice::from_raw_parts_mut(read_buffer.as_mut_ptr() as *mut u8, buffer_byte_size)
        };
        self.memory.unwrap().read(
            self.store.as_ref().unwrap(), 
            offset, 
            buffer,
        ).map_err(|_| DandelionError::InvalidRead)
    }
}

#[derive(Debug)]
pub struct WasmtimeMemoryDomain {}

impl MemoryDomain for WasmtimeMemoryDomain {
    fn init(_config: Vec<u8>) -> DandelionResult<Box<dyn MemoryDomain>> {
        Ok(Box::new(WasmtimeMemoryDomain {}))
    }
    fn acquire_context(&self, size: usize) -> DandelionResult<Context> {
        if size > MAX_WASM_MEMORY_SIZE {
            return Err(DandelionError::InvalidMemorySize);
        }
        Ok(Context::new(
            ContextType::Wasmtime(Box::new(
                WasmtimeContext {
                    store: None,
                    module: None,
                    memory: None,
                }
            )),
            (size + WASM_PAGE_SIZE - 1) & !(WASM_PAGE_SIZE - 1)     // round up to next page
        ))
    }
}

pub fn wasmtime_transfer(
    destination: &mut WasmtimeContext,
    source: &WasmtimeContext,
    destination_offset: usize,
    source_offset: usize,
    size: usize,
) -> DandelionResult<()> {
    
    if source.memory.is_none() || destination.memory.is_none() 
        || source_offset + size > source.memory.unwrap().data_size(source.store.as_ref().unwrap())
        || destination_offset + size > destination.memory.unwrap().data_size(destination.store.as_ref().unwrap())
    {
        return Err(DandelionError::InvalidRead);
    }

    destination.memory.unwrap()
        .data_mut(destination.store.as_mut().unwrap())[destination_offset..destination_offset+size]
        .copy_from_slice(
            &source.memory.unwrap()
            .data(source.store.as_ref().unwrap())[source_offset..source_offset+size]
        );
    
    Ok(())
}
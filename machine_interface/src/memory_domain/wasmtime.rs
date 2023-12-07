use std::{fmt::{Debug, Formatter}, sync::Arc};

use crate::memory_domain::{Context, ContextTrait, ContextType, MemoryDomain};
use dandelion_commons::{DandelionError, DandelionResult};

use wasmtime::{Module, Store, Memory, Engine, MemoryType};

pub struct WasmtimeContext {
    pub engine: Engine,
    pub store: Store<()>,
    pub memory: Option<Memory>,         // must be able to take out the memory during execution
    pub module: Option<Module>,         // module gets compiled in engine
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
            &mut self.store, 
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
            &self.store, 
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
        let mut next_page_size = size;
        if size % 2usize.pow(16) != 0 {
            // TODO: maybe this should throw an error, but would currently break domain tests
            next_page_size = size.checked_add( 2usize.pow(16) - (size % 2usize.pow(16)) )
                .ok_or(DandelionError::OutOfMemory)?;
        }
        let pages = next_page_size / 2usize.pow(16);

        // create memory (which is attached to a store (which is attached to an engine))
        let engine = Engine::default();
        let mut store = Store::new(&engine, ());
        let mem_type = MemoryType::new(pages as u32, Some(pages as u32));
        let memory = Some(
            Memory::new(&mut store, mem_type)
                .map_err(|_| DandelionError::OutOfMemory)?
        );

        Ok(Context::new(
            ContextType::Wasmtime(Box::new(
                WasmtimeContext {
                    store,
                    module: None,
                    engine,
                    memory,
                }
            )),
            next_page_size
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
        || source_offset + size > source.memory.unwrap().data_size(&source.store)
        || destination_offset + size > destination.memory.unwrap().data_size(&destination.store)
    {
        return Err(DandelionError::InvalidRead);
    }

    destination.memory.unwrap()
        .data_mut(&mut destination.store)[destination_offset..destination_offset+size]
        .copy_from_slice(
            &source.memory.unwrap()
            .data(&source.store)[source_offset..source_offset+size]
        );
    
    Ok(())
}
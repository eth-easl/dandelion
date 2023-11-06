use std::{rc::Rc, cell::RefCell};

use crate::memory_domain::{Context, ContextTrait, ContextType, MemoryDomain};
use dandelion_commons::{DandelionError, DandelionResult};
use crate::interface::_32_bit::DandelionSystemData;

use libloading::{Library, Symbol};

#[derive(Debug)]
enum WasmContextState {
    Initializing,
    BuildingSystemData,
    Running,
}

#[derive(Debug)]
pub struct WasmLayoutDescription {
    pub sysdata_region_base: usize,
    pub sysdata_region_end: usize,
    pub sdk_heap_base: usize,
    pub sdk_sysdata_offset: usize,
    pub sdk_sysdata: Rc<RefCell<DandelionSystemData>>,
    pub sysdata_region: Rc<RefCell<Vec<u8>>>,
}

#[derive(Debug)]
pub struct WasmContext {
    pub memsize: usize,  // the virtual memory size of the wasm module; `data` only stores the system data region which is much smaller
    pub layout: Option<WasmLayoutDescription>,
    // pub state: WasmContextState,
}

unsafe impl Send for WasmContext {}
unsafe impl Sync for WasmContext {}

impl ContextTrait for WasmContext {
    fn write<T>(&mut self, offset: usize, data: &[T]) -> DandelionResult<()> {

        // if any of the required fields are not set, we are not ready to write
        // this happens when the function driver tries to write the binary content
        // we just ignore that here
        if self.layout.is_none() {
            return Ok(());
        }

        let layout = &mut self.layout.as_mut().unwrap();

        if offset == layout.sdk_sysdata_offset {
            // trying to write the DandelionSystemData struct
            if !data.len() == 1 { return Err(DandelionError::InvalidWrite); }
            let data = &data[0];
            // allocate struct
            let mut sdk_sysdata = DandelionSystemData::default();
            // copy from data buffer
            sdk_sysdata = unsafe {
                std::mem::transmute_copy::<T, DandelionSystemData>(data)
            };
            layout.sdk_sysdata = Rc::new(RefCell::new(sdk_sysdata));
            return Ok(());
        }

        // otherwise the write should be to the system data region
        if offset < layout.sysdata_region_base || offset >= layout.sysdata_region_end {
            return Err(DandelionError::InvalidWrite);
        }

        // write data to buffer
        let offset = offset - layout.sysdata_region_base;
        let write_size = data.len() * std::mem::size_of::<T>();
        if offset + write_size > layout.sysdata_region.borrow().len() {
            return Err(DandelionError::InvalidWrite);
        }
        let data_bytes: &[u8] = unsafe {
            std::slice::from_raw_parts(data.as_ptr() as *const u8, write_size)
        };
        layout.sysdata_region.borrow_mut()[offset..offset + write_size].copy_from_slice(data_bytes);
        return Ok(());
    }
    fn read<T>(&self, offset: usize, read_buffer: &mut [T]) -> DandelionResult<()> {

        // if any of the required fields are not set, the function driver is trying to read binary sections
        // doing nothing in that case
        if self.layout.is_none() {
            return Ok(());
        }

        let layout = &self.layout.as_ref().unwrap();

        if offset == layout.sdk_sysdata_offset {
            // trying to read the DandelionSystemData struct
            if !read_buffer.len() == 1 { return Err(DandelionError::InvalidRead); }
            let sdk_sysdata = layout.sdk_sysdata.as_ref();
            read_buffer[0] = unsafe {
                std::mem::transmute_copy::<DandelionSystemData, T>(&sdk_sysdata.borrow())
            };
            return Ok(());
        }

        // otherwise the read should be from the system data region
        if offset < layout.sysdata_region_base || offset >= layout.sysdata_region_end {
            return Err(DandelionError::InvalidRead);
        }

        // read data from buffer
        let offset = offset - layout.sysdata_region_base;
        let read_size = read_buffer.len() * std::mem::size_of::<T>();
        if offset + read_size > layout.sysdata_region.borrow().len() {
            return Err(DandelionError::InvalidRead);
        }
        let data_bytes: &[u8] = &layout.sysdata_region.borrow()[offset..offset + read_size];
        unsafe {
            (read_buffer.as_mut_ptr() as *mut u8).copy_from(data_bytes.as_ptr(), read_size)
        };
        return Ok(());
    }
}

impl WasmContext {
    pub fn new(memsize: usize) -> WasmContext {
        // the memsize is the size of the virtual wasm memory
        // we don't store the whole memory, but only the system data region
        // all the `None` fields will be initialized in `prepare_for_inputs`
        WasmContext {
            // sysdata_region_base_offset: None,
            memsize,
            layout: None,
            // state: WasmContextState::Initializing,
        }
    }
    pub fn prepare_for_inputs(
        &mut self, 
        sysdata_region_base: usize, 
        sysdata_region_end: usize, 
        sdk_heap_base: usize, 
        sdk_sysdata_offset: usize
    ) {
        // allocate system data region
        
        self.layout = Some(WasmLayoutDescription {
            sysdata_region_base,
            sysdata_region_end,
            sdk_heap_base,
            sdk_sysdata_offset,
            sdk_sysdata: Rc::new(RefCell::new(DandelionSystemData::default())),
            sysdata_region: Rc::new(RefCell::new(vec![0; sysdata_region_end - sysdata_region_base])),
        });

        // self.state = WasmContextState::BuildingSystemData;
    }
    pub fn prepare_for_run(&mut self) {
        // self.state = WasmContextState::Running;
    }
}

#[derive(Debug)]
pub struct WasmMemoryDomain {}

impl MemoryDomain for WasmMemoryDomain {
    fn init(_config: Vec<u8>) -> DandelionResult<Box<dyn MemoryDomain>> {
        Ok(Box::new(WasmMemoryDomain {}))
    }
    fn acquire_context(&self, size: usize, base_offset: usize) -> DandelionResult<Context> {
        Ok(Context::new(ContextType::Wasm(Box::new(WasmContext::new(size))), size, base_offset))
    }
}

// pub fn wasm_transfer(
//     destination: &mut Context,
//     source: &WasmContext,
//     destination_offset: usize,
//     source_offset: usize,
//     size: usize,
// ) -> DandelionResult<()> {
//     unimplemented!()
// }
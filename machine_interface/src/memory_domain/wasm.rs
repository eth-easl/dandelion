use crate::memory_domain::{Context, ContextTrait, ContextType, MemoryDomain, MemoryResource};
use crate::util::mmapmem::MmapMem;
use dandelion_commons::{DandelionError, DandelionResult};
use nix::sys::mman::ProtFlags;

#[derive(Debug)]
pub struct WasmContext {
    pub mem: MmapMem,
}

unsafe impl Send for WasmContext {}
unsafe impl Sync for WasmContext {}

impl ContextTrait for WasmContext {
    fn write<T>(&mut self, offset: usize, data: &[T]) -> DandelionResult<()> {
        self.mem.write(offset, data)
    }
    fn read<T>(&self, offset: usize, read_buffer: &mut [T]) -> DandelionResult<()> {
        self.mem.read(offset, read_buffer)
    }
}

#[derive(Debug)]
pub struct WasmMemoryDomain {}

impl MemoryDomain for WasmMemoryDomain {
    fn init(_config: MemoryResource) -> DandelionResult<Box<dyn MemoryDomain>> {
        Ok(Box::new(WasmMemoryDomain {}))
    }

    fn acquire_context(&self, size: usize) -> DandelionResult<Context> {
        Ok(Context::new(
            ContextType::Wasm(Box::new(WasmContext {
                mem: MmapMem::create(size, ProtFlags::PROT_READ | ProtFlags::PROT_WRITE, false)?,
            })),
            size,
        ))
    }
}

pub fn wasm_transfer(
    destination: &mut WasmContext,
    source: &WasmContext,
    destination_offset: usize,
    source_offset: usize,
    size: usize,
) -> DandelionResult<()> {
    if source_offset + size > source.mem.size() {
        return Err(DandelionError::InvalidRead);
    }
    destination.mem[destination_offset..destination_offset + size]
        .copy_from_slice(&source.mem[source_offset..source_offset + size]);
    Ok(())
}

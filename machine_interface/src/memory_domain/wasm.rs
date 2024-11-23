use crate::util::mmapmem::MmapMem;
use crate::{
    memory_domain::{Context, ContextTrait, ContextType, MemoryDomain, MemoryResource},
    util::mmapmem::MmapMemPool,
};
use dandelion_commons::{DandelionError, DandelionResult, DomainError};
use nix::sys::mman::ProtFlags;

pub static WASM_PAGE_SIZE: usize = 64 * 1024; // 64KiB
pub static MAX_WASM_MEMORY_SIZE: usize = 4 * 1024 * 1024 * 1024; // 4GiB

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
    fn get_chunk_ref(&self, offset: usize, length: usize) -> DandelionResult<&[u8]> {
        self.mem.get_chunk_ref(offset, length)
    }
}

#[derive(Debug)]
pub struct WasmMemoryDomain {
    memory_pool: MmapMemPool,
}

impl MemoryDomain for WasmMemoryDomain {
    fn init(config: MemoryResource) -> DandelionResult<Box<dyn MemoryDomain>> {
        let size = match config {
            MemoryResource::Anonymous { size } => size,
            _ => return Err(DandelionError::DomainError(DomainError::ConfigMissmatch)),
        };
        let memory_pool =
            MmapMemPool::create(size, ProtFlags::PROT_READ | ProtFlags::PROT_WRITE, None)?;
        Ok(Box::new(WasmMemoryDomain { memory_pool }))
    }

    fn acquire_context(&self, size: usize) -> DandelionResult<Context> {
        if size > MAX_WASM_MEMORY_SIZE {
            return Err(DandelionError::DomainError(DomainError::InvalidMemorySize));
        }
        let size = (size + WASM_PAGE_SIZE - 1) & !(WASM_PAGE_SIZE - 1); // round up to next page size
        let (mem_space, allocated_size) = self
            .memory_pool
            .get_allocation(size, nix::sys::mman::MmapAdvise::MADV_DONTNEED)?;

        Ok(Context::new(
            ContextType::Wasm(Box::new(WasmContext { mem: mem_space })),
            allocated_size,
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
    return Ok(());
}

#[cfg(feature = "bytes_context")]
pub fn bytes_to_wasm_transfer(
    destination: &mut WasmContext,
    source: &crate::memory_domain::bytes_context::BytesContext,
    destination_offset: usize,
    source_offset: usize,
    size: usize,
) -> DandelionResult<()> {
    if destination_offset + size > destination.mem.size() {
        return Err(DandelionError::InvalidWrite);
    }
    let wasm_slice = &mut destination.mem[destination_offset..destination_offset + size];
    source.read(source_offset, wasm_slice)?;
    return Ok(());
}

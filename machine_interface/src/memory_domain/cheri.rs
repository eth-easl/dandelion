use libc::size_t;

#[repr(C)]
struct cheri_cap {
    cap: [i8; 16],
}

#[link(name = "cheri_mem")]
extern "C" {
    fn cheri_alloc(size: size_t) -> *const cheri_cap;
    fn cheri_free(cap: *const cheri_cap) -> ();
}

use super::super::{HardwareError, HwResult};
use super::{Context, ContextTrait, MemoryDomain};

pub struct CheriContext {
    capability: [i8; 16],
}

impl ContextTrait for CheriContext {
    fn write(&mut self, offset: usize, data: Vec<u8>) -> HwResult<()> {
        Err(HardwareError::Default)
    }
    fn read(&mut self, offset: usize, read_size: usize, sanitize: bool) -> HwResult<Vec<u8>> {
        Err(HardwareError::Default)
    }
}

pub struct CheriMemoryDomain {}

impl MemoryDomain for CheriMemoryDomain {
    fn init(config: Vec<u8>) -> HwResult<Box<Self>> {
        Ok(Box::new(CheriMemoryDomain {}))
    }
    fn acquire_context(&self, size: usize) -> HwResult<Context> {
        let mut mem_space = Vec::new();
        if (mem_space.try_reserve_exact(size)) != Ok(()) {
            return Err(HardwareError::OutOfMemory);
        }
        mem_space.resize(size, 0);
        Ok(Context::Cheri(Box::new(CheriContext {
            capability: [0; 16],
        })))
    }
    fn release_context(&self, context: Context) -> HwResult<()> {
        match context {
            Context::Cheri(_) => Ok(()),
            _ => Err(HardwareError::ContextMissmatch),
        }
    }
}

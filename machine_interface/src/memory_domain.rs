// list of memory domain implementations
mod cheri;
mod malloc;
mod pagetable;

// import parent for depenencies
use super::HwResult;

// https://docs.rs/enum_dispatch/latest/enum_dispatch/index.html
// check if this would be better way to do it
pub enum Context {
    Malloc(Box<malloc::MallocContext>),
    Cheri(Box<cheri::CheriContext>),
    Pagetable(Box<pagetable::PagetableContext>),
}

pub trait ContextTrait {
    fn write(&mut self, offset: usize, data: Vec<u8>) -> HwResult<()>;
    fn read(&mut self, offset: usize, read_size: usize, sanitize: bool) -> HwResult<Vec<u8>>;
}

impl ContextTrait for Context {
    fn write(&mut self, offset: usize, data: Vec<u8>) -> HwResult<()> {
        match self {
            Context::Malloc(context) => context.write(offset, data),
            Context::Cheri(context) => context.write(offset, data),
            Context::Pagetable(context) => context.write(offset, data),
        }
    }
    fn read(&mut self, offset: usize, read_size: usize, sanitize: bool) -> HwResult<Vec<u8>> {
        match self {
            Context::Malloc(context) => context.read(offset, read_size, sanitize),
            Context::Cheri(context) => context.read(offset, read_size, sanitize),
            Context::Pagetable(context) => context.read(offset, read_size, sanitize),
        }
    }
}

pub trait MemoryDomain {
    // allocation and distruction
    fn init(config: Vec<u8>) -> HwResult<Box<Self>>;
    fn acquire_context(&self, size: usize) -> HwResult<Context>;
    fn release_context(&self, context: Context) -> HwResult<()>;
}

// Code to specialize transfers between different domains
pub fn transefer_memory(
    mut destination: Context,
    mut source: Context,
    destination_offset: usize,
    source_offset: usize,
    size: usize,
    sanitize: bool,
    callback: impl FnOnce(HwResult<()>, Context, Context) -> (),
) {
    let result = match (&mut destination, &mut source) {
        (Context::Malloc(destination_ctxt), Context::Malloc(source_ctxt)) => {
            malloc::malloc_transfer(
                destination_ctxt,
                source_ctxt,
                destination_offset,
                source_offset,
                size,
                sanitize,
            )
        }
        // default implementation using reads and writes
        (destination, source) => {
            let read_result = source.read(source_offset, size, sanitize);
            match read_result {
                Ok(read_value) => destination.write(destination_offset, read_value),
                Err(err) => Err(err),
            }
        }
    };
    callback(result, destination, source);
}

#[cfg(test)]
mod tests;

use crate::{
    memory_domain::{transfer_memory, Context, MemoryDomain},
    DataRequirementList,
};
use dandelion_commons::{DandelionError, DandelionResult};
use std::sync::Arc;

#[cfg(any(feature = "cheri", feature = "mmu", feature = "kvm", feature = "gpu"))]
pub fn load_u8_from_file(full_path: String) -> DandelionResult<Vec<u8>> {
    let mut file = match std::fs::File::open(full_path) {
        Ok(f) => f,
        Err(_) => return Err(DandelionError::FileError),
    };

    let mut buffer = Vec::<u8>::new();
    use std::io::Read;
    let _file_size = match file.read_to_end(&mut buffer) {
        Ok(s) => s,
        Err(_) => return Err(DandelionError::FileError),
    };
    return Ok(buffer);
}

pub fn load_static(
    domain: &Box<dyn MemoryDomain>,
    static_context: Arc<Context>,
    requirement_list: &DataRequirementList,
    ctx_size: usize,
) -> DandelionResult<Context> {
    let mut function_context = domain.acquire_context(ctx_size)?;

    if static_context.content.len() != 1 {
        return Err(DandelionError::ConfigMissmatch);
    }
    // copy sections to the new context
    let static_set = static_context.content[0]
        .as_ref()
        .ok_or(DandelionError::ConfigMissmatch)?;
    if static_set.buffers.len() != requirement_list.static_requirements.len() {
        return Err(DandelionError::ConfigMissmatch);
    }
    let layout = &static_set.buffers;
    let static_pairs = layout
        .iter()
        .zip(requirement_list.static_requirements.iter());
    let mut max_end = 0;
    for (item, requirement) in static_pairs {
        let position = item.data;
        if requirement.size < position.size {
            return Err(DandelionError::ConfigMissmatch);
        }
        transfer_memory(
            &mut function_context,
            static_context.clone(),
            requirement.offset,
            position.offset,
            position.size,
        )?;
        max_end = core::cmp::max(max_end, requirement.offset + requirement.size);
    }
    // round up to next page
    max_end = ((max_end + 4095) / 4096) * 4096;
    function_context.occupy_space(0, max_end)?;
    return Ok(function_context);
}

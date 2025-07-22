use crate::{
    memory_domain::{Context, ContextTrait, MemoryDomain},
    DataRequirementList,
};
use dandelion_commons::{DandelionError, DandelionResult};

pub fn load_static(
    domain: &Box<dyn MemoryDomain>,
    static_data: &Vec<u8>,
    requirement_list: &DataRequirementList,
    ctx_size: usize,
) -> DandelionResult<Context> {
    let mut function_context = domain.acquire_context(ctx_size)?;

    // copy sections to the new context
    let mut max_end = 0;
    for (requirement, position) in requirement_list.static_requirements.iter() {
        if requirement.size < position.size {
            return Err(DandelionError::ConfigMissmatch);
        }
        let data_end = position.offset + position.size;
        function_context.write(requirement.offset, &static_data[position.offset..data_end])?;
        max_end = core::cmp::max(max_end, requirement.offset + requirement.size);
    }
    // round up to next page
    max_end = ((max_end + 4095) / 4096) * 4096;
    function_context.occupy_space(0, max_end)?;
    return Ok(function_context);
}

use crate::{
    memory_domain::{transefer_memory, Context, MemoryDomain},
    DataRequirementList, Position,
};
use dandelion_commons::{DandelionError, DandelionResult};

pub fn load_static(
    domain: &mut Box<dyn MemoryDomain>,
    static_context: &mut Context,
    requirement_list: &DataRequirementList,
) -> DandelionResult<Context> {
    let mut function_context = domain.acquire_context(requirement_list.size)?;
    let layout = static_context.static_data.to_vec();
    if layout.len() != requirement_list.static_requirements.len() {
        return Err(DandelionError::ConfigMissmatch);
    }
    let static_pairs = layout
        .iter()
        .zip(requirement_list.static_requirements.iter());
    for (position, requirement) in static_pairs {
        if requirement.size < position.size {
            return Err(DandelionError::ConfigMissmatch);
        }
        transefer_memory(
            &mut function_context,
            static_context,
            requirement.offset,
            position.offset,
            position.size,
            false,
        )?;
        function_context.static_data.push(Position {
            offset: requirement.offset,
            size: requirement.size,
        });
    }
    return Ok(function_context);
}

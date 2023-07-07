use crate::{
    memory_domain::{transefer_memory, Context, MemoryDomain},
    DataRequirementList,
};
use dandelion_commons::{DandelionError, DandelionResult};

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
    static_context: &Context,
    requirement_list: &DataRequirementList,
) -> DandelionResult<Context> {
    let mut function_context = domain.acquire_context(requirement_list.size)?;
    if static_context.content.len() != 1
        || static_context.content[0].buffers.len() != requirement_list.static_requirements.len()
    {
        return Err(DandelionError::ConfigMissmatch);
    }
    let layout = &static_context.content[0].buffers;
    let static_pairs = layout
        .iter()
        .zip(requirement_list.static_requirements.iter());
    for (item, requirement) in static_pairs {
        let position = item.data;
        if requirement.size < position.size {
            return Err(DandelionError::ConfigMissmatch);
        }
        function_context.occupy_space(requirement.offset, requirement.size)?;
        transefer_memory(
            &mut function_context,
            static_context,
            requirement.offset,
            position.offset,
            position.size,
        )?;
    }
    return Ok(function_context);
}

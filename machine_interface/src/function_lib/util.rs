use crate::{
    memory_domain::{transefer_memory, Context, MemoryDomain},
    DataRequirementList, Position,
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
        )?;
        function_context.static_data.push(Position {
            offset: requirement.offset,
            size: requirement.size,
        });
    }
    function_context.protection_requirements = static_context.protection_requirements.clone();
    return Ok(function_context);
}

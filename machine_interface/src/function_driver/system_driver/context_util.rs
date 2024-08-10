use dandelion_commons::{DandelionError, DandelionResult};

use crate::{memory_domain::{Context, ContextTrait}, DataSet, DataItem};

pub fn get_dataset_by_ident(context: &Context, ident: String) -> DandelionResult<&DataSet> {
    return match context.content.iter().find(|set_option| {
        if let Some(set) = set_option {
            return set.ident == ident;
        } else {
            return false;
        }
    }) {
        Some(Some(set)) => Ok(set),
        _ => {
            Err(DandelionError::MalformedSystemFuncArg(format!("No dataset {ident}")))
        }
    };
}

pub fn get_dataitem_by_ident(dataset: &DataSet, ident: String) -> DandelionResult<&DataItem> {
    return match dataset.buffers.iter().find(|dataitem| {
        return dataitem.ident == ident;
    }) {
        Some(dataitem) => Ok(dataitem),
        _ => {
            Err(DandelionError::MalformedSystemFuncArg(format!("No dataitem {ident}")))
        }
    };
}

pub fn read_dataitem_content(context: &Context, dataitem: &DataItem) -> DandelionResult<Vec<u8>> {
    let mut request_buffer = Vec::with_capacity(dataitem.data.size);
    request_buffer.resize(dataitem.data.size, 0);
    context.read(dataitem.data.offset, &mut request_buffer)?;
    return Ok(request_buffer);
}

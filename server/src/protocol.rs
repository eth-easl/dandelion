mod body;
mod envelope;
#[cfg(test)]
mod tests;

use crate::proto;
pub use body::{DandelionBody, DandelionBuf};
use bytes::Bytes;
use dandelion_commons::{dandelion_err, DError, DandelionError, DandelionResult, FrontendError};
pub use envelope::PROTOCOL_VERSION;
use machine_interface::{
    memory_domain::{bytes_context::BytesContext, Context, ContextTrait, ContextType},
    DataItem, DataSet, Position,
};

fn invalid(message: impl Into<String>) -> DError {
    dandelion_err!(DandelionError::RequestError(FrontendError::InvalidRequest(
        message.into()
    )))
}

fn encoding_error(message: impl Into<String>) -> DError {
    dandelion_err!(DandelionError::RequestError(FrontendError::InternalError(
        message.into()
    )))
}

#[derive(Debug)]
pub enum InvocationTarget {
    Name(String),
    Composition(String),
}

#[derive(Debug)]
pub struct DandelionRequest {
    pub target: InvocationTarget,
    pub context: Context,
}

#[derive(Debug)]
pub struct RegisterFunction {
    pub name: String,
    pub context_size: usize,
    pub engine_type: String,
    pub local_path: String,
    pub binary: Vec<u8>,
    pub input_sets: Vec<(String, Option<Vec<(String, Vec<u8>)>>)>,
    pub output_sets: Vec<String>,
    pub min_set_bytes: Vec<usize>,
}

#[derive(Debug)]
pub struct RegisterChain {
    pub composition: String,
}

fn payload_size(length: u64, remaining: usize) -> DandelionResult<usize> {
    let size =
        usize::try_from(length).map_err(|_| invalid("Payload length exceeds platform capacity"))?;
    if size > remaining {
        return Err(invalid("Payload is truncated"));
    }
    Ok(size)
}

impl DandelionRequest {
    pub fn decode(frames: Vec<Bytes>) -> DandelionResult<Self> {
        let (metadata, payload) = envelope::decode_frames::<proto::DandelionRequest>(frames)?;
        let target = match metadata
            .target
            .ok_or_else(|| invalid("Missing invocation target"))?
        {
            proto::dandelion_request::Target::Name(name) => InvocationTarget::Name(name),
            proto::dandelion_request::Target::Composition(source) => {
                InvocationTarget::Composition(source)
            }
        };
        let total = payload.iter().map(Bytes::len).sum(); // Already checked by envelope decoding.
        let mut offset = 0;
        let mut sets = Vec::with_capacity(metadata.sets.len());
        for set in metadata.sets {
            let mut buffers = Vec::with_capacity(set.items.len());
            for item in set.items {
                let size = payload_size(item.payload_length, total - offset)?;
                buffers.push(DataItem {
                    ident: item.identifier,
                    key: item.key,
                    data: Position { offset, size },
                });
                offset += size;
            }
            sets.push(Some(DataSet {
                ident: set.identifier,
                buffers,
            }));
        }
        if offset != total {
            return Err(invalid("Unexpected trailing payload bytes"));
        }
        let mut context = Context::new(
            ContextType::Bytes(Box::new(BytesContext::new(payload))),
            total,
        );
        context.occupy_space(0, total)?;
        context.content = sets;
        Ok(Self { target, context })
    }
}

impl RegisterFunction {
    pub fn decode(frames: Vec<Bytes>) -> DandelionResult<Self> {
        let (metadata, payload) = envelope::decode_frames::<proto::RegisterFunction>(frames)?;
        if metadata.name.is_empty() {
            return Err(invalid("Missing function name"));
        }
        if metadata.engine_type.is_empty() {
            return Err(invalid("Missing engine type"));
        }
        let context_size = usize::try_from(metadata.context_size)
            .map_err(|_| invalid("Context size exceeds platform capacity"))?;
        let min_set_bytes = metadata
            .min_set_bytes
            .into_iter()
            .map(|size| {
                usize::try_from(size)
                    .map_err(|_| invalid("Minimum set size exceeds platform capacity"))
            })
            .collect::<DandelionResult<Vec<_>>>()?;
        let total: usize = payload.iter().map(Bytes::len).sum();
        let payload = BytesContext::new(payload);
        let mut offset = 0;
        let mut read_data = |length: u64| -> DandelionResult<Vec<u8>> {
            let size = payload_size(length, total - offset)?;
            let mut data = vec![0; size];
            payload.read(offset, &mut data)?;
            offset += size;
            Ok(data)
        };
        let binary = read_data(metadata.binary_length)?;
        let input_sets = metadata
            .input_sets
            .into_iter()
            .map(|set| {
                let items = set
                    .static_items
                    .map(|items| {
                        items
                            .items
                            .into_iter()
                            .map(|item| Ok((item.identifier, read_data(item.payload_length)?)))
                            .collect::<DandelionResult<Vec<_>>>()
                    })
                    .transpose()?;
                Ok((set.identifier, items))
            })
            .collect::<DandelionResult<Vec<_>>>()?;
        if offset != total {
            return Err(invalid("Unexpected trailing payload bytes"));
        }
        Ok(Self {
            name: metadata.name,
            context_size,
            engine_type: metadata.engine_type,
            local_path: metadata.local_path,
            binary,
            input_sets,
            output_sets: metadata.output_sets,
            min_set_bytes,
        })
    }
}

impl RegisterChain {
    pub fn decode(frames: Vec<Bytes>) -> DandelionResult<Self> {
        let (metadata, payload) = envelope::decode_frames::<proto::RegisterChain>(frames)?;
        if !payload.is_empty() {
            return Err(invalid("Composition registration must not contain payload"));
        }
        Ok(Self {
            composition: metadata.composition,
        })
    }
}

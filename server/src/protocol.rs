mod body;
mod envelope;
#[cfg(test)]
mod tests;

use crate::proto;
pub use body::{DandelionBody, DandelionBuf};
use bytes::Bytes;
use dandelion_commons::{dandelion_err, DandelionError, DandelionResult, FrontendError};
pub use envelope::PROTOCOL_VERSION;
use memory::{
    context::{bytes::BytesContext, ContextDataItem, ContextDataSet, ContextTrait, ContextType},
    Context, Position,
};

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
pub enum Executable {
    LocalPath(String),
    Binary(Vec<u8>),
}

#[derive(Debug)]
pub struct RegisterFunction {
    pub name: String,
    pub context_size: usize,
    pub engine_type: String,
    pub executable: Executable,
    pub input_sets: Vec<(String, Option<Vec<(String, Vec<u8>)>>)>,
    pub output_sets: Vec<String>,
    pub min_set_bytes: Vec<usize>,
}

#[derive(Debug)]
pub struct RegisterChain {
    pub composition: String,
}

fn payload_size(length: u64, remaining: usize) -> DandelionResult<usize> {
    let size = length as usize;
    if size > remaining {
        return Err(dandelion_err!(DandelionError::RequestError(
            FrontendError::StreamEnd
        )));
    }
    Ok(size)
}

impl DandelionRequest {
    pub fn decode(frames: Vec<Bytes>) -> DandelionResult<Self> {
        let (metadata, payload) = envelope::decode_frames::<proto::DandelionRequest>(frames)?;
        let target = match metadata.target.ok_or_else(|| {
            dandelion_err!(DandelionError::RequestError(
                FrontendError::MalformedMessage
            ))
        })? {
            proto::dandelion_request::Target::Name(name) => InvocationTarget::Name(name),
            proto::dandelion_request::Target::Composition(source) => {
                InvocationTarget::Composition(source)
            }
        };
        let total = payload.iter().map(Bytes::len).sum();
        let mut offset = 0;
        let mut sets = Vec::with_capacity(metadata.sets.len());
        for set in metadata.sets {
            let start = offset;
            let mut items = Vec::with_capacity(set.items.len());
            for item in set.items {
                let size = payload_size(item.payload_length, total - offset)?;
                items.push(ContextDataItem {
                    ident: item.identifier,
                    key: item.key,
                    data: Position { offset, size },
                });
                offset += size;
            }
            sets.push(Some(ContextDataSet {
                items,
                total_size: offset - start,
            }));
        }
        if offset != total {
            return Err(dandelion_err!(DandelionError::RequestError(
                FrontendError::ViolatedSpec
            )));
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
            return Err(dandelion_err!(DandelionError::RequestError(
                FrontendError::MalformedMessage
            )));
        }
        if metadata.engine_type.is_empty() {
            return Err(dandelion_err!(DandelionError::RequestError(
                FrontendError::MalformedMessage
            )));
        }
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
        let executable = match metadata.executable.ok_or_else(|| {
            dandelion_err!(DandelionError::RequestError(
                FrontendError::MalformedMessage
            ))
        })? {
            proto::register_function::Executable::LocalPath(path) => Executable::LocalPath(path),
            proto::register_function::Executable::BinaryLength(length) => {
                Executable::Binary(read_data(length)?)
            }
        };
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
            return Err(dandelion_err!(DandelionError::RequestError(
                FrontendError::ViolatedSpec
            )));
        }
        Ok(Self {
            name: metadata.name,
            context_size: metadata.context_size as usize,
            engine_type: metadata.engine_type,
            executable,
            input_sets,
            output_sets: metadata.output_sets,
            min_set_bytes: metadata
                .min_set_bytes
                .into_iter()
                .map(|size| size as usize)
                .collect(),
        })
    }
}

impl RegisterChain {
    pub fn decode(frames: Vec<Bytes>) -> DandelionResult<Self> {
        let (metadata, payload) = envelope::decode_frames::<proto::RegisterChain>(frames)?;
        if !payload.is_empty() {
            return Err(dandelion_err!(DandelionError::RequestError(
                FrontendError::ViolatedSpec
            )));
        }
        Ok(Self {
            composition: metadata.composition,
        })
    }
}

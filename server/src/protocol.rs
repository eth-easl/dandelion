mod body;
mod envelope;
#[cfg(test)]
mod tests;

pub use body::{DandelionBody, DandelionBuf};
pub use envelope::PROTOCOL_VERSION;

use crate::proto;
use bytes::Bytes;
use dandelion_commons::{
    dandelion_err, err_dandelion, DandelionError, DandelionResult, FrontendError,
};
use memory::{context::bytes::BytesContext, DataItem, DataSet, LocalData, Position};
use std::sync::Arc;

#[derive(Debug)]
pub enum InvocationTarget {
    Name(String),
    Composition(String),
}

#[derive(Debug)]
pub struct DandelionRequest {
    pub target: InvocationTarget,
    pub sets: Vec<DataSet>,
}

#[derive(Debug)]
pub enum Executable {
    LocalPath(String),
    Binary(Vec<Bytes>),
}

#[derive(Debug)]
pub struct RegisterFunction {
    pub name: String,
    pub context_size: usize,
    pub engine_type: String,
    pub executable: Executable,
    pub input_sets: Vec<(String, Option<DataSet>)>,
    pub output_sets: Vec<String>,
    pub min_set_bytes: Vec<usize>,
}

#[derive(Debug)]
pub struct RegisterComposition {
    pub composition: String,
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
        let context = Arc::new(BytesContext::create_context(payload, total));
        let sets = metadata
            .sets
            .into_iter()
            .map(|proto_set| {
                let start = offset;
                let items = proto_set
                    .items
                    .into_iter()
                    .map(|item| {
                        let size = item.payload_length as usize;
                        let pos = Position { offset, size };
                        offset += size;
                        DataItem::new_local(
                            item.identifier,
                            item.key,
                            LocalData {
                                ctx: context.clone(),
                                pos,
                            },
                        )
                    })
                    .collect();
                DataSet {
                    items: Arc::new(items),
                    num_unresolved: 0,
                    total_size: offset - start,
                }
            })
            .collect();
        if offset != total {
            return Err(dandelion_err!(DandelionError::RequestError(
                FrontendError::ViolatedSpec
            )));
        }

        Ok(Self { target, sets })
    }
}

impl RegisterFunction {
    pub fn decode(frames: Vec<Bytes>) -> DandelionResult<Self> {
        let (metadata, mut payload) = envelope::decode_frames::<proto::RegisterFunction>(frames)?;
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
        let mut input_frames = Vec::new();
        let mut binary_size = 0;
        let executable = match metadata.executable.ok_or_else(|| {
            dandelion_err!(DandelionError::RequestError(
                FrontendError::MalformedMessage
            ))
        })? {
            proto::register_function::Executable::LocalPath(path) => Executable::LocalPath(path),
            proto::register_function::Executable::BinaryLength(length) => {
                binary_size = length as usize;
                let mut read_bytes = 0;
                let partial_frame = payload.iter().position(|frame| {
                    if read_bytes + frame.len() > binary_size {
                        true
                    } else {
                        read_bytes += frame.len();
                        false
                    }
                });
                match partial_frame {
                    // All frames fully belong to the binary
                    None => {
                        // check that we read all the bytes we expected
                        if read_bytes != binary_size {
                            return err_dandelion!(DandelionError::RequestError(
                                FrontendError::StreamEnd
                            ));
                        }
                    }
                    // there was at least one frame that is only included partially
                    Some(index) => {
                        input_frames = payload.split_off(index);
                        let left_over_bytes = binary_size - read_bytes;
                        let last_binary_frame = input_frames[0].split_to(left_over_bytes);
                        payload.push(last_binary_frame);
                    }
                }
                Executable::Binary(payload)
            }
        };
        let mut offset = binary_size;
        let context = Arc::new(BytesContext::create_context(
            input_frames,
            total - binary_size,
        ));
        let input_sets = metadata
            .input_sets
            .into_iter()
            .map(|registration_set| {
                let set_start = offset;
                let set = registration_set.set.map(|set| {
                    let items = Arc::new(
                        set.items
                            .into_iter()
                            .map(|item| {
                                let item_size = item.payload_length as usize;
                                let item_offset = offset - binary_size;
                                offset += item_size;
                                DataItem::new_local(
                                    item.identifier,
                                    item.key,
                                    LocalData {
                                        pos: Position {
                                            offset: item_offset,
                                            size: item_size,
                                        },
                                        ctx: context.clone(),
                                    },
                                )
                            })
                            .collect(),
                    );
                    DataSet {
                        items,
                        num_unresolved: 0,
                        total_size: offset - set_start,
                    }
                });
                Ok((registration_set.identifier, set))
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

impl RegisterComposition {
    pub fn decode(frames: Vec<Bytes>) -> DandelionResult<Self> {
        let (metadata, payload) = envelope::decode_frames::<proto::RegisterComposition>(frames)?;
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

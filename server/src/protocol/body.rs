use super::{encoding_error, envelope};
use crate::proto;
use bytes::{Buf, Bytes};
use dandelion_commons::{records::Recorder, DandelionError, DandelionResult};
use machine_interface::{
    composition::LocalCompositionSet,
    memory_domain::{Context, ContextTrait},
    Position,
};
use prost::Message;
use std::{collections::VecDeque, io::IoSlice, sync::Arc};

#[derive(Debug)]
enum Segment {
    Metadata(Bytes),
    Payload {
        context: Arc<Context>,
        position: Position,
    },
}
impl Segment {
    fn len(&self) -> usize {
        match self {
            Self::Metadata(bytes) => bytes.len(),
            Self::Payload { position, .. } => position.size,
        }
    }
    fn chunk_at(&self, offset: usize) -> &[u8] {
        match self {
            Self::Metadata(bytes) => &bytes[offset..],
            Self::Payload { context, position } => context
                .get_chunk_ref(position.offset + offset, position.size - offset)
                .expect("Context must contain its declared payload"),
        }
    }
    fn advance(&mut self, count: usize) {
        match self {
            Self::Metadata(bytes) => bytes.advance(count),
            Self::Payload { position, .. } => {
                position.offset += count;
                position.size -= count;
            }
        }
    }
}

/// Streams metadata and context-backed payloads without concatenating them.
#[derive(Debug)]
pub struct DandelionBuf {
    segments: VecDeque<Segment>,
    remaining: usize,
}
impl DandelionBuf {
    pub(super) fn new(
        metadata: &impl Message,
        payloads: Vec<(Position, Arc<Context>)>,
    ) -> DandelionResult<Self> {
        let header = envelope::encode_header(metadata);
        let mut remaining = header.len();
        let mut segments = VecDeque::from([Segment::Metadata(Bytes::from(header))]);
        for (position, context) in payloads {
            if position
                .offset
                .checked_add(position.size)
                .filter(|end| *end <= context.size)
                .is_none()
            {
                return Err(encoding_error("Payload item exceeds its context"));
            }
            remaining = remaining
                .checked_add(position.size)
                .ok_or_else(|| encoding_error("Body size overflows"))?;
            if position.size > 0 {
                segments.push_back(Segment::Payload { context, position });
            }
        }
        Ok(Self {
            segments,
            remaining,
        })
    }
}

impl Buf for DandelionBuf {
    fn remaining(&self) -> usize {
        self.remaining
    }
    fn chunk(&self) -> &[u8] {
        self.segments
            .front()
            .map_or(&[], |segment| segment.chunk_at(0))
    }
    fn advance(&mut self, mut count: usize) {
        assert!(
            count <= self.remaining,
            "Advance exceeds remaining body bytes"
        );
        self.remaining -= count;
        while count > 0 {
            let segment = self.segments.front_mut().unwrap();
            if count < segment.len() {
                segment.advance(count);
                break;
            }
            count -= segment.len();
            self.segments.pop_front();
        }
    }
    fn chunks_vectored<'a>(&'a self, dst: &mut [IoSlice<'a>]) -> usize {
        let mut written = 0;
        for segment in &self.segments {
            let mut offset = 0;
            while offset < segment.len() && written < dst.len() {
                let chunk = segment.chunk_at(offset);
                assert!(!chunk.is_empty(), "Context returned an empty payload chunk");
                dst[written] = IoSlice::new(chunk);
                written += 1;
                offset += chunk.len();
            }
            if written == dst.len() {
                break;
            }
        }
        written
    }
}

pub struct DandelionBody {
    buffer: Option<DandelionBuf>,
}
impl DandelionBody {
    pub fn new(
        sets: Vec<Option<LocalCompositionSet>>,
        _timing: &Recorder,
    ) -> DandelionResult<Self> {
        let mut metadata_sets = Vec::new();
        let mut payloads = Vec::new();
        for set in sets.into_iter().flatten() {
            let identifier = set.get_name().to_string();
            let mut items = Vec::new();
            for (item, context) in set {
                let position = item.data;
                items.push(proto::InputItem {
                    identifier: item.ident,
                    key: item.key,
                    payload_length: position.size as u64,
                });
                payloads.push((position, context));
            }
            metadata_sets.push(proto::InputSet { identifier, items });
        }
        let metadata = proto::DandelionDeserializeResponse {
            sets: metadata_sets,
            timestamps: {
                #[cfg(feature = "timestamp")]
                {
                    Some(format!("{}", _timing))
                }
                #[cfg(not(feature = "timestamp"))]
                {
                    None
                }
            },
        };
        Ok(Self {
            buffer: Some(DandelionBuf::new(&metadata, payloads)?),
        })
    }
    pub fn status(message: impl Into<String>) -> Self {
        let bytes = envelope::encode_header(&proto::StatusResponse {
            message: message.into(),
        });
        let remaining = bytes.len();
        Self {
            buffer: Some(DandelionBuf {
                segments: VecDeque::from([Segment::Metadata(Bytes::from(bytes))]),
                remaining,
            }),
        }
    }
}
impl hyper::body::Body for DandelionBody {
    type Data = DandelionBuf;
    type Error = DandelionError;
    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        std::task::Poll::Ready(
            self.get_mut()
                .buffer
                .take()
                .map(|buffer| Ok(hyper::body::Frame::data(buffer))),
        )
    }
}

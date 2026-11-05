use bytes::{Buf, Bytes};
use dandelion_commons::{dandelion_err, DandelionError, DandelionResult, FrontendError};
use machine_interface::memory_domain::bytes_context::FrameBuf;
use prost::{
    encoding::{decode_varint, encode_varint},
    Message,
};

pub const PROTOCOL_VERSION: u64 = 1;

pub(super) fn encode_header(metadata: &impl Message) -> Vec<u8> {
    let mut header = Vec::new();
    encode_varint(PROTOCOL_VERSION, &mut header);
    header.extend(metadata.encode_length_delimited_to_vec());
    header
}

fn decode_metadata<M: Message + Default>(buf: &mut impl Buf) -> DandelionResult<M> {
    let version = decode_varint(&mut *buf)
        .map_err(|_| dandelion_err!(DandelionError::RequestError(FrontendError::ViolatedSpec)))?;
    if version != PROTOCOL_VERSION {
        return Err(dandelion_err!(DandelionError::RequestError(
            FrontendError::ViolatedSpec
        )));
    }
    let size = prost::decode_length_delimiter(&mut *buf)
        .map_err(|_| dandelion_err!(DandelionError::RequestError(FrontendError::ViolatedSpec)))?;
    if buf.remaining() < size {
        return Err(dandelion_err!(DandelionError::RequestError(
            FrontendError::StreamEnd
        )));
    }
    M::decode(buf.take(size)).map_err(|_| {
        dandelion_err!(DandelionError::RequestError(
            FrontendError::MalformedMessage
        ))
    })
}

pub(super) fn decode_frames<M: Message + Default>(
    mut frames: Vec<Bytes>,
) -> DandelionResult<(M, Vec<Bytes>)> {
    let mut frame_buf = FrameBuf::try_new(&frames)
        .ok_or_else(|| dandelion_err!(DandelionError::UsizeTypeConversionError))?;
    let metadata = decode_metadata(&mut frame_buf)?;
    let (current_frame, current_buf_offset) = frame_buf.position();
    // Keep payload frames intact so invocation data is not copied.
    frames.drain(..current_frame);
    if let Some(frame) = frames.first_mut() {
        frame.advance(current_buf_offset);
    }
    frames.retain(|frame| !frame.is_empty());
    Ok((metadata, frames))
}

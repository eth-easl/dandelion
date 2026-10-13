use super::invalid;
use bytes::{Buf, Bytes};
use dandelion_commons::DandelionResult;
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
        .map_err(|err| invalid(format!("Invalid protocol version: {err}")))?;
    if version != PROTOCOL_VERSION {
        return Err(invalid(format!(
            "Unsupported protocol version {version}, expected {PROTOCOL_VERSION}"
        )));
    }
    let size = prost::decode_length_delimiter(&mut *buf)
        .map_err(|err| invalid(format!("Invalid metadata length: {err}")))?;
    if buf.remaining() < size {
        return Err(invalid("Truncated metadata"));
    }
    M::decode(buf.take(size)).map_err(|err| invalid(format!("Invalid Protobuf metadata: {err}")))
}

pub(super) fn decode_frames<M: Message + Default>(
    mut frames: Vec<Bytes>,
) -> DandelionResult<(M, Vec<Bytes>)> {
    let mut frame_buf = FrameBuf::try_new(&frames).ok_or_else(|| invalid("Body size overflows"))?;
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

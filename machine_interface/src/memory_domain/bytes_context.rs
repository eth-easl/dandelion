use crate::memory_domain::ContextTrait;
use bytes::Bytes;
use dandelion_commons::{err_dandelion, DandelionError, DandelionResult};
use log::error;

pub struct BytesContext {
    frames: Vec<Bytes>,
}

impl BytesContext {
    pub fn new(frames: Vec<Bytes>) -> Self {
        BytesContext { frames }
    }
}

impl core::fmt::Debug for BytesContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("BytesContext {{frames: ["))?;
        for frame in self.frames.iter() {
            f.write_fmt(format_args!("Bytes with len: {}, ", frame.len()))?;
        }
        f.write_fmt(format_args!("]}}"))
    }
}

impl ContextTrait for BytesContext {
    fn write<T>(&mut self, _offset: usize, _data: &[T]) -> dandelion_commons::DandelionResult<()> {
        error!("Tried to write to read only contet");
        err_dandelion!(dandelion_commons::DandelionError::InvalidWrite)
    }
    fn read<T>(
        &self,
        mut offset: usize,
        read_buffer: &mut [T],
    ) -> dandelion_commons::DandelionResult<()> {
        let byte_buffer = unsafe {
            core::slice::from_raw_parts_mut(
                read_buffer.as_ptr() as *mut u8,
                core::mem::size_of_val(read_buffer),
            )
        };
        let mut read_offset = 0usize;
        let mut remaining = byte_buffer.len();
        for frame in self.frames.iter() {
            // check if beginning of read is within current frame
            if frame.len() <= offset {
                offset -= frame.len();
                continue;
            }
            // read starts or has started in current frame
            // check if end is within current frame
            if frame.len() >= offset + remaining {
                // remaining bytes are from current frame
                byte_buffer[read_offset..].copy_from_slice(&frame[offset..offset + remaining]);
                return Ok(());
            } else {
                // end is not in current frame, read current frame until end and go to next
                let subframe = &frame[offset..];
                byte_buffer[read_offset..read_offset + subframe.len()].copy_from_slice(subframe);
                remaining -= subframe.len();
                read_offset += subframe.len();
                offset = 0;
            }
        }
        // Zero-length reads at EOF are valid.
        if offset == 0 && remaining == 0 {
            return Ok(());
        }
        err_dandelion!(DandelionError::InvalidRead)
    }
    fn get_chunk_ref(&self, mut offset: usize, length: usize) -> DandelionResult<&[u8]> {
        for frame in self.frames.iter() {
            if offset >= frame.len() {
                offset -= frame.len();
            } else if frame.len() >= offset + length {
                return Ok(&frame[offset..offset + length]);
            } else {
                return Ok(&frame[offset..]);
            }
        }
        if offset == 0 && length == 0 {
            return Ok(&[]);
        }
        err_dandelion!(DandelionError::InvalidRead)
    }
}

pub struct FrameBuf<'data> {
    byte_iter: &'data Vec<Bytes>,
    remaining: usize,
    current_frame: usize,
    current_buf_offset: usize,
}

impl<'data> FrameBuf<'data> {
    pub fn try_new(byte_iter: &'data Vec<Bytes>) -> Option<Self> {
        let remaining = byte_iter
            .iter()
            .try_fold(0usize, |size, frame| size.checked_add(frame.len()))?;
        Some(Self {
            byte_iter,
            remaining,
            current_frame: 0,
            current_buf_offset: 0,
        })
    }

    pub fn position(&self) -> (usize, usize) {
        (self.current_frame, self.current_buf_offset)
    }
}

impl<'data> bytes::Buf for FrameBuf<'data> {
    fn remaining(&self) -> usize {
        self.remaining
    }

    fn advance(&mut self, cnt: usize) {
        assert!(cnt <= self.remaining);
        let mut to_advance = cnt;
        self.remaining -= cnt;
        while to_advance > 0 {
            let current_frame_size = self.byte_iter[self.current_frame].len();
            let available = current_frame_size - self.current_buf_offset;
            if to_advance < available {
                self.current_buf_offset += to_advance;
                break;
            } else {
                to_advance -= available;
                self.current_buf_offset = 0;
                self.current_frame += 1;
            }
        }
    }

    fn chunk(&self) -> &[u8] {
        self.byte_iter[self.current_frame..]
            .iter()
            .find(|frame| !frame.is_empty())
            .map_or(&[], |frame| &frame[self.current_buf_offset..])
    }
}

#[test]
fn read_test() {
    let first = vec![1u8, 2u8, 3u8, 4u8];
    let second = vec![];
    let third = vec![5u8, 6u8, 7u8];
    let fourth = vec![8u8];
    let frames = vec![first, second, third, fourth]
        .into_iter()
        .map(Bytes::from)
        .collect();
    let read_context = BytesContext { frames };
    let expected_data = vec![1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8, 8u8];
    let mut all_read_vec = vec![0u8; 8];
    read_context
        .read(0, &mut all_read_vec)
        .expect("read should succeed");
    assert_eq!(expected_data, all_read_vec.as_slice());
    let mut partial_read_vec = vec![0u8; 4];
    read_context
        .read(2, &mut partial_read_vec)
        .expect("Partial read should succeed");
    assert_eq!(&expected_data[2..6], partial_read_vec.as_slice());
}

#[test]
fn zero_length_reads_accept_eof_without_allowing_out_of_bounds() {
    for (context, size) in [
        (BytesContext::new(vec![]), 0),
        (BytesContext::new(vec![Bytes::from(vec![1u8, 2u8, 3u8])]), 3),
    ] {
        context.read::<u8>(size, &mut []).unwrap();
        assert!(context.get_chunk_ref(size, 0).unwrap().is_empty());
        assert!(context.read::<u8>(size + 1, &mut []).is_err());
        assert!(context.get_chunk_ref(size + 1, 0).is_err());
        assert!(context.read(size, &mut [0u8]).is_err());
        assert!(context.get_chunk_ref(size, 1).is_err());
    }
}

#[test]
fn frame_buf_handles_empty_frames_and_eof() {
    use bytes::Buf;

    for frames in [
        vec![],
        vec![Bytes::new()],
        vec![Bytes::from(vec![1u8, 2u8]), Bytes::from(vec![3u8, 4u8])],
        vec![
            Bytes::new(),
            Bytes::from(vec![1u8, 2u8]),
            Bytes::new(),
            Bytes::from(vec![3u8, 4u8]),
            Bytes::new(),
        ],
    ] {
        let mut buf = FrameBuf::try_new(&frames).unwrap();
        if buf.has_remaining() {
            assert_eq!(buf.chunk(), &[1u8, 2u8]);
            buf.advance(1);
            assert_eq!(buf.chunk(), &[2u8]);
            buf.advance(2);
            assert_eq!(buf.chunk(), &[4u8]);
            buf.advance(1);
        }
        assert_eq!(buf.remaining(), 0);
        assert!(buf.chunk().is_empty());
        buf.advance(0);
    }
}

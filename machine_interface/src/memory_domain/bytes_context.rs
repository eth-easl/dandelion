use crate::{
    memory_domain::{Context, ContextTrait},
    DataItem, DataSet,
};
use bytes::{Buf, Bytes};
use core::mem::size_of;
use dandelion_commons::{DandelionError, DandelionResult, FrontendError};
use log::{debug, error};

pub struct BytesContext {
    frames: Vec<Bytes>,
}

impl core::fmt::Debug for BytesContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("BytesContext {{frames: ["))?;
        for frame in self.frames.iter() {
            f.write_fmt(format_args!("Bytes with len: {}, ", frame.len()))?;
        }
        f.write_fmt(format_args!("]}}"))?;
        return Ok(());
    }
}

impl ContextTrait for BytesContext {
    fn write<T>(&mut self, _offset: usize, _data: &[T]) -> dandelion_commons::DandelionResult<()> {
        error!("Tried to write to read only contet");
        return Err(dandelion_commons::DandelionError::InvalidWrite);
    }
    fn read<T>(
        &self,
        mut offset: usize,
        read_buffer: &mut [T],
    ) -> dandelion_commons::DandelionResult<()> {
        let byte_buffer = unsafe {
            core::slice::from_raw_parts_mut(
                read_buffer.as_ptr() as *mut u8,
                read_buffer.len() * core::mem::size_of::<T>(),
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
        return Err(DandelionError::InvalidRead);
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
        return Err(DandelionError::InvalidRead);
    }
}

struct FrameBuf<'data> {
    byte_iter: &'data Vec<Bytes>,
    remaining: usize,
    current_frame: usize,
    current_buf_offset: usize,
}

impl<'data> bytes::Buf for FrameBuf<'data> {
    fn remaining(&self) -> usize {
        return self.remaining;
    }

    fn advance(&mut self, cnt: usize) {
        let mut to_advance = cnt;
        self.remaining -= cnt;
        while to_advance > 0 {
            let current_frame_size = self.byte_iter[self.current_frame].len();
            if self.current_buf_offset + to_advance < current_frame_size {
                self.current_buf_offset += to_advance;
                break;
            } else {
                let available = current_frame_size - self.current_buf_offset;
                to_advance -= available;
                self.current_buf_offset = 0;
                self.current_frame += 1;
            }
        }
    }

    fn chunk(&self) -> &[u8] {
        return &(self.byte_iter[self.current_frame])[self.current_buf_offset..];
    }
}

fn check_remaining<T>(buf: &impl bytes::Buf) -> DandelionResult<()> {
    if buf.remaining() < size_of::<T>() {
        return Err(DandelionError::RequestError(FrontendError::StreamEnd));
    } else {
        return Ok(());
    }
}

fn read_type_byte(buf: &mut impl bytes::Buf) -> DandelionResult<i8> {
    check_remaining::<i8>(buf)?;
    return Ok(buf.get_i8());
}

fn read_and_check_termination(buf: &mut impl bytes::Buf) -> DandelionResult<()> {
    check_remaining::<u8>(buf)?;
    if buf.get_u8() == 0 {
        Ok(())
    } else {
        Err(DandelionError::RequestError(FrontendError::ViolatedSpec))
    }
}

fn read_length(buf: &mut impl bytes::Buf) -> DandelionResult<usize> {
    check_remaining::<i32>(buf)?;
    return usize::try_from(buf.get_i32_le()).or(Err(DandelionError::RequestError(
        FrontendError::ViolatedSpec,
    )));
}

fn read_cstring(buf: &mut impl bytes::Buf) -> DandelionResult<String> {
    // cstrings are bytes series of unkown length which are null terminated
    let mut cstring = String::new();
    'string_loop: while buf.remaining() > 0 {
        let mut chunk_index = 0;
        let chunk = buf.chunk();
        while chunk_index < chunk.len() {
            if chunk[chunk_index] == 0 {
                cstring.push_str(std::str::from_utf8(&chunk[0..=chunk_index]).unwrap());
                buf.advance(chunk_index + 1);
                break 'string_loop;
            }
            chunk_index += 1;
        }
        cstring.push_str(std::str::from_utf8(chunk).unwrap());
        buf.advance(chunk_index);
    }
    return Ok(cstring);
}

fn read_and_check_cstring(buf: &mut impl bytes::Buf, expected_string: &str) -> DandelionResult<()> {
    let cstring = read_cstring(buf)?;
    if cstring.as_str() != expected_string {
        debug!(
            "Unexpeced cstring: {:?}, expected: {:?}",
            cstring, expected_string
        );
        return Err(DandelionError::RequestError(
            FrontendError::MalformedMessage,
        ));
    }
    return Ok(());
}

fn read_string(buf: &mut impl bytes::Buf) -> DandelionResult<String> {
    // get string length, remove trainling null char, as they are not needed for rust string
    let string_length = read_length(buf)? - 1;
    let mut byte_buffer = Vec::with_capacity(string_length);
    byte_buffer.resize(string_length, 0);
    if buf.remaining() < string_length {
        return Err(DandelionError::RequestError(FrontendError::StreamEnd));
    }
    // the string length describes a character buffer of lenght n with a trailing null char
    // do not copy null char
    buf.copy_to_slice(byte_buffer.as_mut_slice());
    // read the trailing null bytes
    read_and_check_termination(buf)?;

    let string = String::from_utf8(byte_buffer).or(Err(DandelionError::RequestError(
        FrontendError::ViolatedSpec,
    )))?;
    return Ok(string);
}

fn read_data_item(
    buf: &mut impl bytes::Buf,
    total_length: usize,
) -> DandelionResult<Option<DataItem>> {
    // check item doc type
    let item_doc_type = read_type_byte(buf)?;
    match item_doc_type {
        0 => return Ok(None),
        3 => (),
        _ => {
            debug!("Expected item of type doc, found: {}", item_doc_type);
            return Err(DandelionError::RequestError(
                FrontendError::MalformedMessage,
            ));
        }
    }
    let _doc_name = read_cstring(buf)?;
    let _doc_lenght = read_length(buf)?;

    // allow for arbitrary ordering of data item fields
    let mut ident = "".to_string();
    let mut key = 0;
    let mut offset = 0;
    let mut size = 0;
    let mut check: u8 = 0;
    for _ in 0..3 {
        let next_type = read_type_byte(buf)?;
        match next_type {
            2 => {
                // -> expect the identifier part
                read_and_check_cstring(buf, "identifier\0")?;
                ident = read_string(buf)?;
                check |= 1;
            }
            // accept key either as int64 or int32
            16 => {
                // -> expect the key part
                read_and_check_cstring(buf, "key\0")?;
                key = u32::try_from(buf.get_i32_le()).or(Err(DandelionError::RequestError(
                    FrontendError::MalformedMessage,
                )))?;
                check |= 2;
            }
            18 => {
                // -> expect the key part
                read_and_check_cstring(buf, "key\0")?;
                key = u32::try_from(buf.get_i64_le()).or(Err(DandelionError::RequestError(
                    FrontendError::MalformedMessage,
                )))?;
                check |= 2;
            }
            5 => {
                // -> expect the data part
                read_and_check_cstring(buf, "data\0")?;
                size = read_length(buf)?;
                let _binary_subtype = read_type_byte(buf)?;
                offset = total_length - buf.remaining();
                buf.advance(size);
                check |= 4;
            }
            x => {
                debug!(
                    "Got type {}, expected either 2 (string), 5 (binary_data), 16 (i32), 18 (i64)",
                    x
                );
                let next_id = read_cstring(buf)?;
                debug!("Next identifier is {}", next_id);
                return Err(DandelionError::RequestError(
                    FrontendError::MalformedMessage,
                ));
            }
        };
    }
    read_and_check_termination(buf)?;

    // check that each field has been read
    if check != 7 {
        debug!("Some item field is missing.");
        return Err(DandelionError::RequestError(
            FrontendError::MalformedMessage,
        ));
    }

    return Ok(Some(DataItem {
        ident,
        key,
        data: crate::Position { offset, size },
    }));
}

fn read_data_set(
    buf: &mut impl bytes::Buf,
    total_length: usize,
) -> DandelionResult<Option<DataSet>> {
    // check item doc type
    let item_doc_type = read_type_byte(buf)?;
    match item_doc_type {
        0 => return Ok(None),
        3 => (),
        _ => {
            debug!("Expected set of type doc, found: {}", item_doc_type);
            return Err(DandelionError::RequestError(
                FrontendError::MalformedMessage,
            ));
        }
    }
    let _doc_name = read_cstring(buf)?;
    let _doc_lenght = read_length(buf)?;

    // allow for arbitrary ordering of data set fields
    let mut ident = "".to_string();
    let mut items = Vec::new();
    let mut check: u8 = 0;
    for _ in 0..2 {
        let next_type = read_type_byte(buf)?;
        match next_type {
            2 => {
                // -> expect the identifier part
                read_and_check_cstring(buf, "identifier\0")?;
                ident = read_string(buf)?;
                check |= 1;
            }
            4 => {
                // -> expect the items part
                read_and_check_cstring(buf, "items\0")?;
                let array_end = buf.remaining() - read_length(buf)?;

                // reads all items
                while buf.remaining() > array_end {
                    if let Some(item) = read_data_item(buf, total_length)? {
                        items.push(item);
                    } else {
                        break;
                    }
                }

                check |= 2;
            }
            x => {
                debug!("Got type {}, expected either 2 (string), 4 (array)", x);
                let next_id = read_cstring(buf)?;
                debug!("Next identifier is {}", next_id);
                return Err(DandelionError::RequestError(
                    FrontendError::MalformedMessage,
                ));
            }
        };
    }
    read_and_check_termination(buf)?;

    // check that both fields have been read
    if check != 3 {
        debug!("Some set field is missing.");
        return Err(DandelionError::RequestError(
            FrontendError::MalformedMessage,
        ));
    }

    return Ok(Some(DataSet {
        ident: ident,
        buffers: items,
    }));
}

impl BytesContext {
    pub async fn from_bytes_vec(
        frame_data: Vec<Bytes>,
        total_size: usize,
    ) -> DandelionResult<(String, Context)> {
        let mut frame_buf = FrameBuf {
            byte_iter: &frame_data,
            remaining: total_size,
            current_buf_offset: 0,
            current_frame: 0,
        };

        // check that the actual remaining data size is equal to the expected data size
        let bson_dict_length = read_length(&mut frame_buf)?;
        if frame_buf.remaining() + size_of::<i32>() != bson_dict_length {
            return Err(DandelionError::RequestError(FrontendError::ViolatedSpec));
        }

        // allow for arbitrary ordering of the 'name' and 'sets' fields
        let mut function_name = "".to_string();
        let mut sets = Vec::new();
        let mut check: u8 = 0;
        for _ in 0..2 {
            let next_type = read_type_byte(&mut frame_buf)?;
            match next_type {
                2 => {
                    // -> expect the function name part
                    read_and_check_cstring(&mut frame_buf, "name\0")?;
                    function_name = read_string(&mut frame_buf)?;
                    check |= 1;
                }
                4 => {
                    // -> expect the sets part
                    read_and_check_cstring(&mut frame_buf, "sets\0")?;
                    let array_end = frame_buf.remaining() - read_length(&mut frame_buf)?;

                    // reads all sets
                    while frame_buf.remaining() > array_end {
                        if let Some(set) = read_data_set(&mut frame_buf, bson_dict_length)? {
                            sets.push(Some(set));
                        } else {
                            break;
                        }
                    }

                    check |= 2;
                }
                x => {
                    debug!("Got type {}, expected either 2 (string), 4 (array)", x);
                    let next_id = read_cstring(&mut frame_buf)?;
                    debug!("Next identifier is {}", next_id);
                    return Err(DandelionError::RequestError(
                        FrontendError::MalformedMessage,
                    ));
                }
            };
        }
        read_and_check_termination(&mut frame_buf)?;

        // check that both fields have been read
        if check != 3 {
            debug!("Some byte context field is missing.");
            return Err(DandelionError::RequestError(
                FrontendError::MalformedMessage,
            ));
        }

        // check that we have read the whole buffer
        if frame_buf.remaining > 0 {
            debug!(
                "Finished reading bytes context but frame buffer has {} remaining",
                frame_buf.remaining
            );
            return Err(DandelionError::RequestError(FrontendError::ViolatedSpec));
        }

        // create context
        let mut context = Context::new(
            crate::memory_domain::ContextType::Bytes(Box::new(BytesContext { frames: frame_data })),
            bson_dict_length,
        );
        context.occupy_space(0, bson_dict_length)?;
        context.content = sets;

        Ok((function_name, context))
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
        .map(|vec| Bytes::from(vec))
        .collect();
    let read_context = BytesContext { frames };
    let expected_data = vec![1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8, 8u8];
    let mut all_read_vec = Vec::<u8>::new();
    all_read_vec.resize(8, 0);
    read_context
        .read(0, &mut all_read_vec)
        .expect("read should succeed");
    assert_eq!(expected_data, all_read_vec.as_slice());
    let mut partial_read_vec = Vec::<u8>::new();
    partial_read_vec.resize(4, 0);
    read_context
        .read(2, &mut partial_read_vec)
        .expect("Partial read should succeed");
    assert_eq!(&expected_data[2..6], partial_read_vec.as_slice());
}

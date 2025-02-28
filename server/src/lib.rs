pub mod config;

use dandelion_commons::{records::Recorder, DandelionError};
use dispatcher::composition::CompositionSet;
use hyper::body::Frame;
use machine_interface::memory_domain::{Context, ContextTrait};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, io::IoSlice, sync::Arc};

#[derive(Serialize, Deserialize)]
pub struct DandelionRequest<'data> {
    pub name: String,
    #[serde(borrow)]
    pub sets: Vec<InputSet<'data>>,
}

#[derive(Serialize, Deserialize)]
pub struct DandelionDeserializeResponse<'data> {
    #[serde(borrow)]
    pub sets: Vec<InputSet<'data>>,
    #[cfg(feature = "timestamp")]
    pub timestamps: String,
}

#[derive(Serialize, Deserialize)]
pub struct InputSet<'data> {
    pub identifier: String,
    #[serde(borrow)]
    pub items: Vec<InputItem<'data>>,
}

#[derive(Serialize, Deserialize)]
pub struct InputItem<'data> {
    pub identifier: String,
    pub key: u32,
    #[serde(with = "serde_bytes")]
    pub data: &'data [u8],
}

#[derive(Debug)]
struct ItemData {
    response_offset: usize,
    reponse_size: usize,
    set_index: usize,
    item_index: usize,
    context: Arc<Context>,
}

fn encode_item(
    set_index: usize,
    item_index: usize,
    context: Arc<Context>,
    response: &mut Vec<u8>,
    data_items: &mut Vec<ItemData>,
    array_index: usize,
) -> usize {
    let item_ref = &context.content[set_index].as_ref().unwrap().buffers[item_index];
    // add item to array with doc type and index into array as name
    response.push(3);
    response.extend_from_slice(format!("{}", array_index).as_bytes());
    response.push(0);
    // start item doc
    let doc_start = response.len();
    response.extend_from_slice(&0i32.to_le_bytes());

    // item identifier to be type string, name identifier and item identifier
    response.push(2);
    response.extend_from_slice("identifier\0".as_bytes());
    let identifier = &item_ref.ident;
    response.extend_from_slice(&((identifier.len() + 1) as i32).to_le_bytes());
    response.extend_from_slice(identifier.as_bytes());
    response.push(0);

    // item key to be type int64, name key and item key
    response.push(18);
    response.extend_from_slice("key\0".as_bytes());
    let key = item_ref.key as i64;
    response.extend_from_slice(&key.to_le_bytes());

    // item data to be binary type, name data, with item data
    response.push(5);
    response.extend_from_slice("data\0".as_bytes());
    // binary lenth, generic subtype subtype and then all data
    let data_size = item_ref.data.size as i32;
    response.extend_from_slice(&data_size.to_le_bytes());
    response.push(0);

    let item_size = item_ref.data.size;
    data_items.push(ItemData {
        response_offset: response.len(),
        reponse_size: item_size,
        set_index,
        item_index,
        context,
    });

    // end doc and set length
    response.push(0);
    let doc_size = (response.len() - doc_start + item_size) as i32;
    response[doc_start..doc_start + 4].copy_from_slice(&doc_size.to_le_bytes());

    return item_size;
}

fn encode_sets(
    sets: BTreeMap<usize, CompositionSet>,
    response: &mut Vec<u8>,
    data_items: &mut Vec<ItemData>,
) -> usize {
    let mut all_items = 0;
    // filter out empty sets
    let non_empty_set = sets
        .into_values()
        .filter(|set| !set.context_list.is_empty());
    // if list is empty need to push empty string
    for (index, set) in non_empty_set.enumerate() {
        // add item to array with doc type and name equal to index into the array
        response.push(3);
        response.extend_from_slice(format!("{}", index).as_bytes());
        response.push(0);
        // start doc by making space for size
        let doc_start = response.len();
        response.extend_from_slice(&0i32.to_le_bytes());

        // set identifier: string type, identifier e_name and actual set name as length, string, null byte
        response.push(2);
        response.extend_from_slice("identifier\0".as_bytes());
        let set_name = &set.context_list[0].0.content[set.set_index]
            .as_ref()
            .unwrap()
            .ident;
        response.extend_from_slice(&((set_name.len() + 1) as i32).to_le_bytes());
        response.extend_from_slice(set_name.as_bytes());
        response.push(0);

        // set items: array type, items e_name and a doc for the array
        response.push(4);
        response.extend_from_slice("items\0".as_bytes());
        let item_array_start = response.len();
        response.extend_from_slice(&0i32.to_le_bytes());

        let mut set_items_length = 0;
        for (array_index, (set_index, item_index, context)) in set.into_iter().enumerate() {
            set_items_length += encode_item(
                set_index,
                item_index,
                context,
                response,
                data_items,
                array_index,
            );
        }
        all_items += set_items_length;

        // end array
        response.push(0);
        let array_size = (response.len() - item_array_start + set_items_length) as i32;
        response[item_array_start..item_array_start + 4].copy_from_slice(&array_size.to_le_bytes());

        // end set doc
        response.push(0);
        let doc_size = (response.len() - doc_start + set_items_length) as i32;
        response[doc_start..doc_start + 4].copy_from_slice(&doc_size.to_le_bytes());
    }
    return all_items;
}

fn encode_response(
    sets: BTreeMap<usize, CompositionSet>,
    _timings: &Recorder,
) -> (usize, Vec<u8>, Vec<ItemData>) {
    // lenght of dict, list of items closing 0 byte
    let mut response = Vec::<u8>::new();
    let mut data_items = Vec::new();
    // add space for the doc size
    response.extend_from_slice(&0i32.to_le_bytes());

    // document always has an array of sets
    response.push(4);
    response.extend_from_slice("sets\0".as_bytes());
    let set_length_offset = response.len();
    response.extend_from_slice(&0i32.to_le_bytes());

    // encode sets
    let all_items = encode_sets(sets, &mut response, &mut data_items);
    log::trace!("Response contains items with total size of {}", all_items);
    // end array and set length
    response.push(0);
    let set_array_length = (response.len() - set_length_offset + all_items) as i32;
    response[set_length_offset..set_length_offset + 4]
        .copy_from_slice(&set_array_length.to_le_bytes());

    // if timestamp are on, add in timing information as string
    #[cfg(feature = "timestamp")]
    {
        // timestamps formated as formatted string, consisting of a length, the string and a NULL byte
        response.push(2);
        response.extend_from_slice("timestamps\0".as_bytes());
        let timestamp_length_offset = response.len();
        response.extend_from_slice(&0i32.to_be_bytes());
        let timestamp_string = format!("{}", _timings);
        response.extend_from_slice(timestamp_string.as_bytes());
        response.push(0);
        // set length + 1 to account for terminating 0
        let timestamp_string_len = (timestamp_string.len() + 1) as i32;
        response[timestamp_length_offset..timestamp_length_offset + 4]
            .copy_from_slice(&timestamp_string_len.to_le_bytes());
    }

    // end docuemnt and set length
    response.push(0);
    let doc_length = (response.len() + all_items) as i32;
    response[0..4].copy_from_slice(&doc_length.to_le_bytes());

    return (all_items, response, data_items);
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum ReadMode {
    /// index of the next item and how much to read in serial until hit next item
    ToItem(usize, usize),
    /// index of the item to read and how much to read until hit end of item
    ToItemEnd(usize, usize),
    /// there are no more items, are reading rest of serial
    ToEnd(usize),
}

impl ReadMode {
    fn len(&self) -> usize {
        match self {
            ReadMode::ToItem(_, to_end)
            | ReadMode::ToItemEnd(_, to_end)
            | ReadMode::ToEnd(to_end) => *to_end,
        }
    }
}

#[derive(Debug)]
pub struct DandelionBuf {
    remaining: usize,
    read_offset: ReadMode,
    serial: Vec<u8>,
    items: Vec<ItemData>,
}

impl DandelionBuf {
    fn get_chunk(&self, read_mode: ReadMode) -> &[u8] {
        return match read_mode {
            ReadMode::ToEnd(to_end) => {
                let start = self.serial.len() - to_end;
                &self.serial[start..self.serial.len()]
            }
            ReadMode::ToItem(item_index, to_item) => {
                let item_offset = self.items[item_index].response_offset;
                &self.serial[item_offset - to_item..item_offset]
            }
            ReadMode::ToItemEnd(buf_item_index, to_end) => {
                let ItemData {
                    context,
                    response_offset: _,
                    reponse_size: _,
                    set_index,
                    item_index,
                } = &self.items[buf_item_index];
                let position =
                    context.content[*set_index].as_ref().unwrap().buffers[*item_index].data;
                let start = position.offset + (position.size - to_end);
                context.get_chunk_ref(start, to_end).unwrap()
            }
        };
    }
    fn advance_descriptor(&self, read_mode: ReadMode, to_advance: usize) -> (usize, ReadMode) {
        let (advanced, new_mode) = match read_mode {
            ReadMode::ToEnd(to_end) => {
                if to_end >= to_advance {
                    (to_advance, ReadMode::ToEnd(to_end - to_advance))
                } else {
                    (to_end, ReadMode::ToEnd(0))
                }
            }
            ReadMode::ToItem(item_index, to_item) => {
                if to_item > to_advance {
                    (
                        to_advance,
                        ReadMode::ToItem(item_index, to_item - to_advance),
                    )
                } else {
                    (
                        to_item,
                        ReadMode::ToItemEnd(item_index, self.items[item_index].reponse_size),
                    )
                }
            }
            ReadMode::ToItemEnd(item_index, to_end) => {
                if to_end > to_advance {
                    (
                        to_advance,
                        ReadMode::ToItemEnd(item_index, to_end - to_advance),
                    )
                } else {
                    let next_read = if item_index + 1 >= self.items.len() {
                        ReadMode::ToEnd(self.serial.len() - self.items[item_index].response_offset)
                    } else {
                        let next_index = item_index + 1;
                        let next_size = self.items[next_index].reponse_size;
                        if self.items[next_index].response_offset
                            == self.items[item_index].response_offset
                        {
                            ReadMode::ToItemEnd(next_index, next_size)
                        } else {
                            ReadMode::ToItem(
                                next_index,
                                self.items[next_index].response_offset
                                    - self.items[next_index - 1].response_offset,
                            )
                        }
                    };
                    (to_end, next_read)
                }
            }
        };
        return (advanced, new_mode);
    }
}

impl bytes::Buf for DandelionBuf {
    fn remaining(&self) -> usize {
        return self.remaining;
    }

    fn advance(&mut self, cnt: usize) {
        if cnt > self.remaining {
            self.remaining = 0;
            self.read_offset = ReadMode::ToEnd(0);
            return;
        }
        self.remaining -= cnt;
        let mut to_advance = cnt;
        loop {
            let (read_length, next_reader) = self.advance_descriptor(self.read_offset, to_advance);
            to_advance -= read_length;
            self.read_offset = next_reader;
            if next_reader.len() == 0 && next_reader != ReadMode::ToEnd(0) {
                continue;
            }
            if to_advance == 0 {
                break;
            }
        }
    }

    fn chunk(&self) -> &[u8] {
        self.get_chunk(self.read_offset)
    }

    fn chunks_vectored<'a>(&'a self, dst: &mut [std::io::IoSlice<'a>]) -> usize {
        let mut slice_index = 0;
        let mut next_read = self.read_offset;
        let mut remaining = self.remaining;
        while slice_index < dst.len() && next_read != ReadMode::ToEnd(0) && remaining > 0 {
            let chunk = self.get_chunk(next_read);
            if chunk.len() > 0 {
                dst[slice_index] = IoSlice::new(chunk);
                slice_index += 1;
            }
            remaining -= chunk.len();
            (_, next_read) = self.advance_descriptor(next_read, chunk.len());
        }
        return slice_index;
    }
}

pub struct DandelionBody {
    buffer: Option<DandelionBuf>,
}

impl DandelionBody {
    pub fn new(sets: BTreeMap<usize, CompositionSet>, timing: &Recorder) -> Self {
        let (total_item_size, serial, items) = encode_response(sets, timing);
        let read_offset = if items.len() > 0 {
            if items[0].response_offset > 0 {
                ReadMode::ToItem(0, items[0].response_offset)
            } else {
                ReadMode::ToItemEnd(0, items[0].reponse_size)
            }
        } else {
            ReadMode::ToEnd(serial.len())
        };
        return DandelionBody {
            buffer: Some(DandelionBuf {
                read_offset,
                remaining: total_item_size + serial.len(),
                serial,
                items,
            }),
        };
    }
    pub fn from_vec(array: Vec<u8>) -> Self {
        return DandelionBody {
            buffer: Some(DandelionBuf {
                read_offset: ReadMode::ToEnd(array.len()),
                remaining: array.len(),
                serial: array,
                items: Vec::new(),
            }),
        };
    }
}

impl hyper::body::Body for DandelionBody {
    type Data = DandelionBuf;
    type Error = DandelionError;
    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        let frame_data = self
            .get_mut()
            .buffer
            .take()
            .and_then(|bytes| Some(Ok(Frame::data(bytes))));
        return std::task::Poll::Ready(frame_data);
    }
}

#[cfg(test)]
async fn test_dandelion_body_serialization_async(mut body: DandelionBody, expected: Vec<u8>) {
    use bytes::Buf;
    use futures::future::poll_fn;
    use hyper::body::Body;
    use std::pin::Pin;

    let context_frame = poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
        .await
        .unwrap()
        .unwrap();
    if poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
        .await
        .is_some()
    {
        panic!("body not end of frame or returned second frame");
    }
    let mut context_data = context_frame
        .into_data()
        .expect("Should be able to get data from dandelion body frame");
    let data_length = context_data.remaining();
    let mut data_buf = Vec::with_capacity(data_length);
    data_buf.resize(data_length, 0u8);
    context_data.copy_to_slice(&mut data_buf);
    assert_eq!(expected, data_buf);
}

#[test]
fn test_dandelion_body_serialization() {
    use machine_interface::{
        memory_domain::read_only::ReadOnlyContext, DataItem, DataSet, Position,
    };

    let pattern = [0xCu8, 0xAu8, 0xFu8, 0xEu8];
    let repetitions = 4;
    let mut data = Vec::with_capacity(pattern.len() * repetitions);
    for _ in 0..repetitions {
        data.extend_from_slice(&pattern);
    }
    let data_box = data.clone().into_boxed_slice();

    let recorder = Recorder::new(0, std::time::Instant::now());

    let expected_response_struct = DandelionDeserializeResponse {
        sets: vec![InputSet {
            identifier: String::from("set_ident"),
            items: vec![InputItem {
                identifier: String::from("item_ident"),
                key: 7,
                data: &data_box,
            }],
        }],
        #[cfg(feature = "timestamp")]
        timestamps: format!("{}", recorder),
    };
    let expected_response = bson::to_vec(&expected_response_struct).unwrap();

    let mut new_context = ReadOnlyContext::new(data_box).unwrap();
    new_context.content = vec![Some(DataSet {
        ident: String::from("set_ident"),
        buffers: vec![DataItem {
            key: 7,
            ident: String::from("item_ident"),
            data: Position {
                offset: 0,
                size: repetitions * pattern.len(),
            },
        }],
    })];
    let composition_set = CompositionSet {
        set_index: 0,
        context_list: vec![(Arc::new(new_context), 0..1)],
    };
    let context_map = BTreeMap::from([(0, composition_set)]);
    let context_body = DandelionBody::new(context_map, &recorder);

    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(test_dandelion_body_serialization_async(
            context_body,
            expected_response,
        ));
}

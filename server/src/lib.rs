pub mod config;

use dispatcher::composition::CompositionSet;
use machine_interface::memory_domain::{Context, ContextTrait};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};

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

#[derive(Serialize)]
pub struct DandelionResponse {
    pub sets: Vec<OutputSet>,
}
#[derive(Serialize)]
pub struct OutputSet {
    pub identifier: String,
    pub items: Vec<OutputItem>,
}

#[derive(Serialize)]
pub struct OutputItem {
    pub identifier: String,
    pub key: u32,
    #[serde(with = "serde_bytes")]
    data: ReadSlice,
}

struct ReadSlice {
    context: Arc<Context>,
    offset: usize,
    size: usize,
}

impl core::ops::Deref for ReadSlice {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        return self.context.get_chunk_ref(self.offset, self.size).unwrap();
    }
}

impl serde_bytes::Serialize for ReadSlice {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(&self)
    }
}

pub fn create_response(response_sets: BTreeMap<usize, CompositionSet>) -> Vec<u8> {
    let mut response_data = Vec::new();
    for (_, composition_set) in response_sets {
        let mut composition_set_iter = composition_set.into_iter().peekable();
        let first_elem = composition_set_iter.peek();
        let set_name = if let Some(elem) = first_elem {
            elem.2.content[elem.0].as_ref().unwrap().ident.clone()
        } else {
            continue;
        };
        let mut response_items = Vec::new();
        for (set_index, item_index, context) in composition_set_iter {
            let local_context = context.clone();
            let item = &local_context.content[set_index].as_ref().unwrap().buffers[item_index];
            let offset = item.data.offset;
            let size = item.data.size;
            response_items.push(OutputItem {
                identifier: item.ident.clone(),
                key: item.key,
                data: ReadSlice {
                    context: local_context,
                    offset: offset,
                    size: size,
                },
            });
        }
        response_data.push(OutputSet {
            identifier: set_name,
            items: response_items,
        });
    }
    return bson::to_vec(&DandelionResponse {
        sets: response_data,
    })
    .unwrap();
}

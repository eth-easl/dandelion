pub mod config;

use dispatcher::composition::CompositionSet;
use machine_interface::{
    memory_domain::{read_only::ReadOnlyContext, Context, ContextTrait},
    DataItem, DataSet, Position,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize)]
#[allow(dead_code)]
pub struct DandelionRequest {
    pub name: String,
    pub sets: Vec<InputSet>,
}

#[derive(Serialize, Deserialize)]
#[allow(dead_code)]
pub struct DandelionResponse {
    pub sets: Vec<InputSet>,
}
#[derive(Deserialize, Serialize)]
#[allow(dead_code)]
pub struct InputSet {
    pub identifier: String,
    pub items: Vec<InputItem>,
}

#[derive(Deserialize, Serialize)]
#[allow(dead_code)]
pub struct InputItem {
    pub identifier: String,
    pub key: u32,
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
}

pub fn parse_request(request: DandelionRequest) -> (String, Context) {
    let DandelionRequest { name, sets } = request;
    let mut context_buffer = Vec::new();
    let mut buffer_offset = 0usize;
    let mut context_sets = Vec::new();
    for InputSet { identifier, items } in sets {
        let mut set_items = Vec::new();
        for InputItem {
            identifier,
            key,
            data,
        } in items
        {
            set_items.push(DataItem {
                ident: identifier,
                key,
                data: Position {
                    offset: buffer_offset,
                    size: buffer_offset + data.len(),
                },
            });
            buffer_offset += data.len();
            context_buffer.extend_from_slice(&data);
        }
        context_sets.push(Some(DataSet {
            ident: identifier,
            buffers: set_items,
        }));
    }
    let mut context = ReadOnlyContext::new(context_buffer.into()).unwrap();
    context.content = context_sets;
    return (name, context);
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
            let item = &context.content[set_index].as_ref().unwrap().buffers[item_index];
            let mut read_buffer = Vec::<u8>::with_capacity(item.data.size);
            read_buffer.resize(item.data.size, 0);
            context
                .context
                .read(item.data.offset, &mut read_buffer)
                .unwrap();
            response_items.push(InputItem {
                identifier: item.ident.clone(),
                key: item.key,
                data: read_buffer,
            });
        }
        response_data.push(InputSet {
            identifier: set_name,
            items: response_items,
        });
    }
    return bson::to_vec(&DandelionResponse {
        sets: response_data,
    })
    .unwrap();
}

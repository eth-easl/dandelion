use machine_interface::{
    memory_domain::{read_only::ReadOnlyContext, Context},
    DataItem, DataSet, Position,
};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
#[allow(dead_code)]
pub struct DandelionRequest {
    pub name: String,
    pub sets: Vec<InputSet>,
    pub data: Vec<u8>,
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
    pub data_start: u64,
    pub data_size: u64,
}

pub fn parse_request(request: DandelionRequest) -> (String, Context) {
    let DandelionRequest { name, sets, data } = request;
    let mut context = ReadOnlyContext::new(data.into()).unwrap();
    let map_item = |request_item: InputItem| DataItem {
        ident: request_item.identifier,
        key: request_item.key,
        data: Position {
            offset: usize::try_from(request_item.data_start).unwrap(),
            size: usize::try_from(request_item.data_size).unwrap(),
        },
    };
    let map_set = |request_set: InputSet| {
        Some(DataSet {
            ident: request_set.identifier,
            buffers: request_set.items.into_iter().map(map_item).collect(),
        })
    };
    context.content = sets.into_iter().map(map_set).collect();
    return (name, context);
}

use dandelion_commons::{
    records::{RecordPoint, Recorder}, 
    DandelionResult, DandelionError
};
use serde::{Serialize, Deserialize};
use crate::{function_driver::WorkDone, memory_domain::{Context, ContextTrait}, promise::Debt, DataSet};
use reqwest::Client;
use super::context_util::{get_dataset_by_ident, get_dataitem_by_ident, read_dataitem_content};
use std::{collections::{btree_map::Entry::{Occupied, Vacant}, BTreeMap}, sync::Arc};
use tokio::sync::{mpsc::{self, Sender}, Mutex};
use log::debug;

pub enum IntermediateData {
    Ready(Arc<(DataSet, Vec<u8>)>),
    Pending(Arc<Sender<(DataSet, Vec<u8>)>>)
}

pub struct IntermediateDataPool {
    pool: Mutex<BTreeMap<String, IntermediateData>>,
}

impl IntermediateDataPool {
    pub fn new() -> Self {
        Self {
            pool: Mutex::new(BTreeMap::new()),
        }
    }

    pub async fn post(&self, id: String, content: DataSet, binary: Vec<u8>) -> DandelionResult<()> {
        let mut pool = self.pool.lock().await;
        match pool.entry(id.clone()) {
            Occupied(entry) => {
                match entry.remove() {
                    IntermediateData::Pending(sender) => {
                        match sender.send((content, binary)).await {
                            Ok(_) => {
                                return Ok(())
                            }
                            Err(_) => {
                                return Err(DandelionError::PostDataError(format!("{id} channel send error")));
                            }
                        }
                    }
                    IntermediateData::Ready(..) => {
                        return Err(DandelionError::PostDataError(format!("{id} already in pool")));
                    }
                }

            }
            Vacant(entry) => {
                entry.insert(IntermediateData::Ready(Arc::new((content, binary))));
                return Ok(())
            }
        }
    }

    pub async fn subscribe(&self, id: String) -> DandelionResult<(DataSet, Vec<u8>)> {
        let (sender, mut receiver) = mpsc::channel(1);
        {
            let mut pool = self.pool.lock().await;
            match pool.entry(id.clone()) {
                Occupied(entry) => {
                    match entry.remove() {
                        IntermediateData::Pending(_) => {
                            return Err(DandelionError::PostDataError(format!("{id} already subscribed in pool")));
                        }
                        IntermediateData::Ready(data_arc) => {
                            match Arc::try_unwrap(data_arc) {
                                Ok(data) => {
                                    return Ok(data);
                                }
                                Err(_) => {
                                    return Err(DandelionError::PostDataError(format!("{id} cannot unwrap data")));
                                }
                            }
                        }
                    }
                }
                Vacant(entry) => {
                    entry.insert(IntermediateData::Pending(Arc::new(sender)));
                }
            }
        }
        match receiver.recv().await {
            Some(data) => {
                return Ok(data);
            }
            None => {
                return Err(DandelionError::PostDataError(format!("{id} channel recv error")));
            }
        }
    }

}

#[derive(Serialize, Deserialize, Clone)]
pub struct DandelionSendInformation {
    addr: String,
    pub id: String,
    pub content: DataSet,
    #[serde(skip)]
    pub binary: Vec<u8>
}

fn dandelion_send_setup(context: &Context) -> DandelionResult<DandelionSendInformation> {
    let send_meta_set = get_dataset_by_ident(context, String::from("send_meta"))?;
    let addr_item = get_dataitem_by_ident(send_meta_set, String::from("addr"))?;
    let addr = match String::from_utf8(read_dataitem_content(context, addr_item)?) {
        Ok(addr) => Ok(addr),
        Err(_) => Err(DandelionError::MalformedSystemFuncArg(String::from("send: dandelion addr not valid"))),
    }?;
    let id_item = get_dataitem_by_ident(send_meta_set, String::from("id"))?;
    let id = match String::from_utf8(read_dataitem_content(context, id_item)?) {
        Ok(addr) => Ok(addr),
        Err(_) => Err(DandelionError::MalformedSystemFuncArg(String::from("send: dandelion id not valid"))),
    }?;
    // TODO: the following code should use dandelion_server::DandelionBody::new to construct response
    // consider put id in http header
    let send_data_set = get_dataset_by_ident(context, String::from("send_data"))?;
    let mut content = send_data_set.clone();
    let mut binary: Vec<u8> = vec![];
    let mut offset = 0;
    content.buffers.iter_mut().try_for_each(|item| {
        let mut data = read_dataitem_content(context, item)?;
        binary.append(&mut data);
        item.data.offset = offset;
        offset += item.data.size;
        Ok(())
    })?;
    return Ok(DandelionSendInformation {
        addr,
        id,
        content,
        binary
    });
}

async fn dandelion_send_request(
    client: Client, 
    mut send_info: DandelionSendInformation
) -> DandelionResult<()> {
    debug!("send data, length: {}", send_info.binary.len());
    let mut meta_binary = bson::to_vec(&send_info).unwrap();
    let mut data = bincode::serialize(&meta_binary.len()).unwrap();
    data.append(&mut meta_binary);
    data.append(&mut send_info.binary);
    let addr = &send_info.addr;
    if !client
        .post(format!("{addr}/post_data"))
        .body(data)
        .send()
        .await
        .unwrap()
        .status()
        .is_success() {
            return Err(DandelionError::PostDataError(format!("post dataset {} http error", send_info.content.ident)));
        } else {
            return Ok(())
        }
}

pub async fn dandelion_send(
    mut context: Context,
    client: Client,
    debt: Debt,
    mut recorder: Recorder,
) -> () {
    if let Err(err) = recorder.record(RecordPoint::EngineStart) {
        debt.fulfill(Box::new(Err(err)));
        return;
    }
    let send_info = match dandelion_send_setup(&context) {
        Ok(request) => request,
        Err(err) => {
            debt.fulfill(Box::new(Err(err)));
            return;
        }
    };
    match dandelion_send_request(client, send_info).await {
        Ok(_) => (),
        Err(err) => {
            debt.fulfill(Box::new(Err(err)));
            return;
        }
    };
    context.clear_metadata();
    if let Err(err) = recorder.record(RecordPoint::EngineEnd) {
        debt.fulfill(Box::new(Err(err)));
        return;
    }
    let results = Box::new(Ok(WorkDone::Context(context)));
    debt.fulfill(results);
    return;
}

async fn dandelion_recv_response(context: &Context, data_pool: Arc<IntermediateDataPool>) -> DandelionResult<(DataSet, Vec<u8>)> {
    let recv_meta_set = get_dataset_by_ident(context, String::from("recv_meta"))?;
    let id_item = get_dataitem_by_ident(recv_meta_set, String::from("id"))?;
    let id = match String::from_utf8(read_dataitem_content(context, id_item)?) {
        Ok(addr) => Ok(addr),
        Err(_) => Err(DandelionError::MalformedSystemFuncArg(String::from("recv: dandelion id not valid"))),
    }?;
    return data_pool.subscribe(id).await;
}

fn dandelion_recv_context(context: &mut Context, mut content: DataSet, binary: Vec<u8>) -> DandelionResult<()> {
    let base_offset = context.get_free_space(binary.len(), 128)?;
    content.buffers.iter_mut().for_each(|item| {
        item.data.offset += base_offset;
    });
    if let Some(response_set) = &mut context.content[0] {
        response_set.buffers = content.buffers;
    }
    context.write(base_offset, &binary)?;
    return Ok(());
}

pub async fn dandelion_recv(
    mut context: Context,
    output_set_names: Arc<Vec<String>>,
    data_pool: Arc<IntermediateDataPool>,
    debt: Debt,
    mut recorder: Recorder,
) -> () {
    if let Err(err) = recorder.record(RecordPoint::EngineStart) {
        debt.fulfill(Box::new(Err(err)));
        return;
    }
    let (content, binary) = match dandelion_recv_response(&context, data_pool.clone()).await {
        Ok((content, binary)) => (content, binary),
        Err(err) => {
            debt.fulfill(Box::new(Err(err)));
            return;
        }
    };
    debug!("recv data, length: {}", binary.len());
    context.clear_metadata();
    if !output_set_names.is_empty() {
        context.content = vec![None];
        if output_set_names.iter().any(|elem| elem == "recv_data") {
            context.content[0] = Some(DataSet {
                ident: String::from("recv_data"),
                buffers: vec![],
            })
        }
        if let Err(err) = dandelion_recv_context(&mut context, content, binary) {
            debt.fulfill(Box::new(Err(err)));
            return;
        }
    }
    if let Err(err) = recorder.record(RecordPoint::EngineEnd) {
        debt.fulfill(Box::new(Err(err)));
        return;
    }
    let results = Box::new(Ok(WorkDone::Context(context)));
    debt.fulfill(results);
}
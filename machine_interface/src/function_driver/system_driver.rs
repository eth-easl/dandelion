pub mod reqwest;

use crate::{
    function_driver::{functions::SystemFunction, system_driver::reqwest::resolve_io_item},
    ContextDataSet,
};
use ::reqwest::Client as HttpClient;
use dandelion_commons::{
    err_dandelion, try_with_capacity, DandelionError, DandelionResult, MultinodeError,
};
use log::error;
use memory::data::{DataItem, DataSet, ItemData, LocalItemData, Position, ResolvedItemData};
use memory::Context;
use std::{
    future::Future,
    ops::Deref,
    pin::Pin,
    sync::{Arc, OnceLock},
};
use tokio::sync::{mpsc, OnceCell};

/// HTTP function currently expects one set with requests formated by HTTP standard (in text).
/// This means one line with the reqest method, a space, request url, another space and the protocol version
/// ex.: "PUT /images/logo.png HTTP/1.1"
/// After a line break the headers are one line each with the formatting of key ':' value
/// ex.: "host: www.google.com"
/// After all headers and an empty line the body which can be arbitrary binary data
// TODO: think if we want to also separate this into two sets, one with header one with bodies.
// If we separate, need a way to deal with non matching numbers of bodies and headers and duplicate names.
// Could offer automatic pairing for example for giving a header that can be used with any number of bodies.
// Do not want to overcomplicate things.
const HTTP_INPUT_SETS: [&str; 1] = ["requests"];

/// HTTP outputs two set with response headers and bodies for each request that was in the input set.
/// The response items have the same key as the corresponding request input item.
/// The headers start with a status line containing the protocol used, the response code and possible the reason
/// ex.: "HTTP/1.1 200 OK"
/// On the following lines there are the headers in key value formatted with ':' as separator
/// ex.: "Content-Type: text/html; charset=utf-8"
/// The header and body items all carry the names and keys of the corresponding requests.
/// The user is responsible for ensuring, that requests have names and the names are unique, if they need them to associate
/// the headers with the bodies.
const HTTP_OUTPUT_SETS: [&str; 2] = ["headers", "bodies"];

/// Provides the input set names for a given system function
pub fn get_system_function_input_sets(
    function: SystemFunction,
) -> Vec<(String, Option<ContextDataSet>)> {
    return match function {
        SystemFunction::HTTP => HTTP_INPUT_SETS,
        SystemFunction::MEMCACHED => HTTP_INPUT_SETS,
    }
    .map(|name| (name.to_string(), None))
    .to_vec();
}

/// Provies the output set names for a given system function
pub fn get_system_function_output_sets(function: SystemFunction) -> Vec<String> {
    return match function {
        SystemFunction::HTTP => &HTTP_OUTPUT_SETS,
        SystemFunction::MEMCACHED => &HTTP_OUTPUT_SETS,
    }
    .map(|name| name.to_string())
    .to_vec();
}

pub const SYSTEM_FUNCTIONS: &[SystemFunction] = &[SystemFunction::HTTP];

//------------------------------------------------------------------------------
// IoData
//------------------------------------------------------------------------------

/// Data that needs to be fetched using an IO function.
#[derive(Debug, Clone)]
pub struct IoData {
    pub original_data: Arc<dyn ItemData>,
    // A vec with the resolved outputs for this IO request
    // one entry for each output set of the function.
    // The output item starts at 0 in the context and goes until the end of the context.
    pub resolved: Arc<OnceCell<DandelionResult<Vec<Arc<Context>>>>>,
    pub function: SystemFunction,
    pub set_index: usize,
    pub client: HttpClient,
}

impl ItemData for IoData {
    fn size(&self) -> usize {
        if let Some(res) = self.resolved.get() {
            res.as_ref()
                .map_or(0, |data_vec| data_vec[self.set_index].size())
        } else {
            0
        }
    }
    fn is_local(&self) -> bool {
        self.resolved.get().is_some()
    }
    fn resolve(self: Arc<Self>) -> ResolvedItemData {
        Box::pin(resolve_io_item(self))
    }
    fn data(&self) -> DandelionResult<(Arc<Context>, Position)> {
        match self.resolved.get() {
            Some(resolved_vec) => {
                let ctx = match resolved_vec.as_ref() {
                    Ok(v) => v[self.set_index].clone(),
                    Err(err) => return Err(err.clone()),
                };
                let pos = Position {
                    offset: 0,
                    size: ctx.size(),
                };
                Ok((ctx, pos))
            }
            None => err_dandelion!(DandelionError::InvalidRead),
        }
    }
}

/// Currently assumes the HTTP_INPUT_SETS and HTTP_OUTPUT_SETS
pub fn convert_to_references(
    function: SystemFunction,
    mut inputs: Vec<Option<DataSet>>,
    client: HttpClient,
) -> DandelionResult<Vec<Option<DataSet>>> {
    // check that the function id contains string correcpsonding to system function
    debug_assert_eq!(
        1,
        inputs.len(),
        "all current IO functions expect a single input set"
    );

    // go through all input sets and check if there is already a static one, or on in the input data
    let mut output_vec = try_with_capacity!(Vec, 2)?;
    output_vec.resize(2, None);

    if let Some(input_set) = inputs[0].take() {
        let mut out_0_list = try_with_capacity!(Vec, input_set.len())?;
        let mut out_1_list = try_with_capacity!(Vec, input_set.len())?;
        for item in input_set.into_iter() {
            let set_once = Arc::new(OnceCell::new());
            let header_item = DataItem {
                ident: item.ident.clone(),
                key: item.key,
                data: Arc::new(IoData {
                    original_data: item.data.clone(),
                    resolved: set_once.clone(),
                    function,
                    set_index: 0,
                    client: client.clone(),
                }),
            };
            let body_item = DataItem {
                ident: item.ident.clone(),
                key: item.key,
                data: Arc::new(IoData {
                    original_data: item.data.clone(),
                    resolved: set_once,
                    function,
                    set_index: 1,
                    client: client.clone(),
                }),
            };
            out_0_list.push(Arc::new(header_item));
            out_1_list.push(Arc::new(body_item));
        }

        output_vec[0] = Some(DataSet::from_items(Arc::new(out_0_list)));
        output_vec[1] = Some(DataSet::from_items(Arc::new(out_1_list)));
    }
    Ok(output_vec)
}

//------------------------------------------------------------------------------
// RemoteData
//------------------------------------------------------------------------------

/// A reference to data that lives on another node.
#[derive(Clone, Debug)]
pub struct RemoteData {
    inner: Arc<RemoteDataInner>,
}

/// TODO: for the ones we create locally we are going through the sender too,
/// think if it would be easier to have an enum in the delete sender to perform local drop directly.
#[derive(Debug)]
pub struct RemoteDataInner {
    pub node_id: u64,
    pub data_id: u64,
    delete_sender: Option<mpsc::UnboundedSender<RemoteData>>,
}

impl RemoteData {
    pub fn new(node_id: u64, data_id: u64) -> Self {
        Self {
            inner: Arc::new(RemoteDataInner {
                node_id,
                data_id,
                delete_sender: None,
            }),
        }
    }

    pub fn delete_on_drop(
        node_id: u64,
        data_id: u64,
        delete_sender: mpsc::UnboundedSender<RemoteData>,
    ) -> Self {
        Self {
            inner: Arc::new(RemoteDataInner {
                node_id,
                data_id,
                delete_sender: Some(delete_sender),
            }),
        }
    }
}

impl Deref for RemoteData {
    type Target = RemoteDataInner;

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref()
    }
}

impl Drop for RemoteDataInner {
    fn drop(&mut self) {
        if let Some(delete_sender) = &self.delete_sender {
            if let Err(err) = delete_sender.send(RemoteData::new(self.node_id, self.data_id)) {
                error!(
                    "Failed to send remote data deletion message for node_id {}, data_id {}: {}",
                    self.node_id, self.data_id, err
                );
            }
        }
    }
}

impl ItemData for RemoteData {
    fn size(&self) -> usize {
        0 // TODO: the size is only known once the item has been fetched
    }
    fn is_local(&self) -> bool {
        false
    }
    fn resolve(self: Arc<Self>) -> ResolvedItemData {
        Box::pin(async move {
            let client = get_remote_data_client()?;
            let (ctx, pos) = client.resolve_remote_data((*self).clone()).await?;
            Ok(LocalItemData { ctx, pos })
        })
    }
    fn data(&self) -> DandelionResult<(Arc<Context>, Position)> {
        // remote data has to go through `resolve` first, which yields a `LocalItemData`
        err_dandelion!(DandelionError::InvalidRead)
    }
}

pub trait RemoteDataClient: Send + Sync {
    fn resolve_remote_data(
        &self,
        // Since this assumes ownership should be careful not to drop until we have the data
        data: RemoteData,
    ) -> Pin<Box<dyn Future<Output = DandelionResult<(Arc<Context>, Position)>> + Send + '_>>;

    /// Returns a single context containing all the items that were fetched and updates
    /// their position metadata accordingly
    fn resolve_multiple_data<'meta>(
        &'meta self,
        // Metadata needs to be updated, since the offsets are not correctly set yet
        metadata: &'meta mut Vec<(usize, DataItem)>,
        // Since this assumes ownership should be careful not to drop until we have the data
        data: Vec<RemoteData>,
    ) -> Pin<Box<dyn Future<Output = DandelionResult<Arc<Context>>> + Send + 'meta>>;

    fn delete_remote_data(
        &self,
        data: RemoteData,
    ) -> Pin<Box<dyn Future<Output = DandelionResult<()>> + Send + '_>>;
}

static REMOTE_DATA_CLIENT: OnceLock<Arc<dyn RemoteDataClient>> = OnceLock::new();

pub fn set_remote_data_client(client: Arc<dyn RemoteDataClient>) {
    let _ = REMOTE_DATA_CLIENT.set(client);
}

pub fn get_remote_data_client() -> DandelionResult<Arc<dyn RemoteDataClient>> {
    match REMOTE_DATA_CLIENT.get() {
        Some(client) => Ok(client.clone()),
        None => {
            err_dandelion!(DandelionError::Multinode(MultinodeError::ConfigError(
                "No remote data client configured".to_string(),
            )))
        }
    }
}

#[cfg(test)]
mod system_driver_tests;

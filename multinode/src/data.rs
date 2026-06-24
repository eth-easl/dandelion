use dandelion_commons::{
    dandelion_err, err_dandelion, DandelionError, DandelionResult, MultinodeError,
};
use hyper::{
    body::{Body, Buf, Incoming},
    service::service_fn,
    Method, Request, Response, StatusCode,
};
use log::{debug, error, trace, warn};
use machine_interface::{
    composition::{RemoteData, RemoteDataClient},
    memory_domain::{
        bytes_context::BytesContext, read_only::ReadOnlyContext, Context, ContextTrait, ContextType,
    },
    DataItem, Position,
};
use prost::bytes;
use std::{
    collections::{BTreeMap, VecDeque},
    convert::Infallible,
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex, OnceLock},
};
use tokio::{net::TcpListener, signal::unix::SignalKind, sync::Semaphore};

#[derive(Clone)]
struct ExportedData {
    context: Arc<Context>,
    position: Position,
}

// TODO: use a TCP socket instead of hyper then we don't need this
impl bytes::Buf for ExportedData {
    fn remaining(&self) -> usize {
        self.position.size
    }

    fn advance(&mut self, cnt: usize) {
        self.position.offset += cnt;
        self.position.size -= cnt;
    }

    fn chunk(&self) -> &[u8] {
        self.context
            .get_chunk_ref(self.position.offset, self.position.size)
            .unwrap()
    }

    fn chunks_vectored<'a>(&'a self, dst: &mut [std::io::IoSlice<'a>]) -> usize {
        let size = self.position.size;
        let offset = self.position.offset;
        let mut bytes_read = 0;
        let mut slice_index = 0;
        while bytes_read < size && slice_index < dst.len() {
            let new_chunk = self
                .context
                .get_chunk_ref(offset + bytes_read, size - bytes_read)
                .unwrap();
            dst[slice_index] = std::io::IoSlice::new(new_chunk);
            slice_index += 1;
            bytes_read += new_chunk.len();
        }
        slice_index
    }
}

struct ExportedBody {
    inner: VecDeque<ExportedData>,
}

impl ExportedBody {
    fn new_error(string: String) -> Self {
        let mut inner = VecDeque::with_capacity(1);
        let string_size = string.len();
        inner.push_back(ExportedData {
            context: Arc::new(
                ReadOnlyContext::new(string.into_bytes().into_boxed_slice()).unwrap(),
            ),
            position: Position {
                offset: 0,
                size: string_size,
            },
        });
        Self { inner }
    }

    fn new_single(data: ExportedData) -> Self {
        let mut inner = VecDeque::with_capacity(1);
        inner.push_back(data);
        Self { inner }
    }
}

impl hyper::body::Body for ExportedBody {
    type Data = ExportedData;
    type Error = DandelionError;
    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        return std::task::Poll::Ready(
            self.inner
                .pop_front()
                .map(|data| Ok(hyper::body::Frame::data(data))),
        );
    }
}

// TODO: use composition hashid and item index to uniquely identify data to avoid
// storing multiple copies of the same data if it is used in multiple places.
// TODO: this is not optimized for concurrent access, we might want to shard the registry
// or use a more concurrent data structure to serve remote data requests in parallel
struct ExportRegistryInner {
    next_data_id: u64,
    data: BTreeMap<u64, ExportedData>,
}

#[derive(Clone)]
pub struct ExportRegistry {
    node_id: u64,
    inner: Arc<Mutex<ExportRegistryInner>>,
}

impl ExportRegistry {
    pub fn new(node_id: u64) -> Self {
        Self {
            node_id,
            inner: Arc::new(Mutex::new(ExportRegistryInner {
                next_data_id: 0,
                data: BTreeMap::new(),
            })),
        }
    }

    pub fn get_node_id(&self) -> u64 {
        self.node_id
    }

    pub fn insert_function(
        &self,
        item: &DataItem,
        context: Arc<Context>,
        delete_sender: Option<tokio::sync::mpsc::UnboundedSender<RemoteData>>,
    ) -> RemoteData {
        let mut inner = self.inner.lock().unwrap();
        let data_id = inner.next_data_id;
        inner.next_data_id += 1;
        inner.data.insert(
            data_id,
            ExportedData {
                context,
                position: item.data,
            },
        );
        if let Some(delete_sender) = delete_sender {
            RemoteData::delete_on_drop(self.node_id, data_id, delete_sender)
        } else {
            RemoteData::new(self.node_id, data_id)
        }
    }

    fn get_exported_data(&self, data_id: u64) -> DandelionResult<ExportedData> {
        debug!(
            "Fetching exported data: node_id={}, data_id={}",
            self.node_id, data_id
        );

        let inner = self.inner.lock().unwrap();
        match inner.data.get(&data_id) {
            Some(exported_data) => Ok(exported_data.clone()),
            None => err_dandelion!(DandelionError::Multinode(MultinodeError::RequestFailed(
                format!("Unknown remote data id {}", data_id),
            ))),
        }
    }

    fn get_multiple_data(&self, data_ids: Vec<u64>) -> DandelionResult<VecDeque<ExportedData>> {
        debug!(
            "Fetching exported data: node_id={}, data_ids={:?}",
            self.node_id, data_ids
        );
        let mut result_data = VecDeque::with_capacity(data_ids.len());
        let inner = self.inner.lock().unwrap();
        for data_id in data_ids {
            match inner.data.get(&data_id) {
                Some(context) => result_data.push_back(context.clone()),
                None => {
                    return err_dandelion!(DandelionError::Multinode(
                        MultinodeError::RequestFailed(format!(
                            "Unknown remote data id {}",
                            data_id
                        ),)
                    ));
                }
            };
        }

        Ok(result_data)
    }

    pub fn delete_exported_data(&self, data_id: u64) -> DandelionResult<()> {
        debug!(
            "Deleting exported data: node_id={}, data_id={}",
            self.node_id, data_id
        );
        let mut inner = self.inner.lock().unwrap();
        let Some(_) = inner.data.remove(&data_id) else {
            return err_dandelion!(DandelionError::Multinode(MultinodeError::RequestFailed(
                format!("Unknown remote data id {}", data_id),
            )));
        };
        Ok(())
    }

    pub fn fetch_context(&self, data_id: u64) -> DandelionResult<(Arc<Context>, Position)> {
        let exported_data = self.get_exported_data(data_id)?;
        Ok((exported_data.context, exported_data.position))
    }
}

pub struct HttpRemoteDataClient {
    node_map: BTreeMap<u64, String>,
    local_registry: ExportRegistry,
    client: reqwest::Client,
}

impl HttpRemoteDataClient {
    pub fn new(node_map: BTreeMap<u64, String>, local_registry: ExportRegistry) -> Self {
        Self {
            node_map,
            local_registry: local_registry,
            client: reqwest::Client::new(),
        }
    }

    fn remote_data_url(&self, node_id: u64) -> DandelionResult<String> {
        let address =
            self.node_map
                .get(&node_id)
                .ok_or(dandelion_err!(DandelionError::Multinode(
                    MultinodeError::ConfigError(format!(
                        "No data server configured for node {}",
                        node_id
                    ))
                )))?;
        let base_url = if address.starts_with("http://") || address.starts_with("https://") {
            address.clone()
        } else {
            format!("http://{}", address)
        };
        Ok(format!("{}/data/", base_url.trim_end_matches('/'),))
    }
}

impl RemoteDataClient for HttpRemoteDataClient {
    fn resolve_remote_data(
        &self,
        data: RemoteData,
    ) -> Pin<Box<dyn Future<Output = DandelionResult<(Arc<Context>, Position)>> + Send + '_>> {
        Box::pin(async move {
            trace!(
                "Resolving remote data: node_id={}, data_id={}",
                data.node_id,
                data.data_id
            );
            if data.node_id == self.local_registry.node_id {
                return self.local_registry.fetch_context(data.data_id);
            }

            let mut url = self.remote_data_url(data.node_id)?;
            url.push_str(&format!("{}", data.data_id));
            let mut response = self.client.get(url).send().await.map_err(|err| {
                dandelion_err!(DandelionError::Multinode(MultinodeError::ConnectionFailed(
                    err.to_string(),
                )))
            })?;
            if !response.status().is_success() {
                return err_dandelion!(DandelionError::Multinode(MultinodeError::RequestFailed(
                    response.status().to_string(),
                )));
            }
            let mut size = 0;
            let mut body = Vec::new();
            loop {
                match response.chunk().await {
                    Ok(Some(frame)) => {
                        size += frame.len();
                        body.push(frame)
                    }
                    Ok(None) => break,
                    Err(_) => return err_dandelion!(DandelionError::SystemFuncResponseError),
                }
            }
            trace!("Finished resolving remote data");
            // make sure data is only dropped once we have resolved it
            drop(data);
            Ok((
                Arc::new(Context::new(
                    ContextType::Bytes(Box::new(BytesContext::new(body))),
                    size,
                )),
                Position { offset: 0, size },
            ))
        })
    }

    fn resolve_multiple_data<'meta>(
        &'meta self,
        metadata: &'meta mut Vec<(usize, DataItem)>,
        remote_items: Vec<RemoteData>,
    ) -> Pin<Box<dyn Future<Output = DandelionResult<Arc<Context>>> + Send + 'meta>> {
        Box::pin(async move {
            // collect all ids
            let node_id = remote_items[0].node_id;
            let mut body_data = Vec::with_capacity(remote_items.len() * size_of::<u64>());
            for remote_item in remote_items.iter() {
                body_data.extend(remote_item.data_id.to_le_bytes());
            }

            // for multi item we use the /data/ url without any item id
            let url = self.remote_data_url(node_id)?;
            let mut response =
                self.client
                    .get(url)
                    .body(body_data)
                    .send()
                    .await
                    .map_err(|err| {
                        dandelion_err!(DandelionError::Multinode(MultinodeError::ConnectionFailed(
                            err.to_string(),
                        )))
                    })?;
            if !response.status().is_success() {
                return err_dandelion!(DandelionError::Multinode(MultinodeError::RequestFailed(
                    response.status().to_string(),
                )));
            }

            let mut size = 0;
            let mut body = Vec::new();
            loop {
                match response.chunk().await {
                    Ok(Some(frame)) => {
                        size += frame.len();
                        body.push(frame)
                    }
                    Ok(None) => break,
                    Err(_) => return err_dandelion!(DandelionError::SystemFuncResponseError),
                }
            }
            trace!("Finished resolving remote data");
            // make sure data is only dropped once we have resolved it
            drop(remote_items);

            // assumes the local items sizes are correct
            // also assumes that returned items have the same ordering as they were requested in
            let mut offset = 0;
            for (_, item) in metadata {
                item.data.offset = offset;
                offset += item.data.size;
            }
            // after updating all items, the total offset should be equal to the total amount of data that was received
            assert_eq!(size, offset);

            Ok(Arc::new(Context::new(
                ContextType::Bytes(Box::new(BytesContext::new(body))),
                size,
            )))
        })
    }

    fn delete_remote_data(
        &self,
        data: RemoteData,
    ) -> Pin<Box<dyn Future<Output = DandelionResult<()>> + Send + '_>> {
        trace!(
            "Deleting remote data: node_id={}, data_id={}",
            data.node_id,
            data.data_id
        );
        Box::pin(async move {
            if data.node_id == self.local_registry.node_id {
                return self.local_registry.delete_exported_data(data.data_id);
            }

            let mut url = self.remote_data_url(data.node_id)?;
            url.push_str(&format!("{}", data.data_id));

            let response = self.client.delete(url).send().await.map_err(|err| {
                dandelion_err!(DandelionError::Multinode(MultinodeError::ConnectionFailed(
                    err.to_string(),
                )))
            })?;
            if !response.status().is_success() {
                return err_dandelion!(DandelionError::Multinode(MultinodeError::RequestFailed(
                    response.status().to_string(),
                )));
            }
            Ok(())
        })
    }
}

// TODO: make data service handler copy free
async fn service(
    req: Request<Incoming>,
    export_registry: ExportRegistry,
    semaphore: Arc<Semaphore>,
) -> Result<Response<ExportedBody>, Infallible> {
    let permit = semaphore.acquire_owned().await.unwrap();
    let convert_error_string = |err_string| {
        warn!("Failed to serve remote data request: {}", err_string);
        let mut response = Response::new(ExportedBody::new_error(err_string));
        *response.status_mut() = StatusCode::BAD_REQUEST;
        Ok(response)
    };

    let id_string = match req.uri().path().strip_prefix("/data/") {
        Some(id_string) => id_string,
        None => {
            return convert_error_string(format!("Unknown data server path {}", req.uri().path()))
        }
    };

    match req.method() {
        &Method::DELETE => match id_string.parse::<u64>() {
            Err(err) => return convert_error_string(format!("Invalid data id: {}", err)),
            Ok(data_id) => {
                return match export_registry.delete_exported_data(data_id) {
                    Err(err) => convert_error_string(format!("Delete failed with: {}", err)),
                    Ok(()) => Ok(Response::new(ExportedBody {
                        inner: VecDeque::new(),
                    })),
                }
            }
        },
        &Method::GET => {
            if !id_string.is_empty() {
                match id_string.parse::<u64>() {
                    Err(err) => return convert_error_string(format!("Invalid data id: {}", err)),
                    Ok(data_id) => {
                        return match export_registry.get_exported_data(data_id) {
                            Err(err) => convert_error_string(format!("Get failed with: {}", err)),
                            Ok(data) => Ok(Response::new(ExportedBody::new_single(data))),
                        }
                    }
                }
            }
        }
        method => {
            return convert_error_string(format!("Unsupported data server method {}", method))
        }
    }
    // arrive here means we had GET method, but no single item
    let mut body = req.into_body();
    let mut body_pin = Pin::new(&mut body);
    let mut ids = Vec::new();
    let mut intermediate = [0u8; 8];
    let mut offset = 0;
    // read all the data ids
    while let Some(frame_result) =
        futures::future::poll_fn(|cx| body_pin.as_mut().poll_frame(cx)).await
    {
        let mut frame = match frame_result.unwrap().into_data() {
            Ok(frame) => frame,
            Err(_) => {
                // if the offset is 0, we can simply ignore trailer frames
                if offset == 0 {
                    break;
                } else {
                    return convert_error_string(format!(
                        "Got trailer frame, but still need: {}, to complete data id",
                        8 - offset,
                    ));
                }
            }
        };
        if offset > 0 {
            // This assumes that the next frame should contain at least the remainder of the started u64.
            if frame.try_copy_to_slice(&mut intermediate[offset..]).is_ok() {
                ids.push(u64::from_le_bytes(intermediate));
                offset = 0;
            } else {
                return convert_error_string(format!(
                    "Body did not contain a full array of indices, need: {}, available {}",
                    8 - offset,
                    frame.remaining()
                ));
            }
        }
        while let Ok(data_id) = frame.try_get_u64_le() {
            ids.push(data_id);
        }
        if frame.remaining() > 0 {
            offset = frame.remaining();
            frame.copy_to_slice(&mut intermediate[..offset]);
        }
    }
    if !body_pin.is_end_stream() {
        warn!("Frame poll returned None, but is_end_stream is false");
    }

    if offset > 0 {
        return convert_error_string(format!(
            "Body did not contain a full array of indices, need: {}, with no more frames",
            8 - offset,
        ));
    }

    drop(permit);

    match export_registry.get_multiple_data(ids) {
        Ok(exports) => Ok(Response::new(ExportedBody { inner: exports })),
        Err(err) => convert_error_string(err.to_string()),
    }
}

pub static CONCURRENCY_LIMIT: OnceLock<usize> = OnceLock::new();

pub async fn service_loop(port: u16, export_registry: ExportRegistry) {
    let addr: SocketAddr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr).await.unwrap();
    debug!("Data server ready");

    let mut sigterm_stream = tokio::signal::unix::signal(SignalKind::terminate()).unwrap();
    let mut sigint_stream = tokio::signal::unix::signal(SignalKind::interrupt()).unwrap();
    let mut sigquit_stream = tokio::signal::unix::signal(SignalKind::quit()).unwrap();

    let concurrency_limit = CONCURRENCY_LIMIT
        .get()
        .expect("Should always be initialized by server main in normal operation");
    let semaphore = Arc::new(Semaphore::new(*concurrency_limit));

    loop {
        tokio::select! {
            connection_pair = listener.accept() => {
                let semaphore_clone = semaphore.clone();
                let (stream, _) = connection_pair.unwrap();
                let io = hyper_util::rt::TokioIo::new(stream);
                let service_registry = export_registry.clone();
                tokio::task::spawn(async move {
                    if let Err(err) = hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new())
                        .serve_connection_with_upgrades(
                            io,
                            service_fn(|req| service(req, service_registry.clone(), semaphore_clone.clone())),
                        )
                        .await
                    {
                        error!("Data request serving failed with error: {:?}", err);
                    }
                });
            }
            _ = sigterm_stream.recv() => return,
            _ = sigint_stream.recv() => return,
            _ = sigquit_stream.recv() => return,
        }
    }
}

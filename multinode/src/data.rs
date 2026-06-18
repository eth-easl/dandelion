use dandelion_commons::{
    dandelion_err, err_dandelion, DandelionError, DandelionResult, MultinodeError,
};
use hyper::{body::Incoming, service::service_fn, Method, Request, Response, StatusCode};
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
    collections::BTreeMap,
    convert::Infallible,
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, RwLock},
};
use tokio::{net::TcpListener, signal::unix::SignalKind};

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
    inner: Option<ExportedData>,
}

impl hyper::body::Body for ExportedBody {
    type Data = ExportedData;
    type Error = DandelionError;
    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        let frame = self
            .inner
            .take()
            .and_then(|data| Some(Ok(hyper::body::Frame::data(data))));
        return std::task::Poll::Ready(frame);
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
    inner: Arc<RwLock<ExportRegistryInner>>,
}

impl ExportRegistry {
    pub fn new(node_id: u64) -> Self {
        Self {
            node_id,
            inner: Arc::new(RwLock::new(ExportRegistryInner {
                next_data_id: 0,
                data: BTreeMap::new(),
            })),
        }
    }

    pub fn get_node_id(&self) -> u64 {
        self.node_id
    }

    pub fn insert_context(
        &self,
        position: Position,
        context: Arc<Context>,
        delete_sender: Option<tokio::sync::mpsc::UnboundedSender<RemoteData>>,
    ) -> RemoteData {
        let mut inner = self.inner.write().unwrap();
        let data_id = inner.next_data_id;
        inner.next_data_id += 1;
        inner
            .data
            .insert(data_id, ExportedData { context, position });
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
        let exported_data = {
            let inner = self.inner.read().unwrap();
            inner.data.get(&data_id).cloned()
        };

        let Some(exported_data) = exported_data else {
            return err_dandelion!(DandelionError::Multinode(MultinodeError::RequestFailed(
                format!("Unknown remote data id {}", data_id),
            )));
        };

        Ok(exported_data)
    }

    pub fn delete_exported_data(&self, data_id: u64) -> DandelionResult<()> {
        debug!(
            "Deleting exported data: node_id={}, data_id={}",
            self.node_id, data_id
        );
        let mut inner = self.inner.write().unwrap();
        let Some(_) = inner.data.remove(&data_id) else {
            return err_dandelion!(DandelionError::Multinode(MultinodeError::RequestFailed(
                format!("Unknown remote data id {}", data_id),
            )));
        };
        Ok(())
    }

    fn fetch_bytes(&self, data_id: u64) -> DandelionResult<ExportedData> {
        self.get_exported_data(data_id)
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

    fn remote_data_url(&self, data: RemoteData) -> DandelionResult<String> {
        let address =
            self.node_map
                .get(&data.node_id)
                .ok_or(dandelion_err!(DandelionError::Multinode(
                    MultinodeError::ConfigError(format!(
                        "No data server configured for node {}",
                        data.node_id
                    ))
                )))?;
        let base_url = if address.starts_with("http://") || address.starts_with("https://") {
            address.clone()
        } else {
            format!("http://{}", address)
        };
        Ok(format!(
            "{}/data/{}",
            base_url.trim_end_matches('/'),
            data.data_id
        ))
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

            let url = self.remote_data_url(data)?;
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
            Ok((
                Arc::new(Context::new(
                    ContextType::Bytes(Box::new(BytesContext::new(body))),
                    size,
                )),
                Position { offset: 0, size },
            ))
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

            let url = self.remote_data_url(data)?;
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
) -> Result<Response<ExportedBody>, Infallible> {
    let result = match req.uri().path().strip_prefix("/data/") {
        None => Err(format!("Unknown data server path {}", req.uri().path())),
        Some(id_string) => match id_string.parse::<u64>() {
            Err(err) => Err(format!("Invalid data id: {}", err)),
            Ok(data_id) => match req.method() {
                &Method::GET => export_registry
                    .fetch_bytes(data_id)
                    .map(|data| Some(data))
                    .map_err(|err| err.to_string()),
                &Method::DELETE => export_registry
                    .delete_exported_data(data_id)
                    .map(|_| None)
                    .map_err(|err| err.to_string()),
                method => Err(format!("Unsupported data server method {}", method)),
            },
        },
    };

    match result {
        Ok(bytes) => Ok(Response::new(ExportedBody { inner: bytes })),
        Err(err) => {
            warn!("Failed to serve remote data request: {}", err);
            let error_bytes = err.into_bytes();
            let error_lenth = error_bytes.len();
            let mut response = Response::new(ExportedBody {
                inner: Some(ExportedData {
                    context: Arc::new(
                        ReadOnlyContext::new(error_bytes.into_boxed_slice()).unwrap(),
                    ),
                    position: Position {
                        offset: 0,
                        size: error_lenth,
                    },
                }),
            });
            *response.status_mut() = StatusCode::BAD_REQUEST;
            Ok(response)
        }
    }
}

pub async fn service_loop(port: u16, export_registry: ExportRegistry) {
    let addr: SocketAddr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr).await.unwrap();
    debug!("Data server ready");

    let mut sigterm_stream = tokio::signal::unix::signal(SignalKind::terminate()).unwrap();
    let mut sigint_stream = tokio::signal::unix::signal(SignalKind::interrupt()).unwrap();
    let mut sigquit_stream = tokio::signal::unix::signal(SignalKind::quit()).unwrap();

    loop {
        tokio::select! {
            connection_pair = listener.accept() => {
                let (stream, _) = connection_pair.unwrap();
                let io = hyper_util::rt::TokioIo::new(stream);
                let service_registry = export_registry.clone();
                tokio::task::spawn(async move {
                    if let Err(err) = hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new())
                        .serve_connection_with_upgrades(
                            io,
                            service_fn(|req| service(req, service_registry.clone())),
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

use dandelion_commons::{
    dandelion_err, err_dandelion, DandelionError, DandelionResult, MultinodeError,
};
use hyper::{
    body::{Body, Buf, Incoming},
    service::service_fn,
    Method, Request, Response, StatusCode,
};
use log::{debug, error, trace, warn};
#[cfg(any(feature = "checkpointed-at-least-once", feature = "exactly-once"))]
use machine_interface::function_driver::system_driver::recovery_log::accept_local_io_completion_record;
#[cfg(feature = "at-least-once")]
use machine_interface::{
    composition::{IoCompletionOutcome, IoCoordinationCompletion, IoCoordinationKey},
    function_driver::system_driver::{
        get_system_function_output_sets,
        recovery_log::{
            accept_delivered_io_completion_record, format_io_completion_line,
            parse_io_completion_line, IoCompletionKey,
        },
        IoCompletionData, IoCompletionDisposition, IoCompletionItem, IoCompletionOutputSet,
        IoCompletionRecord,
    },
};
#[cfg(feature = "exactly-once")]
use machine_interface::{
    composition::{IoResolveInput, IoResolveOutcome, IoResolveRequest},
    function_driver::system_driver::{
        recovery_log::{append_delivered_io_completion_record, recovered_io_item_locations},
        RecoveredIoOutput,
    },
};
use machine_interface::{
    composition::{RemoteData, RemoteDataClient},
    memory_domain::{
        bytes_context::BytesContext, read_only::ReadOnlyContext, Context, ContextTrait, ContextType,
    },
    DataItem, Position,
};
use prost::bytes;
#[cfg(feature = "exactly-once")]
use std::collections::{HashMap, HashSet};
use std::{
    collections::{BTreeMap, VecDeque},
    convert::Infallible,
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex, OnceLock},
};
#[cfg(feature = "at-least-once")]
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};
#[cfg(feature = "exactly-once")]
use tokio::sync::watch;
#[cfg(feature = "at-least-once")]
use tokio::sync::Notify;
use tokio::{net::TcpListener, signal::unix::SignalKind, sync::Semaphore};

#[cfg(feature = "at-least-once")]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct IoRemoteRef {
    node_id: u64,
    data_id: u64,
    size: usize,
}

#[cfg(feature = "at-least-once")]
impl IoRemoteRef {
    fn from_remote(data: &RemoteData, size: usize) -> Self {
        Self {
            node_id: data.node_id,
            data_id: data.data_id,
            size,
        }
    }

    #[cfg(feature = "exactly-once")]
    fn into_remote(self) -> (RemoteData, usize) {
        (RemoteData::new(self.node_id, self.data_id), self.size)
    }
}

#[cfg(feature = "exactly-once")]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct IoResolveWireRequest {
    key: IoCoordinationKey,
    set_index: usize,
    requester_node_id: u64,
    original_data: Option<IoRemoteRef>,
}

#[cfg(feature = "exactly-once")]
#[derive(Debug, PartialEq, Eq)]
enum IoResolveWireResponse {
    Execute { input: Option<IoResolveWireInput> },
    Completed { data: IoRemoteRef },
    Failed(String),
    Retry,
}

#[cfg(feature = "exactly-once")]
#[derive(Debug, PartialEq, Eq)]
enum IoResolveWireInput {
    Inline(Vec<u8>),
    Remote(IoRemoteRef),
}

#[cfg(feature = "exactly-once")]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct IoResolvedWireRequest {
    key: IoCoordinationKey,
    outputs: Result<Vec<IoRemoteRef>, String>,
}

#[cfg(feature = "exactly-once")]
#[derive(Clone, Debug, PartialEq, Eq)]
enum SharedIoResult {
    Completed(Vec<IoRemoteRef>),
    Failed(String),
    Retry,
}

#[cfg(feature = "exactly-once")]
enum IoResolutionState {
    Running {
        worker_node_id: u64,
        result: watch::Sender<Option<SharedIoResult>>,
    },
    Completed(Vec<IoRemoteRef>),
    Failed(String),
}

#[cfg(feature = "exactly-once")]
enum RegistryIoResolution {
    Execute(Option<IoRemoteRef>),
    Wait(watch::Receiver<Option<SharedIoResult>>),
    Completed(IoRemoteRef),
    Failed(String),
}

#[cfg(feature = "exactly-once")]
const IO_RESOLVE_EXECUTE_NONE: u8 = 0;
#[cfg(feature = "exactly-once")]
const IO_RESOLVE_EXECUTE_INLINE: u8 = 1;
#[cfg(feature = "exactly-once")]
const IO_RESOLVE_EXECUTE_REMOTE: u8 = 2;
#[cfg(feature = "exactly-once")]
const IO_RESOLVE_COMPLETED: u8 = 3;
#[cfg(feature = "exactly-once")]
const IO_RESOLVE_FAILED: u8 = 4;
#[cfg(feature = "exactly-once")]
const IO_RESOLVE_RETRY: u8 = 5;

#[cfg(feature = "exactly-once")]
fn encode_remote_ref(buffer: &mut Vec<u8>, data: &IoRemoteRef) {
    buffer.extend_from_slice(&data.node_id.to_le_bytes());
    buffer.extend_from_slice(&data.data_id.to_le_bytes());
    buffer.extend_from_slice(&(data.size as u64).to_le_bytes());
}

#[cfg(feature = "exactly-once")]
fn decode_remote_ref(bytes: &[u8]) -> Result<IoRemoteRef, String> {
    if bytes.len() != 24 {
        return Err(format!(
            "Invalid I/O resolve remote reference length {}, expected 24",
            bytes.len()
        ));
    }
    Ok(IoRemoteRef {
        node_id: u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
        data_id: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
        size: usize::try_from(u64::from_le_bytes(bytes[16..24].try_into().unwrap()))
            .map_err(|_| "I/O resolve remote reference size does not fit usize".to_string())?,
    })
}

#[cfg(feature = "exactly-once")]
fn encode_io_resolve_response(response: &IoResolveWireResponse) -> Vec<u8> {
    let mut buffer = Vec::new();
    match response {
        IoResolveWireResponse::Execute { input: None } => {
            buffer.push(IO_RESOLVE_EXECUTE_NONE);
        }
        IoResolveWireResponse::Execute {
            input: Some(IoResolveWireInput::Inline(bytes)),
        } => {
            buffer.push(IO_RESOLVE_EXECUTE_INLINE);
            buffer.extend_from_slice(bytes);
        }
        IoResolveWireResponse::Execute {
            input: Some(IoResolveWireInput::Remote(data)),
        } => {
            buffer.push(IO_RESOLVE_EXECUTE_REMOTE);
            encode_remote_ref(&mut buffer, data);
        }
        IoResolveWireResponse::Completed { data } => {
            buffer.push(IO_RESOLVE_COMPLETED);
            encode_remote_ref(&mut buffer, data);
        }
        IoResolveWireResponse::Failed(error) => {
            buffer.push(IO_RESOLVE_FAILED);
            buffer.extend_from_slice(error.as_bytes());
        }
        IoResolveWireResponse::Retry => buffer.push(IO_RESOLVE_RETRY),
    }
    buffer
}

#[cfg(feature = "exactly-once")]
fn decode_io_resolve_response(bytes: &[u8]) -> Result<IoResolveWireResponse, String> {
    let Some((&tag, payload)) = bytes.split_first() else {
        return Err("Empty I/O resolve response".to_string());
    };
    match tag {
        IO_RESOLVE_EXECUTE_NONE if payload.is_empty() => {
            Ok(IoResolveWireResponse::Execute { input: None })
        }
        IO_RESOLVE_EXECUTE_NONE => Err("I/O resolve execute-none response had a body".to_string()),
        IO_RESOLVE_EXECUTE_INLINE => Ok(IoResolveWireResponse::Execute {
            input: Some(IoResolveWireInput::Inline(payload.to_vec())),
        }),
        IO_RESOLVE_EXECUTE_REMOTE => Ok(IoResolveWireResponse::Execute {
            input: Some(IoResolveWireInput::Remote(decode_remote_ref(payload)?)),
        }),
        IO_RESOLVE_COMPLETED => Ok(IoResolveWireResponse::Completed {
            data: decode_remote_ref(payload)?,
        }),
        IO_RESOLVE_FAILED => String::from_utf8(payload.to_vec())
            .map(IoResolveWireResponse::Failed)
            .map_err(|_| "I/O resolve failure response was not UTF-8".to_string()),
        IO_RESOLVE_RETRY if payload.is_empty() => Ok(IoResolveWireResponse::Retry),
        IO_RESOLVE_RETRY => Err("I/O resolve retry response had a body".to_string()),
        tag => Err(format!("Unknown I/O resolve response tag {}", tag)),
    }
}

#[cfg(feature = "exactly-once")]
async fn registry_resolution_response(
    registry: &ExportRegistry,
    key: IoCoordinationKey,
    resolution: RegistryIoResolution,
    set_index: usize,
) -> DandelionResult<IoResolveWireResponse> {
    Ok(match resolution {
        RegistryIoResolution::Execute(original_data) => {
            let input = match original_data
                .map(|data| registry.inline_local_io_input(data))
                .transpose()
            {
                Ok(input) => input,
                Err(error) => {
                    let message = error.to_string();
                    registry.publish_io_resolution_inner(key, Err(message.clone()))?;
                    return Ok(IoResolveWireResponse::Failed(message));
                }
            };
            IoResolveWireResponse::Execute { input }
        }
        RegistryIoResolution::Completed(data) => IoResolveWireResponse::Completed { data },
        RegistryIoResolution::Failed(error) => IoResolveWireResponse::Failed(error),
        RegistryIoResolution::Wait(mut receiver) => loop {
            if let Some(outcome) = receiver.borrow().clone() {
                break match outcome {
                    SharedIoResult::Completed(outputs) => match outputs.get(set_index).cloned() {
                        Some(data) => IoResolveWireResponse::Completed { data },
                        None => IoResolveWireResponse::Failed(format!(
                            "I/O resolution has no output set {}",
                            set_index
                        )),
                    },
                    SharedIoResult::Failed(error) => IoResolveWireResponse::Failed(error),
                    SharedIoResult::Retry => IoResolveWireResponse::Retry,
                };
            }
            if receiver.changed().await.is_err() {
                break IoResolveWireResponse::Failed(
                    "I/O resolution owner stopped before completion".to_string(),
                );
            }
        },
    })
}

#[cfg(feature = "at-least-once")]
fn completion_record(key: &IoCoordinationKey, outputs: &[IoRemoteRef]) -> IoCompletionRecord {
    let set_names = get_system_function_output_sets(key.function);
    IoCompletionRecord {
        invocation_id: key.invocation_id,
        composition_set_id: key.composition_set_id,
        function: key.function,
        outputs: outputs
            .iter()
            .enumerate()
            .map(|(set_index, output)| IoCompletionOutputSet {
                set_index,
                set_name: set_names
                    .get(set_index)
                    .cloned()
                    .unwrap_or_else(|| format!("output_{}", set_index)),
                items: vec![IoCompletionItem {
                    identifier: key.identifier.clone(),
                    key: key.item_key,
                    location: IoCompletionData::Remote {
                        node_id: output.node_id,
                        data_id: output.data_id,
                        size: output.size,
                    },
                }],
            })
            .collect(),
    }
}

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

    #[cfg(feature = "exactly-once")]
    fn from_bytes(data: Vec<u8>) -> Self {
        if data.is_empty() {
            return Self {
                inner: VecDeque::new(),
            };
        }
        let size = data.len();
        Self::new_single(ExportedData {
            context: Arc::new(ReadOnlyContext::new(data.into_boxed_slice()).unwrap()),
            position: Position { offset: 0, size },
        })
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
const DURABLE_DATA_ID_START: u64 = 1 << 63;
const DURABLE_FETCH_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);
#[cfg(feature = "at-least-once")]
const DURABLE_DATA_FILE_EXTENSION: &str = "data";
#[cfg(feature = "at-least-once")]
const NEXT_DURABLE_DATA_ID_FILE: &str = "next_data_id";
#[cfg(feature = "at-least-once")]
const IO_COMPLETION_JOURNAL_FILE: &str = "io_completions.log";

#[cfg(feature = "at-least-once")]
fn export_registry_error(message: impl Into<String>) -> dandelion_commons::DError {
    dandelion_err!(DandelionError::Multinode(MultinodeError::RequestFailed(
        message.into(),
    )))
}

#[cfg(feature = "at-least-once")]
fn durable_data_path(directory: &Path, data_id: u64) -> PathBuf {
    directory.join(format!("{data_id}.{DURABLE_DATA_FILE_EXTENSION}"))
}

pub fn is_durable_data_id(data_id: u64) -> bool {
    data_id >= DURABLE_DATA_ID_START
}

#[cfg(feature = "at-least-once")]
fn sync_directory(directory: &Path) -> DandelionResult<()> {
    File::open(directory)
        .and_then(|directory| directory.sync_all())
        .map_err(|err| {
            export_registry_error(format!(
                "Failed to sync durable export directory {}: {}",
                directory.display(),
                err
            ))
        })
}

#[cfg(feature = "at-least-once")]
fn write_atomic_file(path: &Path, contents: &[u8]) -> DandelionResult<()> {
    let temporary_path = path.with_extension(format!(
        "{}.tmp",
        path.extension()
            .and_then(|extension| extension.to_str())
            .unwrap_or("file")
    ));
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary_path)
        .map_err(|err| {
            export_registry_error(format!(
                "Failed to create durable export file {}: {}",
                temporary_path.display(),
                err
            ))
        })?;
    file.write_all(contents).map_err(|err| {
        export_registry_error(format!(
            "Failed to write durable export file {}: {}",
            temporary_path.display(),
            err
        ))
    })?;
    file.sync_all().map_err(|err| {
        export_registry_error(format!(
            "Failed to sync durable export file {}: {}",
            temporary_path.display(),
            err
        ))
    })?;
    fs::rename(&temporary_path, path).map_err(|err| {
        export_registry_error(format!(
            "Failed to install durable export file {}: {}",
            path.display(),
            err
        ))
    })?;
    sync_directory(
        path.parent()
            .expect("Durable export files always have a parent directory"),
    )
}

#[cfg(feature = "at-least-once")]
fn load_durable_exports(directory: &Path) -> DandelionResult<(BTreeMap<u64, ExportedData>, u64)> {
    let mut durable_data = BTreeMap::new();
    let mut next_data_id = fs::read_to_string(directory.join(NEXT_DURABLE_DATA_ID_FILE))
        .ok()
        .and_then(|contents| contents.trim().parse::<u64>().ok())
        .filter(|data_id| *data_id >= DURABLE_DATA_ID_START)
        .unwrap_or(DURABLE_DATA_ID_START);

    for entry in fs::read_dir(directory).map_err(|err| {
        export_registry_error(format!(
            "Failed to read durable export directory {}: {}",
            directory.display(),
            err
        ))
    })? {
        let entry = entry.map_err(|err| {
            export_registry_error(format!(
                "Failed to inspect durable export directory {}: {}",
                directory.display(),
                err
            ))
        })?;
        let path = entry.path();
        if path.extension().and_then(|extension| extension.to_str())
            != Some(DURABLE_DATA_FILE_EXTENSION)
        {
            continue;
        }
        let Some(data_id) = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .and_then(|stem| stem.parse::<u64>().ok())
            .filter(|data_id| *data_id >= DURABLE_DATA_ID_START)
        else {
            warn!("Ignoring invalid durable export file {}", path.display());
            continue;
        };
        let bytes = fs::read(&path).map_err(|err| {
            export_registry_error(format!(
                "Failed to read durable export {}: {}",
                path.display(),
                err
            ))
        })?;
        let size = bytes.len();
        durable_data.insert(
            data_id,
            ExportedData {
                context: Arc::new(ReadOnlyContext::new(bytes.into_boxed_slice())?),
                position: Position { offset: 0, size },
            },
        );
        next_data_id = next_data_id.max(
            data_id
                .checked_add(1)
                .ok_or_else(|| export_registry_error("Durable export data id space exhausted"))?,
        );
    }

    write_atomic_file(
        &directory.join(NEXT_DURABLE_DATA_ID_FILE),
        next_data_id.to_string().as_bytes(),
    )?;
    Ok((durable_data, next_data_id))
}

#[cfg(feature = "at-least-once")]
fn load_io_completion_journal(directory: &Path) -> DandelionResult<Vec<IoCompletionRecord>> {
    let journal_path = directory.join(IO_COMPLETION_JOURNAL_FILE);
    let contents = match fs::read_to_string(&journal_path) {
        Ok(contents) => contents,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => {
            return Err(export_registry_error(format!(
                "Failed to read IO completion journal {}: {}",
                journal_path.display(),
                err
            )))
        }
    };

    contents
        .lines()
        .map(|line| {
            parse_io_completion_line(line)?.ok_or_else(|| {
                export_registry_error(format!(
                    "Invalid entry in IO completion journal {}",
                    journal_path.display()
                ))
            })
        })
        .collect()
}

#[cfg(feature = "at-least-once")]
struct DurableExportStore {
    directory: PathBuf,
    next_data_id: u64,
}

struct ExportRegistryInner {
    next_transient_data_id: u64,
    transient_data: BTreeMap<u64, ExportedData>,
    #[cfg(feature = "at-least-once")]
    durable_data: BTreeMap<u64, ExportedData>,
    #[cfg(feature = "at-least-once")]
    pending_io_completions: Vec<IoCompletionRecord>,
    #[cfg(feature = "exactly-once")]
    io_resolutions: HashMap<IoCoordinationKey, IoResolutionState>,
    #[cfg(feature = "at-least-once")]
    durable_store: Option<DurableExportStore>,
}

#[derive(Clone)]
pub struct ExportRegistry {
    node_id: u64,
    inner: Arc<Mutex<ExportRegistryInner>>,
    #[cfg(feature = "at-least-once")]
    pending_io_completions_changed: Arc<Notify>,
}

impl ExportRegistry {
    pub fn new(node_id: u64) -> Self {
        Self {
            node_id,
            inner: Arc::new(Mutex::new(ExportRegistryInner {
                next_transient_data_id: 0,
                transient_data: BTreeMap::new(),
                #[cfg(feature = "at-least-once")]
                durable_data: BTreeMap::new(),
                #[cfg(feature = "at-least-once")]
                pending_io_completions: Vec::new(),
                #[cfg(feature = "exactly-once")]
                io_resolutions: HashMap::new(),
                #[cfg(feature = "at-least-once")]
                durable_store: None,
            })),
            #[cfg(feature = "at-least-once")]
            pending_io_completions_changed: Arc::new(Notify::new()),
        }
    }

    /// Opens a registry with a durable export tier rooted at `storage_root`.
    /// Each node gets its own directory so configured nodes can share a root.
    #[cfg(feature = "at-least-once")]
    pub fn with_durable_storage(
        node_id: u64,
        storage_root: impl AsRef<Path>,
    ) -> DandelionResult<Self> {
        let directory = storage_root.as_ref().join(node_id.to_string());
        fs::create_dir_all(&directory).map_err(|err| {
            export_registry_error(format!(
                "Failed to create durable export directory {}: {}",
                directory.display(),
                err
            ))
        })?;

        let (durable_data, next_data_id) = load_durable_exports(&directory)?;
        let pending_io_completions = load_io_completion_journal(&directory)?;
        Ok(Self {
            node_id,
            inner: Arc::new(Mutex::new(ExportRegistryInner {
                next_transient_data_id: 0,
                transient_data: BTreeMap::new(),
                durable_data,
                pending_io_completions,
                #[cfg(feature = "exactly-once")]
                io_resolutions: HashMap::new(),
                durable_store: Some(DurableExportStore {
                    directory,
                    next_data_id,
                }),
            })),
            pending_io_completions_changed: Arc::new(Notify::new()),
        })
    }

    pub fn get_node_id(&self) -> u64 {
        self.node_id
    }

    #[cfg(all(test, feature = "exactly-once"))]
    fn begin_io_resolution(
        &self,
        key: IoCoordinationKey,
        set_index: usize,
        original_data: Option<IoRemoteRef>,
    ) -> DandelionResult<RegistryIoResolution> {
        self.begin_io_resolution_from_node(key, set_index, self.node_id, original_data)
    }

    #[cfg(feature = "exactly-once")]
    fn begin_io_resolution_from_node(
        &self,
        key: IoCoordinationKey,
        set_index: usize,
        requester_node_id: u64,
        original_data: Option<IoRemoteRef>,
    ) -> DandelionResult<RegistryIoResolution> {
        let output_count = get_system_function_output_sets(key.function).len();
        let recovered = recovered_io_item_locations(
            key.invocation_id,
            key.function,
            key.composition_set_id,
            output_count,
            &key.identifier,
            key.item_key,
        )
        .map(|outputs| {
            outputs
                .into_iter()
                .map(|output| match output {
                    RecoveredIoOutput::Remote { data, size } => {
                        Ok(IoRemoteRef::from_remote(&data, size))
                    }
                    RecoveredIoOutput::Inline(context) => {
                        let item = DataItem {
                            ident: key.identifier.clone(),
                            key: u32::try_from(key.item_key)
                                .expect("I/O item keys originate as u32"),
                            data: Position {
                                offset: 0,
                                size: context.size,
                            },
                        };
                        let data = self.insert_function(&item, context, None);
                        Ok(IoRemoteRef::from_remote(&data, item.data.size))
                    }
                })
                .collect::<DandelionResult<Vec<_>>>()
        })
        .transpose()?;
        let mut inner = self.inner.lock().unwrap();
        match inner.io_resolutions.entry(key) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                if let Some(outputs) = recovered {
                    let output = outputs.get(set_index).cloned().ok_or_else(|| {
                        export_registry_error(format!(
                            "Recovered I/O completion has no output set {}",
                            set_index
                        ))
                    })?;
                    entry.insert(IoResolutionState::Completed(outputs));
                    return Ok(RegistryIoResolution::Completed(output));
                }
                let (result, _) = watch::channel(None);
                entry.insert(IoResolutionState::Running {
                    worker_node_id: requester_node_id,
                    result,
                });
                Ok(RegistryIoResolution::Execute(original_data))
            }
            std::collections::hash_map::Entry::Occupied(entry) => match entry.get() {
                IoResolutionState::Running { result, .. } => {
                    Ok(RegistryIoResolution::Wait(result.subscribe()))
                }
                IoResolutionState::Completed(outputs) => outputs
                    .get(set_index)
                    .cloned()
                    .map(RegistryIoResolution::Completed)
                    .ok_or_else(|| {
                        export_registry_error(format!(
                            "I/O completion has no output set {}",
                            set_index
                        ))
                    }),
                IoResolutionState::Failed(error) => Ok(RegistryIoResolution::Failed(error.clone())),
            },
        }
    }

    #[cfg(feature = "exactly-once")]
    fn publish_io_resolution_inner(
        &self,
        key: IoCoordinationKey,
        outcome: Result<Vec<IoRemoteRef>, String>,
    ) -> DandelionResult<()> {
        let mut inner = self.inner.lock().unwrap();
        let terminal = || match &outcome {
            Ok(outputs) => IoResolutionState::Completed(outputs.clone()),
            Err(error) => IoResolutionState::Failed(error.clone()),
        };
        let shared_outcome = match &outcome {
            Ok(outputs) => SharedIoResult::Completed(outputs.clone()),
            Err(error) => SharedIoResult::Failed(error.clone()),
        };
        match inner.io_resolutions.entry(key) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(terminal());
            }
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                if let IoResolutionState::Running { result, .. } = entry.get() {
                    let result = result.clone();
                    entry.insert(terminal());
                    result.send_replace(Some(shared_outcome));
                }
                // Completed and failed states are terminal: the first result wins.
            }
        }
        Ok(())
    }

    /// Releases unfinished claims owned by a disconnected worker. Completed outputs are durable
    /// and remain valid while the worker process restarts.
    #[cfg(feature = "exactly-once")]
    pub fn invalidate_running_io_for_node(&self, node_id: u64) {
        self.inner
            .lock()
            .unwrap()
            .io_resolutions
            .retain(|_, state| {
                if let IoResolutionState::Running {
                    worker_node_id,
                    result,
                } = state
                {
                    if *worker_node_id == node_id {
                        result.send_replace(Some(SharedIoResult::Retry));
                        return false;
                    }
                }
                true
            });
    }

    #[cfg(feature = "exactly-once")]
    pub fn publish_io_resolution_from_record(
        &self,
        record: &IoCompletionRecord,
    ) -> DandelionResult<()> {
        let key = record.completion_key()?;
        let coordination_key = IoCoordinationKey {
            invocation_id: key.invocation_id,
            composition_set_id: key.composition_set_id,
            function: key.function,
            identifier: key.identifier,
            item_key: key.item_key,
        };
        let mut outputs = record.outputs.clone();
        outputs.sort_by_key(|output| output.set_index);
        let references = outputs
            .into_iter()
            .map(|output| {
                let item = output.items.into_iter().next().ok_or_else(|| {
                    export_registry_error(format!(
                        "I/O completion output set {} is empty",
                        output.set_index
                    ))
                })?;
                match item.location {
                    IoCompletionData::Remote {
                        node_id,
                        data_id,
                        size,
                    } => Ok(IoRemoteRef {
                        node_id,
                        data_id,
                        size,
                    }),
                    IoCompletionData::Inline(_) => Err(export_registry_error(
                        "Remote I/O delivery unexpectedly contained inline data".to_string(),
                    )),
                }
            })
            .collect::<DandelionResult<Vec<_>>>()?;
        self.publish_io_resolution_inner(coordination_key, Ok(references))
    }

    #[cfg(feature = "exactly-once")]
    pub fn io_coordination_outputs(
        &self,
        invocation_id: dandelion_commons::InvocationId,
    ) -> Vec<RemoteData> {
        let inner = self.inner.lock().unwrap();
        let mut seen = HashSet::new();
        inner
            .io_resolutions
            .iter()
            .filter(|(key, _)| key.invocation_id == invocation_id)
            .filter_map(|(_, state)| match state {
                IoResolutionState::Completed(references) => Some(references),
                _ => None,
            })
            .flat_map(|references| references.iter())
            .filter_map(|reference| {
                if seen.insert((reference.node_id, reference.data_id)) {
                    Some(RemoteData::new(reference.node_id, reference.data_id))
                } else {
                    None
                }
            })
            .collect()
    }

    #[cfg(feature = "exactly-once")]
    pub fn remove_io_coordination(&self, invocation_id: dandelion_commons::InvocationId) {
        self.inner
            .lock()
            .unwrap()
            .io_resolutions
            .retain(|key, _| key.invocation_id != invocation_id);
    }

    pub fn insert_function(
        &self,
        item: &DataItem,
        context: Arc<Context>,
        delete_sender: Option<tokio::sync::mpsc::UnboundedSender<RemoteData>>,
    ) -> RemoteData {
        let mut inner = self.inner.lock().unwrap();
        let data_id = inner.next_transient_data_id;
        inner.next_transient_data_id = inner
            .next_transient_data_id
            .checked_add(1)
            .filter(|next_id| *next_id < DURABLE_DATA_ID_START)
            .expect("Transient export data id space exhausted");
        inner.transient_data.insert(
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

    /// Persists one exported item before returning its remote reference.
    /// Durable references are not configured for delete-on-drop; they remain
    /// pinned until `delete_durable_exported_data` is called explicitly.
    #[cfg(feature = "at-least-once")]
    pub fn insert_durable_function(
        &self,
        item: &DataItem,
        context: Arc<Context>,
    ) -> DandelionResult<RemoteData> {
        let mut bytes = vec![0; item.data.size];
        context.read(item.data.offset, &mut bytes)?;

        let mut inner = self.inner.lock().unwrap();
        let store = inner.durable_store.as_mut().ok_or_else(|| {
            export_registry_error(
                "Cannot insert durable data into an in-memory-only export registry".to_string(),
            )
        })?;
        let data_id = store.next_data_id;
        let next_data_id = data_id
            .checked_add(1)
            .ok_or_else(|| export_registry_error("Durable export data id space exhausted"))?;

        let data_path = durable_data_path(&store.directory, data_id);
        write_atomic_file(&data_path, &bytes)?;
        write_atomic_file(
            &store.directory.join(NEXT_DURABLE_DATA_ID_FILE),
            next_data_id.to_string().as_bytes(),
        )?;
        store.next_data_id = next_data_id;

        let size = bytes.len();
        let persisted_context = Arc::new(ReadOnlyContext::new(bytes.into_boxed_slice())?);
        inner.durable_data.insert(
            data_id,
            ExportedData {
                context: persisted_context,
                position: Position { offset: 0, size },
            },
        );
        Ok(RemoteData::new(self.node_id, data_id))
    }

    /// Durably records a completion so delivery can be retried after a worker or connection
    /// failure.
    /// Returns `true` when a new logical completion was appended and `false` for a duplicate.
    #[cfg(feature = "at-least-once")]
    pub fn append_io_completion_record(
        &self,
        record: &IoCompletionRecord,
        mut recorder: Option<dandelion_commons::records::Recorder>,
    ) -> DandelionResult<bool> {
        let mut inner = self.inner.lock().unwrap();
        let store = inner.durable_store.as_ref().ok_or_else(|| {
            export_registry_error(
                "Cannot journal an IO completion in an in-memory-only export registry",
            )
        })?;
        let journal_path = store.directory.join(IO_COMPLETION_JOURNAL_FILE);
        let record_key = record.completion_key()?;
        for pending in &inner.pending_io_completions {
            if pending.completion_key()? == record_key {
                return Ok(false);
            }
        }
        let mut pending_io_completions = inner.pending_io_completions.clone();
        pending_io_completions.push(record.clone());
        if let Some(recorder) = recorder.as_mut() {
            recorder.record(dandelion_commons::records::RecordPoint::IoPayloadEncodeStart);
        }
        let serialization_result = pending_io_completions
            .iter()
            .map(format_io_completion_line)
            .collect::<DandelionResult<String>>();
        if let Some(recorder) = recorder.as_mut() {
            recorder.record(dandelion_commons::records::RecordPoint::IoPayloadEncodeEnd);
        }
        let journal_contents = serialization_result?;
        if let Some(recorder) = recorder.as_mut() {
            recorder.record(dandelion_commons::records::RecordPoint::IoJournalStart);
        }
        let write_result = write_atomic_file(&journal_path, journal_contents.as_bytes());
        if let Some(recorder) = recorder.as_mut() {
            recorder.record(dandelion_commons::records::RecordPoint::IoJournalEnd);
        }
        write_result?;
        inner.pending_io_completions = pending_io_completions;
        drop(inner);
        self.pending_io_completions_changed.notify_one();
        Ok(true)
    }

    #[cfg(feature = "at-least-once")]
    pub fn pending_io_completion_records(&self) -> Vec<IoCompletionRecord> {
        self.inner.lock().unwrap().pending_io_completions.clone()
    }

    #[cfg(feature = "at-least-once")]
    pub async fn wait_for_pending_io_completions(&self) {
        self.pending_io_completions_changed.notified().await;
    }

    /// Removes a completion from the durable delivery journal after the owner has acknowledged
    /// persisting it. Durable output data remains available for subsequent `RemoteData` reads.
    #[cfg(feature = "at-least-once")]
    pub fn acknowledge_io_completion(
        &self,
        completion_key: &IoCompletionKey,
    ) -> DandelionResult<bool> {
        let mut inner = self.inner.lock().unwrap();
        let mut record_index = None;
        for (index, record) in inner.pending_io_completions.iter().enumerate() {
            if record.completion_key()? == *completion_key {
                record_index = Some(index);
                break;
            }
        }
        let Some(record_index) = record_index else {
            return Ok(false);
        };
        let store = inner.durable_store.as_ref().ok_or_else(|| {
            export_registry_error(
                "Cannot acknowledge an IO completion in an in-memory-only export registry",
            )
        })?;
        let journal_path = store.directory.join(IO_COMPLETION_JOURNAL_FILE);
        let mut pending_io_completions = inner.pending_io_completions.clone();
        pending_io_completions.remove(record_index);
        let journal_contents = pending_io_completions
            .iter()
            .map(format_io_completion_line)
            .collect::<DandelionResult<String>>()?;
        write_atomic_file(&journal_path, journal_contents.as_bytes())?;
        inner.pending_io_completions = pending_io_completions;
        Ok(true)
    }

    #[cfg(feature = "at-least-once")]
    pub fn apply_io_completion_ack(
        &self,
        completion_key: &IoCompletionKey,
        disposition: IoCompletionDisposition,
    ) -> DandelionResult<bool> {
        if disposition == IoCompletionDisposition::Delete {
            let record = {
                let inner = self.inner.lock().unwrap();
                inner
                    .pending_io_completions
                    .iter()
                    .find(|record| {
                        record
                            .completion_key()
                            .is_ok_and(|key| key == *completion_key)
                    })
                    .cloned()
            };
            if let Some(record) = record {
                for output in record.outputs {
                    for item in output.items {
                        if let IoCompletionData::Remote { data_id, .. } = item.location {
                            self.delete_durable_exported_data(data_id)?;
                        }
                    }
                }
            }
        }
        self.acknowledge_io_completion(completion_key)
    }

    fn get_exported_data(&self, data_id: u64) -> DandelionResult<ExportedData> {
        debug!(
            "Fetching exported data: node_id={}, data_id={}",
            self.node_id, data_id
        );

        let inner = self.inner.lock().unwrap();
        let exported = inner.transient_data.get(&data_id);
        #[cfg(feature = "at-least-once")]
        let exported = exported.or_else(|| inner.durable_data.get(&data_id));
        match exported {
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
            let exported = inner.transient_data.get(&data_id);
            #[cfg(feature = "at-least-once")]
            let exported = exported.or_else(|| inner.durable_data.get(&data_id));
            match exported {
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
        #[cfg(feature = "at-least-once")]
        if is_durable_data_id(data_id) {
            return self.delete_durable_exported_data(data_id);
        }

        let mut inner = self.inner.lock().unwrap();
        let Some(_) = inner.transient_data.remove(&data_id) else {
            return err_dandelion!(DandelionError::Multinode(MultinodeError::RequestFailed(
                format!("Unknown remote data id {}", data_id),
            )));
        };
        Ok(())
    }

    /// Explicitly releases a durable export. Missing exports are treated as already deleted so
    /// delayed or duplicate reference-drop messages remain idempotent.
    #[cfg(feature = "at-least-once")]
    pub fn delete_durable_exported_data(&self, data_id: u64) -> DandelionResult<()> {
        debug!(
            "Deleting durable exported data: node_id={}, data_id={}",
            self.node_id, data_id
        );
        let mut inner = self.inner.lock().unwrap();
        if !inner.durable_data.contains_key(&data_id) {
            debug!(
                "Durable exported data was already deleted: node_id={}, data_id={}",
                self.node_id, data_id
            );
            return Ok(());
        }
        let store = inner.durable_store.as_ref().ok_or_else(|| {
            export_registry_error("Durable export registry storage is unavailable".to_string())
        })?;
        let data_path = durable_data_path(&store.directory, data_id);
        fs::remove_file(&data_path).map_err(|err| {
            export_registry_error(format!(
                "Failed to delete durable export {}: {}",
                data_path.display(),
                err
            ))
        })?;
        sync_directory(&store.directory)?;
        inner.durable_data.remove(&data_id);
        Ok(())
    }

    /// Drops transient exported data. Coordinated outputs are durable and survive reconnects.
    /// NOTE: We currently assume a centralized scheduler that owns the data. If this assumption
    ///       changes we need to update this function to consider to which master owns the data.
    pub fn clear_exported_data(&self) {
        let mut inner = self.inner.lock().unwrap();
        let before = inner.transient_data.len();
        inner.transient_data.clear();
        let cleared = before - inner.transient_data.len();
        if cleared > 0 {
            debug!("Cleared {} transient exported data contexts", cleared);
        }
    }

    pub fn fetch_context(&self, data_id: u64) -> DandelionResult<(Arc<Context>, Position)> {
        let exported_data = self.get_exported_data(data_id)?;
        Ok((exported_data.context, exported_data.position))
    }

    #[cfg(feature = "exactly-once")]
    fn inline_local_io_input(&self, data: IoRemoteRef) -> DandelionResult<IoResolveWireInput> {
        if data.node_id != self.node_id {
            return Ok(IoResolveWireInput::Remote(data));
        }
        let exported = self.get_exported_data(data.data_id)?;
        if exported.position.size != data.size {
            return Err(export_registry_error(format!(
                "I/O resolve input size mismatch for data {}: reference {}, export {}",
                data.data_id, data.size, exported.position.size
            )));
        }
        let mut bytes = vec![0; exported.position.size];
        exported
            .context
            .read(exported.position.offset, &mut bytes)?;
        Ok(IoResolveWireInput::Inline(bytes))
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

    #[cfg(feature = "exactly-once")]
    fn io_url(&self, node_id: u64, operation: &str) -> DandelionResult<String> {
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
        Ok(format!(
            "{}/io/{}",
            base_url.trim_end_matches('/'),
            operation
        ))
    }
}

impl RemoteDataClient for HttpRemoteDataClient {
    fn local_node_id(&self) -> u64 {
        self.local_registry.node_id
    }

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
            let durable = is_durable_data_id(data.data_id);
            let (size, body) = 'retry: loop {
                let mut response = match self.client.get(&url).send().await {
                    Ok(response) => response,
                    Err(err) if durable => {
                        debug!(
                            "Waiting for node {} to recover durable data {}: {}",
                            data.node_id, data.data_id, err
                        );
                        tokio::time::sleep(DURABLE_FETCH_RETRY_INTERVAL).await;
                        continue;
                    }
                    Err(err) => {
                        return err_dandelion!(DandelionError::Multinode(
                            MultinodeError::ConnectionFailed(err.to_string())
                        ));
                    }
                };
                if !response.status().is_success() {
                    return err_dandelion!(DandelionError::Multinode(
                        MultinodeError::RequestFailed(response.status().to_string())
                    ));
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
                        Err(err) if durable => {
                            debug!(
                                "Durable data fetch from node {} was interrupted, retrying: {}",
                                data.node_id, err
                            );
                            tokio::time::sleep(DURABLE_FETCH_RETRY_INTERVAL).await;
                            continue 'retry;
                        }
                        Err(_) => return err_dandelion!(DandelionError::SystemFuncResponseError),
                    }
                }
                break (size, body);
            };
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
            let durable = remote_items
                .iter()
                .all(|item| is_durable_data_id(item.data_id));
            let (size, body) = 'retry: loop {
                let mut response = match self.client.get(&url).body(body_data.clone()).send().await
                {
                    Ok(response) => response,
                    Err(err) if durable => {
                        debug!(
                            "Waiting for node {} to recover durable data: {}",
                            node_id, err
                        );
                        tokio::time::sleep(DURABLE_FETCH_RETRY_INTERVAL).await;
                        continue;
                    }
                    Err(err) => {
                        return err_dandelion!(DandelionError::Multinode(
                            MultinodeError::ConnectionFailed(err.to_string())
                        ));
                    }
                };
                if !response.status().is_success() {
                    return err_dandelion!(DandelionError::Multinode(
                        MultinodeError::RequestFailed(response.status().to_string())
                    ));
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
                        Err(err) if durable => {
                            debug!(
                                "Durable data fetch from node {} was interrupted, retrying: {}",
                                node_id, err
                            );
                            tokio::time::sleep(DURABLE_FETCH_RETRY_INTERVAL).await;
                            continue 'retry;
                        }
                        Err(_) => return err_dandelion!(DandelionError::SystemFuncResponseError),
                    }
                }
                break (size, body);
            };
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

            let durable = is_durable_data_id(data.data_id);
            loop {
                let response = match self.client.delete(&url).send().await {
                    Ok(response) => response,
                    Err(err) if durable => {
                        debug!(
                            "Waiting for node {} to recover before deleting durable data {}: {}",
                            data.node_id, data.data_id, err
                        );
                        tokio::time::sleep(DURABLE_FETCH_RETRY_INTERVAL).await;
                        continue;
                    }
                    Err(err) => {
                        return err_dandelion!(DandelionError::Multinode(
                            MultinodeError::ConnectionFailed(err.to_string())
                        ));
                    }
                };
                if !response.status().is_success() {
                    return err_dandelion!(DandelionError::Multinode(
                        MultinodeError::RequestFailed(response.status().to_string())
                    ));
                }
                break;
            }
            Ok(())
        })
    }

    #[cfg(feature = "at-least-once")]
    fn publish_io_completion(
        &self,
        completion: IoCoordinationCompletion,
    ) -> Pin<Box<dyn Future<Output = DandelionResult<()>> + Send + '_>> {
        Box::pin(async move {
            let mut recorder = completion.recorder;
            let IoCompletionOutcome::Completed(outputs) = completion.outcome else {
                return Ok(());
            };
            let mut exported = Vec::with_capacity(outputs.len());
            if let Some(recorder) = recorder.as_mut() {
                recorder.record(dandelion_commons::records::RecordPoint::IoOutputExportStart);
            }
            let mut export_error = None;
            for output in outputs {
                match self
                    .local_registry
                    .insert_durable_function(&output.item, output.context)
                {
                    Ok(remote) => {
                        exported.push(IoRemoteRef::from_remote(&remote, output.item.data.size))
                    }
                    Err(error) => {
                        for output in &exported {
                            let _ = self
                                .local_registry
                                .delete_durable_exported_data(output.data_id);
                        }
                        export_error = Some(error);
                        break;
                    }
                }
            }
            if let Some(recorder) = recorder.as_mut() {
                recorder.record(dandelion_commons::records::RecordPoint::IoOutputExportEnd);
            }
            if let Some(error) = export_error {
                return Err(error);
            }

            let record = completion_record(&completion.key, &exported);

            // skip journal entry for local owner
            #[cfg(feature = "checkpointed-at-least-once")]
            if completion.owner_node_id == self.local_registry.node_id {
                if let Some(recorder) = recorder.as_mut() {
                    recorder.record(dandelion_commons::records::RecordPoint::IoOwnerApprovalStart);
                }
                let approval =
                    accept_local_io_completion_record(record.clone(), recorder.clone()).await;
                if let Some(recorder) = recorder.as_mut() {
                    recorder.record(dandelion_commons::records::RecordPoint::IoOwnerApprovalEnd);
                }
                let disposition = approval?;
                if disposition == IoCompletionDisposition::Delete {
                    for output in exported {
                        self.local_registry
                            .delete_durable_exported_data(output.data_id)?;
                    }
                }
                return Ok(());
            }

            if !self
                .local_registry
                .append_io_completion_record(&record, recorder.clone())?
            {
                for output in exported {
                    self.local_registry
                        .delete_durable_exported_data(output.data_id)?;
                }
                return Ok(());
            }

            // A standalone owner has no queue connection to deliver through, so accept locally.
            if completion.owner_node_id == self.local_registry.node_id {
                let disposition = accept_delivered_io_completion_record(&record)?;
                self.local_registry
                    .acknowledge_io_completion(&record.completion_key()?)?;
                if disposition == IoCompletionDisposition::Delete {
                    for output in exported {
                        self.local_registry
                            .delete_durable_exported_data(output.data_id)?;
                    }
                }
            }
            Ok(())
        })
    }

    #[cfg(feature = "exactly-once")]
    fn resolve_io(
        &self,
        request: IoResolveRequest,
    ) -> Pin<Box<dyn Future<Output = DandelionResult<IoResolveOutcome>> + Send + '_>> {
        Box::pin(async move {
            let original_data = request
                .original_data
                .as_ref()
                .map(|(data, size)| IoRemoteRef::from_remote(data, *size));
            let response = loop {
                let response = if request.owner_node_id == self.local_registry.node_id {
                    let resolution = self.local_registry.begin_io_resolution_from_node(
                        request.key.clone(),
                        request.set_index,
                        self.local_registry.node_id,
                        original_data.clone(),
                    )?;
                    registry_resolution_response(
                        &self.local_registry,
                        request.key.clone(),
                        resolution,
                        request.set_index,
                    )
                    .await?
                } else {
                    let url = self.io_url(request.owner_node_id, "resolve")?;
                    let response = self
                        .client
                        .post(url)
                        .json(&IoResolveWireRequest {
                            key: request.key.clone(),
                            set_index: request.set_index,
                            requester_node_id: self.local_registry.node_id,
                            original_data: original_data.clone(),
                        })
                        .send()
                        .await
                        .map_err(|error| {
                            dandelion_err!(DandelionError::Multinode(
                                MultinodeError::ConnectionFailed(error.to_string())
                            ))
                        })?;
                    if !response.status().is_success() {
                        return err_dandelion!(DandelionError::Multinode(
                            MultinodeError::RequestFailed(response.status().to_string())
                        ));
                    }
                    let bytes = response.bytes().await.map_err(|error| {
                        dandelion_err!(DandelionError::Multinode(MultinodeError::RequestFailed(
                            error.to_string()
                        )))
                    })?;
                    decode_io_resolve_response(&bytes).map_err(|error| {
                        dandelion_err!(DandelionError::Multinode(MultinodeError::RequestFailed(
                            error
                        )))
                    })?
                };
                if response == IoResolveWireResponse::Retry {
                    continue;
                }
                break response;
            };

            Ok(match response {
                IoResolveWireResponse::Execute { input } => IoResolveOutcome::Execute {
                    input: input.map(|input| match input {
                        IoResolveWireInput::Inline(bytes) => {
                            let size = bytes.len();
                            let context =
                                Arc::new(ReadOnlyContext::new(bytes.into_boxed_slice()).expect(
                                    "inline I/O resolve bytes should form a read-only context",
                                ));
                            IoResolveInput::Inline {
                                context,
                                position: Position { offset: 0, size },
                            }
                        }
                        IoResolveWireInput::Remote(data) => {
                            let (data, size) = data.into_remote();
                            IoResolveInput::Remote { data, size }
                        }
                    }),
                },
                IoResolveWireResponse::Completed { data } => {
                    let (data, _) = data.into_remote();
                    IoResolveOutcome::Completed { data }
                }
                IoResolveWireResponse::Failed(error) => IoResolveOutcome::Failed(error),
                IoResolveWireResponse::Retry => unreachable!("retry responses are handled above"),
            })
        })
    }

    #[cfg(feature = "exactly-once")]
    fn publish_io_resolution(
        &self,
        completion: IoCoordinationCompletion,
    ) -> Pin<Box<dyn Future<Output = DandelionResult<()>> + Send + '_>> {
        Box::pin(async move {
            let mut exported_data_ids = Vec::new();
            let mut completion_error = None;
            let mut recorder = completion.recorder;
            let mut wire_outcome = match completion.outcome {
                IoCompletionOutcome::Failed(error) => Err(error),
                IoCompletionOutcome::Completed(outputs) => {
                    if let Some(recorder) = recorder.as_mut() {
                        recorder
                            .record(dandelion_commons::records::RecordPoint::IoOutputExportStart);
                    }
                    let mut remote_outputs = Vec::with_capacity(outputs.len());
                    let mut export_error = None;
                    for output in outputs {
                        let size = output.item.data.size;
                        let remote = match self
                            .local_registry
                            .insert_durable_function(&output.item, output.context)
                        {
                            Ok(remote) => remote,
                            Err(error) => {
                                for data_id in &exported_data_ids {
                                    let _ =
                                        self.local_registry.delete_durable_exported_data(*data_id);
                                }
                                export_error = Some(error.to_string());
                                completion_error = Some(error);
                                break;
                            }
                        };
                        exported_data_ids.push(remote.data_id);
                        remote_outputs.push(IoRemoteRef::from_remote(&remote, size));
                    }
                    if let Some(recorder) = recorder.as_mut() {
                        recorder.record(dandelion_commons::records::RecordPoint::IoOutputExportEnd);
                    }
                    match export_error {
                        Some(error) => Err(error),
                        None => Ok(remote_outputs),
                    }
                }
            };

            if completion.owner_node_id == self.local_registry.node_id {
                // skip journal entry for local owner
                if let Ok(outputs) = &wire_outcome {
                    let record = completion_record(&completion.key, outputs);
                    if let Some(recorder) = recorder.as_mut() {
                        recorder
                            .record(dandelion_commons::records::RecordPoint::IoOwnerApprovalStart);
                    }
                    let approval =
                        accept_local_io_completion_record(record, recorder.clone()).await;
                    if let Some(recorder) = recorder.as_mut() {
                        recorder
                            .record(dandelion_commons::records::RecordPoint::IoOwnerApprovalEnd);
                    }
                    if let Err(error) = approval {
                        for data_id in &exported_data_ids {
                            let _ = self.local_registry.delete_durable_exported_data(*data_id);
                        }
                        wire_outcome = Err(error.to_string());
                        completion_error = Some(error);
                    }
                }
                self.local_registry
                    .publish_io_resolution_inner(completion.key, wire_outcome)?;
                return match completion_error {
                    Some(error) => Err(error),
                    None => Ok(()),
                };
            }

            // A remote worker journals every successful result until its owner acknowledges it,
            // so delivery can resume after a worker or connection restart.
            let delivery_record = match &wire_outcome {
                Ok(outputs) => {
                    let record = completion_record(&completion.key, outputs);
                    match self
                        .local_registry
                        .append_io_completion_record(&record, recorder.clone())
                    {
                        Ok(_) => Some(record),
                        Err(error) => {
                            for data_id in &exported_data_ids {
                                let _ = self.local_registry.delete_durable_exported_data(*data_id);
                            }
                            wire_outcome = Err(error.to_string());
                            completion_error = Some(error);
                            None
                        }
                    }
                }
                Err(_) => None,
            };

            let url = self.io_url(completion.owner_node_id, "resolved")?;
            let request = IoResolvedWireRequest {
                key: completion.key,
                outputs: wire_outcome,
            };
            loop {
                match self.client.post(&url).json(&request).send().await {
                    Ok(response) if response.status().is_success() => break,
                    Ok(response) => warn!(
                        "Owner rejected coordinated I/O completion with {}, retrying",
                        response.status()
                    ),
                    Err(error) => debug!(
                        "Coordinated I/O completion delivery failed, retrying: {}",
                        error
                    ),
                }
                tokio::time::sleep(DURABLE_FETCH_RETRY_INTERVAL).await;
            }
            if let Some(record) = delivery_record {
                self.local_registry
                    .acknowledge_io_completion(&record.completion_key()?)?;
            }
            match completion_error {
                Some(error) => Err(error),
                None => Ok(()),
            }
        })
    }

    #[cfg(feature = "exactly-once")]
    fn clear_io_coordination(
        &self,
        invocation_id: dandelion_commons::InvocationId,
    ) -> Pin<Box<dyn Future<Output = DandelionResult<()>> + Send + '_>> {
        Box::pin(async move {
            let outputs = self.local_registry.io_coordination_outputs(invocation_id);
            for output in outputs {
                self.delete_remote_data(output).await?;
            }
            self.local_registry.remove_io_coordination(invocation_id);
            Ok(())
        })
    }
}

#[cfg(feature = "exactly-once")]
async fn read_request_body(mut req: Request<Incoming>) -> Result<Vec<u8>, String> {
    let mut body = Pin::new(req.body_mut());
    let mut bytes = Vec::new();
    while let Some(frame_result) = futures::future::poll_fn(|cx| body.as_mut().poll_frame(cx)).await
    {
        let mut frame = frame_result.map_err(|error| error.to_string())?;
        if let Some(data) = frame.data_mut() {
            while data.has_remaining() {
                let chunk = data.chunk();
                bytes.extend_from_slice(chunk);
                let length = chunk.len();
                data.advance(length);
            }
        }
    }
    Ok(bytes)
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

    #[cfg(feature = "exactly-once")]
    if req.uri().path() == "/io/resolve" && req.method() == Method::POST {
        let body = match read_request_body(req).await {
            Ok(body) => body,
            Err(error) => return convert_error_string(error),
        };
        let request: IoResolveWireRequest = match serde_json::from_slice(&body) {
            Ok(request) => request,
            Err(error) => return convert_error_string(format!("Invalid I/O resolve: {}", error)),
        };
        let key = request.key.clone();
        let resolution = match export_registry.begin_io_resolution_from_node(
            request.key,
            request.set_index,
            request.requester_node_id,
            request.original_data,
        ) {
            Ok(resolution) => resolution,
            Err(error) => return convert_error_string(error.to_string()),
        };
        // A waiter may keep this response open for the duration of the winning I/O, and the winner
        // may receive its original request bytes inline. Neither should retain a bounded data
        // service permit while waiting or copying from the export registry.
        drop(permit);
        let response = match registry_resolution_response(
            &export_registry,
            key,
            resolution,
            request.set_index,
        )
        .await
        {
            Ok(response) => response,
            Err(error) => return convert_error_string(error.to_string()),
        };
        return Ok(Response::new(ExportedBody::from_bytes(
            encode_io_resolve_response(&response),
        )));
    }

    #[cfg(feature = "exactly-once")]
    if req.uri().path() == "/io/resolved" && req.method() == Method::POST {
        let body = match read_request_body(req).await {
            Ok(body) => body,
            Err(error) => return convert_error_string(error),
        };
        let request: IoResolvedWireRequest = match serde_json::from_slice(&body) {
            Ok(request) => request,
            Err(error) => {
                return convert_error_string(format!("Invalid I/O resolved message: {}", error))
            }
        };
        if let Ok(outputs) = &request.outputs {
            if let Err(error) =
                append_delivered_io_completion_record(&completion_record(&request.key, outputs))
            {
                return convert_error_string(format!(
                    "Failed to persist I/O completion: {}",
                    error
                ));
            }
        }
        return match export_registry.publish_io_resolution_inner(request.key, request.outputs) {
            Ok(()) => Ok(Response::new(ExportedBody::from_bytes(Vec::new()))),
            Err(error) => convert_error_string(error.to_string()),
        };
    }

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

#[cfg(all(test, feature = "at-least-once"))]
mod checkpoint_tests {
    use super::*;
    use dandelion_commons::InvocationId;

    fn pending_completion(registry: &ExportRegistry) -> (IoCompletionKey, u64) {
        let bytes = b"checkpoint".to_vec();
        let item = DataItem {
            ident: "item".to_string(),
            key: 7,
            data: Position {
                offset: 0,
                size: bytes.len(),
            },
        };
        let remote = registry
            .insert_durable_function(
                &item,
                Arc::new(ReadOnlyContext::new(bytes.into_boxed_slice()).unwrap()),
            )
            .unwrap();
        let record = IoCompletionRecord {
            invocation_id: InvocationId::now_v7(),
            composition_set_id: 3,
            function: machine_interface::function_driver::functions::SystemFunction::HTTP,
            outputs: vec![IoCompletionOutputSet {
                set_index: 0,
                set_name: "headers".to_string(),
                items: vec![IoCompletionItem {
                    identifier: item.ident,
                    key: item.key.into(),
                    location: IoCompletionData::Remote {
                        node_id: remote.node_id,
                        data_id: remote.data_id,
                        size: item.data.size,
                    },
                }],
            }],
        };
        let key = record.completion_key().unwrap();
        registry.append_io_completion_record(&record, None).unwrap();
        (key, remote.data_id)
    }

    fn registry_for_test(name: &str) -> (PathBuf, ExportRegistry) {
        let root =
            std::env::temp_dir().join(format!("dandelion-{name}-{}", InvocationId::now_v7()));
        let registry = ExportRegistry::with_durable_storage(7, &root).unwrap();
        (root, registry)
    }

    #[test]
    fn delete_ack_removes_losing_output_and_journal_entry() {
        let (root, registry) = registry_for_test("alo-delete-ack");
        let (key, data_id) = pending_completion(&registry);
        assert!(registry
            .apply_io_completion_ack(&key, IoCompletionDisposition::Delete)
            .unwrap());
        assert!(registry.pending_io_completion_records().is_empty());
        assert!(registry.fetch_context(data_id).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn retain_ack_keeps_winner_output() {
        let (root, registry) = registry_for_test("alo-retain-ack");
        let (key, data_id) = pending_completion(&registry);
        assert!(registry
            .apply_io_completion_ack(&key, IoCompletionDisposition::Retain)
            .unwrap());
        assert!(registry.pending_io_completion_records().is_empty());
        assert!(registry.fetch_context(data_id).is_ok());
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[cfg(all(test, feature = "exactly-once"))]
mod tests {
    use super::*;
    #[cfg(feature = "timestamp")]
    use dandelion_commons::records::{RecordPoint, Recorder};
    use dandelion_commons::InvocationId;
    use machine_interface::composition::IoCompletedOutput;
    use machine_interface::memory_domain::{read_only::ReadOnlyContext, ContextTrait};
    use std::{path::PathBuf, sync::OnceLock, time::Duration};

    fn configure_test_recovery_log() {
        static TEST_RECOVERY_ROOT: OnceLock<PathBuf> = OnceLock::new();
        let root = TEST_RECOVERY_ROOT.get_or_init(|| {
            std::env::temp_dir().join(format!(
                "dandelion-multinode-recovery-tests-{}",
                std::process::id()
            ))
        });
        machine_interface::function_driver::system_driver::recovery_log::set_recovery_log_root(
            root.clone(),
        )
        .unwrap();
    }

    async fn spawn_test_data_server(
        export_registry: ExportRegistry,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let semaphore = Arc::new(Semaphore::new(16));
        let handle = tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let io = hyper_util::rt::TokioIo::new(stream);
                let service_registry = export_registry.clone();
                let service_semaphore = semaphore.clone();
                tokio::spawn(async move {
                    hyper_util::server::conn::auto::Builder::new(
                        hyper_util::rt::TokioExecutor::new(),
                    )
                    .serve_connection_with_upgrades(
                        io,
                        service_fn(move |request| {
                            service(request, service_registry.clone(), service_semaphore.clone())
                        }),
                    )
                    .await
                    .unwrap();
                });
            }
        });
        (format!("http://{}", address), handle)
    }

    fn context_bytes(context: &Context, position: Position) -> Vec<u8> {
        let mut bytes = vec![0; position.size];
        context.read(position.offset, &mut bytes).unwrap();
        bytes
    }

    fn coordination_key() -> IoCoordinationKey {
        IoCoordinationKey {
            invocation_id: InvocationId::now_v7(),
            composition_set_id: 17,
            function: machine_interface::function_driver::functions::SystemFunction::HTTP,
            identifier: "request".to_string(),
            item_key: 3,
        }
    }

    #[tokio::test]
    async fn http_io_resolution_waits_publishes_and_fetches_winner_output() {
        configure_test_recovery_log();
        let owner_node_id = 1;
        let winner_node_id = 2;
        let duplicate_node_id = 3;
        let test_root = std::env::temp_dir().join(format!(
            "dandelion-coordinated-http-test-{}",
            InvocationId::now_v7()
        ));
        let owner_registry = ExportRegistry::new(owner_node_id);
        let winner_registry =
            ExportRegistry::with_durable_storage(winner_node_id, &test_root).unwrap();
        let duplicate_registry = ExportRegistry::new(duplicate_node_id);

        let original_bytes = b"owner-local HTTP request".to_vec();
        let original_item = DataItem {
            ident: "request".to_string(),
            key: 3,
            data: Position {
                offset: 0,
                size: original_bytes.len(),
            },
        };
        let original_context =
            Arc::new(ReadOnlyContext::new(original_bytes.clone().into_boxed_slice()).unwrap());
        let original_data = owner_registry.insert_function(&original_item, original_context, None);

        let (owner_url, owner_server) = spawn_test_data_server(owner_registry.clone()).await;
        let (winner_url, winner_server) = spawn_test_data_server(winner_registry.clone()).await;
        let node_map = BTreeMap::from([(owner_node_id, owner_url), (winner_node_id, winner_url)]);
        let winner_client = Arc::new(HttpRemoteDataClient::new(node_map.clone(), winner_registry));
        let duplicate_client = Arc::new(HttpRemoteDataClient::new(node_map, duplicate_registry));
        let key = coordination_key();
        let resolve_request = IoResolveRequest {
            owner_node_id,
            key: key.clone(),
            set_index: 0,
            original_data: Some((original_data, original_bytes.len())),
        };

        let winner_resolution = winner_client
            .resolve_io(resolve_request.clone())
            .await
            .unwrap();
        let (input_context, input_position) = match winner_resolution {
            IoResolveOutcome::Execute {
                input: Some(IoResolveInput::Inline { context, position }),
            } => (context, position),
            _ => panic!("first worker should execute with owner-local input bytes"),
        };
        assert_eq!(
            original_bytes,
            context_bytes(input_context.as_ref(), input_position)
        );

        let duplicate_client_clone = duplicate_client.clone();
        let duplicate_resolution =
            tokio::spawn(async move { duplicate_client_clone.resolve_io(resolve_request).await });
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let waiter_registered = {
                    let inner = owner_registry.inner.lock().unwrap();
                    matches!(
                        inner.io_resolutions.get(&key),
                        Some(IoResolutionState::Running { result, .. })
                            if result.receiver_count() > 0
                    )
                };
                if waiter_registered {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("duplicate HTTP resolve should register as a waiter");
        assert!(
            !duplicate_resolution.is_finished(),
            "duplicate resolve should remain blocked until the winner publishes"
        );

        let output_bytes = b"HTTP operation result from first worker".to_vec();
        let output_item = DataItem {
            ident: "response".to_string(),
            key: 3,
            data: Position {
                offset: 0,
                size: output_bytes.len(),
            },
        };
        let output_context =
            Arc::new(ReadOnlyContext::new(output_bytes.clone().into_boxed_slice()).unwrap());
        winner_client
            .publish_io_resolution(IoCoordinationCompletion {
                owner_node_id,
                key,
                outcome: IoCompletionOutcome::Completed(vec![IoCompletedOutput {
                    item: output_item,
                    context: output_context,
                }]),
                recorder: None,
            })
            .await
            .unwrap();

        let resolved_data = match tokio::time::timeout(Duration::from_secs(2), duplicate_resolution)
            .await
            .expect("duplicate resolve should wake after publication")
            .unwrap()
            .unwrap()
        {
            IoResolveOutcome::Completed { data } => data,
            _ => panic!("duplicate worker should receive the completed remote reference"),
        };
        assert_eq!(winner_node_id, resolved_data.node_id);
        let (resolved_context, resolved_position) = duplicate_client
            .resolve_remote_data(resolved_data)
            .await
            .unwrap();
        assert_eq!(
            output_bytes,
            context_bytes(resolved_context.as_ref(), resolved_position)
        );

        owner_server.abort();
        winner_server.abort();
        std::fs::remove_dir_all(test_root).unwrap();
    }

    #[tokio::test]
    async fn first_io_resolve_wins_and_waiter_receives_requested_output() {
        let registry = ExportRegistry::new(1);
        let key = coordination_key();
        let original = IoRemoteRef {
            node_id: 9,
            data_id: 4,
            size: 21,
        };
        assert!(matches!(
            registry
                .begin_io_resolution(key.clone(), 0, Some(original.clone()))
                .unwrap(),
            RegistryIoResolution::Execute(Some(data)) if data == original
        ));
        let mut waiter = match registry.begin_io_resolution(key.clone(), 1, None).unwrap() {
            RegistryIoResolution::Wait(waiter) => waiter,
            _ => panic!("second resolve should wait"),
        };

        registry
            .publish_io_resolution_inner(
                key,
                Ok(vec![
                    IoRemoteRef {
                        node_id: 8,
                        data_id: 11,
                        size: 4,
                    },
                    IoRemoteRef {
                        node_id: 8,
                        data_id: 12,
                        size: 9,
                    },
                ]),
            )
            .unwrap();
        waiter.changed().await.unwrap();
        let SharedIoResult::Completed(outputs) = waiter.borrow().clone().unwrap() else {
            panic!("waiter should receive completed outputs");
        };
        assert_eq!(12, outputs[1].data_id);
        assert_eq!(9, outputs[1].size);
    }

    #[test]
    fn io_resolve_wire_response_round_trips() {
        let remote = IoRemoteRef {
            node_id: 7,
            data_id: 11,
            size: 29,
        };
        let responses = [
            IoResolveWireResponse::Execute { input: None },
            IoResolveWireResponse::Execute {
                input: Some(IoResolveWireInput::Inline(vec![0, 1, 2, 255])),
            },
            IoResolveWireResponse::Execute {
                input: Some(IoResolveWireInput::Remote(remote.clone())),
            },
            IoResolveWireResponse::Completed {
                data: remote.clone(),
            },
            IoResolveWireResponse::Failed("request failed".to_string()),
            IoResolveWireResponse::Retry,
        ];

        for response in responses {
            let encoded = encode_io_resolve_response(&response);
            assert_eq!(response, decode_io_resolve_response(&encoded).unwrap());
        }
    }

    #[tokio::test]
    async fn first_io_resolve_inlines_owner_local_input() {
        let registry = ExportRegistry::new(3);
        let expected = b"owner-local request bytes".to_vec();
        let item = DataItem {
            ident: "request".to_string(),
            key: 1,
            data: Position {
                offset: 0,
                size: expected.len(),
            },
        };
        let context = Arc::new(ReadOnlyContext::new(expected.clone().into_boxed_slice()).unwrap());
        let remote = registry.insert_function(&item, context, None);
        let key = coordination_key();
        let resolution = registry
            .begin_io_resolution(
                key.clone(),
                0,
                Some(IoRemoteRef::from_remote(&remote, expected.len())),
            )
            .unwrap();

        let response = registry_resolution_response(&registry, key, resolution, 0)
            .await
            .unwrap();
        assert_eq!(
            IoResolveWireResponse::Execute {
                input: Some(IoResolveWireInput::Inline(expected)),
            },
            response
        );
    }

    #[tokio::test]
    async fn inline_input_failure_wakes_waiters_and_becomes_terminal() {
        let registry = ExportRegistry::new(3);
        let key = coordination_key();
        let missing = IoRemoteRef {
            node_id: 3,
            data_id: 999,
            size: 10,
        };
        let winner = registry
            .begin_io_resolution(key.clone(), 0, Some(missing))
            .unwrap();
        let mut waiter = match registry.begin_io_resolution(key.clone(), 1, None).unwrap() {
            RegistryIoResolution::Wait(waiter) => waiter,
            _ => panic!("duplicate resolve should wait"),
        };

        let response = registry_resolution_response(&registry, key.clone(), winner, 0)
            .await
            .unwrap();
        assert!(matches!(response, IoResolveWireResponse::Failed(_)));

        waiter.changed().await.unwrap();
        let waiter_outcome = waiter.borrow().clone();
        assert!(matches!(waiter_outcome, Some(SharedIoResult::Failed(_))));
        assert!(matches!(
            registry.begin_io_resolution(key, 0, None).unwrap(),
            RegistryIoResolution::Failed(_)
        ));
    }

    #[tokio::test]
    async fn first_io_resolve_preserves_third_node_input_reference() {
        let registry = ExportRegistry::new(3);
        let remote = IoRemoteRef {
            node_id: 9,
            data_id: 4,
            size: 21,
        };
        let key = coordination_key();
        let resolution = registry
            .begin_io_resolution(key.clone(), 0, Some(remote.clone()))
            .unwrap();

        let response = registry_resolution_response(&registry, key, resolution, 0)
            .await
            .unwrap();
        assert_eq!(
            IoResolveWireResponse::Execute {
                input: Some(IoResolveWireInput::Remote(remote)),
            },
            response
        );
    }

    #[tokio::test]
    async fn io_failure_is_terminal_for_waiters_and_later_resolves() {
        let registry = ExportRegistry::new(1);
        let key = coordination_key();
        assert!(matches!(
            registry.begin_io_resolution(key.clone(), 0, None).unwrap(),
            RegistryIoResolution::Execute(None)
        ));
        let mut waiter = match registry.begin_io_resolution(key.clone(), 1, None).unwrap() {
            RegistryIoResolution::Wait(waiter) => waiter,
            _ => panic!("second resolve should wait"),
        };

        registry
            .publish_io_resolution_inner(key.clone(), Err("same failure".to_string()))
            .unwrap();
        waiter.changed().await.unwrap();
        assert_eq!(
            SharedIoResult::Failed("same failure".to_string()),
            waiter.borrow().clone().unwrap()
        );
        assert!(matches!(
            registry.begin_io_resolution(key, 0, None).unwrap(),
            RegistryIoResolution::Failed(error) if error == "same failure"
        ));
    }

    #[test]
    fn completion_before_resolve_is_reused() {
        let registry = ExportRegistry::new(1);
        let key = coordination_key();
        registry
            .publish_io_resolution_inner(
                key.clone(),
                Ok(vec![IoRemoteRef {
                    node_id: 2,
                    data_id: 5,
                    size: 7,
                }]),
            )
            .unwrap();
        assert!(matches!(
            registry.begin_io_resolution(key, 0, None).unwrap(),
            RegistryIoResolution::Completed(data)
                if data.node_id == 2 && data.data_id == 5 && data.size == 7
        ));
    }

    #[test]
    fn recovered_multi_output_resolve_returns_requested_output() {
        let registry = ExportRegistry::new(1);
        let key = coordination_key();
        let recovered_record = IoCompletionRecord {
            invocation_id: key.invocation_id,
            composition_set_id: key.composition_set_id,
            function: key.function,
            outputs: vec![
                IoCompletionOutputSet {
                    set_index: 0,
                    set_name: "headers".to_string(),
                    items: vec![IoCompletionItem {
                        identifier: key.identifier.clone(),
                        key: key.item_key,
                        location: IoCompletionData::Remote {
                            node_id: 8,
                            data_id: 11,
                            size: 4,
                        },
                    }],
                },
                IoCompletionOutputSet {
                    set_index: 1,
                    set_name: "bodies".to_string(),
                    items: vec![IoCompletionItem {
                        identifier: key.identifier.clone(),
                        key: key.item_key,
                        location: IoCompletionData::Remote {
                            node_id: 8,
                            data_id: 12,
                            size: 9,
                        },
                    }],
                },
            ],
        };
        machine_interface::function_driver::system_driver::recovery_log::install_recovered_io_records(
            key.invocation_id,
            vec![recovered_record],
        )
        .unwrap();

        let resolution = registry.begin_io_resolution(key.clone(), 1, None).unwrap();
        machine_interface::function_driver::system_driver::recovery_log::clear_recovered_io(
            key.invocation_id,
        );

        assert!(matches!(
            resolution,
            RegistryIoResolution::Completed(data)
                if data.node_id == 8 && data.data_id == 12 && data.size == 9
        ));
    }

    #[test]
    fn disconnected_waiter_does_not_block_completion() {
        let registry = ExportRegistry::new(1);
        let key = coordination_key();
        assert!(matches!(
            registry.begin_io_resolution(key.clone(), 0, None).unwrap(),
            RegistryIoResolution::Execute(None)
        ));
        let waiter = registry.begin_io_resolution(key.clone(), 1, None).unwrap();
        assert!(matches!(waiter, RegistryIoResolution::Wait(_)));
        drop(waiter);

        registry
            .publish_io_resolution_inner(
                key.clone(),
                Ok(vec![
                    IoRemoteRef {
                        node_id: 4,
                        data_id: 1,
                        size: 2,
                    },
                    IoRemoteRef {
                        node_id: 4,
                        data_id: 2,
                        size: 3,
                    },
                ]),
            )
            .unwrap();
        assert!(matches!(
            registry.begin_io_resolution(key, 1, None).unwrap(),
            RegistryIoResolution::Completed(data) if data.data_id == 2
        ));
    }

    #[tokio::test]
    async fn disconnected_running_worker_releases_one_new_winner() {
        let registry = ExportRegistry::new(1);
        let key = coordination_key();
        assert!(matches!(
            registry
                .begin_io_resolution_from_node(key.clone(), 0, 2, None)
                .unwrap(),
            RegistryIoResolution::Execute(None)
        ));
        let mut old_waiter = match registry
            .begin_io_resolution_from_node(key.clone(), 0, 3, None)
            .unwrap()
        {
            RegistryIoResolution::Wait(waiter) => waiter,
            _ => panic!("second worker should wait"),
        };

        registry.invalidate_running_io_for_node(2);
        old_waiter.changed().await.unwrap();
        assert_eq!(Some(SharedIoResult::Retry), old_waiter.borrow().clone());

        assert!(matches!(
            registry
                .begin_io_resolution_from_node(key.clone(), 0, 3, None)
                .unwrap(),
            RegistryIoResolution::Execute(None)
        ));
        assert!(matches!(
            registry
                .begin_io_resolution_from_node(key, 0, 4, None)
                .unwrap(),
            RegistryIoResolution::Wait(_)
        ));
    }

    #[tokio::test]
    async fn coordinated_output_survives_registry_restart() {
        configure_test_recovery_log();
        let test_root = std::env::temp_dir().join(format!(
            "dandelion-coordinated-io-restart-test-{}",
            InvocationId::now_v7()
        ));
        let node_id = 7;
        let registry = ExportRegistry::with_durable_storage(node_id, &test_root).unwrap();
        let client = HttpRemoteDataClient::new(BTreeMap::new(), registry.clone());
        let key = coordination_key();
        machine_interface::function_driver::system_driver::recovery_log::append_invocation_log_line(
            key.invocation_id,
            &format!(
                "event=invocation_submitted invocation_id={} request_len=0 request_b64= is_cold=false\n",
                key.invocation_id
            ),
        )
        .unwrap();
        assert!(matches!(
            registry.begin_io_resolution(key.clone(), 0, None).unwrap(),
            RegistryIoResolution::Execute(None)
        ));

        let expected = b"durable synchronous IO output".to_vec();
        let output = IoCompletedOutput {
            item: DataItem {
                ident: key.identifier.clone(),
                key: u32::try_from(key.item_key).unwrap(),
                data: Position {
                    offset: 0,
                    size: expected.len(),
                },
            },
            context: Arc::new(ReadOnlyContext::new(expected.clone().into_boxed_slice()).unwrap()),
        };
        #[cfg(feature = "timestamp")]
        let recorder = Recorder::new(
            key.invocation_id,
            Arc::new("IO:HTTP:0:0000000000000000".to_string()),
            std::time::Instant::now(),
        );
        client
            .publish_io_resolution(IoCoordinationCompletion {
                owner_node_id: node_id,
                key: key.clone(),
                outcome: IoCompletionOutcome::Completed(vec![output]),
                #[cfg(feature = "timestamp")]
                recorder: Some(recorder.clone()),
                #[cfg(not(feature = "timestamp"))]
                recorder: None,
            })
            .await
            .unwrap();

        #[cfg(feature = "timestamp")]
        {
            assert_ne!(
                recorder.get_timestamp(RecordPoint::IoOutputExportEnd),
                Duration::ZERO
            );
            assert_ne!(
                recorder.get_timestamp(RecordPoint::IoPayloadEncodeEnd),
                Duration::ZERO
            );
            assert_ne!(
                recorder.get_timestamp(RecordPoint::IoJournalEnd),
                Duration::ZERO
            );
            assert_ne!(
                recorder.get_timestamp(RecordPoint::IoOwnerApprovalEnd),
                Duration::ZERO
            );
        }

        assert!(registry.pending_io_completion_records().is_empty());
        assert!(!test_root
            .join(node_id.to_string())
            .join(IO_COMPLETION_JOURNAL_FILE)
            .exists());
        let invocation_log =
            machine_interface::function_driver::system_driver::recovery_log::read_invocation_log(
                key.invocation_id,
            )
            .unwrap();
        assert!(invocation_log.contains("event=io_function_completed "));

        let remote = match registry.begin_io_resolution(key, 0, None).unwrap() {
            RegistryIoResolution::Completed(remote) => remote,
            _ => panic!("owner should retain the completed durable reference"),
        };
        assert!(is_durable_data_id(remote.data_id));
        drop(client);
        drop(registry);

        let restored = ExportRegistry::with_durable_storage(node_id, &test_root).unwrap();
        let (context, position) = restored.fetch_context(remote.data_id).unwrap();
        assert_eq!(expected, context_bytes(context.as_ref(), position));
        std::fs::remove_dir_all(test_root).unwrap();
    }

    #[tokio::test]
    async fn clearing_coordination_deletes_durable_winner_outputs() {
        configure_test_recovery_log();
        let test_root = std::env::temp_dir().join(format!(
            "dandelion-coordinated-cleanup-test-{}",
            InvocationId::now_v7()
        ));
        let node_id = 8;
        let registry = ExportRegistry::with_durable_storage(node_id, &test_root).unwrap();
        let client = HttpRemoteDataClient::new(BTreeMap::new(), registry.clone());
        let key = coordination_key();
        registry.begin_io_resolution(key.clone(), 0, None).unwrap();

        let bytes = b"temporary coordinated output".to_vec();
        client
            .publish_io_resolution(IoCoordinationCompletion {
                owner_node_id: node_id,
                key: key.clone(),
                outcome: IoCompletionOutcome::Completed(vec![IoCompletedOutput {
                    item: DataItem {
                        ident: key.identifier.clone(),
                        key: u32::try_from(key.item_key).unwrap(),
                        data: Position {
                            offset: 0,
                            size: bytes.len(),
                        },
                    },
                    context: Arc::new(ReadOnlyContext::new(bytes.into_boxed_slice()).unwrap()),
                }]),
                recorder: None,
            })
            .await
            .unwrap();
        let remote = match registry.begin_io_resolution(key.clone(), 0, None).unwrap() {
            RegistryIoResolution::Completed(remote) => remote,
            _ => panic!("completion should be stored"),
        };
        let path = durable_data_path(&test_root.join(node_id.to_string()), remote.data_id);
        assert!(path.exists());

        client
            .clear_io_coordination(key.invocation_id)
            .await
            .unwrap();
        assert!(!path.exists());
        assert!(!registry
            .inner
            .lock()
            .unwrap()
            .io_resolutions
            .contains_key(&key));
        std::fs::remove_dir_all(test_root).unwrap();
    }

    #[test]
    fn durable_export_survives_registry_restart() {
        let test_root = std::env::temp_dir().join(format!(
            "dandelion-durable-export-test-{}",
            InvocationId::now_v7()
        ));
        let node_id = 7;
        let expected = b"durable remote result".to_vec();
        let item = DataItem {
            ident: "result".to_string(),
            key: 0,
            data: Position {
                offset: 0,
                size: expected.len(),
            },
        };

        let registry = ExportRegistry::with_durable_storage(node_id, &test_root).unwrap();
        let context = Arc::new(ReadOnlyContext::new(expected.clone().into_boxed_slice()).unwrap());
        let remote = registry.insert_durable_function(&item, context).unwrap();
        assert!(is_durable_data_id(remote.data_id));
        drop(registry);

        let restored = ExportRegistry::with_durable_storage(node_id, &test_root).unwrap();
        let (context, position) = restored.fetch_context(remote.data_id).unwrap();
        let mut actual = vec![0; position.size];
        context.read(position.offset, &mut actual).unwrap();
        assert_eq!(expected, actual);

        std::fs::remove_dir_all(&test_root).unwrap();
    }

    #[test]
    fn ordinary_delete_releases_durable_export_idempotently() {
        let test_root = std::env::temp_dir().join(format!(
            "dandelion-durable-delete-test-{}",
            InvocationId::now_v7()
        ));
        let node_id = 8;
        let expected = b"delete durable remote result".to_vec();
        let item = DataItem {
            ident: "result".to_string(),
            key: 0,
            data: Position {
                offset: 0,
                size: expected.len(),
            },
        };

        let registry = ExportRegistry::with_durable_storage(node_id, &test_root).unwrap();
        let context = Arc::new(ReadOnlyContext::new(expected.into_boxed_slice()).unwrap());
        let remote = registry.insert_durable_function(&item, context).unwrap();
        let data_path = durable_data_path(&test_root.join(node_id.to_string()), remote.data_id);
        assert!(data_path.exists());

        registry.delete_exported_data(remote.data_id).unwrap();
        assert!(!data_path.exists());
        registry.delete_exported_data(remote.data_id).unwrap();

        std::fs::remove_dir_all(&test_root).unwrap();
    }
}

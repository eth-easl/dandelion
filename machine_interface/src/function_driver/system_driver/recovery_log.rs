#[cfg(feature = "at-least-once")]
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
#[cfg(feature = "at-least-once")]
use dandelion_commons::err_dandelion;
use dandelion_commons::{
    dandelion_err, DandelionError, DandelionResult, FrontendError, InvocationId,
};
use log::warn;
#[cfg(feature = "at-least-once")]
use std::collections::HashSet;
use std::{
    collections::HashMap,
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
};
#[cfg(any(feature = "checkpointed-at-least-once", feature = "exactly-once"))]
use std::{
    sync::mpsc::{self, Receiver, Sender},
    thread,
};
#[cfg(any(feature = "checkpointed-at-least-once", feature = "exactly-once"))]
use tokio::sync::oneshot;

#[cfg(feature = "at-least-once")]
use crate::{
    composition::RemoteData, function_driver::functions::SystemFunction,
    memory_domain::read_only::ReadOnlyContext,
};

const IO_LOG_DIR_NAME: &str = "io_logs";
static RECOVERY_LOG_ROOT: OnceLock<PathBuf> = OnceLock::new();
static INVOCATION_LOG_LOCKS: OnceLock<Mutex<HashMap<InvocationId, Arc<Mutex<()>>>>> =
    OnceLock::new();
#[cfg(any(feature = "checkpointed-at-least-once", feature = "exactly-once"))]
static LOCAL_COMPLETION_COMMITTER: OnceLock<Sender<LocalCompletionCommitRequest>> = OnceLock::new();
#[cfg(feature = "at-least-once")]
static ACTIVE_ASYNC_INVOCATIONS: OnceLock<Mutex<HashSet<InvocationId>>> = OnceLock::new();
#[cfg(feature = "at-least-once")]
static RECOVERED_IO_OUTPUTS: OnceLock<
    Mutex<HashMap<RecoveredIoKey, HashMap<RecoveredIoItemKey, HashMap<usize, RecoveredIoOutput>>>>,
> = OnceLock::new();

#[cfg(feature = "at-least-once")]
/// In-memory representation of one durable `io_function_completed` recovery event.
#[derive(Debug, Clone)]
pub struct IoCompletionRecord {
    pub invocation_id: InvocationId,
    /// One used composition output-set id uniquely identifies the IO function
    /// within an invocation.
    pub composition_set_id: usize,
    pub function: SystemFunction,
    pub outputs: Vec<IoCompletionOutputSet>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoCompletionDisposition {
    Retain,
    Delete,
}

#[cfg(any(feature = "checkpointed-at-least-once", feature = "exactly-once"))]
struct LocalCompletionCommitRequest {
    record: IoCompletionRecord,
    recorder: Option<dandelion_commons::records::Recorder>,
    completion: oneshot::Sender<DandelionResult<IoCompletionDisposition>>,
}

#[cfg(feature = "at-least-once")]
/// Stable identity of one logical IO input item across execution, delivery, and recovery.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IoCompletionKey {
    pub invocation_id: InvocationId,
    pub composition_set_id: usize,
    pub function: SystemFunction,
    pub identifier: String,
    pub item_key: u64,
}

#[cfg(feature = "at-least-once")]
impl IoCompletionRecord {
    /// Derives the logical input item represented by this completion.
    pub fn completion_key(&self) -> DandelionResult<IoCompletionKey> {
        let mut items = self.outputs.iter().flat_map(|output| output.items.iter());
        let first = items.next().ok_or_else(|| {
            internal_error("IO completion record does not contain an output item")
        })?;
        if items.any(|item| item.identifier != first.identifier || item.key != first.key) {
            return Err(internal_error(
                "IO completion record contains outputs for multiple logical items",
            ));
        }
        Ok(IoCompletionKey {
            invocation_id: self.invocation_id,
            composition_set_id: self.composition_set_id,
            function: self.function,
            identifier: first.identifier.clone(),
            item_key: first.key,
        })
    }
}

#[cfg(feature = "at-least-once")]
/// One output set emitted by a completed IO function.
#[derive(Debug, Clone)]
pub struct IoCompletionOutputSet {
    pub set_index: usize,
    pub set_name: String,
    pub items: Vec<IoCompletionItem>,
}

#[cfg(feature = "at-least-once")]
/// One output item emitted by a completed IO function.
#[derive(Debug, Clone)]
pub struct IoCompletionItem {
    pub identifier: String,
    pub key: u64,
    pub location: IoCompletionData,
}

#[cfg(feature = "at-least-once")]
#[derive(Debug, Clone)]
pub enum IoCompletionData {
    Inline(Vec<u8>),
    Remote {
        node_id: u64,
        data_id: u64,
        size: usize,
    },
}

#[cfg(feature = "at-least-once")]
#[derive(Debug, Clone)]
pub enum RecoveredIoOutput {
    Inline(Arc<crate::memory_domain::Context>),
    Remote { data: RemoteData, size: usize },
}

#[cfg(feature = "at-least-once")]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RecoveredIoKey {
    invocation_id: InvocationId,
    composition_set_id: usize,
}

#[cfg(feature = "at-least-once")]
/// Metadata used to find one completed item inside an invocation/composition-set entry.
/// It deliberately is not part of the recovery entry's identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RecoveredIoItemKey {
    function: SystemFunction,
    identifier: String,
    key: u64,
}

fn internal_error(message: impl Into<String>) -> dandelion_commons::DError {
    dandelion_err!(DandelionError::RequestError(FrontendError::InternalError(
        message.into(),
    )))
}

fn io_log_dir(root: &Path) -> PathBuf {
    root.join(IO_LOG_DIR_NAME)
}

#[cfg(feature = "at-least-once")]
fn parse_log_fields(line: &str) -> HashMap<&str, &str> {
    line.split_whitespace()
        .filter_map(|part| part.split_once('='))
        .collect()
}

pub fn set_recovery_log_root(root: PathBuf) -> DandelionResult<()> {
    let log_dir = io_log_dir(&root);
    fs::create_dir_all(&log_dir).map_err(|_| {
        internal_error(format!(
            "Failed to create IO recovery log directory {}",
            log_dir.display()
        ))
    })?;
    match RECOVERY_LOG_ROOT.set(root.clone()) {
        Ok(()) => Ok(()),
        Err(existing_root) => {
            if existing_root == root {
                Ok(())
            } else {
                Err(internal_error(format!(
                    "IO recovery log root already initialized as {}",
                    existing_root.display()
                )))
            }
        }
    }
}

pub fn recovery_log_root() -> DandelionResult<&'static Path> {
    RECOVERY_LOG_ROOT
        .get()
        .map(|path| path.as_path())
        .ok_or(internal_error(
            "IO recovery log root was not configured before use",
        ))
}

// list all invocation ids in the recovery log directory
pub fn list_invocation_log_ids() -> DandelionResult<Vec<InvocationId>> {
    let mut invocation_ids = Vec::new();
    for entry in fs::read_dir(io_log_dir(recovery_log_root()?))
        .map_err(|_| internal_error("Failed to read invocation log directory".to_string()))?
    {
        let entry =
            entry.map_err(|_| internal_error("Failed to iterate invocation logs".to_string()))?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("log") {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
            continue;
        };
        match InvocationId::parse_str(stem) {
            Ok(invocation_id) => invocation_ids.push(invocation_id),
            Err(_) => warn!("Ignoring invocation log with invalid file name {}", stem),
        }
    }
    invocation_ids.sort_unstable();
    Ok(invocation_ids)
}

pub fn invocation_log_path(invocation_id: InvocationId) -> DandelionResult<PathBuf> {
    Ok(io_log_dir(recovery_log_root()?).join(format!("{invocation_id}.log")))
}

fn invocation_log_lock(invocation_id: InvocationId) -> Arc<Mutex<()>> {
    let lock_map = INVOCATION_LOG_LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut lock_map_guard = lock_map
        .lock()
        .expect("IO recovery invocation lock map poisoned");
    lock_map_guard
        .entry(invocation_id)
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

#[cfg(feature = "at-least-once")]
fn active_async_invocations() -> &'static Mutex<HashSet<InvocationId>> {
    ACTIVE_ASYNC_INVOCATIONS.get_or_init(|| Mutex::new(HashSet::new()))
}

#[cfg(feature = "at-least-once")]
pub fn activate_async_invocation_logging(invocation_id: InvocationId) {
    active_async_invocations()
        .lock()
        .expect("Async invocation logging set poisoned")
        .insert(invocation_id);
}

#[cfg(feature = "at-least-once")]
pub fn deactivate_async_invocation_logging(invocation_id: InvocationId) {
    active_async_invocations()
        .lock()
        .expect("Async invocation logging set poisoned")
        .remove(&invocation_id);
}

pub fn append_invocation_log_line(invocation_id: InvocationId, line: &str) -> DandelionResult<()> {
    let log_path = invocation_log_path(invocation_id)?;
    let invocation_lock = invocation_log_lock(invocation_id);
    let _invocation_lock_guard = invocation_lock
        .lock()
        .expect("IO recovery invocation log lock poisoned");
    append_invocation_log_line_locked(&log_path, line)
}

/// Returns only fully written, newline-terminated recovery-log records.
pub fn complete_log_lines(content: &str) -> impl Iterator<Item = &str> {
    content
        .split_inclusive('\n')
        .filter_map(|line| line.strip_suffix('\n'))
}

fn append_invocation_log_line_locked(log_path: &Path, line: &str) -> DandelionResult<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .map_err(|_| {
            internal_error(format!(
                "Failed to open invocation log {}",
                log_path.display()
            ))
        })?;
    let existing = fs::read(log_path).map_err(|_| {
        internal_error(format!(
            "Failed to inspect invocation log {}",
            log_path.display()
        ))
    })?;
    if !existing.is_empty() && existing.last() != Some(&b'\n') {
        let complete_length = existing
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map_or(0, |position| position + 1);
        file.set_len(complete_length as u64).map_err(|_| {
            internal_error(format!(
                "Failed to discard incomplete invocation log record in {}",
                log_path.display()
            ))
        })?;
    }
    file.write_all(line.as_bytes()).map_err(|_| {
        internal_error(format!(
            "Failed to append invocation log {}",
            log_path.display()
        ))
    })?;
    file.sync_data().map_err(|_| {
        internal_error(format!(
            "Failed to sync invocation log {}",
            log_path.display()
        ))
    })?;
    Ok(())
}

pub fn read_invocation_log(invocation_id: InvocationId) -> DandelionResult<String> {
    let log_path = invocation_log_path(invocation_id)?;
    fs::read_to_string(&log_path).map_err(|_| {
        dandelion_err!(DandelionError::RequestError(FrontendError::InvalidRequest(
            format!("Unknown async invocation {}", invocation_id.simple())
        )))
    })
}

#[cfg(feature = "at-least-once")]
pub fn parse_io_completion_line(line: &str) -> DandelionResult<Option<IoCompletionRecord>> {
    let fields = parse_log_fields(line);
    if fields.get("event").copied() != Some("io_function_completed") {
        return Ok(None);
    }
    let invocation_id = fields
        .get("invocation_id")
        .copied()
        .ok_or(internal_error(
            "Missing invocation_id field in IO completion record",
        ))
        .and_then(|raw| {
            InvocationId::parse_str(raw)
                .map_err(|_| internal_error("Invalid invocation_id in IO completion record"))
        })?;
    let composition_set_id = fields
        .get("composition_set_id")
        .copied()
        .ok_or(internal_error(
            "Missing composition_set_id field in IO completion record",
        ))?
        .parse::<usize>()
        .map_err(|_| internal_error("Invalid composition_set_id in IO completion record"))?;
    let function = SystemFunction::from(fields.get("function").copied().ok_or(internal_error(
        "Missing function field in IO completion record",
    ))?);
    let outputs = decode_io_completion_payload(fields.get("payload_b64").copied().ok_or(
        internal_error("Missing payload_b64 field in IO completion record"),
    )?)?;
    Ok(Some(IoCompletionRecord {
        invocation_id,
        composition_set_id,
        function,
        outputs,
    }))
}

#[cfg(feature = "at-least-once")]
pub fn load_io_completion_records(
    invocation_id: InvocationId,
) -> DandelionResult<Vec<IoCompletionRecord>> {
    let content = read_invocation_log(invocation_id)?;
    let mut records = Vec::new();
    for line in complete_log_lines(&content) {
        if let Some(record) = parse_io_completion_line(line)? {
            records.push(record);
        }
    }
    Ok(records)
}

#[cfg(feature = "at-least-once")]
/// Lists the unique worker-owned durable exports referenced by one invocation's IO completion
/// records. Inline/local completion data is deliberately excluded.
pub fn remote_io_exports(invocation_id: InvocationId) -> DandelionResult<Vec<RemoteData>> {
    Ok(remote_io_exports_from_records(load_io_completion_records(
        invocation_id,
    )?))
}

#[cfg(feature = "at-least-once")]
fn remote_io_exports_from_records(records: Vec<IoCompletionRecord>) -> Vec<RemoteData> {
    let mut seen = HashSet::new();
    let mut exports = Vec::new();

    for record in records {
        for output in record.outputs {
            for item in output.items {
                if let IoCompletionData::Remote {
                    node_id, data_id, ..
                } = item.location
                {
                    if seen.insert((node_id, data_id)) {
                        exports.push(RemoteData::new(node_id, data_id));
                    }
                }
            }
        }
    }

    exports
}

#[cfg(feature = "at-least-once")]
/// Returns whether terminal cleanup has already released this invocation's durable IO exports.
pub fn recovery_exports_released(invocation_id: InvocationId) -> DandelionResult<bool> {
    let content = read_invocation_log(invocation_id)?;
    Ok(recovery_exports_released_in(&content))
}

#[cfg(feature = "at-least-once")]
fn recovery_exports_released_in(content: &str) -> bool {
    complete_log_lines(content).any(|line| {
        parse_log_fields(line).get("event").copied() == Some("recovery_exports_released")
    })
}

#[cfg(feature = "at-least-once")]
/// Durably records successful terminal cleanup. Repeating cleanup before this marker is written is
/// safe because durable export deletion is idempotent.
pub fn mark_recovery_exports_released(invocation_id: InvocationId) -> DandelionResult<()> {
    append_invocation_log_line(
        invocation_id,
        &format!(
            "event=recovery_exports_released invocation_id={}\n",
            invocation_id
        ),
    )
}

#[cfg(all(test, feature = "at-least-once"))]
mod cleanup_tests {
    use super::*;

    fn completion(invocation_id: InvocationId, data_id: u64) -> IoCompletionRecord {
        IoCompletionRecord {
            invocation_id,
            composition_set_id: 3,
            function: SystemFunction::HTTP,
            outputs: vec![IoCompletionOutputSet {
                set_index: 0,
                set_name: "headers".to_string(),
                items: vec![IoCompletionItem {
                    identifier: "item".to_string(),
                    key: 7,
                    location: IoCompletionData::Remote {
                        node_id: 2,
                        data_id,
                        size: 4,
                    },
                }],
            }],
        }
    }

    #[test]
    fn remote_io_exports_are_unique_and_exclude_inline_data() {
        let invocation_id = InvocationId::from_u128(1);
        let remote_item = IoCompletionItem {
            identifier: "item".to_string(),
            key: 7,
            location: IoCompletionData::Remote {
                node_id: 2,
                data_id: 1 << 63,
                size: 4,
            },
        };
        let records = vec![IoCompletionRecord {
            invocation_id,
            composition_set_id: 3,
            function: SystemFunction::HTTP,
            outputs: vec![
                IoCompletionOutputSet {
                    set_index: 0,
                    set_name: "headers".to_string(),
                    items: vec![remote_item.clone()],
                },
                IoCompletionOutputSet {
                    set_index: 1,
                    set_name: "bodies".to_string(),
                    items: vec![
                        remote_item,
                        IoCompletionItem {
                            identifier: "inline".to_string(),
                            key: 8,
                            location: IoCompletionData::Inline(vec![1, 2, 3]),
                        },
                    ],
                },
            ],
        }];

        let exports = remote_io_exports_from_records(records);
        assert_eq!(exports.len(), 1);
        assert_eq!(exports[0].node_id, 2);
        assert_eq!(exports[0].data_id, 1 << 63);
    }

    #[test]
    fn release_marker_is_detected_without_affecting_other_events() {
        assert!(!recovery_exports_released_in(
            "event=invocation_completed invocation_id=abc\n"
        ));
        assert!(recovery_exports_released_in(
            "event=invocation_completed invocation_id=abc\n\
             event=recovery_exports_released invocation_id=abc\n"
        ));
    }

    #[test]
    fn append_discards_an_incomplete_tail() {
        let log_path = std::env::temp_dir().join(format!(
            "dandelion-recovery-tail-test-{}.log",
            InvocationId::now_v7()
        ));
        fs::write(&log_path, b"event=first\nevent=incomplete").unwrap();

        append_invocation_log_line_locked(&log_path, "event=second\n").unwrap();

        assert_eq!(
            fs::read_to_string(&log_path).unwrap(),
            "event=first\nevent=second\n"
        );
        fs::remove_file(log_path).unwrap();
    }

    #[test]
    fn first_completion_wins_and_redelivery_is_retained() {
        let invocation_id = InvocationId::from_u128(11);
        let winner = completion(invocation_id, 100);
        let submitted = format!(
            "event=invocation_submitted invocation_id={} request_len=0 request_b64= is_cold=false\n",
            invocation_id
        );
        assert_eq!(
            delivered_io_completion_disposition(&submitted, &winner).unwrap(),
            None
        );

        let log = format!(
            "{}{}",
            submitted,
            format_io_completion_line(&winner).unwrap()
        );
        assert_eq!(
            delivered_io_completion_disposition(&log, &winner).unwrap(),
            Some(IoCompletionDisposition::Retain)
        );
        assert_eq!(
            delivered_io_completion_disposition(&log, &completion(invocation_id, 101)).unwrap(),
            Some(IoCompletionDisposition::Delete)
        );
    }

    #[test]
    fn unknown_and_terminal_completions_are_deleted() {
        let invocation_id = InvocationId::from_u128(12);
        let record = completion(invocation_id, 100);
        assert_eq!(
            delivered_io_completion_disposition("", &record).unwrap(),
            Some(IoCompletionDisposition::Delete)
        );
        let terminal = format!(
            "event=invocation_submitted invocation_id={} request_len=0 request_b64= is_cold=false\n\
             event=invocation_completed invocation_id={} result_len=0 result_b64=\n",
            invocation_id, invocation_id
        );
        assert_eq!(
            delivered_io_completion_disposition(&terminal, &record).unwrap(),
            Some(IoCompletionDisposition::Delete)
        );

        let terminal_after_winner = format!(
            "event=invocation_submitted invocation_id={} request_len=0 request_b64= is_cold=false\n\
             {}event=invocation_completed invocation_id={} result_len=0 result_b64=\n",
            invocation_id,
            format_io_completion_line(&record).unwrap(),
            invocation_id
        );
        assert_eq!(
            delivered_io_completion_disposition(&terminal_after_winner, &record).unwrap(),
            Some(IoCompletionDisposition::Delete)
        );
    }
}

#[cfg(feature = "at-least-once")]
fn recovered_io_outputs() -> &'static Mutex<
    HashMap<RecoveredIoKey, HashMap<RecoveredIoItemKey, HashMap<usize, RecoveredIoOutput>>>,
> {
    RECOVERED_IO_OUTPUTS.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(feature = "at-least-once")]
pub fn install_recovered_io_records(
    invocation_id: InvocationId,
    records: Vec<IoCompletionRecord>,
) -> DandelionResult<()> {
    let mut recovered_outputs = recovered_io_outputs()
        .lock()
        .expect("Recovered IO output map poisoned");
    for record in records {
        let recovery_entry = recovered_outputs
            .entry(RecoveredIoKey {
                invocation_id,
                composition_set_id: record.composition_set_id,
            })
            .or_default();

        for output in &record.outputs {
            for item in &output.items {
                let recovered_output = match &item.location {
                    IoCompletionData::Inline(data) => RecoveredIoOutput::Inline(Arc::new(
                        ReadOnlyContext::new(data.clone().into_boxed_slice())?,
                    )),
                    IoCompletionData::Remote {
                        node_id,
                        data_id,
                        size,
                    } => RecoveredIoOutput::Remote {
                        data: RemoteData::new(*node_id, *data_id),
                        size: *size,
                    },
                };
                recovery_entry
                    .entry(RecoveredIoItemKey {
                        function: record.function,
                        identifier: item.identifier.clone(),
                        key: item.key,
                    })
                    .or_default()
                    .insert(output.set_index, recovered_output);
            }
        }
    }
    Ok(())
}

#[cfg(feature = "at-least-once")]
/// Returns all recovered output contexts for one system-function input item.
pub fn recovered_io_item_locations(
    invocation_id: InvocationId,
    function: SystemFunction,
    composition_set_id: usize,
    output_count: usize,
    identifier: &str,
    key: u64,
) -> Option<Vec<RecoveredIoOutput>> {
    let recovered_outputs = recovered_io_outputs()
        .lock()
        .expect("Recovered IO output map poisoned");

    let recovery_entry = recovered_outputs.get(&RecoveredIoKey {
        invocation_id,
        composition_set_id,
    })?;
    let item_outputs = recovery_entry.get(&RecoveredIoItemKey {
        function,
        identifier: identifier.to_string(),
        key,
    })?;

    (0..output_count)
        .map(|set_index| item_outputs.get(&set_index).cloned())
        .collect()
}

/// Returns one recovered output for the uncoordinated at-least-once path.
#[cfg(feature = "at-least-once")]
pub fn recovered_io_item_location(
    invocation_id: InvocationId,
    function: SystemFunction,
    composition_set_id: usize,
    set_index: usize,
    identifier: &str,
    key: u64,
) -> Option<RecoveredIoOutput> {
    let recovered_outputs = recovered_io_outputs()
        .lock()
        .expect("Recovered IO output map poisoned");
    recovered_outputs
        .get(&RecoveredIoKey {
            invocation_id,
            composition_set_id,
        })?
        .get(&RecoveredIoItemKey {
            function,
            identifier: identifier.to_string(),
            key,
        })?
        .get(&set_index)
        .cloned()
}

#[cfg(feature = "at-least-once")]
pub fn clear_recovered_io(invocation_id: InvocationId) {
    recovered_io_outputs()
        .lock()
        .expect("Recovered IO output map poisoned")
        .retain(|entry_key, _| entry_key.invocation_id != invocation_id);
}

#[cfg(feature = "at-least-once")]
fn push_u32(buffer: &mut Vec<u8>, value: usize) -> DandelionResult<()> {
    let value = u32::try_from(value).map_err(|_| {
        dandelion_err!(DandelionError::RequestError(FrontendError::InternalError(
            "IO completion payload length exceeds u32".to_string(),
        )))
    })?;
    buffer.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

#[cfg(feature = "at-least-once")]
fn push_string(buffer: &mut Vec<u8>, value: &str) -> DandelionResult<()> {
    push_u32(buffer, value.len())?;
    buffer.extend_from_slice(value.as_bytes());
    Ok(())
}

#[cfg(feature = "at-least-once")]
fn push_u64(buffer: &mut Vec<u8>, value: usize) -> DandelionResult<()> {
    let value = u64::try_from(value)
        .map_err(|_| internal_error("IO completion payload size exceeds u64".to_string()))?;
    buffer.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

#[cfg(feature = "at-least-once")]
fn read_u8(buffer: &[u8], offset: &mut usize) -> DandelionResult<u8> {
    let value = *buffer.get(*offset).ok_or_else(|| {
        internal_error("Unexpected end of IO completion payload while reading u8".to_string())
    })?;
    *offset += 1;
    Ok(value)
}

#[cfg(feature = "at-least-once")]
fn read_u32(buffer: &[u8], offset: &mut usize) -> DandelionResult<u32> {
    let end = *offset + std::mem::size_of::<u32>();
    let bytes = buffer
        .get(*offset..end)
        .ok_or(dandelion_err!(DandelionError::RequestError(
            FrontendError::InternalError(
                "Unexpected end of IO completion payload while reading u32".to_string(),
            )
        )))?;
    *offset = end;
    Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
}

#[cfg(feature = "at-least-once")]
fn read_u64(buffer: &[u8], offset: &mut usize) -> DandelionResult<u64> {
    let end = *offset + std::mem::size_of::<u64>();
    let bytes = buffer
        .get(*offset..end)
        .ok_or(dandelion_err!(DandelionError::RequestError(
            FrontendError::InternalError(
                "Unexpected end of IO completion payload while reading u64".to_string(),
            )
        )))?;
    *offset = end;
    Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
}

#[cfg(feature = "at-least-once")]
fn read_bytes(buffer: &[u8], offset: &mut usize, length: usize) -> DandelionResult<Vec<u8>> {
    let end = *offset + length;
    let bytes = buffer
        .get(*offset..end)
        .ok_or(dandelion_err!(DandelionError::RequestError(
            FrontendError::InternalError(
                "Unexpected end of IO completion payload while reading bytes".to_string(),
            )
        )))?;
    *offset = end;
    Ok(bytes.to_vec())
}

#[cfg(feature = "at-least-once")]
fn read_string(buffer: &[u8], offset: &mut usize) -> DandelionResult<String> {
    let length = read_u32(buffer, offset)? as usize;
    let bytes = read_bytes(buffer, offset, length)?;
    String::from_utf8(bytes).map_err(|_| {
        dandelion_err!(DandelionError::RequestError(FrontendError::InternalError(
            "Invalid UTF-8 in IO completion payload string".to_string(),
        )))
    })
}

#[cfg(feature = "at-least-once")]
pub fn encode_io_completion_payload(outputs: &[IoCompletionOutputSet]) -> DandelionResult<String> {
    let mut buffer = Vec::new();
    push_u32(&mut buffer, outputs.len())?;
    for output in outputs {
        push_u32(&mut buffer, output.set_index)?;
        push_string(&mut buffer, &output.set_name)?;
        push_u32(&mut buffer, output.items.len())?;
        for item in &output.items {
            push_string(&mut buffer, &item.identifier)?;
            buffer.extend_from_slice(&item.key.to_le_bytes());
            match &item.location {
                IoCompletionData::Inline(data) => {
                    buffer.push(0);
                    push_u32(&mut buffer, data.len())?;
                    buffer.extend_from_slice(data);
                }
                IoCompletionData::Remote {
                    node_id,
                    data_id,
                    size,
                } => {
                    buffer.push(1);
                    buffer.extend_from_slice(&node_id.to_le_bytes());
                    buffer.extend_from_slice(&data_id.to_le_bytes());
                    push_u64(&mut buffer, *size)?;
                }
            }
        }
    }
    Ok(BASE64_STANDARD.encode(buffer))
}

#[cfg(feature = "at-least-once")]
pub fn decode_io_completion_payload(
    payload_b64: &str,
) -> DandelionResult<Vec<IoCompletionOutputSet>> {
    let payload = BASE64_STANDARD.decode(payload_b64).map_err(|_| {
        dandelion_err!(DandelionError::RequestError(FrontendError::InternalError(
            "Invalid base64 IO completion payload".to_string(),
        )))
    })?;
    let mut offset = 0usize;
    let set_count = read_u32(&payload, &mut offset)? as usize;
    let mut outputs = Vec::with_capacity(set_count);
    for _ in 0..set_count {
        let set_index = read_u32(&payload, &mut offset)? as usize;
        let set_name = read_string(&payload, &mut offset)?;
        let item_count = read_u32(&payload, &mut offset)? as usize;
        let mut items = Vec::with_capacity(item_count);
        for _ in 0..item_count {
            let identifier = read_string(&payload, &mut offset)?;
            let key = read_u64(&payload, &mut offset)?;
            let location = match read_u8(&payload, &mut offset)? {
                0 => {
                    let data_length = read_u32(&payload, &mut offset)? as usize;
                    IoCompletionData::Inline(read_bytes(&payload, &mut offset, data_length)?)
                }
                1 => {
                    let node_id = read_u64(&payload, &mut offset)?;
                    let data_id = read_u64(&payload, &mut offset)?;
                    let size = usize::try_from(read_u64(&payload, &mut offset)?).map_err(|_| {
                        internal_error("Remote IO completion size exceeds usize".to_string())
                    })?;
                    IoCompletionData::Remote {
                        node_id,
                        data_id,
                        size,
                    }
                }
                tag => {
                    return Err(internal_error(format!(
                        "Unknown IO completion data location tag {tag}"
                    )))
                }
            };
            items.push(IoCompletionItem {
                identifier,
                key,
                location,
            });
        }
        outputs.push(IoCompletionOutputSet {
            set_index,
            set_name,
            items,
        });
    }
    if offset != payload.len() {
        return err_dandelion!(DandelionError::RequestError(FrontendError::InternalError(
            "Trailing bytes in IO completion payload".to_string(),
        )));
    }
    Ok(outputs)
}

#[cfg(feature = "at-least-once")]
pub fn format_io_completion_line(record: &IoCompletionRecord) -> DandelionResult<String> {
    let payload_b64 = encode_io_completion_payload(&record.outputs)?;
    Ok(format!(
        "event=io_function_completed invocation_id={} composition_set_id={} function={} payload_b64={}\n",
        record.invocation_id,
        record.composition_set_id,
        record.function,
        payload_b64
    ))
}

#[cfg(feature = "at-least-once")]
pub fn append_io_completion_record(record: &IoCompletionRecord) -> DandelionResult<()> {
    if !active_async_invocations()
        .lock()
        .expect("Async invocation logging set poisoned")
        .contains(&record.invocation_id)
    {
        return Ok(());
    }
    let line = format_io_completion_line(record)?;
    append_invocation_log_line(record.invocation_id, &line)
}

#[cfg(feature = "at-least-once")]
/// Durably accepts the first successful completion for a logical I/O key. Redelivery of that
/// exact winner is retained; a different duplicate or a completion for terminal work is deleted.
pub fn accept_delivered_io_completion_record(
    record: &IoCompletionRecord,
) -> DandelionResult<IoCompletionDisposition> {
    let log_path = invocation_log_path(record.invocation_id)?;
    let invocation_lock = invocation_log_lock(record.invocation_id);
    let _invocation_lock_guard = invocation_lock
        .lock()
        .expect("IO recovery invocation log lock poisoned");
    let existing_log = match fs::read_to_string(&log_path) {
        Ok(existing_log) => existing_log,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(_) => {
            return Err(internal_error(format!(
                "Failed to read invocation log {}",
                log_path.display()
            )))
        }
    };
    if let Some(disposition) = delivered_io_completion_disposition(&existing_log, record)? {
        return Ok(disposition);
    }
    let line = format_io_completion_line(record)?;
    append_invocation_log_line_locked(&log_path, &line)?;
    Ok(IoCompletionDisposition::Retain)
}

// create a new channel and spawn a thread that waits to receive local completion commit requests
#[cfg(any(feature = "checkpointed-at-least-once", feature = "exactly-once"))]
fn local_completion_committer() -> &'static Sender<LocalCompletionCommitRequest> {
    LOCAL_COMPLETION_COMMITTER.get_or_init(|| {
        let (sender, receiver) = mpsc::channel();
        thread::Builder::new()
            .name("dandelion-local-completion-committer".to_string())
            .spawn(move || run_local_completion_committer(receiver))
            .expect("Failed to start local completion commit thread");
        sender
    })
}

/// Durably accepts a completion produced on its coordination-owner node.
///
/// The first pending completion starts a commit immediately. Completions that
/// arrive while that commit is syncing accumulate in the channel and share the
/// next append and `sync_data`. Callers are released only after the batch that
/// contains their record is durable.
#[cfg(any(feature = "checkpointed-at-least-once", feature = "exactly-once"))]
pub async fn accept_local_io_completion_record(
    record: IoCompletionRecord,
    recorder: Option<dandelion_commons::records::Recorder>,
) -> DandelionResult<IoCompletionDisposition> {
    let (completion, committed) = oneshot::channel();
    local_completion_committer()
        .send(LocalCompletionCommitRequest {
            record,
            recorder,
            completion,
        })
        .map_err(|_| internal_error("Local completion commit thread stopped"))?;
    committed
        .await
        .map_err(|_| internal_error("Local completion commit thread dropped a request"))?
}

#[cfg(any(feature = "checkpointed-at-least-once", feature = "exactly-once"))]
fn run_local_completion_committer(receiver: Receiver<LocalCompletionCommitRequest>) {
    while let Ok(first) = receiver.recv() {
        let mut batch = vec![first];
        while let Ok(request) = receiver.try_recv() {
            batch.push(request);
        }
        commit_local_completion_batch(batch);
    }
}

// commits a batch of local completion records
#[cfg(any(feature = "checkpointed-at-least-once", feature = "exactly-once"))]
fn commit_local_completion_batch(batch: Vec<LocalCompletionCommitRequest>) {
    let mut by_invocation: HashMap<InvocationId, Vec<LocalCompletionCommitRequest>> =
        HashMap::new();
    // group requests by invocation id
    for request in batch {
        by_invocation
            .entry(request.record.invocation_id)
            .or_default()
            .push(request);
    }
    // commit each invocation's completion records
    for (invocation_id, requests) in by_invocation {
        commit_invocation_completion_batch(invocation_id, requests);
    }
}

#[cfg(any(feature = "checkpointed-at-least-once", feature = "exactly-once"))]
fn commit_invocation_completion_batch(
    invocation_id: InvocationId,
    requests: Vec<LocalCompletionCommitRequest>,
) {
    let commit = || -> DandelionResult<Vec<IoCompletionDisposition>> {
        let log_path = invocation_log_path(invocation_id)?;
        let invocation_lock = invocation_log_lock(invocation_id);
        let _invocation_lock_guard = invocation_lock
            .lock()
            .expect("IO recovery invocation log lock poisoned");
        let mut existing_log = match fs::read_to_string(&log_path) {
            Ok(existing_log) => existing_log,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(_) => {
                return Err(internal_error(format!(
                    "Failed to read invocation log {}",
                    log_path.display()
                )))
            }
        };

        let mut appended = String::new();
        let mut journal_recorders = Vec::new();
        let mut dispositions = Vec::with_capacity(requests.len());
        for request in &requests {
            // check if the completion record is already in the log
            match delivered_io_completion_disposition(&existing_log, &request.record)? {
                Some(disposition) => dispositions.push(disposition),
                None => {
                    let mut recorder = request.recorder.clone();
                    if let Some(recorder) = recorder.as_mut() {
                        recorder
                            .record(dandelion_commons::records::RecordPoint::IoPayloadEncodeStart);
                    }
                    let line = format_io_completion_line(&request.record)?;
                    if let Some(recorder) = recorder.as_mut() {
                        recorder
                            .record(dandelion_commons::records::RecordPoint::IoPayloadEncodeEnd);
                        journal_recorders.push(recorder.clone());
                    }
                    existing_log.push_str(&line);
                    appended.push_str(&line);
                    dispositions.push(IoCompletionDisposition::Retain);
                }
            }
        }

        if !appended.is_empty() {
            for recorder in &mut journal_recorders {
                recorder.record(dandelion_commons::records::RecordPoint::IoJournalStart);
            }
            append_invocation_log_line_locked(&log_path, &appended)?;
            for recorder in &mut journal_recorders {
                recorder.record(dandelion_commons::records::RecordPoint::IoJournalEnd);
            }
        }
        Ok(dispositions)
    }();

    match commit {
        Ok(dispositions) => {
            for (request, disposition) in requests.into_iter().zip(dispositions) {
                let _ = request.completion.send(Ok(disposition));
            }
        }
        Err(error) => {
            let message = error.to_string();
            for request in requests {
                let _ = request
                    .completion
                    .send(Err(internal_error(message.clone())));
            }
        }
    }
}

#[cfg(feature = "at-least-once")]
fn delivered_io_completion_disposition(
    existing_log: &str,
    record: &IoCompletionRecord,
) -> DandelionResult<Option<IoCompletionDisposition>> {
    let record_key = record.completion_key()?;
    let mut submitted = false;
    let mut terminal = false;
    let mut matching_completion = None;
    for line in complete_log_lines(&existing_log) {
        match parse_log_fields(line).get("event").copied() {
            Some("invocation_submitted") => {
                submitted = true;
                terminal = false;
            }
            Some("invocation_completed" | "invocation_failed") => terminal = true,
            _ => {}
        }
        if let Some(existing) = parse_io_completion_line(line)? {
            if existing.completion_key()? == record_key {
                let same_outputs = encode_io_completion_payload(&existing.outputs)?
                    == encode_io_completion_payload(&record.outputs)?;
                matching_completion = Some(if same_outputs {
                    IoCompletionDisposition::Retain
                } else {
                    IoCompletionDisposition::Delete
                });
            }
        }
    }
    if !submitted || terminal {
        return Ok(Some(IoCompletionDisposition::Delete));
    }
    Ok(matching_completion)
}

/// Compatibility wrapper for exactly-once call sites that already elected a single winner.
#[cfg(feature = "at-least-once")]
pub fn append_delivered_io_completion_record(record: &IoCompletionRecord) -> DandelionResult<()> {
    let _ = accept_delivered_io_completion_record(record)?;
    Ok(())
}

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use dandelion_commons::{
    dandelion_err, err_dandelion, DandelionError, DandelionResult, FrontendError, InvocationId,
};
use std::{
    collections::{HashMap, HashSet},
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
};

use crate::function_driver::functions::SystemFunction;

const IO_LOG_DIR_NAME: &str = "io_logs";
static RECOVERY_LOG_ROOT: OnceLock<PathBuf> = OnceLock::new();
static INVOCATION_LOG_LOCKS: OnceLock<Mutex<HashMap<InvocationId, Arc<Mutex<()>>>>> =
    OnceLock::new();
static ACTIVE_ASYNC_INVOCATIONS: OnceLock<Mutex<HashSet<InvocationId>>> = OnceLock::new();

/// In-memory representation of one durable `io_function_completed` recovery event.
#[derive(Debug, Clone)]
pub struct IoCompletionRecord {
    pub invocation_id: InvocationId,
    pub composition_node_id: String,
    pub function: SystemFunction,
    pub outputs: Vec<IoCompletionOutputSet>,
}

/// One output set emitted by a completed IO function.
#[derive(Debug, Clone)]
pub struct IoCompletionOutputSet {
    pub set_index: usize,
    pub set_name: String,
    pub items: Vec<IoCompletionItem>,
}

/// One output item emitted by a completed IO function.
#[derive(Debug, Clone)]
pub struct IoCompletionItem {
    pub identifier: String,
    pub key: u64,
    pub data: Vec<u8>,
}

fn internal_error(message: impl Into<String>) -> dandelion_commons::DError {
    dandelion_err!(DandelionError::RequestError(FrontendError::InternalError(
        message.into(),
    )))
}

fn io_log_dir(root: &Path) -> PathBuf {
    root.join(IO_LOG_DIR_NAME)
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

fn active_async_invocations() -> &'static Mutex<HashSet<InvocationId>> {
    ACTIVE_ASYNC_INVOCATIONS.get_or_init(|| Mutex::new(HashSet::new()))
}

pub fn activate_async_invocation_logging(invocation_id: InvocationId) {
    active_async_invocations()
        .lock()
        .expect("Async invocation logging set poisoned")
        .insert(invocation_id);
}

pub fn deactivate_async_invocation_logging(invocation_id: InvocationId) {
    active_async_invocations()
        .lock()
        .expect("Async invocation logging set poisoned")
        .remove(&invocation_id);
}

pub fn delete_invocation_log(invocation_id: InvocationId) -> DandelionResult<()> {
    let log_path = invocation_log_path(invocation_id)?;
    match fs::remove_file(&log_path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(internal_error(format!(
            "Failed to delete IO recovery log {}",
            log_path.display()
        ))),
    }
}

pub fn append_invocation_log_line(
    invocation_id: InvocationId,
    line: &str,
) -> DandelionResult<()> {
    let log_path = invocation_log_path(invocation_id)?;
    let invocation_lock = invocation_log_lock(invocation_id);
    let _invocation_lock_guard = invocation_lock
        .lock()
        .expect("IO recovery invocation log lock poisoned");
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
    file.write_all(line.as_bytes()).map_err(|_| {
        internal_error(format!(
            "Failed to append invocation log {}",
            log_path.display()
        ))
    })?;
    file.flush().map_err(|_| {
        internal_error(format!(
            "Failed to flush invocation log {}",
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

fn push_u32(buffer: &mut Vec<u8>, value: usize) -> DandelionResult<()> {
    let value = u32::try_from(value).map_err(|_| {
        dandelion_err!(DandelionError::RequestError(FrontendError::InternalError(
            "IO completion payload length exceeds u32".to_string(),
        )))
    })?;
    buffer.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn push_string(buffer: &mut Vec<u8>, value: &str) -> DandelionResult<()> {
    push_u32(buffer, value.len())?;
    buffer.extend_from_slice(value.as_bytes());
    Ok(())
}

fn read_u32(buffer: &[u8], offset: &mut usize) -> DandelionResult<u32> {
    let end = *offset + std::mem::size_of::<u32>();
    let bytes = buffer.get(*offset..end).ok_or(dandelion_err!(
        DandelionError::RequestError(FrontendError::InternalError(
            "Unexpected end of IO completion payload while reading u32".to_string(),
        ))
    ))?;
    *offset = end;
    Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
}

fn read_u64(buffer: &[u8], offset: &mut usize) -> DandelionResult<u64> {
    let end = *offset + std::mem::size_of::<u64>();
    let bytes = buffer.get(*offset..end).ok_or(dandelion_err!(
        DandelionError::RequestError(FrontendError::InternalError(
            "Unexpected end of IO completion payload while reading u64".to_string(),
        ))
    ))?;
    *offset = end;
    Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
}

fn read_bytes(buffer: &[u8], offset: &mut usize, length: usize) -> DandelionResult<Vec<u8>> {
    let end = *offset + length;
    let bytes = buffer.get(*offset..end).ok_or(dandelion_err!(
        DandelionError::RequestError(FrontendError::InternalError(
            "Unexpected end of IO completion payload while reading bytes".to_string(),
        ))
    ))?;
    *offset = end;
    Ok(bytes.to_vec())
}

fn read_string(buffer: &[u8], offset: &mut usize) -> DandelionResult<String> {
    let length = read_u32(buffer, offset)? as usize;
    let bytes = read_bytes(buffer, offset, length)?;
    String::from_utf8(bytes).map_err(|_| {
        dandelion_err!(DandelionError::RequestError(FrontendError::InternalError(
            "Invalid UTF-8 in IO completion payload string".to_string(),
        )))
    })
}

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
            push_u32(&mut buffer, item.data.len())?;
            buffer.extend_from_slice(&item.data);
        }
    }
    Ok(BASE64_STANDARD.encode(buffer))
}

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
            let data_length = read_u32(&payload, &mut offset)? as usize;
            let data = read_bytes(&payload, &mut offset, data_length)?;
            items.push(IoCompletionItem {
                identifier,
                key,
                data,
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

pub fn format_io_completion_line(record: &IoCompletionRecord) -> DandelionResult<String> {
    let payload_b64 = encode_io_completion_payload(&record.outputs)?;
    Ok(format!(
        "event=io_function_completed invocation_id={} composition_node_id={} function={} payload_b64={}\n",
        record.invocation_id, record.composition_node_id, record.function, payload_b64
    ))
}

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

#[cfg(test)]
mod tests {
    use super::{
        activate_async_invocation_logging, append_io_completion_record,
        format_io_completion_line, invocation_log_path, IoCompletionItem,
        IoCompletionOutputSet, IoCompletionRecord,
    };
    use crate::function_driver::functions::SystemFunction;
    use dandelion_commons::InvocationId;
    use std::{
        fs,
        path::PathBuf,
        sync::OnceLock,
        thread,
        time::{SystemTime, UNIX_EPOCH},
    };

    static TEST_ROOT: OnceLock<PathBuf> = OnceLock::new();

    fn test_root() -> PathBuf {
        TEST_ROOT
            .get_or_init(|| {
                let unique = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos();
                std::env::temp_dir().join(format!("dandelion_io_recovery_log_{unique}"))
            })
            .clone()
    }

    fn sample_record() -> IoCompletionRecord {
        IoCompletionRecord {
            invocation_id: InvocationId::nil(),
            composition_node_id: "node_hash".to_string(),
            function: SystemFunction::HTTP,
            outputs: vec![IoCompletionOutputSet {
                set_index: 0,
                set_name: "headers".to_string(),
                items: vec![IoCompletionItem {
                    identifier: "".to_string(),
                    key: 0,
                    data: b"HTTP/1.1 200 OK\n".to_vec(),
                }],
            }],
        }
    }

    #[test]
    fn completion_line_has_human_readable_prefix() {
        let line = format_io_completion_line(&sample_record()).unwrap();
        assert!(line.starts_with(
            "event=io_function_completed invocation_id=00000000-0000-0000-0000-000000000000 composition_node_id=node_hash function=HTTP payload_b64="
        ));
        assert!(line.ends_with('\n'));
    }

    #[test]
    fn appends_completion_to_invocation_log() {
        let root = test_root();
        super::set_recovery_log_root(root.clone()).unwrap();
        let record = sample_record();
        activate_async_invocation_logging(record.invocation_id);
        append_io_completion_record(&record).unwrap();

        let log_path = invocation_log_path(record.invocation_id).unwrap();
        let content = fs::read_to_string(&log_path).unwrap();
        assert!(content.contains("event=io_function_completed"));
        assert!(content.contains("composition_node_id=node_hash"));
    }

    #[test]
    fn concurrent_appends_do_not_corrupt_invocation_log() {
        let root = test_root();
        super::set_recovery_log_root(root).unwrap();
        let invocation_id = InvocationId::now_v7();

        let mut threads = Vec::new();
        for index in 0..8 {
            threads.push(thread::spawn(move || {
                activate_async_invocation_logging(invocation_id);
                let record = IoCompletionRecord {
                    invocation_id,
                    composition_node_id: format!("node_{index}"),
                    function: SystemFunction::HTTP,
                    outputs: vec![IoCompletionOutputSet {
                        set_index: 0,
                        set_name: "headers".to_string(),
                        items: vec![IoCompletionItem {
                            identifier: "".to_string(),
                            key: index,
                            data: format!("HTTP/1.1 20{index} OK\n").into_bytes(),
                        }],
                    }],
                };
                append_io_completion_record(&record).unwrap();
            }));
        }

        for thread in threads {
            thread.join().unwrap();
        }

        let log_path = invocation_log_path(invocation_id).unwrap();
        let content = fs::read_to_string(log_path).unwrap();
        let lines = content.lines().collect::<Vec<_>>();
        assert_eq!(8, lines.len());
        for index in 0..8 {
            assert!(
                lines
                    .iter()
                    .any(|line| line.contains(&format!("composition_node_id=node_{index}"))),
                "missing line for node_{index}"
            );
            assert!(lines.iter().all(|line| line.starts_with("event=io_function_completed ")));
        }
    }
}

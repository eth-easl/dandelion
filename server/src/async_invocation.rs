use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use dandelion_commons::{
    dandelion_err, err_dandelion, DandelionError, DandelionResult, FrontendError, InvocationId,
};
use dandelion_server::{AsyncInvocationState, AsyncInvocationStatusResponse};
use machine_interface::function_driver::system_driver::recovery_log::{
    append_invocation_log_line, list_invocation_log_ids, read_invocation_log,
};
use std::collections::HashMap;

fn internal_error(message: impl Into<String>) -> dandelion_commons::DError {
    dandelion_err!(DandelionError::RequestError(FrontendError::InternalError(
        message.into(),
    )))
}

fn encode_base64(data: &[u8]) -> String {
    BASE64_STANDARD.encode(data)
}

fn decode_base64(data: &str, field_name: &str) -> DandelionResult<Vec<u8>> {
    BASE64_STANDARD.decode(data).map_err(|_| {
        internal_error(format!(
            "Invalid base64 payload in async invocation field {}",
            field_name
        ))
    })
}

fn append_event(invocation_id: InvocationId, event: &str) -> DandelionResult<()> {
    append_invocation_log_line(invocation_id, event)
}

fn parse_log_fields(line: &str) -> HashMap<&str, &str> {
    line.split_whitespace()
        .filter_map(|part| part.split_once('='))
        .collect()
}

#[derive(Debug, Clone)]
pub struct RecoverableInvocation {
    pub invocation_id: InvocationId,
    pub request_bytes: Vec<u8>,
}

pub fn persist_submitted(invocation_id: InvocationId, request_bytes: &[u8]) -> DandelionResult<()> {
    append_event(
        invocation_id,
        &format!(
            "event=invocation_submitted invocation_id={} request_b64={}\n",
            invocation_id,
            encode_base64(request_bytes)
        ),
    )
}

pub fn persist_completed(invocation_id: InvocationId, result_bytes: &[u8]) -> DandelionResult<()> {
    append_event(
        invocation_id,
        &format!(
            "event=invocation_completed invocation_id={} result_b64={}\n",
            invocation_id,
            encode_base64(result_bytes)
        ),
    )
}

pub fn persist_failed(invocation_id: InvocationId, error: String) -> DandelionResult<()> {
    append_event(
        invocation_id,
        &format!(
            "event=invocation_failed invocation_id={} error_b64={}\n",
            invocation_id,
            encode_base64(error.as_bytes())
        ),
    )
}

pub fn load_status(invocation_id: InvocationId) -> DandelionResult<AsyncInvocationStatusResponse> {
    let content = read_invocation_log(invocation_id)?;
    let mut state = AsyncInvocationState::Running;
    let mut error = None;
    let mut saw_submission = false;

    for line in content.lines() {
        let fields = parse_log_fields(line);
        match fields.get("event").copied() {
            Some("invocation_submitted") => {
                saw_submission = true;
                state = AsyncInvocationState::Running;
                error = None;
            }
            Some("invocation_completed") => {
                saw_submission = true;
                state = AsyncInvocationState::Completed;
                error = None;
            }
            Some("invocation_failed") => {
                saw_submission = true;
                state = AsyncInvocationState::Failed;
                error = Some(String::from_utf8(
                    decode_base64(
                        fields.get("error_b64").copied().ok_or(internal_error(
                            "Missing error_b64 field in async invocation failure record",
                        ))?,
                        "error_b64",
                    )?,
                )
                .map_err(|_| internal_error("Invalid UTF-8 in async invocation error payload"))?);
            }
            _ => {}
        }
    }

    if !saw_submission {
        return err_dandelion!(DandelionError::RequestError(
            FrontendError::InvalidRequest(format!(
                "Unknown async invocation {}",
                invocation_id.simple()
            )),
        ));
    }

    Ok(AsyncInvocationStatusResponse {
        invocation_id,
        state,
        error,
    })
}

pub fn load_result(invocation_id: InvocationId) -> DandelionResult<Option<Vec<u8>>> {
    let content = read_invocation_log(invocation_id)?;
    let mut saw_submission = false;
    let mut latest_result = None;
    let mut latest_error = None;

    for line in content.lines() {
        let fields = parse_log_fields(line);
        match fields.get("event").copied() {
            Some("invocation_submitted") => {
                saw_submission = true;
                latest_result = None;
                latest_error = None;
            }
            Some("invocation_completed") => {
                saw_submission = true;
                latest_error = None;
                latest_result = Some(decode_base64(
                    fields.get("result_b64").copied().ok_or(internal_error(
                        "Missing result_b64 field in async invocation completion record",
                    ))?,
                    "result_b64",
                )?);
            }
            Some("invocation_failed") => {
                saw_submission = true;
                latest_result = None;
                latest_error = Some(
                    String::from_utf8(
                        decode_base64(
                            fields.get("error_b64").copied().ok_or(internal_error(
                                "Missing error_b64 field in async invocation failure record",
                            ))?,
                            "error_b64",
                        )?,
                    )
                    .map_err(|_| internal_error("Invalid UTF-8 in async invocation error payload"))?,
                );
            }
            _ => {}
        }
    }

    if !saw_submission {
        return err_dandelion!(DandelionError::RequestError(
            FrontendError::InvalidRequest(format!(
                "Unknown async invocation {}",
                invocation_id.simple()
            )),
        ));
    }

    if let Some(error) = latest_error {
        return err_dandelion!(DandelionError::RequestError(
            FrontendError::InvalidRequest(error),
        ));
    }

    Ok(latest_result)
}

pub fn list_recoverable_invocations() -> DandelionResult<Vec<RecoverableInvocation>> {
    let mut recoverable = Vec::new();
    for invocation_id in list_invocation_log_ids()? {
        let content = read_invocation_log(invocation_id)?;
        let mut saw_submission = false;
        let mut terminal = false;
        let mut request_bytes = None;

        for line in content.lines() {
            let fields = parse_log_fields(line);
            match fields.get("event").copied() {
                Some("invocation_submitted") => {
                    saw_submission = true;
                    terminal = false;
                    request_bytes = Some(decode_base64(
                        fields.get("request_b64").copied().ok_or(internal_error(
                            "Missing request_b64 field in async invocation submission record",
                        ))?,
                        "request_b64",
                    )?);
                }
                Some("invocation_completed") | Some("invocation_failed") => {
                    terminal = true;
                }
                _ => {}
            }
        }

        if saw_submission && !terminal {
            if let Some(request_bytes) = request_bytes {
                recoverable.push(RecoverableInvocation {
                    invocation_id,
                    request_bytes,
                });
            }
        }
    }
    Ok(recoverable)
}

#[cfg(test)]
mod tests {
    use super::{
        list_recoverable_invocations, load_result, load_status, persist_completed, persist_failed,
        persist_submitted,
    };
    use dandelion_commons::InvocationId;
    use dandelion_server::AsyncInvocationState;
    use std::{
        path::PathBuf,
        sync::OnceLock,
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
                std::env::temp_dir().join(format!("dandelion_async_invocations_{unique}"))
            })
            .clone()
    }

    #[test]
    fn persists_async_invocation_lifecycle() {
        let root = test_root();
        machine_interface::function_driver::system_driver::recovery_log::set_recovery_log_root(
            root,
        )
        .unwrap();
        let invocation_id = InvocationId::now_v7();
        let result = vec![1, 2, 3, 4];

        persist_submitted(invocation_id, b"request").unwrap();
        assert_eq!(
            load_status(invocation_id).unwrap().state,
            AsyncInvocationState::Running
        );
        assert!(load_result(invocation_id).unwrap().is_none());

        persist_completed(invocation_id, &result).unwrap();
        assert_eq!(
            load_status(invocation_id).unwrap().state,
            AsyncInvocationState::Completed
        );
        assert_eq!(load_result(invocation_id).unwrap().unwrap(), result);
    }

    #[test]
    fn persists_async_failures() {
        let root = test_root();
        machine_interface::function_driver::system_driver::recovery_log::set_recovery_log_root(
            root,
        )
        .unwrap();
        let invocation_id = InvocationId::now_v7();

        persist_submitted(invocation_id, b"request").unwrap();
        persist_failed(invocation_id, "boom".to_string()).unwrap();

        let status = load_status(invocation_id).unwrap();
        assert_eq!(status.state, AsyncInvocationState::Failed);
        assert_eq!(status.error.as_deref(), Some("boom"));
    }

    #[test]
    fn lists_unfinished_invocations_for_recovery() {
        let root = test_root();
        machine_interface::function_driver::system_driver::recovery_log::set_recovery_log_root(
            root,
        )
        .unwrap();
        let running_invocation = InvocationId::now_v7();
        let completed_invocation = InvocationId::now_v7();

        persist_submitted(running_invocation, b"running").unwrap();
        persist_submitted(completed_invocation, b"completed").unwrap();
        persist_completed(completed_invocation, b"done").unwrap();

        let recoverable = list_recoverable_invocations().unwrap();
        assert!(recoverable
            .iter()
            .any(|entry| entry.invocation_id == running_invocation && entry.request_bytes == b"running"));
        assert!(recoverable
            .iter()
            .all(|entry| entry.invocation_id != completed_invocation));
    }
}

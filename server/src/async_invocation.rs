use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use dandelion_commons::{
    dandelion_err, err_dandelion, DandelionError, DandelionResult, FrontendError, InvocationId,
};
use dandelion_server::{AsyncInvocationState, AsyncInvocationStatusResponse};
use machine_interface::function_driver::system_driver::recovery_log::{
    append_invocation_log_line, complete_log_lines, list_invocation_log_ids, read_invocation_log,
};
use std::{
    collections::HashMap,
    sync::{Mutex, OnceLock},
};
use tokio::sync::watch;

static TERMINAL_NOTIFIERS: OnceLock<Mutex<HashMap<InvocationId, watch::Sender<bool>>>> =
    OnceLock::new();

fn terminal_notifiers() -> &'static Mutex<HashMap<InvocationId, watch::Sender<bool>>> {
    TERMINAL_NOTIFIERS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn terminal_receiver(invocation_id: InvocationId) -> watch::Receiver<bool> {
    let mut notifiers = terminal_notifiers()
        .lock()
        .expect("Async invocation notifier lock poisoned");
    notifiers
        .entry(invocation_id)
        .or_insert_with(|| watch::channel(false).0)
        .subscribe()
}

fn notify_terminal(invocation_id: InvocationId) {
    let sender = terminal_notifiers()
        .lock()
        .expect("Async invocation notifier lock poisoned")
        .remove(&invocation_id);
    if let Some(sender) = sender {
        sender.send_replace(true);
    }
}

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

fn validated_payload<'a>(
    fields: &HashMap<&'a str, &'a str>,
    payload_field: &str,
    length_field: &str,
) -> Option<&'a str> {
    let encoded = fields.get(payload_field).copied()?;
    let expected_length = fields.get(length_field)?.parse::<usize>().ok()?;
    let decoded = BASE64_STANDARD.decode(encoded).ok()?;
    (decoded.len() == expected_length).then_some(encoded)
}

#[derive(Debug)]
struct SubmissionRecord<'a> {
    request_b64: Option<&'a str>,
    is_cold: Option<&'a str>,
}

#[derive(Debug, Default)]
struct ParsedInvocationLog<'a> {
    state: Option<AsyncInvocationState>,
    submission: Option<SubmissionRecord<'a>>,
    result_b64: Option<&'a str>,
    error_b64: Option<&'a str>,
}

fn parse_invocation_log(content: &str) -> ParsedInvocationLog<'_> {
    let mut parsed = ParsedInvocationLog::default();
    for line in complete_log_lines(content) {
        let fields = parse_log_fields(line);
        match fields.get("event").copied() {
            Some("invocation_submitted") => {
                let Some(request_b64) = validated_payload(&fields, "request_b64", "request_len")
                else {
                    continue;
                };
                let Some(is_cold) = fields
                    .get("is_cold")
                    .copied()
                    .filter(|value| value.parse::<bool>().is_ok())
                else {
                    continue;
                };
                parsed.state = Some(AsyncInvocationState::Running);
                parsed.submission = Some(SubmissionRecord {
                    request_b64: Some(request_b64),
                    is_cold: Some(is_cold),
                });
                parsed.result_b64 = None;
                parsed.error_b64 = None;
            }
            Some("invocation_completed") => {
                let Some(result_b64) = validated_payload(&fields, "result_b64", "result_len")
                else {
                    continue;
                };
                parsed.state = Some(AsyncInvocationState::Completed);
                parsed.result_b64 = Some(result_b64);
                parsed.error_b64 = None;
            }
            Some("invocation_failed") => {
                let Some(error_b64) = validated_payload(&fields, "error_b64", "error_len") else {
                    continue;
                };
                parsed.state = Some(AsyncInvocationState::Failed);
                parsed.result_b64 = None;
                parsed.error_b64 = Some(error_b64);
            }
            _ => {}
        }
    }
    parsed
}

fn unknown_invocation_error(invocation_id: InvocationId) -> dandelion_commons::DError {
    dandelion_err!(DandelionError::RequestError(FrontendError::InvalidRequest(
        format!("Unknown async invocation {}", invocation_id.simple())
    )))
}

fn decode_invocation_error(error_b64: Option<&str>) -> DandelionResult<String> {
    String::from_utf8(decode_base64(
        error_b64.ok_or(internal_error(
            "Missing error_b64 field in async invocation failure record",
        ))?,
        "error_b64",
    )?)
    .map_err(|_| internal_error("Invalid UTF-8 in async invocation error payload"))
}

#[derive(Debug, Clone)]
pub struct RecoverableInvocation {
    pub invocation_id: InvocationId,
    pub request_bytes: Vec<u8>,
    pub is_cold: bool,
}

pub fn persist_submitted(
    invocation_id: InvocationId,
    request_bytes: &[u8],
    is_cold: bool,
) -> DandelionResult<()> {
    append_event(
        invocation_id,
        &format!(
            "event=invocation_submitted invocation_id={} request_len={} request_b64={} is_cold={}\n",
            invocation_id,
            request_bytes.len(),
            encode_base64(request_bytes),
            is_cold,
        ),
    )
}

pub fn persist_completed(invocation_id: InvocationId, result_bytes: &[u8]) -> DandelionResult<()> {
    append_event(
        invocation_id,
        &format!(
            "event=invocation_completed invocation_id={} result_len={} result_b64={}\n",
            invocation_id,
            result_bytes.len(),
            encode_base64(result_bytes)
        ),
    )?;
    notify_terminal(invocation_id);
    Ok(())
}

pub fn persist_failed(invocation_id: InvocationId, error: String) -> DandelionResult<()> {
    append_event(
        invocation_id,
        &format!(
            "event=invocation_failed invocation_id={} error_len={} error_b64={}\n",
            invocation_id,
            error.len(),
            encode_base64(error.as_bytes())
        ),
    )?;
    notify_terminal(invocation_id);
    Ok(())
}

pub fn load_status(invocation_id: InvocationId) -> DandelionResult<AsyncInvocationStatusResponse> {
    let content = read_invocation_log(invocation_id)?;
    let parsed = parse_invocation_log(&content);
    let state = parsed
        .state
        .ok_or_else(|| unknown_invocation_error(invocation_id))?;
    let error = if state == AsyncInvocationState::Failed {
        Some(decode_invocation_error(parsed.error_b64)?)
    } else {
        None
    };

    Ok(AsyncInvocationStatusResponse {
        invocation_id,
        state,
        error,
    })
}

pub fn load_result(invocation_id: InvocationId) -> DandelionResult<Option<Vec<u8>>> {
    let content = read_invocation_log(invocation_id)?;
    let parsed = parse_invocation_log(&content);
    match parsed
        .state
        .ok_or_else(|| unknown_invocation_error(invocation_id))?
    {
        AsyncInvocationState::Running => Ok(None),
        AsyncInvocationState::Completed => Ok(Some(decode_base64(
            parsed.result_b64.ok_or(internal_error(
                "Missing result_b64 field in async invocation completion record",
            ))?,
            "result_b64",
        )?)),
        AsyncInvocationState::Failed => {
            let error = decode_invocation_error(parsed.error_b64)?;
            err_dandelion!(DandelionError::RequestError(FrontendError::InvalidRequest(
                error
            )))
        }
    }
}

async fn wait_for_result_with<F>(
    invocation_id: InvocationId,
    mut load: F,
) -> DandelionResult<Option<Vec<u8>>>
where
    F: FnMut() -> DandelionResult<Option<Vec<u8>>>,
{
    if let Some(result) = load()? {
        notify_terminal(invocation_id);
        return Ok(Some(result));
    }

    // Register before checking durable state again. If completion races with
    // registration, either the second load observes it or the retained watch
    // value wakes us; no completion notification can fall into the gap.
    let mut terminal = terminal_receiver(invocation_id);
    if let Some(result) = load()? {
        // Completion may have happened just before registration, when there was
        // no sender to notify. Remove the newly-created entry and wake any other
        // waiter that joined it in the meantime.
        notify_terminal(invocation_id);
        return Ok(Some(result));
    }

    let _ = terminal.wait_for(|is_terminal| *is_terminal).await;
    load()
}

/// Wait for an invocation to reach durable terminal state. The caller controls
/// cancellation by dropping the future, for example when its HTTP connection closes.
pub async fn wait_for_result(invocation_id: InvocationId) -> DandelionResult<Option<Vec<u8>>> {
    wait_for_result_with(invocation_id, || load_result(invocation_id)).await
}

pub fn list_recoverable_invocations() -> DandelionResult<Vec<RecoverableInvocation>> {
    let mut recoverable = Vec::new();
    for invocation_id in list_invocation_log_ids()? {
        let content = read_invocation_log(invocation_id)?;
        let parsed = parse_invocation_log(&content);
        if parsed.state != Some(AsyncInvocationState::Running) {
            continue;
        }
        let Some(submission) = parsed.submission else {
            continue;
        };
        let is_cold = submission
            .is_cold
            .ok_or(internal_error(
                "Missing is_cold field in async invocation submission record",
            ))?
            .parse::<bool>()
            .map_err(|_| {
                internal_error("Invalid is_cold field in async invocation submission record")
            })?;
        let request_bytes = decode_base64(
            submission.request_b64.ok_or(internal_error(
                "Missing request_b64 field in async invocation submission record",
            ))?,
            "request_b64",
        )?;
        recoverable.push(RecoverableInvocation {
            invocation_id,
            request_bytes,
            is_cold,
        });
    }
    Ok(recoverable)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    use std::time::Duration;

    const SUBMISSION: &str =
        "event=invocation_submitted invocation_id=1 request_len=3 request_b64=cmVx is_cold=false\n";

    #[test]
    fn complete_terminal_record_is_recognized() {
        let content = format!(
            "{}event=invocation_completed invocation_id=1 result_len=6 result_b64=cmVzdWx0\n",
            SUBMISSION
        );
        let parsed = parse_invocation_log(&content);
        assert_eq!(parsed.state, Some(AsyncInvocationState::Completed));
        assert_eq!(parsed.result_b64, Some("cmVzdWx0"));
    }

    #[test]
    fn unterminated_terminal_record_is_ignored() {
        let content = format!(
            "{}event=invocation_completed invocation_id=1 result_len=6 result_b64=cmVz",
            SUBMISSION
        );
        let parsed = parse_invocation_log(&content);
        assert_eq!(parsed.state, Some(AsyncInvocationState::Running));
        assert!(parsed.result_b64.is_none());
    }

    #[test]
    fn valid_base64_with_wrong_length_is_ignored() {
        let content = format!(
            "{}event=invocation_completed invocation_id=1 result_len=6 result_b64=cmVz\n",
            SUBMISSION
        );
        let parsed = parse_invocation_log(&content);
        assert_eq!(parsed.state, Some(AsyncInvocationState::Running));
        assert!(parsed.result_b64.is_none());
    }

    #[tokio::test]
    async fn wait_returns_an_already_available_result_immediately() {
        let invocation_id = InvocationId::from_u128(1001);
        let result = wait_for_result_with(invocation_id, || Ok(Some(b"ready".to_vec())))
            .await
            .unwrap();

        assert_eq!(result, Some(b"ready".to_vec()));
    }

    #[tokio::test]
    async fn terminal_notification_wakes_a_waiter() {
        let invocation_id = InvocationId::from_u128(1002);
        let completed = Arc::new(AtomicBool::new(false));
        let waiter_state = completed.clone();
        let waiter = tokio::spawn(async move {
            wait_for_result_with(invocation_id, || {
                Ok(waiter_state
                    .load(Ordering::Acquire)
                    .then(|| b"completed".to_vec()))
            })
            .await
            .unwrap()
        });

        tokio::task::yield_now().await;
        completed.store(true, Ordering::Release);
        notify_terminal(invocation_id);

        assert_eq!(waiter.await.unwrap(), Some(b"completed".to_vec()));
    }

    #[tokio::test]
    async fn wait_remains_pending_until_terminal_notification() {
        let invocation_id = InvocationId::from_u128(1003);
        let completed = Arc::new(AtomicBool::new(false));
        let waiter_state = completed.clone();
        let mut waiter = tokio::spawn(async move {
            wait_for_result_with(invocation_id, || {
                Ok(waiter_state
                    .load(Ordering::Acquire)
                    .then(|| b"completed".to_vec()))
            })
            .await
            .unwrap()
        });

        assert!(tokio::time::timeout(Duration::from_millis(5), &mut waiter)
            .await
            .is_err());

        completed.store(true, Ordering::Release);
        notify_terminal(invocation_id);

        assert_eq!(waiter.await.unwrap(), Some(b"completed".to_vec()));
    }
}

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use dandelion_commons::{
    dandelion_err, err_dandelion, DandelionError, DandelionResult, FrontendError, RunId,
};
use dandelion_server::{AsyncInvocationState, AsyncInvocationStatusResponse};
use log::info;
use machine_interface::function_driver::system_driver::recovery_log::{
    append_run_log_line, complete_log_lines, list_run_log_ids, read_run_log,
};
use std::{
    collections::HashMap,
    future::Future,
    sync::{Arc, Mutex, OnceLock},
};
use tokio::sync::watch;

#[derive(Clone, Debug)]
enum TerminalNotification {
    Pending,
    #[cfg(not(feature = "exactly-once"))]
    LiveResult(Arc<Vec<u8>>),
    Durable,
}

static TERMINAL_NOTIFIERS: OnceLock<Mutex<HashMap<RunId, watch::Sender<TerminalNotification>>>> =
    OnceLock::new();

fn terminal_notifiers() -> &'static Mutex<HashMap<RunId, watch::Sender<TerminalNotification>>> {
    TERMINAL_NOTIFIERS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn terminal_receiver(run_id: RunId) -> watch::Receiver<TerminalNotification> {
    let mut notifiers = terminal_notifiers()
        .lock()
        .expect("Async invocation notifier lock poisoned");
    notifiers
        .entry(run_id)
        .or_insert_with(|| watch::channel(TerminalNotification::Pending).0)
        .subscribe()
}

/// Make a completed result available to live waiters before durable persistence finishes.
/// The sender remains registered until the corresponding terminal record is durable, so a
/// waiter arriving in the persistence window can still consume the in-memory result.
#[cfg(not(feature = "exactly-once"))]
pub fn publish_live_result(run_id: RunId, result: Arc<Vec<u8>>) {
    let mut notifiers = terminal_notifiers()
        .lock()
        .expect("Async invocation notifier lock poisoned");
    let sender = notifiers
        .entry(run_id)
        .or_insert_with(|| watch::channel(TerminalNotification::Pending).0);
    sender.send_replace(TerminalNotification::LiveResult(result));
}

fn notify_terminal(run_id: RunId) {
    let sender = terminal_notifiers()
        .lock()
        .expect("Async invocation notifier lock poisoned")
        .remove(&run_id);
    if let Some(sender) = sender {
        sender.send_replace(TerminalNotification::Durable);
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

async fn append_event(run_id: RunId, event: &str) -> DandelionResult<()> {
    append_run_log_line(run_id, event).await
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

fn parse_run_log(content: &str) -> ParsedInvocationLog<'_> {
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

fn unknown_invocation_error(run_id: RunId) -> dandelion_commons::DError {
    dandelion_err!(DandelionError::RequestError(FrontendError::InvalidRequest(
        format!("Unknown async invocation {}", run_id.simple())
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
    pub run_id: RunId,
    pub request_bytes: Vec<u8>,
    pub is_cold: bool,
}

pub async fn persist_submitted(
    run_id: RunId,
    request_bytes: &[u8],
    is_cold: bool,
) -> DandelionResult<()> {
    append_event(
        run_id,
        &format!(
            "event=invocation_submitted run_id={} request_len={} request_b64={} is_cold={}\n",
            run_id,
            request_bytes.len(),
            encode_base64(request_bytes),
            is_cold,
        ),
    )
    .await
}

pub async fn persist_completed(run_id: RunId, result_bytes: Arc<Vec<u8>>) -> DandelionResult<()> {
    append_event(
        run_id,
        &format!(
            "event=invocation_completed run_id={} result_len={} result_b64={}\n",
            run_id,
            result_bytes.len(),
            encode_base64(result_bytes.as_slice())
        ),
    )
    .await?;
    info!("Async invocation {} entered completed state", run_id);
    notify_terminal(run_id);
    Ok(())
}

pub async fn persist_failed(run_id: RunId, error: String) -> DandelionResult<()> {
    append_event(
        run_id,
        &format!(
            "event=invocation_failed run_id={} error_len={} error_b64={}\n",
            run_id,
            error.len(),
            encode_base64(error.as_bytes())
        ),
    )
    .await?;
    info!("Async invocation {} entered failed state", run_id);
    notify_terminal(run_id);
    Ok(())
}

pub async fn load_status(run_id: RunId) -> DandelionResult<AsyncInvocationStatusResponse> {
    let content = read_run_log(run_id).await?;
    let parsed = parse_run_log(&content);
    let state = parsed
        .state
        .ok_or_else(|| unknown_invocation_error(run_id))?;
    let error = if state == AsyncInvocationState::Failed {
        Some(decode_invocation_error(parsed.error_b64)?)
    } else {
        None
    };

    Ok(AsyncInvocationStatusResponse {
        run_id,
        state,
        error,
    })
}

pub async fn load_result(run_id: RunId) -> DandelionResult<Option<Vec<u8>>> {
    let content = read_run_log(run_id).await?;
    let parsed = parse_run_log(&content);
    match parsed
        .state
        .ok_or_else(|| unknown_invocation_error(run_id))?
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

async fn wait_for_result_with<F, Fut>(
    run_id: RunId,
    mut load: F,
) -> DandelionResult<Option<Vec<u8>>>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = DandelionResult<Option<Vec<u8>>>>,
{
    info!("Async invocation {} result wait requested", run_id);
    if let Some(result) = load().await? {
        info!("Async invocation {} result was already available", run_id);
        notify_terminal(run_id);
        return Ok(Some(result));
    }

    // Register before checking durable state again. If completion races with
    // registration, either the second load observes it or the retained watch
    // value wakes us; no completion notification can fall into the gap.
    let mut terminal = terminal_receiver(run_id);
    info!("Async invocation {} result waiter registered", run_id);
    if let Some(result) = load().await? {
        // Completion may have happened just before registration, when there was
        // no sender to notify. Remove the newly-created entry and wake any other
        // waiter that joined it in the meantime.
        notify_terminal(run_id);
        return Ok(Some(result));
    }

    let _ = terminal
        .wait_for(|notification| !matches!(notification, TerminalNotification::Pending))
        .await;
    info!("Async invocation {} result waiter notified", run_id);
    let notification = terminal.borrow().clone();
    match notification {
        #[cfg(not(feature = "exactly-once"))]
        TerminalNotification::LiveResult(result) => Ok(Some(result.as_ref().clone())),
        TerminalNotification::Durable => load().await,
        TerminalNotification::Pending => load().await,
    }
}

/// Wait for an invocation result. At-least-once modes may return a live result while its
/// terminal record is still being persisted. The caller controls cancellation by dropping the
/// future, for example when its HTTP connection closes.
pub async fn wait_for_result(run_id: RunId) -> DandelionResult<Option<Vec<u8>>> {
    wait_for_result_with(run_id, || load_result(run_id)).await
}

pub async fn list_recoverable_invocations() -> DandelionResult<Vec<RecoverableInvocation>> {
    let mut recoverable = Vec::new();
    for run_id in list_run_log_ids().await? {
        let content = read_run_log(run_id).await?;
        let parsed = parse_run_log(&content);
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
            run_id,
            request_bytes,
            is_cold,
        });
    }
    Ok(recoverable)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    const SUBMISSION: &str =
        "event=invocation_submitted run_id=1 request_len=3 request_b64=cmVx is_cold=false\n";

    #[test]
    fn complete_terminal_record_is_recognized() {
        let content = format!(
            "{}event=invocation_completed run_id=1 result_len=6 result_b64=cmVzdWx0\n",
            SUBMISSION
        );
        let parsed = parse_run_log(&content);
        assert_eq!(parsed.state, Some(AsyncInvocationState::Completed));
        assert_eq!(parsed.result_b64, Some("cmVzdWx0"));
    }

    #[test]
    fn unterminated_terminal_record_is_ignored() {
        let content = format!(
            "{}event=invocation_completed run_id=1 result_len=6 result_b64=cmVz",
            SUBMISSION
        );
        let parsed = parse_run_log(&content);
        assert_eq!(parsed.state, Some(AsyncInvocationState::Running));
        assert!(parsed.result_b64.is_none());
    }

    #[test]
    fn valid_base64_with_wrong_length_is_ignored() {
        let content = format!(
            "{}event=invocation_completed run_id=1 result_len=6 result_b64=cmVz\n",
            SUBMISSION
        );
        let parsed = parse_run_log(&content);
        assert_eq!(parsed.state, Some(AsyncInvocationState::Running));
        assert!(parsed.result_b64.is_none());
    }

    #[tokio::test]
    async fn wait_returns_an_already_available_result_immediately() {
        let run_id = RunId::from_u128(1001);
        let result = wait_for_result_with(run_id, || async { Ok(Some(b"ready".to_vec())) })
            .await
            .unwrap();

        assert_eq!(result, Some(b"ready".to_vec()));
    }

    #[tokio::test]
    async fn terminal_notification_wakes_a_waiter() {
        let run_id = RunId::from_u128(1002);
        let completed = Arc::new(AtomicBool::new(false));
        let waiter_state = completed.clone();
        let waiter = tokio::spawn(async move {
            wait_for_result_with(run_id, || {
                let waiter_state = waiter_state.clone();
                async move {
                    Ok(waiter_state
                        .load(Ordering::Acquire)
                        .then(|| b"completed".to_vec()))
                }
            })
            .await
            .unwrap()
        });

        tokio::task::yield_now().await;
        completed.store(true, Ordering::Release);
        notify_terminal(run_id);

        assert_eq!(waiter.await.unwrap(), Some(b"completed".to_vec()));
    }

    #[tokio::test]
    #[cfg(not(feature = "exactly-once"))]
    async fn live_result_wakes_a_waiter_without_durable_result() {
        let run_id = RunId::from_u128(1004);
        let mut waiter = tokio::spawn(async move {
            wait_for_result_with(run_id, || async { Ok(None) })
                .await
                .unwrap()
        });

        assert!(tokio::time::timeout(Duration::from_millis(5), &mut waiter)
            .await
            .is_err());
        publish_live_result(run_id, Arc::new(b"live".to_vec()));

        assert_eq!(waiter.await.unwrap(), Some(b"live".to_vec()));
        notify_terminal(run_id);
    }

    #[tokio::test]
    #[cfg(not(feature = "exactly-once"))]
    async fn waiter_arriving_after_live_result_receives_it_without_loading_disk() {
        let run_id = RunId::from_u128(1005);
        publish_live_result(run_id, Arc::new(b"live".to_vec()));

        let result = wait_for_result_with(run_id, || async { Ok(None) })
            .await
            .unwrap();

        assert_eq!(result, Some(b"live".to_vec()));
        notify_terminal(run_id);
    }

    #[tokio::test]
    async fn wait_remains_pending_until_terminal_notification() {
        let run_id = RunId::from_u128(1003);
        let completed = Arc::new(AtomicBool::new(false));
        let waiter_state = completed.clone();
        let mut waiter = tokio::spawn(async move {
            wait_for_result_with(run_id, || {
                let waiter_state = waiter_state.clone();
                async move {
                    Ok(waiter_state
                        .load(Ordering::Acquire)
                        .then(|| b"completed".to_vec()))
                }
            })
            .await
            .unwrap()
        });

        assert!(tokio::time::timeout(Duration::from_millis(5), &mut waiter)
            .await
            .is_err());

        completed.store(true, Ordering::Release);
        notify_terminal(run_id);

        assert_eq!(waiter.await.unwrap(), Some(b"completed".to_vec()));
    }
}

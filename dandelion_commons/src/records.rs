use crate::{DandelionError, DandelionResult};
use hdrhist::HDRHist;
use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RecordPoint {
    Arrival,
    LoadStart,
    TransferStart,
    TransferEnd,
    EngineStart,
    EngineEnd,
    FutureReturn,
}

// TODO find better way to pass references without
// needing to propagate lifetimes everywhere
// also using sync mutex with futures is not ideal
struct RecorderState {
    checkpoint_time: Instant,
    last_checkpoint: RecordPoint,
    archive: Arc<Mutex<Archive>>,
}

#[derive(Clone)]
pub struct Recorder {
    state: Arc<Mutex<RecorderState>>,
}

impl Recorder {
    pub fn new(archive: Arc<Mutex<Archive>>, last_checkpoint: RecordPoint) -> Recorder {
        return Recorder {
            state: Arc::new(Mutex::new(RecorderState {
                checkpoint_time: Instant::now(),
                last_checkpoint: last_checkpoint,
                archive,
            })),
        };
    }
    pub fn record(&mut self, current_point: RecordPoint) -> DandelionResult<()> {
        let current_time = Instant::now();
        let mut state_guard = match self.state.lock() {
            Ok(guard) => guard,
            Err(_) => return Err(DandelionError::RecordLockFailure),
        };
        {
            let mut archive_guard = match state_guard.archive.lock() {
                Ok(guard) => guard,
                Err(_) => return Err(DandelionError::RecordLockFailure),
            };
            let hist = match (&state_guard.last_checkpoint, &current_point) {
                (RecordPoint::Arrival, RecordPoint::LoadStart) => &mut archive_guard.initial_time,
                (RecordPoint::LoadStart, RecordPoint::TransferStart) => {
                    &mut archive_guard.load_time
                }
                (RecordPoint::TransferStart, RecordPoint::TransferEnd) => {
                    &mut archive_guard.transfer_time
                }
                (RecordPoint::TransferEnd, RecordPoint::EngineStart) => {
                    &mut archive_guard.dispatch_time
                }
                (RecordPoint::EngineStart, RecordPoint::EngineEnd) => {
                    &mut archive_guard.engine_time
                }
                (RecordPoint::EngineEnd, RecordPoint::FutureReturn) => {
                    &mut archive_guard.return_time
                }
                (last, current) => {
                    return Err(DandelionError::RecordSequencingFailure(
                        last.clone(),
                        current.clone(),
                    ))
                }
            };
            let duration = u64::try_from(
                current_time
                    .duration_since(state_guard.checkpoint_time)
                    .as_micros(),
            )
            .unwrap_or(u64::MAX);
            hist.add_value(duration);
        }
        state_guard.last_checkpoint = current_point;
        state_guard.checkpoint_time = current_time;

        return Ok(());
    }
}

pub struct Archive {
    initial_time: HDRHist,
    load_time: HDRHist,
    transfer_time: HDRHist,
    dispatch_time: HDRHist,
    engine_time: HDRHist,
    return_time: HDRHist,
}

impl Archive {
    pub fn new() -> Archive {
        return Archive {
            initial_time: HDRHist::new(),
            load_time: HDRHist::new(),
            transfer_time: HDRHist::new(),
            dispatch_time: HDRHist::new(),
            engine_time: HDRHist::new(),
            return_time: HDRHist::new(),
        };
    }
    pub fn get_summary(&self) -> String {
        format!(
            "Current statistics summary:\n\
            Initial time:\n{}\n\
            Load time:\n{}\n\
            Transfer time:\n{}\n\
            Dispatch time:\n{}\n\
            Engine time:\n{}\n\
            Return time:\n{}\n",
            self.initial_time.summary_string(),
            self.load_time.summary_string(),
            self.transfer_time.summary_string(),
            self.dispatch_time.summary_string(),
            self.engine_time.summary_string(),
            self.return_time.summary_string(),
        )
    }
}

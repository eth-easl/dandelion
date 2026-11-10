#[cfg(feature = "trace-timestamps")]
use core::fmt;
#[cfg(feature = "trace-timestamps")]
use std::{
    cell::UnsafeCell,
    time::{Duration, Instant},
    write,
};

/// Compiler check to assert that each `Timestep`` gets a unique number between 0 and this.
const LAST_RECORD_POINT: usize = 23;

#[repr(usize)]
pub enum Timestamp {
    /// Frontend has finished deserializing.
    DeserializationEnd,
    /// Request enters dispatcher.
    EnterDispatcher,
    /// Parsing finishes (e.g. single-shot composition).
    DynamicParsingEnd,
    /// TODO: Sharding computation starts.
    ShardingStart,

    /// Invocation is inserted into IO queue.
    IOQueueStart,
    /// Invocation is taken out of IO queue.
    IOQueueEnd,
    /// Input set fetching starts.
    FetchStart,
    /// Input set fetching finishes.
    FetchEnd,
    /// Invocation is inserted into compute queue.
    ComputeQueueStart,
    /// Invocation is taken out of compute queue.
    ComputeQueueEnd,

    /// Remote nodes takes invocation from queue. Used to anchor the remote timings.
    RemoteTake,
    /// Invocation is inserted into IO queue on remote node.
    RemoteIOQueueStart,
    /// Invocation is taken out of IO queue on remote node.
    RemoteIOQueueEnd,
    /// Input set fetching starts on remote node.
    RemoteFetchStart,
    /// Input set fetching finishes on remote node.
    RemoteFetchEnd,
    /// Invocation is inserted into compute queue on remote node.
    RemoteComputeQueueStart,
    /// Invocation is taken out of compute queue on remote node.
    RemoteComputeQueueEnd,

    /// Binary/ELF parsing starts.
    BinaryParsingStart,
    /// Binary/ELF parsing ends.
    BinaryParsingEnd,
    /// Context allocation and binary loading starts.
    LoadStart,
    /// Data (input set) transfer into the context starts.
    TransferStart,
    /// Execution on the engine starts.
    EngineStart,
    /// Execution on the engine finishes.
    EngineEnd,
    /// Output sets return.
    FutureReturn = LAST_RECORD_POINT,
}

#[cfg(feature = "trace-timestamps")]
pub(crate) struct TimestampCollection {
    pub(crate) start: Instant,
    timestamps: [UnsafeCell<Duration>; LAST_RECORD_POINT + 1],
}

#[cfg(feature = "trace-timestamps")]
unsafe impl Send for TimestampCollection {}
#[cfg(feature = "trace-timestamps")]
unsafe impl Sync for TimestampCollection {}

#[cfg(feature = "trace-timestamps")]
impl TimestampCollection {
    pub(crate) fn new(start: Instant) -> Self {
        Self {
            start,
            timestamps: [const { UnsafeCell::new(Duration::ZERO) }; LAST_RECORD_POINT + 1],
        }
    }

    fn write(&self, ts: Timestamp, elapsed: Duration) {
        // Each Timestamp should only be set once in the code, so overwriting it here is safe.
        let reference = UnsafeCell::raw_get(&self.timestamps[ts as usize]);
        unsafe { *reference = elapsed };
    }

    pub(crate) fn set_now(&self, ts: Timestamp) {
        let elapsed = self.start.elapsed();
        self.write(ts, elapsed);
    }

    pub(crate) fn set(&self, ts: Timestamp, time: Instant) {
        let elapsed = time.duration_since(self.start);
        self.write(ts, elapsed);
    }
}

#[cfg(feature = "trace-timestamps")]
impl fmt::Debug for TimestampCollection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(self.timestamps.iter().map(|cell| unsafe { *cell.get() }))
            .finish()
    }
}

#[cfg(feature = "trace-timestamps")]
impl fmt::Display for TimestampCollection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Formats as a JSON array: `[{"items": n, "size": s}, ...]`.
        write!(f, "[",)?;
        // write own time points
        for index in 0..LAST_RECORD_POINT {
            let duration = unsafe { *self.timestamps[index].get() };
            write!(f, "{},", duration.as_micros())?;
        }
        let duration = unsafe { *self.timestamps[LAST_RECORD_POINT].get() };
        write!(f, "{}]", duration.as_micros())
    }
}

use crate::FunctionId;
use core::fmt;
use std::time::Instant;

/// Maximum usize to expect when converting a record point to a usize
/// By setting the last element to this explicitly, the compiler will throw an error,
/// if there are more than this, because it enumerates from 0 and won't allow a number to be assigned twice.
const LAST_RECORD_POINT: usize = 12;
/// The first timestamp that should come from the engine running the function
const FIRST_ENGINE_POINT: usize = 4;
const LAST_ENGINE_POINT: usize = 11;

pub const ENGINE_RECORD_POINTS: usize = LAST_ENGINE_POINT - FIRST_ENGINE_POINT + 1;

#[repr(usize)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RecordPoint {
    /// Frontend has finished deserializing
    DeserializationEnd,
    /// When the request first enters the dispatcher
    EnterDispatcher,
    /// Queue to get the function executed on the engine (async)
    ExecutionQueue,
    /// Time when a request was taken from the queue to send to the remote.
    /// Used to anchor the remote timings.
    RemoteTake,
    /// Start fetching of input sets
    FetchingStart = FIRST_ENGINE_POINT,
    /// Finished the fetching, all input sets now local
    FetchingEnd,
    /// Start parsing (sync)
    ParsingStart,
    /// Finished Parsing (sync)
    ParsingEnd,
    /// Start loading code + alloc ctx (sync)
    LoadStart,
    /// Start data transfer to the ctx (sync)
    TransferStart,
    /// Start execution of the function on the engine (sync)
    EngineStart,
    /// End execution of the function on the engine (sync)
    EngineEnd = LAST_ENGINE_POINT,
    /// Return from execution engine (async)
    FutureReturn = LAST_RECORD_POINT,
}

#[cfg(feature = "timestamp")]
struct FunctionTimestamp {
    start_time: Instant,
    time_points: [core::cell::UnsafeCell<std::time::Duration>; LAST_RECORD_POINT + 1],
}
#[cfg(feature = "timestamp")]
unsafe impl Send for FunctionTimestamp {}
#[cfg(feature = "timestamp")]
unsafe impl Sync for FunctionTimestamp {}

#[cfg(feature = "timestamp")]
impl FunctionTimestamp {
    fn new(start_time: Instant) -> Self {
        return Self {
            start_time,
            time_points: [const { core::cell::UnsafeCell::new(std::time::Duration::ZERO) };
                LAST_RECORD_POINT + 1],
        };
    }

    fn record(self: &Self, current_point: RecordPoint) {
        let new_duration = self.start_time.elapsed();
        // each point is only present once in the code, so we can be sure we can write there safely,
        // and sice it is in arc know the memory exists and will not be dropped during writing
        let reference = core::cell::UnsafeCell::raw_get(&self.time_points[current_point as usize]);
        unsafe { *reference = new_duration };
    }
}

#[cfg(feature = "timestamp")]
impl fmt::Debug for FunctionTimestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(self.time_points.iter().map(|cell| unsafe { *cell.get() }))
            .finish()
    }
}

#[cfg(feature = "timestamp")]
impl fmt::Display for FunctionTimestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// General implementation of recorder struct, additional functionality enabled by flags
#[cfg(feature = "timestamp")]
struct InnerRecorder {
    /// The function ID for which this recorder was run.
    /// Needs to be enabled if any of the timestamp features are on.
    function_id: FunctionId,
    /// The timestamps for this invocation of a function / composition
    timestamps: FunctionTimestamp,
    /// If the function was a composition collect the timestamps of the child functions.
    /// Each function can have sharding, which is why we have a Vec<Vec<_>>, the outer Vec
    /// is indexed per function, while the inner ones are separate entries for each invocation.
    children: core::cell::OnceCell<Vec<Option<Vec<Recorder>>>>,
}

/// Structure to to hold all timestamps related to a single function invocation
/// All time is relative to the given global start time of the request
#[derive(Clone)]
pub struct Recorder {
    #[cfg(feature = "timestamp")]
    inner: std::sync::Arc<InnerRecorder>,
}

#[cfg(feature = "timestamp")]
unsafe impl Send for Recorder {}
#[cfg(feature = "timestamp")]
unsafe impl Sync for Recorder {}

impl Recorder {
    pub fn new(_function_id: FunctionId, _start: Instant) -> Self {
        return Self {
            #[cfg(feature = "timestamp")]
            inner: std::sync::Arc::new(InnerRecorder {
                function_id: _function_id,
                timestamps: FunctionTimestamp::new(_start),
                children: core::cell::OnceCell::new(),
            }),
        };
    }

    pub fn new_from_parent(_function_id: FunctionId, _parent: &Self) -> Self {
        return Self {
            #[cfg(feature = "timestamp")]
            inner: std::sync::Arc::new(InnerRecorder {
                function_id: _function_id,
                timestamps: FunctionTimestamp::new(_parent.inner.timestamps.start_time),
                children: core::cell::OnceCell::new(),
            }),
        };
    }

    pub fn record(&mut self, _current_point: RecordPoint) {
        #[cfg(feature = "timestamp")]
        self.inner.timestamps.record(_current_point);
    }

    pub fn add_children(&mut self, _children: Vec<Option<Vec<Recorder>>>) {
        #[cfg(feature = "timestamp")]
        let _ = self.inner.children.set(_children);
    }

    /// Get a slice with the timestamps related to engine execution
    #[cfg(feature = "timestamp")]
    pub fn get_timestamp(&self, point: RecordPoint) -> std::time::Duration {
        unsafe { *self.inner.timestamps.time_points[point as usize].get() }
    }

    #[cfg(feature = "timestamp")]
    pub fn set_timestamp(&mut self, point: RecordPoint, time_micros: u64) {
        use std::time::Duration;
        let reference =
            core::cell::UnsafeCell::raw_get(&self.inner.timestamps.time_points[point as usize]);
        unsafe { *reference = Duration::from_micros(time_micros) };
    }
}

impl fmt::Debug for Recorder {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        #[cfg(feature = "timestamp")]
        {
            use std::sync::Arc;
            if Arc::strong_count(&self.inner) + Arc::weak_count(&self.inner) > 1 {
                write!(_f, "Formatting Recorder with more than 1 references:\n")?;
            };
            _f.debug_struct("Recorder")
                .field("Function ID", &self.inner.function_id)
                .field("Timestamps", &self.inner.timestamps)
                .finish()
        }
        #[cfg(not(feature = "timestamp"))]
        Ok(())
    }
}

impl fmt::Display for Recorder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

use crate::FunctionId;
use core::fmt;
use std::{sync::Arc, time::Instant};

/// Maximum usize to expect when converting a record point to a usize
/// By setting the last element to this explicitly, the compiler will throw an error,
/// if there are more than this, because it enumerates from 0 and won't allow a number to be assigned twice.
const LAST_RECORD_POINT: usize = 11;

#[repr(usize)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RecordPoint {
    /// Frontend has finished deserializing
    DeserializationEnd,
    /// When the request first enters the dispatcher
    EnterDispatcher,
    /// Queue to get the function executed on the engine (async)
    ExecutionQueue,
    /// Start fetching of input sets
    FetchingStart,
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
    EngineEnd,
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
struct InnerRecorder {
    /// The function ID for which this recorder was run.
    /// Needs to be enabled if any of the timestamp features are on.
    #[cfg(feature = "timestamp")]
    function_id: FunctionId,
    /// The timestamps for this invocation of a function / composition
    #[cfg(feature = "timestamp")]
    timestamps: FunctionTimestamp,
    /// If the function was a composition collect the timestamps of the child functions.
    /// Each function can have sharding, which is why we have a Vec<Vec<_>>, the outer Vec
    /// is indexed per function, while the inner ones are separate entries for each invocation.
    #[cfg(feature = "timestamp")]
    children: core::cell::OnceCell<Vec<Option<Vec<Recorder>>>>,
}

/// Structure to to hold all timestamps related to a single function invocation
/// All time is relative to the given global start time of the request
#[derive(Clone)]
pub struct Recorder {
    inner: Arc<InnerRecorder>,
}

#[cfg(feature = "timestamp")]
unsafe impl Send for Recorder {}
#[cfg(feature = "timestamp")]
unsafe impl Sync for Recorder {}

impl Recorder {
    pub fn new(_function_id: FunctionId, _start: Instant) -> Self {
        return Self {
            inner: Arc::new(InnerRecorder {
                #[cfg(feature = "timestamp")]
                function_id: _function_id,
                #[cfg(feature = "timestamp")]
                timestamps: FunctionTimestamp::new(_start),
                #[cfg(feature = "timestamp")]
                children: core::cell::OnceCell::new(),
            }),
        };
    }

    pub fn new_from_parent(_function_id: FunctionId, _parent: &Self) -> Self {
        return Self {
            inner: Arc::new(InnerRecorder {
                #[cfg(feature = "timestamp")]
                function_id: _function_id,
                #[cfg(feature = "timestamp")]
                timestamps: FunctionTimestamp::new(_parent.inner.timestamps.start_time),
                #[cfg(feature = "timestamp")]
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
}

impl fmt::Debug for Recorder {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        #[cfg(feature = "timestamp")]
        {
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

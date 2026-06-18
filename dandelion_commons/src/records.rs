use crate::FunctionId;
use core::fmt;
use std::time::Instant;

#[cfg(feature = "timestamp")]
use core::cell::{OnceCell, UnsafeCell};

/// Maximum usize to expect when converting a record point to a usize
/// By setting the last element to this explicitly, the compiler will throw an error,
/// if there are more than this, because it enumerates from 0 and won't allow a number to be assigned twice.
const LAST_RECORD_POINT: usize = 21;
/// The first timestamp that should come from the engine running the function
const FIRST_ENGINE_POINT: usize = 13;
const LAST_ENGINE_POINT: usize = 20;

pub const ENGINE_RECORD_POINTS: usize = LAST_ENGINE_POINT - FIRST_ENGINE_POINT + 1;

#[repr(usize)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RecordPoint {
    /// Frontend has finished deserializing
    DeserializationEnd,
    /// When the request first enters the dispatcher
    EnterDispatcher,
    /// Queue to get the function executed on the engine (async)
    IOQueueStart,
    IOQueueEnd,
    ComputeQueueStart,
    ComputeQueueEnd,
    /// Time spend fetching on the composition owner, before offloading.
    /// Only set when the function was offloaded, otherwise the fetching time is recorded in fetching start  and end.
    MasterFetchStart,
    MasterFetchEnd,
    /// Time when a request was taken from the queue to send to the remote.
    /// Used to anchor the remote timings.
    RemoteTake,
    RemoteIOQueueStart,
    RemoteIOQueueEnd,
    RemoteComputeQueueStart,
    RemoteComputeQueueEnd,
    /// Start of fetching input sets on the node the function ends up running on
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
    time_points: [UnsafeCell<std::time::Duration>; LAST_RECORD_POINT + 1],
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
            time_points: [const { UnsafeCell::new(std::time::Duration::ZERO) };
                LAST_RECORD_POINT + 1],
        };
    }

    fn record(self: &Self, current_point: RecordPoint) {
        let new_duration = self.start_time.elapsed();
        // each point is only present once in the code, so we can be sure we can write there safely,
        // and sice it is in arc know the memory exists and will not be dropped during writing
        let reference = UnsafeCell::raw_get(&self.time_points[current_point as usize]);
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
        write!(f, "[",)?;
        // write own time points
        for index in 0..LAST_RECORD_POINT {
            let duration = unsafe { *self.time_points[index].get() };
            write!(f, "{},", duration.as_micros())?;
        }
        let duration = unsafe { *self.time_points[LAST_RECORD_POINT].get() };
        write!(f, "{}]", duration.as_micros())
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
    /// Id of the node that executed the function if it was executed remotely.
    /// Defaults to 0 for anything running locally regardless of actual node id.
    remote_node_id: UnsafeCell<u64>,
    /// Total number of input items
    input_items: UnsafeCell<u64>,
    /// Total size of all inputs
    input_size: UnsafeCell<u64>,
    /// If the function was a composition collect the timestamps of the child functions.
    /// Each function can have sharding, which is why we have a Vec<Vec<_>>, the outer Vec
    /// is indexed per function, while the inner ones are separate entries for each invocation.
    children: OnceCell<Vec<Option<Vec<Recorder>>>>,
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
                children: OnceCell::new(),
                remote_node_id: UnsafeCell::new(0),
                input_items: UnsafeCell::new(0),
                input_size: UnsafeCell::new(0),
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
                remote_node_id: UnsafeCell::new(0),
                input_items: UnsafeCell::new(0),
                input_size: UnsafeCell::new(0),
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

    #[cfg(feature = "timestamp")]
    pub fn record_input(&mut self, input_items: u64, input_size: u64) {
        unsafe {
            *UnsafeCell::raw_get(&self.inner.input_items) = input_items;
            *UnsafeCell::raw_get(&self.inner.input_size) = input_size;
        }
    }

    #[cfg(feature = "timestamp")]
    pub fn set_node_id(&mut self, node_id: u64) {
        unsafe {
            *UnsafeCell::raw_get(&self.inner.remote_node_id) = node_id;
        }
    }

    /// Get a slice with the timestamps related to engine execution
    #[cfg(feature = "timestamp")]
    pub fn get_timestamp(&self, point: RecordPoint) -> std::time::Duration {
        unsafe { *self.inner.timestamps.time_points[point as usize].get() }
    }

    #[cfg(feature = "timestamp")]
    pub fn set_timestamp(&mut self, point: RecordPoint, time_micros: u64) {
        use std::time::Duration;
        let reference = UnsafeCell::raw_get(&self.inner.timestamps.time_points[point as usize]);
        unsafe { *reference = Duration::from_micros(time_micros) };
    }

    /// Get a slice with the timestamps related to engine execution
    #[cfg(feature = "timestamp")]
    pub fn set_master_fetching(&self) {
        unsafe {
            *self.inner.timestamps.time_points[RecordPoint::MasterFetchStart as usize].get() =
                *self.inner.timestamps.time_points[RecordPoint::FetchingStart as usize].get();
            *self.inner.timestamps.time_points[RecordPoint::MasterFetchEnd as usize].get() =
                *self.inner.timestamps.time_points[RecordPoint::FetchingEnd as usize].get();
        }
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
                .field("Children", &self.inner.children)
                .field("Node ID", &self.inner.remote_node_id)
                .field("#Input items", &self.inner.input_items)
                .field("Total input size", &self.inner.input_size)
                .finish()
        }
        #[cfg(not(feature = "timestamp"))]
        Ok(())
    }
}

impl fmt::Display for Recorder {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        #[cfg(feature = "timestamp")]
        {
            let node_id = unsafe { *self.inner.remote_node_id.get() };
            let input_items = unsafe { *self.inner.input_items.get() };
            let input_size = unsafe { *self.inner.input_size.get() };
            write!(
                _f,
                "{{\"id\": \"{}\", \"ts\": {}, \"node id\": {}, \"items\": {}, \"input size\": {}, \"children\": [",
                self.inner.function_id, self.inner.timestamps, node_id, input_items, input_size,
            )?;
            let mut need_comma = false;
            if let Some(children) = self.inner.children.get() {
                for child in children.iter() {
                    if let Some(child_recorders) = child {
                        if need_comma {
                            write!(_f, ",")?;
                        }
                        write!(_f, "[")?;
                        let mut has_prev = false;
                        for r in child_recorders.iter() {
                            // FIXME: we ignore the (empty) HTTP recorders, ideally we do not create
                            //        them in the first place
                            if *r.inner.function_id == "HTTP" {
                                continue;
                            }
                            if has_prev {
                                write!(_f, ",")?;
                            }
                            write!(_f, "{}", r)?;
                            has_prev = true;
                        }
                        write!(_f, "]")?;
                        need_comma = true;
                    }
                }
            }
            write!(_f, "]}}")
        }
        #[cfg(not(feature = "timestamp"))]
        Ok(())
    }
}

use crate::FunctionId;
use core::fmt;
use std::time::Instant;
use std::sync::{Arc, Mutex};

/// Maximum usize to expect when converting a record point to a usize
/// By setting the last element to this explicitly, the compiler will throw an error,
/// if there are more than this, because it enumerates from 0 and won't allow a number to be assigned twice.
const LAST_RECORD_POINT: usize = 23;

#[repr(usize)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RecordPoint {
    /// Queue to load the function code + ctx
    PrepareEnvQueue,
    /// Enqueue parsing operation (async)
    ParsingQueue,
    /// Start parsing (sync)
    ParsingStart,
    /// Finished Parsing (sync)
    ParsingEnd,
    /// Dequeue from parsing (async)
    ParsingDequeue,
    /// Load function code (async)
    LoadQueue,
    /// Start loading code + alloc ctx (sync)
    LoadStart,
    /// End loading coad and ctx allocation (sync)
    LoadEnd,
    /// Promise await on loading returned (async)
    LoadDequeue,
    /// Enqueue transfer on work queue (async)
    TransferQueue,
    /// Start data transfer to the ctx (sync)
    TransferStart,
    /// End data transfer to the ctx (sync)
    TransferEnd,
    /// Promise await on transfer returned (async)
    TransferDequeue,
    /// Queue to get an engine for execution
    GetEngineQueue,
    /// Queue to get the function executed on the engine (async)
    ExecutionQueue,
    /// Start execution of the function on the engine (sync)
    EngineStart,
    /// End execution of the function on the engine (sync)
    EngineEnd,
    /// --- GPU ---
    /// Start GPU inputs and buffers load (sync)
    GPUTransferStart,
    /// End GPU inputs and buffers load (sync)
    GPUTransferEnd,
    /// Start GPU kernel executions (sync)
    GPUInferenceStart,
    /// End GPU kernel executions (sync)
    GPUInferenceEnd,
    /// Start GPU output read (sync)
    GPUOutputStart,
    /// End GPU output read (sync)
    GPUOutputEnd,
    /// Return from execution engine (async)
    FutureReturn = LAST_RECORD_POINT,
}

#[cfg(feature = "timestamp")]
struct FunctionTimestamp {
    function_id: FunctionId,
    creation: Instant,
    time_points: [core::cell::UnsafeCell<std::time::Duration>; LAST_RECORD_POINT + 1],
    children: std::sync::Mutex<Vec<FunctionTimestamp>>,
}
#[cfg(feature = "timestamp")]
unsafe impl Send for FunctionTimestamp {}
#[cfg(feature = "timestamp")]
unsafe impl Sync for FunctionTimestamp {}

#[cfg(feature = "timestamp")]
impl FunctionTimestamp {
    fn new(function_id: FunctionId, creation: Instant) -> std::sync::Arc<Self> {
        return std::sync::Arc::new(Self {
            function_id,
            creation,
            time_points: [const { core::cell::UnsafeCell::new(std::time::Duration::ZERO) };
                LAST_RECORD_POINT + 1],
            children: std::sync::Mutex::new(Vec::new()),
        });
    }

    fn record(self: &std::sync::Arc<Self>, current_point: RecordPoint) {
        let new_duration = self.creation.elapsed();
        // each point is only present once in the code, so we can be sure we can write there safely,
        // and sice it is in arc know the memory exists and will not be dropped during writing
        let reference = core::cell::UnsafeCell::raw_get(&self.time_points[current_point as usize]);
        unsafe { *reference = new_duration };
    }

    fn add_children(self: &mut std::sync::Arc<Self>, new_child: std::sync::Arc<Self>) {
        let mut guard = self.children.lock().unwrap();
        guard.push(std::sync::Arc::into_inner(new_child).unwrap());
    }
}

#[cfg(feature = "timestamp")]
impl fmt::Display for FunctionTimestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "function_id {}, time_points: ", self.function_id)?;
        // write own time points
        for index in 0..LAST_RECORD_POINT {
            let duration = unsafe { *self.time_points[index].get() };
            write!(f, "{},", duration.as_micros())?;
        }
        let duration = unsafe { *self.time_points[LAST_RECORD_POINT].get() };
        write!(f, "{}, children: {{", duration.as_micros())?;
        let child_guard = self.children.lock().unwrap();
        let num_children = child_guard.len();
        if num_children > 0 {
            for index in 0..num_children - 1 {
                write!(f, "[{}],", child_guard[index])?;
            }
            write!(f, "[{}]", child_guard[num_children - 1])?;
        }
        write!(f, "}}")?;
        Ok(())
    }
}

#[cfg(feature = "timestamp")]
struct TimestampArchive {
    collected_timestamps: std::sync::Mutex<Vec<FunctionTimestamp>>,
}

#[cfg(feature = "timestamp")]
impl TimestampArchive {
    fn init() -> Self {
        return Self {
            collected_timestamps: std::sync::Mutex::new(Vec::new()),
        };
    }

    fn insert(&self, new_timestamp: FunctionTimestamp) {
        let mut guard = self.collected_timestamps.lock().unwrap();
        guard.push(new_timestamp);
    }

    fn reset(&self) {
        let mut guard = self.collected_timestamps.lock().unwrap();
        *guard = Vec::new();
    }

    fn append_timestamps(
        &self,
        timestamp: &FunctionTimestamp,
        summary: &mut String,
        indent: usize,
    ) {
        // push self
        summary.push_str(&format!(
            "{}function id:{}, creation:{:?}, durations: {:?}",
            "-".repeat(indent),
            timestamp.function_id,
            timestamp.creation,
            timestamp.time_points
        ));
        let child_guard = timestamp.children.lock().unwrap();
        if child_guard.is_empty() {
            summary.push_str(", children: ");
        }
        summary.push('\n');
        for child in child_guard.iter() {
            self.append_timestamps(child, summary, indent + 1);
        }
    }

    fn get_summary(&self, summary: &mut String) {
        for recorder in self.collected_timestamps.lock().unwrap().iter() {
            self.append_timestamps(recorder, summary, 0);
            summary.push_str("\n");
        }
    }
}

#[cfg(feature = "reuse_weights")]
struct ReuseWeightsArchive {
    collected_gpu_cache_hit: std::sync::Mutex<Vec<bool>>,
}

#[cfg(feature = "reuse_weights")]
impl ReuseWeightsArchive {
    fn init() -> Self {
        return Self {
            collected_gpu_cache_hit: std::sync::Mutex::new(Vec::new()),
        };
    }

    fn insert(&self, new_gpu_cache_hit: bool) {
        let mut guard = self.collected_gpu_cache_hit.lock().unwrap();
        guard.push(new_gpu_cache_hit);
    }

    fn reset(&self) {
        let mut guard = self.collected_gpu_cache_hit.lock().unwrap();
        *guard = Vec::new();
    }

    fn append_gpu_cache_hit(
        &self,
        gpu_cache_hit: bool,
        summary: &mut String,
        indent: usize,
    ) {
        // push self
        summary.push_str(&format!(
            "{}gpu_cache_hit:{}",
            "-".repeat(indent),
            gpu_cache_hit
        ));
    }

    fn get_summary(&self, summary: &mut String) {
        for recorder in self.collected_gpu_cache_hit.lock().unwrap().iter() {
            self.append_gpu_cache_hit(*recorder, summary, 0);
            summary.push_str("\n");
        }
    }
}

/// General implementation of recorder struct, additional functionality enabled by flags
pub struct Recorder {
    #[cfg(feature = "timestamp")]
    timestamps: std::sync::Arc<FunctionTimestamp>,
    #[cfg(feature = "reuse_weights")]
    gpu_cache_hit: Arc<Mutex<bool>>,
}

impl Recorder {
    pub fn new(_function_id: FunctionId, _start: Instant) -> Self {
        return Self {
            #[cfg(feature = "timestamp")]
            timestamps: FunctionTimestamp::new(_function_id, _start),
            #[cfg(feature = "reuse_weights")]
            gpu_cache_hit: Arc::new(Mutex::new(false)),
        };
    }

    pub fn new_from_parent(_function_id: FunctionId, _parent: &Self) -> Self {
        return Self {
            #[cfg(feature = "timestamp")]
            timestamps: FunctionTimestamp::new(_function_id, _parent.timestamps.creation),
            #[cfg(feature = "reuse_weights")]
            gpu_cache_hit: Arc::new(Mutex::new(false)),
        };
    }

    pub fn record(&mut self, _current_point: RecordPoint) {
        #[cfg(feature = "timestamp")]
        self.timestamps.record(_current_point);
    }

    pub fn set_gpu_cache_hit(&mut self, _gpu_cache_hit: bool) {
        #[cfg(feature = "reuse_weights")]
        {
            let mut gpu_cache_hit = self.gpu_cache_hit.lock().unwrap();
            *gpu_cache_hit = _gpu_cache_hit;
        }
    }

    pub fn add_children(&mut self, _new_children: Vec<Recorder>) {
        #[cfg(feature = "timestamp")]
        for child in _new_children {
            self.timestamps.add_children(child.timestamps);
        }
    }

    pub fn get_sub_recorder(&self) -> Self {
        let recorder = Recorder {
            #[cfg(feature = "timestamp")]
            timestamps: self.timestamps.clone(),
            #[cfg(feature = "reuse_weights")]
            gpu_cache_hit: self.gpu_cache_hit.clone(),
        };
        return recorder;
    }
}

impl fmt::Display for Recorder {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        #[cfg(feature = "timestamp")]
        {
            if std::sync::Arc::strong_count(&self.timestamps) != 1
                && std::sync::Arc::weak_count(&self.timestamps) != 0
            {
                panic!("Trying to format recorder that still has more than one reference");
            }
            self.timestamps.fmt(_f)?;
        }
        #[cfg(feature = "reuse_weights")]
        {
            if std::sync::Arc::strong_count(&self.gpu_cache_hit) != 1
                && std::sync::Arc::weak_count(&self.gpu_cache_hit) != 0
            {
                panic!("Trying to format recorder that still has more than one reference");
            }
            #[cfg(feature = "timestamp")]
            write!(_f, ",")?;
            write!(_f, "gpu_cache_hit: {}", self.gpu_cache_hit.lock().unwrap())?;
        }
        Ok(())
    }
}

pub struct Archive {
    #[cfg(feature = "timestamp")]
    timestamp_archive: TimestampArchive,
    #[cfg(feature = "reuse_weights")]
    gpu_cache_hit_archive: ReuseWeightsArchive,
}

pub struct ArchiveInit {
    #[cfg(feature = "timestamp")]
    pub timestamp_count: usize,
}

impl Archive {
    pub fn init() -> Self {
        return Archive {
            #[cfg(feature = "timestamp")]
            timestamp_archive: TimestampArchive::init(),
            #[cfg(feature = "reuse_weights")]
            gpu_cache_hit_archive: ReuseWeightsArchive::init(),
        };
    }

    pub fn insert_recorder(&self, _recorder: Recorder) {
        #[cfg(feature = "timestamp")]
        self.timestamp_archive
            .insert(std::sync::Arc::into_inner(_recorder.timestamps).unwrap());
        #[cfg(feature = "reuse_weights")]
        self.gpu_cache_hit_archive
            .insert(std::sync::Arc::into_inner((*_recorder.gpu_cache_hit.lock().unwrap()).into()).unwrap());
    }

    pub fn get_summary(&self) -> String {
        // For each recorder, print the timestamps
        #[allow(unused_mut)]
        let mut summary = String::new();
        #[cfg(feature = "timestamp")]
        self.timestamp_archive.get_summary(&mut summary);
        #[cfg(feature = "reuse_weights")]
        self.gpu_cache_hit_archive.get_summary(&mut summary);
        println!("{}", summary);
        return summary;
    }

    pub fn reset(&self) {
        #[cfg(feature = "timestamp")]
        self.timestamp_archive.reset();
        #[cfg(feature = "reuse_weights")]
        self.gpu_cache_hit_archive.reset();
    }
}

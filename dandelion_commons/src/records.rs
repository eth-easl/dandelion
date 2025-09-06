use crate::FunctionId;
use core::fmt;
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Maximum usize to expect when converting a record point to a usize
/// By setting the last element to this explicitly, the compiler will throw an error,
/// if there are more than this, because it enumerates from 0 and won't allow a number to be assigned twice.
const LAST_RECORD_POINT: usize = 25;

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
    /// Start GPU output read (sync)
    BatchAtomStart,
    /// End GPU output read (sync)
    BatchAtomEnd,
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
    collected_gpu_id: std::sync::Mutex<Vec<u8>>,
}

#[cfg(feature = "reuse_weights")]
impl ReuseWeightsArchive {
    fn init() -> Self {
        return Self {
            collected_gpu_cache_hit: std::sync::Mutex::new(Vec::new()),
            collected_gpu_id: std::sync::Mutex::new(Vec::new()),
        };
    }

    fn insert(&self, new_gpu_cache_hit: bool, new_gpu_id: u8) {
        let mut guard_cache = self.collected_gpu_cache_hit.lock().unwrap();
        guard_cache.push(new_gpu_cache_hit);

        let mut guard_gpu = self.collected_gpu_id.lock().unwrap();
        guard_gpu.push(new_gpu_id);
    }

    fn reset(&self) {
        let mut guard_cache = self.collected_gpu_cache_hit.lock().unwrap();
        *guard_cache = Vec::new();

        let mut guard_gpu = self.collected_gpu_id.lock().unwrap();
        *guard_gpu = Vec::new();
    }

    fn append_gpu_info(&self, gpu_info: (bool, u8), summary: &mut String, indent: usize) {
        // push self
        summary.push_str(&format!(
            "{}gpu_cache_hit:{}, gpu_id:{}",
            "-".repeat(indent),
            gpu_info.0,
            gpu_info.1,
        ));
    }

    fn get_summary(&self, summary: &mut String) {
        for (gpu_cache_hit, gpu_id) in self.collected_gpu_cache_hit.lock().unwrap().iter().zip(self.collected_gpu_id.lock().unwrap().iter()) {
            let gpu_info = (*gpu_cache_hit, *gpu_id);
            self.append_gpu_info(gpu_info, summary, 0);
            summary.push_str("\n");
        }
    }
}

#[cfg(feature = "auto_batching")]
struct BatchArchive {
    collected_batch_size: std::sync::Mutex<Vec<usize>>,
}

#[cfg(feature = "auto_batching")]
impl BatchArchive {
    fn init() -> Self {
        return Self {
            collected_batch_size: std::sync::Mutex::new(Vec::new()),
        };
    }

    fn insert(&self, new_batch_size: usize) {
        let mut guard = self.collected_batch_size.lock().unwrap();
        guard.push(new_batch_size);
    }

    fn reset(&self) {
        let mut guard = self.collected_batch_size.lock().unwrap();
        *guard = Vec::new();
    }

    fn append_batch_size(&self, batch_size: usize, summary: &mut String, indent: usize) {
        // push self
        summary.push_str(&format!("{}batch_size:{}", "-".repeat(indent), batch_size));
    }

    fn get_summary(&self, summary: &mut String) {
        for recorder in self.collected_batch_size.lock().unwrap().iter() {
            self.append_batch_size(*recorder, summary, 0);
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
    #[cfg(feature = "reuse_weights")]
    gpu_id: Arc<Mutex<u8>>,
    #[cfg(feature = "auto_batching")]
    batch_size: Arc<Mutex<usize>>,
}

impl Recorder {
    pub fn new(_function_id: FunctionId, _start: Instant) -> Self {
        return Self {
            #[cfg(feature = "timestamp")]
            timestamps: FunctionTimestamp::new(_function_id, _start),
            #[cfg(feature = "reuse_weights")]
            gpu_cache_hit: Arc::new(Mutex::new(false)),
            #[cfg(feature = "reuse_weights")]
            gpu_id: Arc::new(Mutex::new(u8::MAX)),
            #[cfg(feature = "auto_batching")]
            batch_size: Arc::new(Mutex::new(0)),
        };
    }

    pub fn new_from_parent(_function_id: FunctionId, _parent: &Self) -> Self {
        return Self {
            #[cfg(feature = "timestamp")]
            timestamps: FunctionTimestamp::new(_function_id, _parent.timestamps.creation),
            #[cfg(feature = "reuse_weights")]
            gpu_cache_hit: Arc::new(Mutex::new(false)),
            #[cfg(feature = "reuse_weights")]
            gpu_id: Arc::new(Mutex::new(u8::MAX)),
            #[cfg(feature = "auto_batching")]
            batch_size: Arc::new(Mutex::new(0)),
        };
    }

    pub fn record(&mut self, _current_point: RecordPoint) {
        #[cfg(feature = "timestamp")]
        self.timestamps.record(_current_point);
    }

    pub fn set_gpu_info(&mut self, _gpu_cache_hit: bool, _gpu_id: u8) {
        #[cfg(feature = "reuse_weights")]
        {
            let mut gpu_cache_hit = self.gpu_cache_hit.lock().unwrap();
            *gpu_cache_hit = _gpu_cache_hit;
            let mut gpu_id = self.gpu_id.lock().unwrap();
            *gpu_id = _gpu_id;
        }
    }

    pub fn set_batch_size(&mut self, _batch_size: usize) {
        #[cfg(feature = "auto_batching")]
        {
            let mut batch_size = self.batch_size.lock().unwrap();
            *batch_size = _batch_size;
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
            #[cfg(feature = "reuse_weights")]
            gpu_id: self.gpu_id.clone(),
            #[cfg(feature = "auto_batching")]
            batch_size: self.batch_size.clone(),
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
            write!(_f, " gpu_cache_hit: {}, gpu_id: {}", self.gpu_cache_hit.lock().unwrap(), self.gpu_id.lock().unwrap())?;
        }
        #[cfg(feature = "auto_batching")]
        {
            if std::sync::Arc::strong_count(&self.batch_size) != 1
                && std::sync::Arc::weak_count(&self.batch_size) != 0
            {
                panic!("Trying to format recorder that still has more than one reference");
            }
            #[cfg(feature = "timestamp")]
            write!(_f, ",")?;
            write!(_f, " batch_size: {}", self.batch_size.lock().unwrap())?;
        }
        Ok(())
    }
}

pub struct Archive {
    #[cfg(feature = "timestamp")]
    timestamp_archive: TimestampArchive,
    #[cfg(feature = "reuse_weights")]
    gpu_info_archive: ReuseWeightsArchive,
    #[cfg(feature = "auto_batching")]
    batch_archive: BatchArchive,
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
            gpu_info_archive: ReuseWeightsArchive::init(),
            #[cfg(feature = "auto_batching")]
            batch_archive: BatchArchive::init(),
        };
    }

    pub fn insert_recorder(&self, _recorder: Recorder) {
        #[cfg(feature = "timestamp")]
        self.timestamp_archive
            .insert(std::sync::Arc::into_inner(_recorder.timestamps).unwrap());
        #[cfg(feature = "reuse_weights")]
        self.gpu_info_archive.insert(
            std::sync::Arc::into_inner((*_recorder.gpu_cache_hit.lock().unwrap()).into()).unwrap(),
            std::sync::Arc::into_inner((*_recorder.gpu_id.lock().unwrap()).into()).unwrap(),
        );
        #[cfg(feature = "auto_batching")]
        self.batch_archive.insert(
            std::sync::Arc::into_inner((*_recorder.batch_size.lock().unwrap()).into()).unwrap(),
        );
    }

    pub fn get_summary(&self) -> String {
        // For each recorder, print the timestamps
        #[allow(unused_mut)]
        let mut summary = String::new();
        #[cfg(feature = "timestamp")]
        self.timestamp_archive.get_summary(&mut summary);
        #[cfg(feature = "reuse_weights")]
        self.gpu_info_archive.get_summary(&mut summary);
        #[cfg(feature = "auto_batching")]
        self.batch_archive.get_summary(&mut summary);
        println!("{}", summary);
        return summary;
    }

    pub fn reset(&self) {
        #[cfg(feature = "timestamp")]
        self.timestamp_archive.reset();
        #[cfg(feature = "reuse_weights")]
        self.gpu_info_archive.reset();
        #[cfg(feature = "auto_batching")]
        self.batch_archive.reset();
    }
}

use crate::DandelionResult;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RecordPoint {
    /// Function request arrives at the server
    Arrival,
    /// Queue request in the dispatcher
    QueueFunctionDispatcher,
    /// Queue to load the function code + ctx
    PrepareEnvQueue,
    /// Load function code (async)
    LoadQueue,
    /// Start loading code + alloc ctx
    LoadStart,
    /// End loading coad and ctx allocation
    LoadEnd,
    /// Promise await on loading returned
    LoadDequeue,
    /// Enqueue transfer on work queue
    TransferQueue,
    /// Start data transfer to the ctx (async)
    TransferStart,
    /// End data transfer to the ctx (async)
    TransferEnd,
    /// Promise await on transfer returned
    TransferDequeue,
    /// Queue to get an engine for execution
    GetEngineQueue,
    /// Queue to get the function executed on the engine
    ExecutionQueue,
    /// Start execution of the function on the engine (sync)
    EngineStart,
    /// End execution of the function on the engine (sync)
    EngineEnd,
    /// Return from execution engine
    FutureReturn,
    /// Send response back to the client
    EndService,
}

#[cfg(feature = "timestamp")]
mod timestamp {
    use crate::{DandelionError, DandelionResult};
    use std::{
        sync::{atomic::AtomicU16, Arc, Mutex},
        time::Instant,
    };

    use super::RecordPoint;

    struct Timestamp {
        current_span: u16,
        parent_span: u16,
        time: Instant,
        point: RecordPoint,
        previous: Option<Box<Timestamp>>,
    }

    pub struct TimestampContainer {
        parent_span: u16,
        current_span: u16,
        spans_latest: Arc<(AtomicU16, Mutex<Option<Box<Timestamp>>>)>,
    }

    impl TimestampContainer {
        pub fn new() -> Self {
            return Self {
                parent_span: 0,
                current_span: 0,
                spans_latest: Arc::new((AtomicU16::new(0), Mutex::new(None))),
            };
        }

        pub fn record(
            &mut self,
            current_point: &super::RecordPoint,
            archive: &'static TimestampArchive,
        ) -> DandelionResult<()> {
            let mut new_timestamp = archive.get_timestamp()?;
            let mut span_guard = self.spans_latest.1.lock().unwrap();
            new_timestamp.point = *current_point;
            new_timestamp.time = Instant::now();
            new_timestamp.parent_span = self.parent_span;
            new_timestamp.current_span = self.current_span;
            new_timestamp.previous = span_guard.take();
            let _ = span_guard.insert(new_timestamp);
            return Ok(());
        }

        pub fn get_sub_recorder(&self) -> Self {
            let current_span = self
                .spans_latest
                .0
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                + 1;
            Self {
                parent_span: self.current_span,
                current_span,
                spans_latest: self.spans_latest.clone(),
            }
        }
    }

    pub struct TimestampArchive {
        start_time: Instant,
        free_timestamps: Mutex<Vec<Box<Timestamp>>>,
        used_timestamps: Mutex<Vec<Box<Timestamp>>>,
    }

    impl TimestampArchive {
        pub fn init(timestamp_number: usize) -> Self {
            let zero_time = Instant::now();
            let mut free_pool = Vec::new();
            free_pool.resize_with(timestamp_number, || {
                Box::new(Timestamp {
                    parent_span: 0,
                    current_span: 0,
                    time: zero_time,
                    point: RecordPoint::Arrival,
                    previous: None,
                })
            });
            let mut used_pool = Vec::new();
            used_pool.reserve(timestamp_number);
            return Self {
                start_time: zero_time,
                free_timestamps: Mutex::new(free_pool),
                used_timestamps: Mutex::new(used_pool),
            };
        }

        fn get_timestamp(&self) -> DandelionResult<Box<Timestamp>> {
            return self
                .free_timestamps
                .lock()
                .unwrap()
                .pop()
                .ok_or(DandelionError::RecorderNotAvailable);
        }

        pub fn return_timestamp(&self, used: TimestampContainer) {
            if let Some(timestamp) = Arc::into_inner(used.spans_latest)
                .unwrap()
                .1
                .into_inner()
                .unwrap()
                .take()
            {
                self.used_timestamps.lock().unwrap().push(timestamp);
            }
        }

        pub fn reset(&self) {
            // For each recorder in the used_recorders, reset it and move it to the free_recorders.
            // Recorders are recursive: each has a vector of sub-recorders.
            // TODO if we always set all values on record there is no need to do the resetting, apart from putting them into the free queue
            let mut free_guard = self.free_timestamps.lock().unwrap();
            for mut timestamp in self.used_timestamps.lock().unwrap().drain(..) {
                timestamp.parent_span = 0;
                timestamp.current_span = 0;
                timestamp.point = RecordPoint::Arrival;
                let mut timestamp_opt = timestamp.previous.take();
                free_guard.push(timestamp);
                while let Some(mut prev_timestamp) = timestamp_opt {
                    prev_timestamp.parent_span = 0;
                    prev_timestamp.current_span = 0;
                    prev_timestamp.point = RecordPoint::Arrival;
                    timestamp_opt = prev_timestamp.previous.take();
                    free_guard.push(prev_timestamp)
                }
            }
        }

        fn append_timestamps(&self, timestamp: &Box<Timestamp>, summary: &mut String) {
            if let Some(previous) = &timestamp.previous {
                self.append_timestamps(previous, summary);
            }
            summary.push_str(&format!(
                "parent:{}, span:{}, time:{}, point:{:?}, ",
                timestamp.parent_span,
                timestamp.current_span,
                timestamp.time.duration_since(self.start_time).as_nanos(),
                timestamp.point
            ))
        }

        pub fn get_summary(&self, summary: &mut String) {
            for recorder in self.used_timestamps.lock().unwrap().iter() {
                self.append_timestamps(recorder, summary);
                summary.push_str("\n");
            }
        }
    }
}

/// General implementation of recorder struct, additional functionality enabled by flags
pub struct Recorder {
    #[cfg(feature = "recorder")]
    archive: &'static Archive,
    #[cfg(feature = "timestamp")]
    timestamps: timestamp::TimestampContainer,
}

impl Recorder {
    pub fn record(&mut self, _current_point: RecordPoint) -> DandelionResult<()> {
        #[cfg(feature = "timestamp")]
        self.timestamps
            .record(&_current_point, &self.archive.timestamp_archive)?;
        Ok(())
    }

    pub fn get_sub_recorder(&self) -> DandelionResult<Self> {
        let recorder = Recorder {
            #[cfg(feature = "recorder")]
            archive: self.archive,
            #[cfg(feature = "timestamp")]
            timestamps: self.timestamps.get_sub_recorder(),
        };
        return Ok(recorder);
    }
}

pub struct Archive {
    #[cfg(feature = "timestamp")]
    timestamp_archive: timestamp::TimestampArchive,
}

pub struct ArchiveInit {
    #[cfg(feature = "timestamp")]
    pub timestamp_count: usize,
}

impl Archive {
    pub fn init(#[allow(unused_variables)] init_args: ArchiveInit) -> Self {
        return Archive {
            #[cfg(feature = "timestamp")]
            timestamp_archive: timestamp::TimestampArchive::init(init_args.timestamp_count),
        };
    }

    pub fn get_recorder(&'static self) -> DandelionResult<Recorder> {
        return Ok(Recorder {
            #[cfg(feature = "recorder")]
            archive: self,
            #[cfg(feature = "timestamp")]
            timestamps: timestamp::TimestampContainer::new(),
        });
    }

    pub fn return_recorder(&self, _recorder: Recorder) {
        #[cfg(feature = "timestamp")]
        self.timestamp_archive
            .return_timestamp(_recorder.timestamps);
    }

    pub fn get_summary(&self) -> String {
        // For each recorder, print the timestamps
        #[allow(unused_mut)]
        let mut summary = String::new();
        #[cfg(feature = "timestamp")]
        self.timestamp_archive.get_summary(&mut summary);
        return summary;
    }

    pub fn reset(&self) {
        #[cfg(feature = "timestamp")]
        self.timestamp_archive.reset();
    }
}

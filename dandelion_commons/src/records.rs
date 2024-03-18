use crate::DandelionResult;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RecordPoint {
    Arrival,                 // Function request arrives at the server
    QueueFunctionDispatcher, // Queue request in the dispatcher
    PrepareEnvQueue,         // Queue to load the function code + ctx
    LoadQueue,               // Load function code (async)
    LoadStart,               // Start loading code + alloc ctx
    TransferStart,           // Start data transfer to the ctx (async)
    TransferEnd,             // End data transfer to the ctx (async)
    GetEngineQueue,          // Queue to get an engine for execution
    ExecutionQueue,          // Queue to get the function executed on the engine
    EngineStart,             // Start execution of the function on the engine (sync)
    EngineEnd,               // End execution of the function on the engine (sync)
    FutureReturn,            // Return from execution engine
    EndService,              // Send response back to the client
}

#[cfg(feature = "timestamp")]
mod timestamp {
    use crate::{DandelionError, DandelionResult};
    use std::{
        env,
        sync::{Arc, Mutex},
        time::Instant,
    };

    pub struct TimestampContainer {
        timestamps: Vec<Instant>,
        pub recorders: Arc<Mutex<Vec<TimestampContainer>>>,
    }

    impl TimestampContainer {
        pub fn record(&mut self, current_point: &super::RecordPoint) {
            self.timestamps[*current_point as usize] = Instant::now();
        }
        fn reset(&mut self, zero_time: Instant, freed: &mut Vec<TimestampContainer>) {
            self.timestamps = vec![zero_time; 13];
            for mut container in Arc::get_mut(&mut self.recorders)
                .unwrap()
                .get_mut()
                .unwrap()
                .drain(..)
            {
                container.reset(zero_time, freed);
                freed.push(container);
            }
        }
    }

    pub struct TimestampArchive {
        start_time: Instant,
        free_recorders: Mutex<Vec<TimestampContainer>>,
        used_recorders: Mutex<Vec<TimestampContainer>>,
    }

    impl TimestampArchive {
        pub fn init() -> Self {
            let pool_size = match env::var("DANDELION_TIMESTAMP_COUNT") {
                Ok(container_count_string) => container_count_string.parse().unwrap_or(100),
                Err(_) => 100,
            };
            let zero_time = Instant::now();
            let mut free_pool = Vec::new();
            free_pool.resize_with(pool_size, || TimestampContainer {
                timestamps: vec![zero_time; 13],
                recorders: Arc::new(Mutex::new(vec![])),
            });
            let mut used_pool = Vec::new();
            used_pool.reserve(pool_size);
            return Self {
                start_time: zero_time,
                free_recorders: Mutex::new(free_pool),
                used_recorders: Mutex::new(used_pool),
            };
        }

        pub fn get_timestamp_container(&self) -> DandelionResult<TimestampContainer> {
            return self
                .free_recorders
                .lock()
                .unwrap()
                .pop()
                .ok_or(DandelionError::RecorderNotAvailable);
        }

        pub fn return_timestamp_container(&self, used: TimestampContainer) {
            self.used_recorders.lock().unwrap().push(used);
        }

        pub fn reset(&self) {
            // For each recorder in the used_recorders, reset it and move it to the free_recorders.
            // Recorders are recursive: each has a vector of sub-recorders.
            let mut free_guard = self.free_recorders.lock().unwrap();
            for mut container in self.used_recorders.lock().unwrap().drain(..) {
                container.reset(self.start_time, &mut free_guard);
                free_guard.push(container);
            }
        }

        fn append_timestamps(
            &self,
            timestamps: &TimestampContainer,
            summary: &mut String,
            level: usize,
        ) {
            let tabs = "\t".repeat(level);
            for (i, timestamp) in timestamps.timestamps.iter().enumerate() {
                let duration = timestamp.duration_since(self.start_time).as_nanos();
                if duration > 0 {
                    summary.push_str(&format!("{}{}: {}\n", tabs, i, duration));
                };
            }

            for sub_recorder in timestamps.recorders.lock().unwrap().iter() {
                self.append_timestamps(sub_recorder, summary, level + 1);
            }
        }

        pub fn get_summary(&self, summary: &mut String) {
            for recorder in self.used_recorders.lock().unwrap().iter() {
                self.append_timestamps(recorder, summary, 0);
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
        self.timestamps.record(&_current_point);
        Ok(())
    }

    pub fn get_new_recorder(&self) -> DandelionResult<Self> {
        #[cfg(feature = "recorder")]
        return self.archive.get_recorder();
        #[cfg(not(feature = "recorder"))]
        return Ok(Recorder {});
    }

    pub fn link_recorder(&mut self, _new_recorder: Recorder) {
        #[cfg(feature = "timestamp")]
        self.timestamps
            .recorders
            .lock()
            .unwrap()
            .push(_new_recorder.timestamps);
    }
}

pub struct Archive {
    #[cfg(feature = "timestamp")]
    timestamp_archive: timestamp::TimestampArchive,
}

impl Archive {
    pub fn init() -> Self {
        return Archive {
            #[cfg(feature = "timestamp")]
            timestamp_archive: timestamp::TimestampArchive::init(),
        };
    }

    pub fn get_recorder(&'static self) -> DandelionResult<Recorder> {
        return Ok(Recorder {
            #[cfg(feature = "recorder")]
            archive: self,
            #[cfg(feature = "timestamp")]
            timestamps: self.timestamp_archive.get_timestamp_container()?,
        });
    }

    pub fn return_recorder(&self, _recorder: Recorder) {
        #[cfg(feature = "timestamp")]
        self.timestamp_archive
            .return_timestamp_container(_recorder.timestamps);
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

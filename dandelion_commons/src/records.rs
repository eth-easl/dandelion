use crate::{DandelionError, DandelionResult};
use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

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

// TODO: becomes a trait + feature flag to enable/disable in the server (?) that switches between different recorders.
pub struct Recorder {
    archive: Arc<Mutex<Archive>>,
    timestamps: Vec<Instant>,
    recorders: Vec<Recorder>,
}

impl Recorder {
    pub fn new(archive: Arc<Mutex<Archive>>) -> Recorder {
        let start_time = archive.lock().unwrap().start_time;
        Recorder {
            archive,
            timestamps: vec![start_time; 13],
            recorders: Vec::with_capacity(10),
        }
    }

    pub fn record(&mut self, current_point: RecordPoint) -> DandelionResult<()> {
        self.timestamps[current_point as usize] = Instant::now();
        Ok(())
    }

    pub fn get_new_recorder(&mut self) -> DandelionResult<Recorder> {
        match self.archive.lock().unwrap().get_recorder() {
            Some(recorder) => Ok(recorder),
            None => Err(DandelionError::RecorderNotAvailable),
        }
    }

    pub fn link_new_recorder(&mut self, new_recorder: Recorder) {
        self.recorders.push(new_recorder);
    }

    pub fn reset_recorder(&mut self, zero_time: Instant, free_recorders: &mut Vec<Recorder>) {
        // Reset the timestamps
        self.timestamps = vec![zero_time; 13];
        // Reset the sub-recorders
        for mut recorder in self.recorders.drain(..) {
            recorder.reset_recorder(zero_time, free_recorders);
            free_recorders.push(recorder);
        }
    }
}

pub struct Archive {
    start_time: Instant,
    free_recorders: Vec<Recorder>,
    used_recorders: Vec<Recorder>,
}

impl Archive {
    pub fn new() -> Archive {
        Archive {
            start_time: Instant::now(),
            free_recorders: Vec::new(),
            used_recorders: Vec::new(),
        }
    }

    pub fn init(&mut self, free_recorders: Vec<Recorder>) {
        self.free_recorders = free_recorders;
        self.used_recorders.reserve(self.free_recorders.capacity());
    }

    pub fn get_recorder(&mut self) -> Option<Recorder> {
        self.free_recorders.pop()
    }

    pub fn add_recorder(&mut self, recorder: Recorder) {
        self.used_recorders.push(recorder);
    }

    fn append_timestamps(&self, recorder: &Recorder, summary: &mut String, level: usize) {
        let tabs = "\t".repeat(level);
        for (i, timestamp) in recorder.timestamps.iter().enumerate() {
            let duration = timestamp.duration_since(self.start_time).as_nanos();
            if duration > 0 {
                summary.push_str(&format!("{}{}: {}\n", tabs, i, duration));
            };
        }

        for sub_recorder in &recorder.recorders {
            self.append_timestamps(sub_recorder, summary, level + 1);
        }
    }

    pub fn get_summary(&self) -> String {
        // For each recorder, print the timestamps
        let mut summary = String::new();
        for recorder in &self.used_recorders {
            self.append_timestamps(recorder, &mut summary, 0);
        }
        summary
    }

    pub fn reset_all(&mut self) {
        // For each recorder in the used_recorders, reset it and move it to the free_recorders.
        // Recorders are recursive: each has a vector of sub-recorders.
        for mut recorder in self.used_recorders.drain(..) {
            recorder.reset_recorder(self.start_time, &mut self.free_recorders);
            self.free_recorders.push(recorder);
        }
    }
}

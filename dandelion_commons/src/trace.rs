#[cfg(any(feature = "trace-data", feature = "trace-timestamps"))]
use std::{
    cell::{OnceCell, UnsafeCell},
    sync::Arc,
};
use std::{fmt, time::Instant};

#[cfg(feature = "trace-data")]
use crate::trace::data::DataCollection;
#[cfg(feature = "trace-timestamps")]
use crate::trace::timestamp::TimestampCollection;
use crate::{trace::timestamp::Timestamp, FunctionId};

#[cfg(feature = "trace-data")]
pub mod data;
// Always compiled as `Timestamp` is part of the Recorder API even when tracing is disabled.
pub mod timestamp;

#[cfg(any(feature = "trace-data", feature = "trace-timestamps"))]
struct Inner {
    /// The function id for which this recorder was run. TODO: what is it for compositions?
    function_id: FunctionId,
    /// The id of the node that executed the function.
    /// Defaults to 0 for local execution regardless of the actual node id.
    node_id: UnsafeCell<u64>,
    /// The timestamps of all functions if this recorder records a composition execution.
    /// The outer vector indexes the function, the inner vector indexes the invocation of that function.
    children: OnceCell<Vec<Option<Vec<Recorder>>>>,

    /// The timestamp traces (only collected with `trace-timestamps` feature).
    #[cfg(feature = "trace-timestamps")]
    timestamps: TimestampCollection,

    /// The data traces (only collected with `trace-data` feature).
    #[cfg(feature = "trace-data")]
    data: DataCollection,
}

#[cfg(any(feature = "trace-data", feature = "trace-timestamps"))]
unsafe impl Send for Inner {}
#[cfg(any(feature = "trace-data", feature = "trace-timestamps"))]
unsafe impl Sync for Inner {}

#[derive(Clone)]
pub struct Recorder {
    #[cfg(any(feature = "trace-data", feature = "trace-timestamps"))]
    inner: std::sync::Arc<Inner>,
}

unsafe impl Send for Recorder {}
unsafe impl Sync for Recorder {}

impl Drop for Recorder {
    fn drop(&mut self) {}
}

impl Recorder {
    /// `_num_sets` is the number of data sets whose size is traced (with `trace-data`).
    pub fn new(_function_id: FunctionId, _num_sets: usize, _start: Instant) -> Self {
        Self {
            #[cfg(any(feature = "trace-data", feature = "trace-timestamps"))]
            inner: Arc::new(Inner {
                function_id: _function_id,
                node_id: UnsafeCell::new(0),
                children: OnceCell::new(),
                #[cfg(feature = "trace-timestamps")]
                timestamps: TimestampCollection::new(_start),
                #[cfg(feature = "trace-data")]
                data: DataCollection::new(_num_sets),
            }),
        }
    }

    pub fn new_from_parent(_function_id: FunctionId, _num_sets: usize, _parent: &Self) -> Self {
        Self {
            #[cfg(any(feature = "trace-data", feature = "trace-timestamps"))]
            inner: std::sync::Arc::new(Inner {
                function_id: _function_id,
                node_id: UnsafeCell::new(0),
                children: core::cell::OnceCell::new(),
                #[cfg(feature = "trace-timestamps")]
                timestamps: TimestampCollection::new(_parent.inner.timestamps.start),
                #[cfg(feature = "trace-data")]
                data: DataCollection::new(_num_sets),
            }),
        }
    }

    pub fn add_children(&mut self, _children: Vec<Option<Vec<Recorder>>>) {
        #[cfg(any(feature = "trace-data", feature = "trace-timestamps"))]
        let _ = self.inner.children.set(_children);
    }

    /// Assuming this is only set once for a node.
    pub fn set_node_id(&mut self, _node_id: u64) {
        #[cfg(any(feature = "trace-data", feature = "trace-timestamps"))]
        unsafe {
            *UnsafeCell::raw_get(&self.inner.node_id) = _node_id;
        }
    }

    /// Assuming a timestamp is set only once.
    pub fn set_timestamp_now(&self, _ts: Timestamp) {
        #[cfg(feature = "trace-timestamps")]
        self.inner.timestamps.set_now(_ts);
    }

    /// Assuming a timestamp is set only once.
    pub fn set_timestamp(&self, _ts: Timestamp, _time: Instant) {
        #[cfg(feature = "trace-timestamps")]
        self.inner.timestamps.set(_ts, _time);
    }

    /// Assuming the data of a set is recorded only once.
    pub fn set_data(&self, _set_index: usize, _items: u64, _size: u64) {
        #[cfg(feature = "trace-data")]
        self.inner.data.set(_set_index, _items, _size);
    }
}

impl fmt::Debug for Recorder {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        #[cfg(any(feature = "trace-data", feature = "trace-timestamps"))]
        {
            use std::sync::Arc;
            if Arc::strong_count(&self.inner) + Arc::weak_count(&self.inner) > 1 {
                writeln!(_f, "Formatting Recorder with more than 1 references:")?;
            };
            let mut dbg_struct = _f.debug_struct("Recorder");
            dbg_struct
                .field("function_id", &self.inner.function_id)
                .field("node_id", &self.inner.node_id)
                .field("children", &self.inner.children);

            #[cfg(feature = "trace-timestamps")]
            dbg_struct.field("timestamps", &self.inner.timestamps);
            #[cfg(feature = "trace-data")]
            dbg_struct.field("data", &self.inner.data);

            dbg_struct.finish()
        }
        #[cfg(all(not(feature = "trace-data"), not(feature = "trace-timestamps")))]
        Ok(())
    }
}

impl fmt::Display for Recorder {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Formats as a JSON struct.
        #[cfg(any(feature = "trace-data", feature = "trace-timestamps"))]
        {
            let node_id = unsafe { *self.inner.node_id.get() };
            write!(
                _f,
                "{{\"id\": \"{}\", \"nodeId\": {}",
                self.inner.function_id, node_id,
            )?;

            #[cfg(feature = "trace-timestamps")]
            write!(_f, ", \"ts\": {}", self.inner.timestamps)?;

            #[cfg(feature = "trace-data")]
            write!(_f, ", \"data\": {}", self.inner.data)?;

            write!(_f, ", \"children\": [")?;
            let mut need_comma = false;
            if let Some(children) = self.inner.children.get() {
                for child_recorders in children.iter().flatten() {
                    if need_comma {
                        write!(_f, ",")?;
                    }
                    write!(_f, "[")?;
                    let mut has_prev = false;
                    for r in child_recorders.iter() {
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
            write!(_f, "]}}")
        }
        #[cfg(all(not(feature = "trace-data"), not(feature = "trace-timestamps")))]
        Ok(())
    }
}

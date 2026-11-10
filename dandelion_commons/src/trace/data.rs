use std::{cell::UnsafeCell, fmt};

/// Data recorded for a single set.
#[derive(Default)]
struct SetData {
    /// Number of items in the set.
    items: UnsafeCell<u64>,
    /// Total size of all items in the set in bytes.
    size: UnsafeCell<u64>,
}

/// Per-set data traces. The number of sets is fixed at creation but only known at runtime.
pub(crate) struct DataCollection {
    sets: Box<[SetData]>,
}

unsafe impl Send for DataCollection {}
unsafe impl Sync for DataCollection {}

impl DataCollection {
    pub(crate) fn new(num_sets: usize) -> Self {
        Self {
            sets: (0..num_sets).map(|_| SetData::default()).collect(),
        }
    }

    pub(crate) fn set(&self, set_index: usize, items: u64, size: u64) {
        // Each set should only be set once in the code, so overwriting it here is safe.
        match self.sets.get(set_index) {
            Some(set) => unsafe {
                *UnsafeCell::raw_get(&set.items) = items;
                *UnsafeCell::raw_get(&set.size) = size;
            },
            None => debug_assert!(
                false,
                "set index {} out of range for recorder with {} sets",
                set_index,
                self.sets.len()
            ),
        }
    }
}

impl SetData {
    fn values(&self) -> (u64, u64) {
        unsafe { (*self.items.get(), *self.size.get()) }
    }
}

impl fmt::Debug for DataCollection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list()
            .entries(self.sets.iter().map(SetData::values))
            .finish()
    }
}

impl fmt::Display for DataCollection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Formats as a JSON array: `[{"items": n, "size": s}, ...]`.
        write!(f, "[")?;
        for (index, set) in self.sets.iter().enumerate() {
            if index > 0 {
                write!(f, ",")?;
            }
            let (items, size) = set.values();
            write!(f, "{{\"items\": {}, \"size\": {}}}", items, size)?;
        }
        write!(f, "]")
    }
}

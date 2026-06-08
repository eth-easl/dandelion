use std::collections::BTreeMap;

use crate::composition::CompositionSet;

pub struct CacheRegistry {
    // Maps from cache key to cached result
    inner: BTreeMap<u64, Vec<Option<CompositionSet>>>,
}

impl CacheRegistry {
    pub fn new() -> Self {
        CacheRegistry {
            inner: BTreeMap::new(),
        }
    }

    pub fn get(&self, key: u64) -> Option<Vec<Option<CompositionSet>>> {
        self.inner.get(&key).cloned()
    }

    pub fn insert(&mut self, key: u64, value: Vec<Option<CompositionSet>>) {
        self.inner.insert(key, value);
    }
}

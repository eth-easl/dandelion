use crate::composition::RemoteData;
use std::{collections::BTreeMap, sync::Mutex};

#[derive(Clone, Debug)]
pub struct HttpCacheEntry {
    pub header: RemoteData,
    pub body: RemoteData,
}

pub struct CacheRegistry {
    // Maps from cache key to cached result
    inner: Mutex<BTreeMap<u64, HttpCacheEntry>>,
}

impl CacheRegistry {
    pub fn new() -> Self {
        CacheRegistry {
            inner: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn get(&self, key: u64) -> Option<HttpCacheEntry> {
        self.inner.lock().unwrap().get(&key).cloned()
    }

    pub fn insert(&self, key: u64, value: HttpCacheEntry) {
        self.inner.lock().unwrap().insert(key, value);
    }
}

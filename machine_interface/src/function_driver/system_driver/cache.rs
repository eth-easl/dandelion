use crate::composition::RemoteData;
use std::{collections::BTreeMap, sync::RwLock};

#[derive(Clone, Debug)]
pub struct HttpCacheEntry {
    pub header: RemoteData,
    pub body: RemoteData,
}

pub struct CacheRegistry {
    // Maps from cache key to cached result
    inner: RwLock<BTreeMap<u64, HttpCacheEntry>>,
}

impl CacheRegistry {
    pub fn new() -> Self {
        CacheRegistry {
            inner: RwLock::new(BTreeMap::new()),
        }
    }

    pub fn get(&self, key: u64) -> Option<HttpCacheEntry> {
        self.inner.read().unwrap().get(&key).cloned()
    }

    pub fn insert(&self, key: u64, value: HttpCacheEntry) {
        self.inner.write().unwrap().insert(key, value);
    }
}

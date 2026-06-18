use crate::composition::RemoteData;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::RwLock,
};

pub const DEFAULT_MAX_CACHE_SIZE: usize = 1024 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct HttpCacheEntry {
    pub header: RemoteData,
    pub header_size: usize,
    pub body: RemoteData,
    pub body_size: usize,
}

impl HttpCacheEntry {
    pub fn size(&self) -> usize {
        self.header_size.saturating_add(self.body_size)
    }
}

struct CacheRecord {
    entry: HttpCacheEntry,
    last_access: u64,
}

struct CacheState {
    entries: BTreeMap<u64, CacheRecord>,
    access_order: VecDeque<(u64, u64)>,
    current_size: usize,
    next_access: u64,
    max_size: usize,
}

pub struct CacheRegistry {
    // Maps from cache key to cached result
    inner: RwLock<CacheState>,
}

impl CacheRegistry {
    pub fn new() -> Self {
        Self::with_max_size(DEFAULT_MAX_CACHE_SIZE)
    }

    pub fn with_max_size(max_size: usize) -> Self {
        CacheRegistry {
            inner: RwLock::new(CacheState {
                entries: BTreeMap::new(),
                access_order: VecDeque::new(),
                current_size: 0,
                next_access: 0,
                max_size,
            }),
        }
    }

    pub fn get(&self, key: u64) -> Option<HttpCacheEntry> {
        let mut state = self.inner.write().unwrap();
        let access = state.next_access;
        state.next_access = state.next_access.wrapping_add(1);
        if let Some(record) = state.entries.get_mut(&key) {
            record.last_access = access;
            let entry = record.entry.clone();
            state.access_order.push_back((key, access));
            Some(entry)
        } else {
            None
        }
    }

    pub fn insert(&self, key: u64, value: HttpCacheEntry) {
        let mut state = self.inner.write().unwrap();
        let access = state.next_access;
        state.next_access = state.next_access.wrapping_add(1);
        let value_size = value.size();
        if let Some(record) = state.entries.remove(&key) {
            state.current_size = state.current_size.saturating_sub(record.entry.size());
        }
        state.current_size = state.current_size.saturating_add(value_size);
        state.entries.insert(
            key,
            CacheRecord {
                entry: value,
                last_access: access,
            },
        );
        state.access_order.push_back((key, access));
        self.evict_if_needed(&mut state);
    }

    pub fn set_max_size(&self, max_size: usize) {
        let mut state = self.inner.write().unwrap();
        state.max_size = max_size;
        self.evict_if_needed(&mut state);
    }

    fn evict_if_needed(&self, state: &mut CacheState) {
        while state.current_size > state.max_size {
            let Some((key, access)) = state.access_order.pop_front() else {
                break;
            };
            let is_current = match state.entries.get(&key) {
                Some(record) => record.last_access == access,
                None => false,
            };
            if !is_current {
                continue;
            }
            if let Some(record) = state.entries.remove(&key) {
                state.current_size = state.current_size.saturating_sub(record.entry.size());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(key: u64, header_size: usize, body_size: usize) -> HttpCacheEntry {
        HttpCacheEntry {
            header: RemoteData::new(0, key * 2),
            header_size,
            body: RemoteData::new(0, key * 2 + 1),
            body_size,
        }
    }

    #[test]
    fn returns_cached_entry_with_sizes() {
        let cache = CacheRegistry::with_max_size(100);
        cache.insert(1, entry(1, 10, 20));

        let cached = cache.get(1).expect("entry should be cached");
        assert_eq!(10, cached.header_size);
        assert_eq!(20, cached.body_size);
        assert_eq!(30, cached.size());
    }

    #[test]
    fn evicts_least_recently_used_entries_over_max_size() {
        let cache = CacheRegistry::with_max_size(50);
        cache.insert(1, entry(1, 10, 10));
        cache.insert(2, entry(2, 10, 10));
        assert!(cache.get(1).is_some());

        cache.insert(3, entry(3, 10, 20));

        assert!(cache.get(1).is_some());
        assert!(cache.get(2).is_none());
        assert!(cache.get(3).is_some());
    }

    #[test]
    fn does_not_keep_entries_larger_than_max_size() {
        let cache = CacheRegistry::with_max_size(10);
        cache.insert(1, entry(1, 10, 1));

        assert!(cache.get(1).is_none());
    }
}

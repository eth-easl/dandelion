use crate::composition::{CompositionSet, ShardingMode};
use dandelion_commons::FunctionId;
use futures::lock::Mutex;
use log::trace;
use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hash, Hasher},
};

pub fn get_output_hash_id(
    function_id: FunctionId,
    inputs: &Vec<Option<(ShardingMode, CompositionSet)>>,
) -> u64 {
    trace!("Getting hash id for function {} output", function_id);
    let mut state = DefaultHasher::new();
    function_id.hash(&mut state);
    inputs.hash(&mut state);
    let hash = state.finish();
    trace!("Got hash id for function {} output = {}", function_id, hash);
    hash
}

pub struct OutputCache {
    inner: Mutex<HashMap<u64, Vec<Option<CompositionSet>>>>,
}

impl OutputCache {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }

    pub async fn insert(&self, key: u64, value: Vec<Option<CompositionSet>>) {
        let value_old = self.inner.lock().await.insert(key, value);
        assert!(value_old.is_none());
    }

    pub async fn get(&self, key: u64) -> Option<Vec<Option<CompositionSet>>> {
        self.inner.lock().await.get(&key).map(|o| o.clone())
    }
}

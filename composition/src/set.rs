use std::{
    debug_assert_matches, mem,
    sync::{Arc, Mutex},
};

use dandelion_commons::data::{DataItem, DataSet, DataSetAccumulator, Invocation};
use log::error;

use crate::{sharding::AnyShardingMode, Function};

#[derive(Debug)]
enum State {
    Pending(DataSetAccumulator),
    Complete(DataSet),
    /// State while collecting the accumulator and putting the dataset into the Complete state.
    Transitioning,
}

struct Inner {
    state: State,
    consumers_blocking: Vec<(Arc<Function>, usize)>,
    consumers_streaming: Vec<(Arc<Function>, usize)>,
    requires_sorting: bool,
    requires_retention: bool,
}

pub struct CompositionSet {
    inner: Mutex<Inner>,
}

impl CompositionSet {
    pub fn new() -> CompositionSet {
        CompositionSet {
            inner: Mutex::new(Inner {
                state: State::Pending(DataSetAccumulator::new()),
                consumers_blocking: Vec::new(),
                consumers_streaming: Vec::new(),
                requires_sorting: false,
                requires_retention: false,
            }),
        }
    }

    pub fn add_consumer(
        &self,
        consumer: Arc<Function>,
        consumer_in_set_idx: usize,
        blocking: bool,
        requires_sorting: bool,
        consumer_num_params: usize,
    ) {
        let mut inner = self.inner.lock().expect("CompositionSet lock poisoned!");
        inner.requires_sorting |= requires_sorting;
        inner.requires_retention |= consumer_num_params > 1;
        if blocking {
            inner.requires_retention = true;
            inner
                .consumers_blocking
                .push((consumer, consumer_in_set_idx));
        } else {
            inner
                .consumers_streaming
                .push((consumer, consumer_in_set_idx));
        }
    }

    /// Keeps the set retained regardless of the consumers.
    pub fn mark_retained(&self) {
        self.inner
            .lock()
            .expect("CompositionSet lock poisoned!")
            .requires_retention = true;
    }

    pub fn is_complete(&self) -> bool {
        let inner = self.inner.lock().expect("CompositionSet lock poisoned!");
        match inner.state {
            State::Complete(_) => true,
            _ => false,
        }
    }

    /// Pushes new items to the set.
    /// The items are forwarded immediately to all streaming consumers and retained for blocking
    /// consumers that are only informed when the set is complete.
    pub fn push_items(
        &self,
        items: Arc<Vec<Arc<DataItem>>>,
        complete: bool,
        any_sharding_mode: &AnyShardingMode,
    ) -> Vec<Invocation> {
        let mut inner = self.inner.lock().expect("CompositionSet lock poisoned!");
        debug_assert!(
            !matches!(inner.state, State::Complete(_)),
            "Tried adding an item to a complete CompositionSet!"
        );

        let mut invocations = Vec::new();
        for (f, set_idx) in inner.consumers_streaming.iter() {
            invocations.extend(f.push_streaming_items(
                *set_idx,
                items.clone(),
                complete,
                any_sharding_mode,
            ));
        }
        if inner.requires_retention {
            if let State::Pending(acc) = &mut inner.state {
                acc.push_items(&items);
            } else {
                error!(
                    "CompositionSet requires retention but does not have a pending accumulator."
                );
            }
        }

        let blocking_consumers = if complete {
            if let State::Pending(acc) = mem::replace(&mut inner.state, State::Transitioning) {
                if inner.requires_sorting {
                    inner.state = State::Complete(acc.collect());
                } else {
                    inner.state = State::Complete(acc.collect_unsorted());
                }
            }
            inner.consumers_blocking.clone()
        } else {
            Vec::new()
        };
        // Drop the inner lock so any consumer trying to call get_set on this set doesn't deadlock.
        drop(inner);

        for (f, _) in blocking_consumers.iter() {
            invocations.extend(f.in_set_complete(any_sharding_mode));
        }
        invocations
    }

    /// Sets the composition set to the given set.
    /// Assumes the set is ordered if the input requires ordering (unchecked) and does not notify
    /// consumers that the set is now complete.
    pub fn set_composition_input(&self, set: DataSet) {
        let mut inner = self.inner.lock().expect("CompositionSet lock poisoned!");
        debug_assert!(
            !matches!(inner.state, State::Complete(_)),
            "CompositionSet is alreay complete!"
        );
        let prev_state = mem::replace(&mut inner.state, State::Complete(set));
        debug_assert_matches!(prev_state, State::Pending(_));
    }

    /// Returns the current set.
    /// If a caller is given that is currently listed as a blocking caller, it will be automatically
    ///  turned into a streaming consumer.
    pub fn get_set(&self, caller_opt: Option<Arc<Function>>) -> DataSet {
        let mut inner = self.inner.lock().expect("CompositionSet lock poisoned!");

        if let Some(caller) = caller_opt {
            if let Some(pos) = inner
                .consumers_blocking
                .iter()
                .position(|(c, _)| Arc::ptr_eq(&c, &caller))
            {
                let consumer = inner.consumers_blocking.swap_remove(pos);
                inner.consumers_streaming.push(consumer);
            }
        }

        match inner.state {
            State::Pending(ref acc) => acc.clone_set(),
            State::Complete(ref set) => set.clone(),
            _ => panic!("Unexpected CompositionSet state"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::Function;
    use dandelion_commons::data::Position;

    fn item(key: u32) -> Arc<DataItem> {
        Arc::new(DataItem {
            ident: format!("item-{key}"),
            data: Position { offset: 0, size: 0 },
            key,
        })
    }

    fn items(keys: &[u32]) -> Arc<Vec<Arc<DataItem>>> {
        Arc::new(keys.iter().copied().map(item).collect())
    }

    #[test]
    fn fresh_set_is_pending_and_empty() {
        let set = CompositionSet::new();
        assert!(!set.is_complete());
        assert!(set.get_set(None).is_empty());
    }

    #[test]
    fn blocking_consumer_is_retained_and_notified_on_completion() {
        let set = CompositionSet::new();
        // A freshly constructed Function hasn't been resolved by `in_set_complete` yet; `set`
        // notifying it as a blocking consumer below is exactly what resolves it.
        let consumer = Arc::new(Function::new(0, Arc::new("Consumer".to_string()), vec![]));
        set.add_consumer(consumer.clone(), 0, /* blocking */ true, false, 1);

        let invocations = set.push_items(items(&[1, 2]), true, &AnyShardingMode::MaxSharding);
        assert!(
            invocations.is_empty(),
            "the consumer has no configured inputs/outputs of its own, so it produces nothing"
        );
        assert!(set.is_complete());
        assert!(
            !set.get_set(None).is_empty(),
            "a blocking consumer must retain the pushed items so it can read them once complete"
        );
        assert!(
            consumer.is_complete(),
            "the blocking consumer should have been notified via in_set_complete"
        );
    }

    #[test]
    fn streaming_only_single_param_consumer_is_not_retained_but_still_completes() {
        let set = CompositionSet::new();
        let consumer = Arc::new(Function::new(0, Arc::new("Consumer".to_string()), vec![]));
        consumer.update_io(vec![], vec![], vec![]);
        set.add_consumer(consumer.clone(), 0, /* blocking */ false, false, 1);
        // A function must be resolved by `in_set_complete` at least once (as
        // `Composition::start_execution` does for every function) before it's valid to receive a
        // streaming push.
        consumer.in_set_complete(&AnyShardingMode::MaxSharding);

        set.push_items(items(&[1]), true, &AnyShardingMode::MaxSharding);

        assert!(set.is_complete());
        // A lone single-param streaming consumer already received the items directly through
        // `push_streaming_items`, so the set itself doesn't need to retain a copy.
        assert!(set.get_set(None).is_empty());
        assert!(consumer.is_complete());
    }

    #[test]
    fn mark_retained_keeps_items_even_without_any_consumer() {
        let set = CompositionSet::new();
        set.mark_retained();

        set.push_items(items(&[1, 2, 3]), true, &AnyShardingMode::MaxSharding);

        let result = set.get_set(None);
        assert_eq!(result.items.len(), 3);
    }
}

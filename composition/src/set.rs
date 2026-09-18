use std::{
    mem,
    sync::{Arc, Mutex},
};

use log::error;
use memory::data::{DataItem, DataSet, DataSetAccumulator, Invocation};

use crate::{sharding::AnyShardingMode, Function};

#[derive(Debug)]
enum State {
    Empty,
    Pending(DataSetAccumulator),
    Complete(DataSet),
}

struct Inner {
    state: State,
    consumers_blocking: Vec<(Arc<Function>, usize)>,
    consumers_streaming: Vec<(Arc<Function>, usize)>,
    requires_sorting: bool,
    requires_retention: bool,
}

pub(crate) struct CompositionSet {
    inner: Mutex<Inner>,
}

impl CompositionSet {
    pub(crate) fn new() -> CompositionSet {
        CompositionSet {
            inner: Mutex::new(Inner {
                state: State::Empty,
                consumers_blocking: Vec::new(),
                consumers_streaming: Vec::new(),
                requires_sorting: false,
                requires_retention: false,
            }),
        }
    }

    pub(crate) fn add_blocking_consumer(
        &self,
        consumer: Arc<Function>,
        consumer_in_set_idx: usize,
        requires_sorting: bool,
    ) {
        let mut inner = self.inner.lock().expect("CompositionSet lock poisoned!");
        inner
            .consumers_blocking
            .push((consumer, consumer_in_set_idx));
        inner.requires_sorting |= requires_sorting;
        inner.requires_retention = true;
        if matches!(inner.state, State::Empty) {
            inner.state = State::Pending(DataSetAccumulator::new());
        }
    }

    pub(crate) fn add_non_blocking_consumer(
        &self,
        consumer: Arc<Function>,
        consumer_in_set_idx: usize,
    ) {
        let mut inner = self.inner.lock().expect("CompositionSet lock poisoned!");
        inner
            .consumers_streaming
            .push((consumer, consumer_in_set_idx));
    }

    pub(crate) fn mark_retained(&self) {
        let mut inner = self.inner.lock().expect("CompositionSet lock poisoned!");
        inner.requires_retention = true;
        if matches!(inner.state, State::Empty) {
            inner.state = State::Pending(DataSetAccumulator::new());
        }
    }

    /// Pushes new items to the set.
    /// The items are forwarded immediately to all streaming consumers and retained if there are
    /// any other (blocking or non-blocking) consumers.
    pub(crate) fn push_items(
        &self,
        items: Arc<Vec<Arc<DataItem>>>,
        complete: bool,
        any_sharding_mode: &AnyShardingMode,
        out: &mut impl Extend<Invocation>,
    ) {
        let mut inner = self.inner.lock().expect("CompositionSet lock poisoned!");
        debug_assert!(
            !matches!(inner.state, State::Complete(_)),
            "Tried adding an item to a complete CompositionSet!"
        );

        for (f, set_idx) in inner.consumers_streaming.iter() {
            f.push_streaming_items(*set_idx, items.clone(), complete, any_sharding_mode, out);
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

        if complete {
            if let State::Pending(acc) = mem::replace(&mut inner.state, State::Empty) {
                let data_set = match inner.requires_sorting {
                    true => acc.collect(),
                    false => acc.collect_unsorted(),
                };
                for (c, set_idx) in inner.consumers_blocking.iter() {
                    c.in_set_complete(data_set.clone(), *set_idx, any_sharding_mode, out);
                }
                inner.state = State::Complete(data_set);
            }
        }
    }

    /// Sets the composition set to the given set.
    /// Assumes the set is ordered if the input requires ordering (unchecked).
    pub(crate) fn set_composition_input(
        &self,
        set: DataSet,
        any_sharding_mode: &AnyShardingMode,
        out: &mut impl Extend<Invocation>,
    ) {
        let inner = self.inner.lock().expect("CompositionSet lock poisoned!");
        debug_assert!(
            !matches!(inner.state, State::Complete(_)),
            "CompositionSet is alreay complete!"
        );

        for (c, set_idx) in inner.consumers_blocking.iter() {
            c.in_set_complete(set.clone(), *set_idx, any_sharding_mode, out);
        }
        for (c, set_idx) in inner.consumers_streaming.iter() {
            c.push_streaming_items(*set_idx, set.items.clone(), true, any_sharding_mode, out);
        }
    }

    pub(crate) fn get_set(&self) -> DataSet {
        let inner = self.inner.lock().expect("CompositionSet lock poisoned!");
        match inner.state {
            State::Pending(ref acc) => acc.clone_set(),
            State::Complete(ref set) => set.clone(),
            State::Empty => DataSet::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{function::Function, sharding::Sharding};
    use memory::{data::Position, Context};

    /// A small real context for tests; the composition layer only tracks items, it never reads
    /// their bytes, so any context of sufficient size will do.
    fn test_context() -> Arc<Context> {
        use memory::context::{malloc::MallocMemoryDomain, MemoryDomain, MemoryResource};
        let domain = MallocMemoryDomain::init(MemoryResource::None).expect("malloc domain");
        Arc::new(domain.acquire_context(4096).expect("context"))
    }

    fn item(key: u32) -> Arc<DataItem> {
        Arc::new(DataItem::new_local(
            format!("item-{key}"),
            key,
            test_context(),
            Position { offset: 0, size: 0 },
        ))
    }

    fn items(keys: &[u32]) -> Arc<Vec<Arc<DataItem>>> {
        Arc::new(keys.iter().copied().map(item).collect())
    }

    #[test]
    fn fresh_set_is_pending_and_empty() {
        let set = CompositionSet::new();
        assert!(set.get_set().is_empty());
    }

    #[test]
    fn blocking_consumer_is_retained_and_notified_on_completion() {
        let set = CompositionSet::new();
        // `set` notifying its blocking consumer below is what resolves the function.
        let mut consumer = Function::new(0, Arc::new("Consumer".to_string()), vec![0].into());
        consumer.update_io(&[Some((Sharding::All, false))], vec![], vec![]);
        let consumer = Arc::new(consumer);
        set.add_blocking_consumer(consumer.clone(), 0, false);

        let mut invocations = Vec::new();
        set.push_items(
            items(&[1, 2]),
            true,
            &AnyShardingMode::MaxSharding,
            &mut invocations,
        );
        assert_eq!(
            invocations.len(),
            1,
            "the consumer's single `all` input runs exactly once"
        );
        assert!(
            !set.get_set().is_empty(),
            "a blocking consumer must retain the pushed items so it can read them once complete"
        );
        assert!(
            !consumer.is_complete(),
            "the blocking consumer should have been notified via in_set_complete and started its invocation"
        );
        consumer.add_invocation_output(vec![], &AnyShardingMode::MaxSharding, &mut Vec::new());
        assert!(consumer.is_complete());
    }

    #[test]
    fn streaming_only_single_param_consumer_is_not_retained_but_still_completes() {
        let set = CompositionSet::new();
        let mut consumer = Function::new(0, Arc::new("Consumer".to_string()), vec![0].into());
        consumer.update_io(&[Some((Sharding::Each, false))], vec![], vec![]);
        let consumer = Arc::new(consumer);
        set.add_non_blocking_consumer(consumer.clone(), 0);

        let mut invocations = Vec::new();
        set.push_items(
            items(&[1]),
            true,
            &AnyShardingMode::MaxSharding,
            &mut invocations,
        );
        assert_eq!(
            invocations.len(),
            1,
            "one invocation for the single streamed item"
        );

        // A lone single-param streaming consumer already received the items directly through
        // `push_streaming_items`, so the set itself doesn't need to retain a copy.
        assert!(set.get_set().is_empty());
        assert!(
            !consumer.is_complete(),
            "its invocation is still outstanding"
        );
        consumer.add_invocation_output(vec![], &AnyShardingMode::MaxSharding, &mut Vec::new());
        assert!(consumer.is_complete());
    }

    #[test]
    fn mark_retained_keeps_items_even_without_any_consumer() {
        let set = CompositionSet::new();
        set.mark_retained();

        set.push_items(
            items(&[1, 2, 3]),
            true,
            &AnyShardingMode::MaxSharding,
            &mut Vec::new(),
        );

        let result = set.get_set();
        assert_eq!(result.items.len(), 3);
    }
}

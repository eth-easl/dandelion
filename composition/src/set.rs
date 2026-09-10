use std::{
    debug_assert_matches, mem,
    sync::{Arc, Mutex, Weak},
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
    consumers_blocking: Vec<(Weak<Function>, usize)>,
    consumers_streaming: Vec<(Weak<Function>, usize)>,
}

pub struct CompositionSet {
    requires_sorting: bool,
    requires_retention: bool,
    inner: Mutex<Inner>,
}

impl CompositionSet {
    pub fn new(
        consumers_blocking: Vec<(Weak<Function>, usize)>,
        consumers_streaming: Vec<(Weak<Function>, usize)>,
        requires_sorting: bool,
        requires_retention: bool,
    ) -> CompositionSet {
        CompositionSet {
            requires_sorting,
            requires_retention,
            inner: Mutex::new(Inner {
                state: State::Pending(DataSetAccumulator::new()),
                consumers_blocking,
                consumers_streaming,
            }),
        }
    }

    pub fn is_empty(&self) -> bool {
        let inner = self.inner.lock().expect("CompositionSet lock poisoned!");
        match inner.state {
            State::Pending(ref acc) => acc.is_empty(),
            State::Complete(ref set) => set.is_empty(),
            _ => panic!("Unexpected CompositionSet state"),
        }
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
            invocations.extend(f.upgrade().unwrap().push_streaming_items(
                *set_idx,
                items.clone(),
                complete,
                any_sharding_mode,
            ));
        }
        if self.requires_retention {
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
                if self.requires_sorting {
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
            invocations.extend(f.upgrade().unwrap().in_set_complete(any_sharding_mode));
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
                .position(|(c, _)| Arc::ptr_eq(&c.upgrade().unwrap(), &caller))
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

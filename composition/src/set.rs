use std::{
    mem,
    sync::{Arc, Mutex, Weak},
};

use dandelion_commons::data::{DataItem, DataSet, DataSetAccumulator, Invocation};

use crate::{sharding::AnyShardingMode, Function};

enum State {
    Pending(DataSetAccumulator),
    Complete(DataSet),
    Transitioning, // TODO: can we get rid of this state?
}

struct Inner {
    state: State,
    consumers_blocking: Vec<(Weak<Function>, usize)>,
    consumers_streaming: Vec<(Weak<Function>, usize)>,
}

pub struct CompositionSet {
    requires_sorting: bool,
    requires_retention: bool,
    inner: Mutex<Inner>, // TODO: better to use a read-write-lock here?
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

    pub fn push_items(
        &self,
        items: Vec<Arc<DataItem>>,
        complete: bool,
        any_sharding_mode: &AnyShardingMode,
    ) -> Vec<Invocation> {
        let mut inner = self.inner.lock().expect("CompositionSet lock poisoned!");
        assert!(
            !self.is_complete(),
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
            }
        }

        if complete {
            if let State::Pending(acc) = mem::replace(&mut inner.state, State::Transitioning) {
                if self.requires_sorting {
                    inner.state = State::Complete(acc.collect());
                } else {
                    inner.state = State::Complete(acc.collect_unsorted());
                }
            }
            for (f, _) in inner.consumers_blocking.iter() {
                invocations.extend(f.upgrade().unwrap().in_set_complete(any_sharding_mode));
            }
        }
        invocations
    }

    pub fn get_set(&self, blocking_caller: &Arc<Function>) -> DataSet {
        let mut inner = self.inner.lock().expect("CompositionSet lock poisoned!");

        if let Some(pos) = inner
            .consumers_blocking
            .iter()
            .position(|(c, _)| Arc::ptr_eq(&c.upgrade().unwrap(), blocking_caller))
        {
            let consumer = inner.consumers_blocking.swap_remove(pos);
            inner.consumers_streaming.push(consumer);
        }

        match inner.state {
            State::Pending(ref acc) => acc.clone_set(),
            State::Complete(ref set) => set.clone(),
            _ => panic!("Unexpected CompositionSet state"),
        }
    }
}

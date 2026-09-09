mod join_iterator;
mod set;
mod sharding;

use std::{
    mem,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, Weak,
    },
};

use crate::{
    set::CompositionSet,
    sharding::{create_sharding_iter, AnyShardingMode, Sharding},
};
use dandelion_commons::{
    data::{DataItem, DataSet, Invocation},
    FunctionId,
};
use log::trace;

struct Inner {
    inputs: Vec<(Arc<CompositionSet>, Sharding)>,
    blocking: bool,
    all_started: bool,
}

pub struct Function {
    composition_idx: usize,
    num_outstanding: AtomicUsize,
    outputs: Vec<Weak<CompositionSet>>,

    inner: Mutex<Inner>, // TODO: better to use a read-write-lock here?

    function_id: FunctionId,
    join_order: Vec<usize>,
    min_set_bytes: Vec<usize>,
}

impl Function {
    pub fn new(
        idx: usize,
        function_id: FunctionId,
        inputs: Vec<(Arc<CompositionSet>, Sharding)>,
        join_order: Vec<usize>,
        min_set_bytes: Vec<usize>,
        outputs: Vec<Weak<CompositionSet>>,
    ) -> Function {
        let blocking = inputs.iter().any(|(_, sharding)| sharding.is_blocking());
        Function {
            composition_idx: idx,
            num_outstanding: AtomicUsize::new(0),
            outputs,
            inner: Mutex::new(Inner {
                inputs,
                blocking,
                all_started: false,
            }),
            function_id,
            join_order,
            min_set_bytes,
        }
    }

    pub fn is_complete(&self) -> bool {
        {
            let inner = self.inner.lock().expect("Function lock poisoned!");
            if !inner.all_started {
                return false;
            }
        }
        self.num_outstanding
            .load(std::sync::atomic::Ordering::Acquire)
            == 0
    }

    pub fn in_set_complete(
        self: &Arc<Self>,
        any_sharding_mode: &AnyShardingMode,
    ) -> Vec<Invocation> {
        let mut inner = self.inner.lock().expect("Function lock poisoned!");
        assert!(
            inner.blocking,
            "Function.in_set_complete called on non-blocking function."
        );

        let mut blocking = false;
        let mut complete = false;
        for (set, sharding) in &inner.inputs {
            let set_complete = set.is_complete();
            blocking |= sharding.is_blocking() && !set_complete;
            complete |= set_complete;
            if complete && blocking {
                break;
            }
        }

        let invocations = if !blocking {
            let sets = inner
                .inputs
                .iter()
                .map(|(set, sharding)| (set.get_set(self), sharding.clone()))
                .collect();
            self.create_invocations(sets, any_sharding_mode)
        } else {
            vec![]
        };

        self.num_outstanding
            .fetch_add(invocations.len(), Ordering::AcqRel);
        inner.all_started = complete;
        invocations
    }

    pub fn push_streaming_items(
        self: &Arc<Self>,
        set_idx: usize,
        mut items: Vec<Arc<DataItem>>,
        complete: bool,
        any_sharding_mode: &AnyShardingMode,
    ) -> Vec<Invocation> {
        let mut inner = self.inner.lock().expect("Function lock poisoned!");
        assert!(
            !inner.blocking,
            "Function.push_streaming_item called on blocking function."
        );

        let mut sets = Vec::with_capacity(inner.inputs.len());
        for (idx, (set, sharding)) in inner.inputs.iter().enumerate() {
            if idx == set_idx {
                // this is the case once so we can pass ownership with a mem::take
                sets.push((DataSet::from_items(mem::take(&mut items)), sharding.clone()));
            } else {
                sets.push((set.get_set(self), sharding.clone()));
            }
        }

        let invocations = self.create_invocations(sets, any_sharding_mode);

        self.num_outstanding
            .fetch_add(invocations.len(), Ordering::AcqRel);
        if complete {
            inner.all_started = inner.inputs.iter().all(|(set, _)| set.is_complete());
        }
        invocations
    }

    fn create_invocations(
        &self,
        sets: Vec<(DataSet, Sharding)>,
        any_sharding_mode: &AnyShardingMode,
    ) -> Vec<Invocation> {
        let num_sets = sets.len();

        let sharding_iter = create_sharding_iter(
            sets,
            self.join_order.clone(), // TODO: can we do this better so we do not need to clone here?
            any_sharding_mode,
            self.min_set_bytes.clone(), // TODO: can we do this better so we do not need to clone here?
        );

        let mut invocations = Vec::new();
        if let Some(mut iter) = sharding_iter {
            let mut new_sets = Vec::with_capacity(num_sets);
            new_sets.resize(num_sets, DataSet::default());
            iter.fill_in(&mut new_sets);
            invocations.push(Invocation {
                function_id: self.function_id.clone(),
                composition_idx: self.composition_idx,
                input: new_sets,
            });
            while iter.advance() {
                let mut advance_sets = Vec::with_capacity(num_sets);
                advance_sets.resize(num_sets, DataSet::default());
                iter.fill_in(&mut advance_sets);
                invocations.push(Invocation {
                    function_id: self.function_id.clone(),
                    composition_idx: self.composition_idx,
                    input: advance_sets,
                });
            }
        }
        trace!("Computed sharding: {:?}", invocations);
        invocations
    }

    pub fn add_invocation_output(
        &self,
        sets: Vec<DataSet>,
        any_sharding_mode: &AnyShardingMode,
    ) -> Vec<Invocation> {
        debug_assert_eq!(sets.len(), self.outputs.len());

        self.num_outstanding.fetch_sub(1, Ordering::AcqRel);
        let complete = self.is_complete();

        if complete {
            let mut inner = self.inner.lock().expect("Function lock poisoned!");
            inner.inputs.clear();
        }

        let mut invocations = Vec::new();
        for (i, set) in sets.into_iter().enumerate() {
            invocations.extend(
                self.outputs[i]
                    .upgrade()
                    .expect("Output set is gone")
                    .push_items(set.items, complete, any_sharding_mode),
            );
        }
        invocations
    }
}

pub struct Composition {
    functions: Vec<Arc<Function>>,
    any_sharding_mode: AnyShardingMode,
    outstanding: AtomicUsize, // track when composition execution is complete
    input_sets: Vec<Weak<CompositionSet>>,
    output_sets: Vec<Arc<CompositionSet>>,
}

impl Composition {
    pub fn new() -> Self {
        todo!("implement composition creation (from parser directly)")
    }

    pub fn start_execution(&self, composition_inputs: Vec<DataSet>) -> Vec<Invocation> {
        todo!("add composition input to sets, then trigger an in_set_complete for each function to get the initial runnable functions")
    }

    pub fn push_invocation_output(
        &self,
        output: Vec<DataSet>,
        composition_idx: usize,
    ) -> Vec<Invocation> {
        todo!("add the invocation output to the function that created the invocation, it will forward the data to downstream functions and return any new invocations")
    }
}

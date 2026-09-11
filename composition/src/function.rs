use std::{
    mem,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
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
    inputs: Vec<Option<(Arc<CompositionSet>, Sharding, bool)>>,
    outputs: Vec<Option<Arc<CompositionSet>>>,
    blocking: bool,
    all_started: bool,
}

pub struct Function {
    composition_idx: usize,
    num_outstanding: AtomicUsize,

    inner: Mutex<Inner>,

    function_id: FunctionId,
    join_order: Vec<usize>,
    min_set_bytes: Vec<usize>, // TODO: currently broken -> fix with metadata reference
}

impl Function {
    pub fn new(
        idx: usize,
        function_id: FunctionId,
        join_order: Vec<usize>,
        min_set_bytes: Vec<usize>,
    ) -> Function {
        Function {
            composition_idx: idx,
            num_outstanding: AtomicUsize::new(0),
            inner: Mutex::new(Inner {
                inputs: vec![],
                outputs: vec![],
                blocking: true,
                all_started: false,
            }),
            function_id,
            join_order,
            min_set_bytes,
        }
    }

    pub fn update_io(
        &self,
        inputs: Vec<Option<(Arc<CompositionSet>, Sharding, bool)>>,
        outputs: Vec<Option<Arc<CompositionSet>>>,
    ) {
        let mut inner = self.inner.lock().expect("Function lock poisoned!");
        inner.blocking = inputs.iter().any(|in_opt| {
            in_opt
                .as_ref()
                .map_or(false, |(_, sharding, _)| sharding.is_blocking())
        });
        // min_set_bytes.resize(inputs.len(), 0);
        inner.inputs = inputs;
        inner.outputs = outputs;
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

        let mut blocking = false;
        let mut complete = true;
        for in_opt in &inner.inputs {
            if let Some((set, sharding, _)) = in_opt {
                let set_complete = set.is_complete();
                blocking |= sharding.is_blocking() && !set_complete;
                complete &= set_complete;
                if !complete && blocking {
                    break;
                }
            }
        }

        let mut invocations = Vec::new();
        if !blocking {
            debug_assert!(
                inner.blocking,
                "Function.in_set_complete can only transition once from blocking to non-blocking!"
            );
            inner.blocking = false;

            let mut sets = Vec::with_capacity(inner.inputs.len());
            let mut runnable = true;
            for in_opt in inner.inputs.iter() {
                if let Some((set, sharding, optional)) = in_opt {
                    let data_set = set.get_set(Some(self.clone()));
                    if !optional && data_set.is_empty() {
                        runnable = false;
                        break;
                    }
                    sets.push(Some((data_set, sharding.clone())));
                } else {
                    sets.push(None);
                }
            }
            if runnable {
                invocations = self.create_invocations(sets, any_sharding_mode);
            }
        }

        self.num_outstanding
            .fetch_add(invocations.len(), Ordering::AcqRel);
        inner.all_started = complete;
        drop(inner);

        if complete && invocations.is_empty() {
            // If this function ever created invocations it became non-blocking after which the
            // push_streaming_item function is called instead of this one. Therefore, we can
            // assume that if invocations is empty there are also no outstanding invocations.
            invocations = self.push_output_sets(vec![], true, any_sharding_mode);
        }

        invocations
    }

    pub fn push_streaming_items(
        self: &Arc<Self>,
        set_idx: usize,
        mut items: Arc<Vec<Arc<DataItem>>>,
        complete: bool,
        any_sharding_mode: &AnyShardingMode,
    ) -> Vec<Invocation> {
        let mut inner = self.inner.lock().expect("Function lock poisoned!");
        debug_assert!(
            !inner.blocking,
            "Function.push_streaming_item called on blocking function."
        );

        let sets = inner
            .inputs
            .iter()
            .enumerate()
            .map(|(idx, in_opt)| {
                in_opt.as_ref().map(|(set, sharding, _)| {
                    // TODO: handle weird streaming
                    if idx == set_idx {
                        // this is the case once so we can pass ownership with a mem::take
                        (DataSet::from_items(mem::take(&mut items)), sharding.clone())
                    } else {
                        (set.get_set(Some(self.clone())), sharding.clone())
                    }
                })
            })
            .collect();

        let invocations = self.create_invocations(sets, any_sharding_mode);

        self.num_outstanding
            .fetch_add(invocations.len(), Ordering::AcqRel);
        if complete {
            inner.all_started = inner.inputs.iter().all(|in_opt| {
                in_opt
                    .as_ref()
                    .map_or(true, |(set, _, _)| set.is_complete())
            });
        }
        invocations
    }

    fn create_invocations(
        &self,
        sets: Vec<Option<(DataSet, Sharding)>>,
        any_sharding_mode: &AnyShardingMode,
    ) -> Vec<Invocation> {
        let num_sets = sets.len();

        let sharding_iter = create_sharding_iter(
            sets,
            &self.join_order,
            any_sharding_mode,
            &self.min_set_bytes,
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
        self.num_outstanding.fetch_sub(1, Ordering::AcqRel);
        let complete = self.is_complete();
        self.push_output_sets(sets, complete, any_sharding_mode)
    }

    fn push_output_sets(
        &self,
        sets: Vec<DataSet>,
        complete: bool,
        any_sharding_mode: &AnyShardingMode,
    ) -> Vec<Invocation> {
        let mut inner = self.inner.lock().expect("Function lock poisoned!");
        if complete {
            inner.inputs.clear();
        }

        let mut invocations = Vec::new();
        if sets.is_empty() {
            let empty_items = Arc::new(vec![]);
            for out_set_opt in inner.outputs.iter() {
                if let Some(out_set) = out_set_opt {
                    invocations.extend(out_set.push_items(
                        empty_items.clone(),
                        complete,
                        any_sharding_mode,
                    ));
                }
            }
        } else {
            debug_assert_eq!(sets.len(), inner.outputs.len());
            for (data, out_set_opt) in sets.into_iter().zip(inner.outputs.iter()) {
                if let Some(out_set) = out_set_opt {
                    invocations.extend(out_set.push_items(data.items, complete, any_sharding_mode));
                }
            }
        }

        if complete {
            inner.outputs.clear()
        }

        invocations
    }
}

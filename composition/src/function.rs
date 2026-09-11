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
    min_set_bytes: Vec<usize>, // TODO: might want a metadata reference here?
    resolved: bool,
    all_started: bool,
}

pub struct Function {
    composition_idx: usize,
    num_outstanding: AtomicUsize,

    inner: Mutex<Inner>,

    function_id: FunctionId,
    join_order: Vec<usize>,
}

impl Function {
    pub fn new(idx: usize, function_id: FunctionId, join_order: Vec<usize>) -> Function {
        Function {
            composition_idx: idx,
            num_outstanding: AtomicUsize::new(0),
            inner: Mutex::new(Inner {
                inputs: vec![],
                outputs: vec![],
                min_set_bytes: vec![],
                resolved: false,
                all_started: false,
            }),
            function_id,
            join_order,
        }
    }

    pub fn update_io(
        &self,
        inputs: Vec<Option<(Arc<CompositionSet>, Sharding, bool)>>,
        outputs: Vec<Option<Arc<CompositionSet>>>,
        mut min_set_bytes: Vec<usize>,
    ) {
        let mut inner = self.inner.lock().expect("Function lock poisoned!");
        min_set_bytes.resize(inputs.len(), 0);

        inner.inputs = inputs;
        inner.outputs = outputs;
        inner.min_set_bytes = min_set_bytes
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
                !inner.resolved,
                "Function.in_set_complete can only run its one-time transition once!"
            );
            inner.resolved = true;

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
                invocations =
                    self.create_invocations(sets, any_sharding_mode, &inner.min_set_bytes);
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
            inner.resolved,
            "Function.push_streaming_items called before in_set_complete resolved this function."
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

        let invocations = self.create_invocations(sets, any_sharding_mode, &inner.min_set_bytes);

        self.num_outstanding
            .fetch_add(invocations.len(), Ordering::AcqRel);
        if complete {
            inner.all_started = inner.inputs.iter().enumerate().all(|(idx, in_opt)| {
                in_opt
                    .as_ref()
                    .map_or(true, |(set, _, _)| idx == set_idx || set.is_complete())
            });
        }
        invocations
    }

    fn create_invocations(
        &self,
        sets: Vec<Option<(DataSet, Sharding)>>,
        any_sharding_mode: &AnyShardingMode,
        min_set_bytes: &Vec<usize>,
    ) -> Vec<Invocation> {
        let num_sets = sets.len();

        let sharding_iter =
            create_sharding_iter(sets, &self.join_order, any_sharding_mode, min_set_bytes);

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

#[cfg(test)]
mod tests {
    use super::*;
    use dandelion_commons::data::Position;

    fn fid(name: &str) -> FunctionId {
        Arc::new(name.to_string())
    }

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
    fn blocking_input_defers_invocation_until_complete() {
        let function = Arc::new(Function::new(0, fid("F"), vec![0]));
        let input_set = Arc::new(CompositionSet::new());
        input_set.add_consumer(function.clone(), 0, true, false, 1);
        function.update_io(
            vec![Some((input_set.clone(), Sharding::All, false))],
            vec![None],
            vec![0],
        );

        // The input hasn't arrived yet, so checking completion produces nothing.
        let invocations = function.in_set_complete(&AnyShardingMode::MaxSharding);
        assert!(invocations.is_empty());
        assert!(!function.is_complete());

        let invocations = input_set.push_items(items(&[1, 2]), true, &AnyShardingMode::MaxSharding);
        assert_eq!(
            invocations.len(),
            1,
            "an `all`-sharded input runs exactly once"
        );
        assert_eq!(invocations[0].input[0].items.len(), 2);
    }

    #[test]
    fn all_input_combines_with_each_input_producing_one_invocation_per_each_item() {
        let function = Arc::new(Function::new(0, fid("F"), vec![0, 1]));
        let all_set = Arc::new(CompositionSet::new());
        let each_set = Arc::new(CompositionSet::new());
        all_set.add_consumer(function.clone(), 0, true, false, 2);
        each_set.add_consumer(function.clone(), 1, false, false, 2);
        function.update_io(
            vec![
                Some((all_set.clone(), Sharding::All, false)),
                Some((each_set.clone(), Sharding::Each, false)),
            ],
            vec![None],
            vec![0, 0],
        );

        // As `Composition::start_execution` does for composition-level inputs: both arrive
        // "instantly complete", the `each` input first so it's already there once the blocking
        // `all` input fires.
        each_set.set_composition_input(DataSet::from_items(items(&[10, 11])));
        let invocations = all_set.push_items(items(&[1]), true, &AnyShardingMode::MaxSharding);

        assert_eq!(
            invocations.len(),
            2,
            "one invocation per item in the `each` input"
        );
        for invocation in &invocations {
            assert_eq!(
                invocation.input[0].items.len(),
                1,
                "the whole `all` set every time"
            );
            assert_eq!(
                invocation.input[1].items.len(),
                1,
                "one `each` item per invocation"
            );
        }
        let mut each_keys: Vec<_> = invocations
            .iter()
            .map(|inv| inv.input[1].items[0].key)
            .collect();
        each_keys.sort();
        assert_eq!(each_keys, vec![10, 11]);
    }

    #[test]
    fn add_invocation_output_completes_function_and_forwards_downstream() {
        let function = Arc::new(Function::new(0, fid("F"), vec![0]));
        let input_set = Arc::new(CompositionSet::new());
        let output_set = Arc::new(CompositionSet::new());
        output_set.mark_retained();
        input_set.add_consumer(function.clone(), 0, true, false, 1);
        function.update_io(
            vec![Some((input_set.clone(), Sharding::All, false))],
            vec![Some(output_set.clone())],
            vec![0],
        );

        let invocations = input_set.push_items(items(&[1, 2]), true, &AnyShardingMode::MaxSharding);
        assert_eq!(invocations.len(), 1);
        assert!(
            !function.is_complete(),
            "the single invocation is still outstanding"
        );

        function.add_invocation_output(
            vec![DataSet::from_items(items(&[100]))],
            &AnyShardingMode::MaxSharding,
        );

        assert!(function.is_complete());
        assert_eq!(output_set.get_set(None).items.len(), 1);
    }

    #[test]
    fn non_optional_empty_input_skips_invocation_but_still_completes() {
        let function = Arc::new(Function::new(0, fid("F"), vec![0]));
        let input_set = Arc::new(CompositionSet::new());
        input_set.add_consumer(function.clone(), 0, true, false, 1);
        function.update_io(
            vec![Some((input_set.clone(), Sharding::All, false))],
            vec![None],
            vec![0],
        );

        let invocations =
            input_set.push_items(Arc::new(vec![]), true, &AnyShardingMode::MaxSharding);
        assert!(
            invocations.is_empty(),
            "a non-optional empty input must not produce an invocation"
        );
        assert!(function.is_complete());
    }
}

use std::{
    iter, mem,
    sync::{Arc, Mutex},
};

use crate::{
    set::CompositionSet,
    sharding::{create_sharding_iter, AnyShardingMode, Sharding},
};
use dandelion_commons::{
    data::{DataItem, DataSet, DataSetAccumulator, Invocation},
    FunctionId,
};
use log::trace;

enum InputSet {
    /// This set is blocking and only set to complete once complete.
    Incomplete,
    /// A set retaining streaming input.
    StreamingPending(DataSetAccumulator),
    /// A complete set.
    Complete(DataSet),
}

impl InputSet {
    fn is_complete(&self) -> bool {
        match self {
            InputSet::Complete(_) => true,
            _ => false,
        }
    }

    fn get_set(&self) -> DataSet {
        match self {
            InputSet::Incomplete => panic!("get_set() called on incomplete set!"),
            InputSet::StreamingPending(acc) => acc.clone_set(),
            InputSet::Complete(set) => set.clone(),
        }
    }
}

struct Inner {
    inputs: Vec<Option<(InputSet, Sharding, bool, bool)>>,
    outputs: Vec<Option<Arc<CompositionSet>>>,
    needs_retention: bool,
    is_blocking: bool,
    num_outstanding: usize,
    all_started: bool,
}

pub struct Function {
    composition_idx: usize,

    inner: Mutex<Inner>,

    min_set_bytes: Vec<usize>, // TODO: might want a metadata reference here?

    function_id: FunctionId,
    join_order: Arc<Vec<usize>>,
}

impl Function {
    pub fn new(idx: usize, function_id: FunctionId, join_order: Arc<Vec<usize>>) -> Function {
        Function {
            composition_idx: idx,
            inner: Mutex::new(Inner {
                inputs: vec![],
                outputs: vec![],
                is_blocking: false,
                needs_retention: false,
                num_outstanding: 0,
                all_started: false,
            }),
            min_set_bytes: vec![],
            function_id,
            join_order,
        }
    }

    pub fn update_io(
        &mut self,
        inputs: &[Option<(Sharding, bool)>],
        outputs: Vec<Option<Arc<CompositionSet>>>,
        mut min_set_bytes: Vec<usize>,
    ) {
        min_set_bytes.resize(inputs.len(), 0);
        self.min_set_bytes = min_set_bytes;

        let mut inner = self.inner.lock().expect("Function lock poisoned!");

        inner.is_blocking = inputs.iter().any(|in_opt| {
            in_opt
                .as_ref()
                .map_or(false, |(sharding, _)| sharding.is_blocking())
        });
        // TODO: Retention is only needed when more than one input is still streaming. So this could
        //       be improved further.
        inner.needs_retention = inputs.len() > 1;
        inner.inputs = inputs
            .iter()
            .map(|in_opt| {
                in_opt.as_ref().map(|(sharding, optional)| {
                    let in_set = if !sharding.is_blocking() {
                        InputSet::StreamingPending(DataSetAccumulator::new())
                    } else {
                        InputSet::Incomplete
                    };
                    (in_set, sharding.clone(), *optional, false)
                })
            })
            .collect();
        inner.outputs = outputs;
    }

    pub fn in_set_complete(
        &self,
        data_set: DataSet,
        set_idx: usize,
        any_sharding_mode: &AnyShardingMode,
        out: &mut impl Extend<Invocation>,
    ) {
        let mut inner = self.inner.lock().expect("Function lock poisoned!");

        inner.inputs[set_idx]
            .as_mut()
            .map(|(set, _, _, _)| *set = InputSet::Complete(data_set));

        let mut blocking = false;
        let mut complete = true;
        for in_opt in &inner.inputs {
            if let Some((set, sharding, _, _)) = in_opt {
                let set_complete = set.is_complete();
                blocking |= sharding.is_blocking() && !set_complete;
                complete &= set_complete;
                if !complete && blocking {
                    break;
                }
            }
        }

        let mut num_created = 0;
        if !blocking {
            debug_assert!(
                inner.is_blocking,
                "in_set_complete was called on non-blocking function!"
            );
            inner.is_blocking = false;

            let mut sets = Vec::with_capacity(inner.inputs.len());
            let mut runnable = true;
            for in_opt in inner.inputs.iter() {
                if let Some((set, sharding, optional, _)) = in_opt {
                    let data_set = set.get_set();
                    if data_set.is_empty() {
                        if *optional && set.is_complete() {
                            sets.push(None);
                            continue;
                        } else {
                            runnable = false;
                            break;
                        }
                    }
                    sets.push(Some((data_set, sharding.clone())));
                } else {
                    sets.push(None);
                }
            }
            if runnable {
                num_created =
                    self.create_invocations(sets, any_sharding_mode, &self.min_set_bytes, out);
            }
        }

        inner.num_outstanding += num_created;
        inner.all_started = complete;

        if complete && num_created == 0 {
            // If this function ever created invocations it became non-blocking after which the
            // push_streaming_item function is called instead of this one. Therefore, we can
            // assume that if no invocations were created there are also no outstanding invocations.
            inner.inputs.clear();
            let empty_items = Arc::new(vec![]);
            for out_set_opt in inner.outputs.iter() {
                if let Some(out_set) = out_set_opt {
                    out_set.push_items(empty_items.clone(), complete, any_sharding_mode, out);
                }
            }
            inner.outputs.clear()
        }
    }

    pub fn push_streaming_items(
        &self,
        set_idx: usize,
        mut items: Arc<Vec<Arc<DataItem>>>,
        complete: bool,
        any_sharding_mode: &AnyShardingMode,
        out: &mut impl Extend<Invocation>,
    ) {
        let mut inner = self.inner.lock().expect("Function lock poisoned!");

        let needs_retention = inner.needs_retention;
        if let Some((ref mut in_set, _, _, ref mut received_items)) = inner.inputs[set_idx] {
            *received_items |= !items.is_empty();
            if needs_retention {
                match in_set {
                    InputSet::StreamingPending(acc) => acc.push_items(&items),
                    _ => panic!("Streamed input set is not a pending local set."),
                }
            }
        } else {
            panic!("Streamed input set is None.");
        }

        let mut runnable = !inner.is_blocking;
        let mut sets = Vec::with_capacity(inner.inputs.len());
        if runnable {
            for (idx, in_opt) in inner.inputs.iter().enumerate() {
                if let Some((set, sharding, optional, received_items)) = in_opt {
                    let (data_set, set_complete) = if idx == set_idx {
                        // this is the case once so we can pass ownership with a mem::take
                        (DataSet::from_items(mem::take(&mut items)), complete)
                    } else {
                        (set.get_set(), set.is_complete())
                    };
                    if data_set.is_empty() {
                        if *optional && set_complete && !*received_items {
                            sets.push(None);
                            continue;
                        } else {
                            runnable = false;
                            break;
                        }
                    }
                    sets.push(Some((data_set, sharding.clone())));
                } else {
                    sets.push(None);
                }
            }
        }
        drop(inner);

        let num_created = if runnable {
            self.create_invocations(sets, any_sharding_mode, &self.min_set_bytes, out)
        } else {
            0
        };

        let mut inner = self.inner.lock().expect("Function lock poisoned!");
        inner.num_outstanding += num_created;
        if complete {
            if inner.needs_retention {
                if let Some((InputSet::StreamingPending(acc), sharding, optional, received_items)) =
                    inner.inputs[set_idx].take()
                {
                    inner.inputs[set_idx] = Some((
                        InputSet::Complete(acc.collect_unsorted()),
                        sharding,
                        optional,
                        received_items,
                    ));
                }
            }

            inner.all_started = inner.inputs.iter().enumerate().all(|(idx, in_opt)| {
                in_opt
                    .as_ref()
                    .map_or(true, |(set, _, _, _)| idx == set_idx || set.is_complete())
            });

            if inner.all_started && inner.num_outstanding == 0 {
                inner.inputs.clear();
                let empty_items = Arc::new(vec![]);
                for out_set_opt in inner.outputs.iter() {
                    if let Some(out_set) = out_set_opt {
                        out_set.push_items(empty_items.clone(), complete, any_sharding_mode, out);
                    }
                }
                inner.outputs.clear()
            }
        }
    }

    /// Extends `out` with the invocations for the given sets and returns how many were created.
    fn create_invocations(
        &self,
        sets: Vec<Option<(DataSet, Sharding)>>,
        any_sharding_mode: &AnyShardingMode,
        min_set_bytes: &[usize],
        out: &mut impl Extend<Invocation>,
    ) -> usize {
        let num_sets = sets.len();

        let Some(mut iter) =
            create_sharding_iter(sets, &self.join_order, any_sharding_mode, min_set_bytes)
        else {
            return 0;
        };

        let mut num_created = 0;
        out.extend(iter::from_fn(|| {
            // The sharding iterator starts out on its first element, so only advance after that.
            // Once `advance` returned false we're done.
            if num_created > 0 && !iter.advance() {
                return None;
            }
            let mut input = vec![DataSet::default(); num_sets];
            iter.fill_in(&mut input);
            let invocation = Invocation {
                function_id: self.function_id.clone(),
                composition_idx: self.composition_idx,
                input,
            };
            trace!("Computed sharding invocation: {:?}", invocation);
            num_created += 1;
            Some(invocation)
        }));
        num_created
    }

    pub fn add_invocation_output(
        &self,
        sets: Vec<DataSet>,
        any_sharding_mode: &AnyShardingMode,
        out: &mut impl Extend<Invocation>,
    ) {
        let mut inner = self.inner.lock().expect("Function lock poisoned!");

        inner.num_outstanding -= 1;
        let complete = inner.all_started && inner.num_outstanding == 0;
        if complete {
            inner.inputs.clear();
        }

        if sets.is_empty() {
            let empty_items = Arc::new(vec![]);
            for out_set_opt in inner.outputs.iter() {
                if let Some(out_set) = out_set_opt {
                    out_set.push_items(empty_items.clone(), complete, any_sharding_mode, out);
                }
            }
        } else {
            debug_assert_eq!(sets.len(), inner.outputs.len());
            for (data, out_set_opt) in sets.into_iter().zip(inner.outputs.iter()) {
                if let Some(out_set) = out_set_opt {
                    out_set.push_items(data.items, complete, any_sharding_mode, out);
                }
            }
        }

        if complete {
            inner.outputs.clear()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dandelion_commons::data::Position;

    impl Function {
        /// Whether all invocations were started and have finished.
        pub fn is_complete(&self) -> bool {
            let inner = self.inner.lock().expect("Function lock poisoned!");
            inner.all_started && inner.num_outstanding == 0
        }
    }

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

    fn push(set: &CompositionSet, keys: &[u32], complete: bool) -> Vec<Invocation> {
        let mut invocations = Vec::new();
        set.push_items(
            items(keys),
            complete,
            &AnyShardingMode::MaxSharding,
            &mut invocations,
        );
        invocations
    }

    #[test]
    fn blocking_input_defers_invocation_until_complete() {
        let input_set = Arc::new(CompositionSet::new());
        let mut function = Function::new(0, fid("F"), vec![0].into());
        function.update_io(&[Some((Sharding::All, false))], vec![None], vec![0]);
        let function = Arc::new(function);
        input_set.add_blocking_consumer(function.clone(), 0, false);

        // The input hasn't arrived yet.
        assert!(!function.is_complete());

        let invocations = push(&input_set, &[1, 2], true);
        assert_eq!(
            invocations.len(),
            1,
            "an `all`-sharded input runs exactly once"
        );
        assert_eq!(invocations[0].input[0].items.len(), 2);
    }

    #[test]
    fn all_input_combines_with_each_input_producing_one_invocation_per_each_item() {
        let all_set = Arc::new(CompositionSet::new());
        let each_set = Arc::new(CompositionSet::new());
        let mut function = Function::new(0, fid("F"), vec![0, 1].into());
        function.update_io(
            &[Some((Sharding::All, false)), Some((Sharding::Each, false))],
            vec![None],
            vec![0, 0],
        );
        let function = Arc::new(function);
        all_set.add_blocking_consumer(function.clone(), 0, false);
        each_set.add_non_blocking_consumer(function.clone(), 1);

        // As `Composition::start_execution` does for composition-level inputs: both arrive
        // "instantly complete", the `each` input first so it's already there once the blocking
        // `all` input fires.
        each_set.set_composition_input(
            DataSet::from_items(items(&[10, 11])),
            &AnyShardingMode::MaxSharding,
            &mut Vec::new(),
        );
        let invocations = push(&all_set, &[1], true);

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
        let input_set = Arc::new(CompositionSet::new());
        let output_set = Arc::new(CompositionSet::new());
        output_set.mark_retained();
        let mut function = Function::new(0, fid("F"), vec![0].into());
        function.update_io(
            &[Some((Sharding::All, false))],
            vec![Some(output_set.clone())],
            vec![0],
        );
        let function = Arc::new(function);
        input_set.add_blocking_consumer(function.clone(), 0, false);

        let invocations = push(&input_set, &[1, 2], true);
        assert_eq!(invocations.len(), 1);
        assert!(
            !function.is_complete(),
            "the single invocation is still outstanding"
        );

        function.add_invocation_output(
            vec![DataSet::from_items(items(&[100]))],
            &AnyShardingMode::MaxSharding,
            &mut Vec::new(),
        );

        assert!(function.is_complete());
        assert_eq!(output_set.get_set().items.len(), 1);
    }

    #[test]
    fn non_optional_empty_input_skips_invocation_but_still_completes() {
        let input_set = Arc::new(CompositionSet::new());
        let mut function = Function::new(0, fid("F"), vec![0].into());
        function.update_io(&[Some((Sharding::All, false))], vec![None], vec![0]);
        let function = Arc::new(function);
        input_set.add_blocking_consumer(function.clone(), 0, false);

        let invocations = push(&input_set, &[], true);
        assert!(
            invocations.is_empty(),
            "a non-optional empty input must not produce an invocation"
        );
        assert!(function.is_complete());
    }

    #[test]
    fn two_each_inputs_only_combine_once_both_have_items() {
        let set_a = Arc::new(CompositionSet::new());
        let set_b = Arc::new(CompositionSet::new());
        let mut function = Function::new(0, fid("F"), vec![0, 1].into());
        function.update_io(
            &[Some((Sharding::Each, false)), Some((Sharding::Each, false))],
            vec![None],
            vec![0, 0],
        );
        let function = Arc::new(function);
        set_a.add_non_blocking_consumer(function.clone(), 0);
        set_b.add_non_blocking_consumer(function.clone(), 1);

        // B is still completely untouched: pushing to A alone must not create an invocation
        // (previously this would panic trying to shard an empty `each` set for B).
        let invocations = push(&set_a, &[1], false);
        assert!(
            invocations.is_empty(),
            "B has no items yet, so nothing should fire"
        );

        // Now B gets an item too: the pending combination fires.
        let invocations = push(&set_b, &[10], false);
        assert_eq!(invocations.len(), 1);
        assert_eq!(invocations[0].input[0].items.len(), 1);
        assert_eq!(invocations[0].input[0].items[0].key, 1);
        assert_eq!(invocations[0].input[1].items.len(), 1);
        assert_eq!(invocations[0].input[1].items[0].key, 10);
    }

    /// Same as `two_each_inputs_only_combine_once_both_have_items`, but pushes to the *second*
    /// input while the *first* is still untouched. `push_streaming_items` walks inputs in order
    /// and bails out as soon as it finds a non-optional empty one - here that's input 0 (A),
    /// which comes *before* input 1 (B, the one actually being pushed to), so the loop returns
    /// without ever reaching the `idx == set_idx` branch that would `mem::take` the pushed item.
    /// That must not lose the item: since a multi-param function retains its streaming inputs
    /// itself (`needs_retention`), it has already durably retained the item in its local
    /// accumulator for input 1 regardless, and a later push to A picks it back up.
    #[test]
    fn each_each_second_input_arriving_first_is_not_lost() {
        let set_a = Arc::new(CompositionSet::new());
        let set_b = Arc::new(CompositionSet::new());
        let mut function = Function::new(0, fid("F"), vec![0, 1].into());
        function.update_io(
            &[Some((Sharding::Each, false)), Some((Sharding::Each, false))],
            vec![None],
            vec![0, 0],
        );
        let function = Arc::new(function);
        set_a.add_non_blocking_consumer(function.clone(), 0);
        set_b.add_non_blocking_consumer(function.clone(), 1);

        // B (input 1) arrives first, while A (input 0) is still completely empty.
        let invocations = push(&set_b, &[10], false);
        assert!(
            invocations.is_empty(),
            "A has no items yet, so nothing should fire"
        );

        // A arrives: B's earlier item must still be there, not silently dropped.
        let invocations = push(&set_a, &[1], false);
        assert_eq!(invocations.len(), 1);
        assert_eq!(invocations[0].input[0].items[0].key, 1);
        assert_eq!(
            invocations[0].input[1].items.len(),
            1,
            "B's earlier item must not be lost"
        );
        assert_eq!(invocations[0].input[1].items[0].key, 10);
    }
}

use crate::{
    composition::join_iterator::JoinIterator,
    memory_domain::{Context, ContextTrait},
};
use dandelion_commons::{
    err_dandelion, DandelionError, DandelionResult, DispatcherError, FunctionId,
};
use std::{cmp, collections::BTreeMap, sync::Arc, vec};

#[cfg(test)]
use crate::memory_domain::read_only::ReadOnlyContext;
#[cfg(test)]
use itertools::Itertools;

mod join_iterator;

/// A composition has a composition wide id space that maps ids of
/// the input and output sets to sets of individual functions to a unified
/// namespace. The ids in this namespace are used to find out which
/// functions have become ready.
#[derive(Clone, Debug)]
pub struct Composition {
    pub dependencies: Vec<FunctionDependencies>,
    pub output_map: BTreeMap<usize, usize>,
}

/// Modes for the composition set iteratior to return sharding
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ShardingMode {
    All,
    Each,
    Key,
    AnyEach,
    AnyKey,
}

// TODO remove  one of left/right to simplify handling, push switching order into the parsing layer
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum JoinStrategy {
    Inner,
    Left,
    Right,
    Outer,
    /// Produces the cross product of each set on both sides of the join.
    /// If this is used for further joins, the keys of the right set of the cross join are used.
    Cross,
}

/// Describes an input set the function dependencies of a composition.
#[derive(Clone, Copy, Debug)]
pub struct InputSetDescriptor {
    /// the composition wide set id corresponding to this input set
    pub composition_id: usize,
    /// the sharding mode to apply
    pub sharding: ShardingMode,
    /// If false, the set needs to contain at least one item for the function to run.
    pub optional: bool,
}

#[derive(Clone, Debug)]
pub struct FunctionDependencies {
    pub function: FunctionId,
    /// Input set dependencies, function local set ids are given implicitly by the vector index of the descriptor
    pub input_set_ids: Vec<Option<InputSetDescriptor>>,
    pub join_info: (Vec<usize>, Vec<JoinStrategy>),
    /// the composition ids for the output sets of the function,
    /// if the id is none, that set is not needed for the composition
    pub output_set_ids: Vec<Option<usize>>,
}

impl ShardingMode {
    pub fn from_parser_sharding(sharding: &dparser::Sharding) -> Self {
        match sharding {
            dparser::Sharding::All => Self::All,
            dparser::Sharding::Keyed => Self::Key,
            dparser::Sharding::Each => Self::Each,
            dparser::Sharding::AnyKeyed => Self::AnyKey,
            dparser::Sharding::AnyEach => Self::AnyEach,
        }
    }
}

impl JoinStrategy {
    pub fn from_parser_strategy(sharding: &dparser::JoinFilterStrategy) -> Self {
        match sharding {
            dparser::JoinFilterStrategy::Inner => Self::Inner,
            dparser::JoinFilterStrategy::Left => Self::Left,
            dparser::JoinFilterStrategy::Right => Self::Right,
            dparser::JoinFilterStrategy::Full => Self::Outer,
            dparser::JoinFilterStrategy::Cross => panic!("Parser should not produce cross"),
        }
    }
}

/// Struct that has all locations belonging to one set, that is potentially spread over multiple contexts.
#[derive(Clone, Debug)]
pub struct CompositionSet {
    /// items identfied by tuple of key, item index and the context reference
    item_list: Vec<(u32, usize, Arc<Context>)>,
    /// the set side inside the contexts the composition set represents
    set_index: usize,
}

impl CompositionSet {
    pub fn is_empty(&self) -> bool {
        self.item_list.is_empty()
    }

    pub fn len(&self) -> usize {
        self.item_list.len()
    }

    pub fn get_set_idx(&self) -> usize {
        self.set_index
    }

    /// Used for serializing the data to protobuf
    pub fn get_item(&self, idx: usize) -> (String, u32, Vec<u8>) {
        let item = &self.item_list[idx];
        let context_item = &item.2.content[self.set_index].as_ref().unwrap().buffers[item.1];
        let mut data_bytes = Vec::<u8>::with_capacity(context_item.data.size);
        let data_slice = item
            .2
            .get_chunk_ref(context_item.data.offset, context_item.data.size)
            .expect("Failed to read item!");
        data_bytes.extend_from_slice(data_slice);
        (context_item.ident.clone(), context_item.key, data_bytes)
    }

    pub fn combine(&mut self, additional: CompositionSet) -> DandelionResult<()> {
        let CompositionSet {
            item_list,
            set_index,
        } = additional;
        if self.set_index != set_index {
            return err_dandelion!(DandelionError::Dispatcher(
                DispatcherError::CompositionCombine,
            ));
        }
        self.item_list.extend(item_list.into_iter());
        self.item_list.sort_unstable_by_key(|a| a.0);
        return Ok(());
    }
}

impl From<(usize, Vec<Arc<Context>>)> for CompositionSet {
    fn from(pair: (usize, Vec<Arc<Context>>)) -> Self {
        let (set_index, context_vec) = pair;
        let mut item_list = Vec::new();
        for context in context_vec.into_iter() {
            if let Some(Some(set)) = context.content.get(set_index) {
                for (item_index, buffer) in set.buffers.iter().enumerate() {
                    item_list.push((buffer.key, item_index, context.clone()));
                }
            }
        }
        item_list.sort_unstable_by_key(|a| a.0);
        return CompositionSet {
            item_list,
            set_index,
        };
    }
}

pub struct CompositionSetTransferIterator<'origin> {
    /// set for which this iterator is implemented
    set_iterator: std::slice::Iter<'origin, (u32, usize, Arc<Context>)>,
    set_index: usize,
}

impl Iterator for CompositionSetTransferIterator<'_> {
    type Item = (usize, usize, Arc<Context>);

    fn next(&mut self) -> Option<Self::Item> {
        self.set_iterator
            .next()
            .and_then(|(_, item_index, context)| {
                Some((self.set_index, *item_index, context.clone()))
            })
    }
}

impl<'origin> IntoIterator for &'origin CompositionSet {
    type Item = (usize, usize, Arc<Context>);
    type IntoIter = CompositionSetTransferIterator<'origin>;
    fn into_iter(self) -> Self::IntoIter {
        Self::IntoIter {
            set_iterator: self.item_list.iter(),
            set_index: self.set_index,
        }
    }
}

/// Computes the sharding for the given sets following the given join order and join strategies.
/// The `join_order` vector is expected to be of length `sets.len()`, the `join_strategies` vector
/// is expected to be of size `sets.len() - 1`.
/// The function tries to produce an output vector of size `target_parallelism` if possible grouping
/// `AnyEach` and `AnyKey` sets accordingly. If a `target_parallelism` of 0 is given it will use
/// maximum possible parallellism.
pub fn get_sharding(
    mut sets: Vec<Option<(ShardingMode, CompositionSet)>>,
    join_order: Vec<usize>,
    join_strategies: Vec<JoinStrategy>,
    target_parallelism: usize,
) -> Vec<Vec<Option<CompositionSet>>> {
    let set_num = sets.len();
    debug_assert_eq!(join_order.len(), set_num);
    debug_assert_eq!(
        join_strategies.len(),
        if set_num == 0 { 0 } else { set_num - 1 }
    );

    let mut final_sharding = Vec::new();
    if set_num == 0 {
        return final_sharding;
    }

    // first create the iterators for any keyed shardings
    let mut key_join_iter = None;
    let mut fixed_parallelism = 1;
    let mut join_group_key_set: Vec<u32> = vec![];
    let mut i = 0;
    while i < join_order.len() {
        let set_idx = join_order[i];
        if let Some((sharding, set)) = sets[set_idx].take() {
            if sharding != ShardingMode::Key {
                if join_group_key_set.len() > 0 {
                    fixed_parallelism *= join_group_key_set.len();
                    join_group_key_set.clear();
                }
                sets[set_idx] = Some((sharding, set)); // put back into sets
                break; // continue building all other iterators in the second loop
            }

            let strategy = if i > 0 {
                join_strategies[i - 1]
            } else {
                JoinStrategy::Cross
            };

            if strategy == JoinStrategy::Cross {
                if join_group_key_set.len() > 0 {
                    fixed_parallelism *= join_group_key_set.len();
                    join_group_key_set.clear();
                }
            }

            key_join_iter = join_iterator::SetKeyIterator::new(
                key_join_iter,
                set,
                strategy,
                set_idx,
                &mut join_group_key_set,
            );
        }
        i += 1;
    }

    // second create the iterators for all remaining shardings
    let mut any_parallelisms: Vec<(usize, usize)> = Vec::new(); // vec of (max parallelism, chosen parallelism)
    let mut join_iter = key_join_iter.map(|i| i as Box<dyn JoinIterator>);
    while i < join_order.len() {
        let set_idx = join_order[i];
        if let Some((sharding, set)) = sets[set_idx].take() {
            match sharding {
                ShardingMode::All => {
                    join_iter = join_iterator::SetAllIterator::new(join_iter, set, set_idx);
                }
                ShardingMode::Each => {
                    let parallelism;
                    (join_iter, parallelism) =
                        join_iterator::SetEachIterator::new(join_iter, set, set_idx);
                    fixed_parallelism *= parallelism;
                }
                ShardingMode::AnyEach => {
                    let (any_join_iter, max_parallelism) = join_iterator::AnyIterator::new(
                        join_iter,
                        vec![set],
                        vec![],
                        vec![set_idx],
                        sharding,
                    );
                    if any_join_iter.is_some() {
                        any_parallelisms.push((max_parallelism, 1));
                    }
                    join_iter = any_join_iter.map(|i| i as Box<dyn JoinIterator>);
                }
                ShardingMode::AnyKey => {
                    // get all sets that are joined together (i.e. find the next cross join)
                    let mut joined_sets = vec![set];
                    let mut joined_set_idcs = vec![set_idx];
                    let mut joined_strategies = vec![];
                    while i + 1 < join_order.len() {
                        let next_strategy = join_strategies[i];
                        if next_strategy != JoinStrategy::Cross {
                            let next_set_idx = join_order[i + 1];
                            // if the set is none we just skip it -> this allows for empty optional sets
                            if let Some((_, set)) = sets[next_set_idx].take() {
                                joined_sets.push(set);
                                joined_set_idcs.push(next_set_idx);
                                joined_strategies.push(next_strategy);
                            }
                            i += 1;
                        } else {
                            // a cross join breaks the chain
                            break;
                        }
                    }

                    let (any_join_iter, max_parallelism) = join_iterator::AnyIterator::new(
                        join_iter,
                        joined_sets,
                        joined_strategies,
                        joined_set_idcs,
                        sharding,
                    );
                    if any_join_iter.is_some() {
                        any_parallelisms.push((max_parallelism, 1));
                    }
                    join_iter = any_join_iter.map(|i| i as Box<dyn JoinIterator>);
                }
                ShardingMode::Key => {
                    panic!("Expecting key shardings to preceed all other shardings.");
                }
            }
        }
        i += 1;
    }

    // compute the parallelism for the any shardings
    if fixed_parallelism < target_parallelism && !any_parallelisms.is_empty() {
        let mut leftover_parallelism = target_parallelism / fixed_parallelism;
        loop {
            // find the best suitable set to parallelize over
            let mut best_idx = 0;
            let mut best_dist = 0.0;
            for (i, (max_p, chosen_p)) in any_parallelisms.iter().enumerate() {
                // check that we have not yet assigned a parallelism to this any set
                if *chosen_p == 1 {
                    let remainder = max_p % leftover_parallelism;
                    if remainder == 0 {
                        best_idx = i;
                        best_dist = 0.0;
                        break;
                    }
                    let dist = (remainder) as f64 / (*max_p) as f64;
                    if best_dist == 0.0 || dist < best_dist {
                        best_idx = i;
                        best_dist = dist;
                    }
                }
            }

            // if the best set has fewer elements than the leftover parallelization we continue and
            // check if we can find another set to parallelize over in addition to the found one
            if best_dist >= 1.0 {
                let max_parallelism = any_parallelisms[best_idx].0;
                any_parallelisms[best_idx].1 = max_parallelism;
                leftover_parallelism /= max_parallelism;
                if leftover_parallelism <= 1 {
                    break;
                }
            } else {
                if any_parallelisms[best_idx].1 == 1 {
                    let chosen_parallelism =
                        cmp::min(leftover_parallelism, any_parallelisms[best_idx].0);
                    any_parallelisms[best_idx].1 = chosen_parallelism;
                }
                break;
            }
        }
    }
    if target_parallelism > 0 {
        if let Some(iter) = join_iter.as_mut() {
            iter.reduce_any_parallelism(any_parallelisms);
        }
    }

    // generate the sharding sets
    if let Some(mut iter) = join_iter {
        let mut new_sets = Vec::with_capacity(set_num);
        new_sets.resize(set_num, None);
        iter.fill_in(&mut new_sets);
        final_sharding.push(new_sets);
        while iter.advance() {
            let mut advance_sets = Vec::with_capacity(set_num);
            advance_sets.resize(set_num, None);
            iter.fill_in(&mut advance_sets);
            final_sharding.push(advance_sets);
        }
    }

    final_sharding
}

//=======
// TESTS

/// Creates a dummy set from a vector of keys.
/// The item indexes in the composition set are qual to the index of the key in the input.
/// Keys do not need to be in correct order, but they will be sorted after producing.
/// This is to allow to have keys with lower item indexes but higher keys and vice versa.
#[cfg(test)]
fn create_dummy_set(keys: Vec<u32>) -> CompositionSet {
    let dummy_context: Arc<Context> = Arc::new(ReadOnlyContext::new_static::<u8>(&mut []));
    let items = keys
        .into_iter()
        .enumerate()
        .map(|(i, k)| (k, i, dummy_context.clone()))
        .sorted_by_key(|tuple| tuple.0)
        .collect();
    CompositionSet {
        item_list: items,
        set_index: 0,
    }
}

/// An array of options for expected sets in the input set vec produced by a sharing.
#[cfg(test)]
type SetGroup = Vec<Option<ExpectedSet>>;

/// An array of tuples with the expected keys and item indexes for the items in a set.
#[cfg(test)]
type ExpectedSet = Vec<(u32, usize)>;

#[cfg(test)]
/// The expected is a list of all vectors of generated sets.
fn check_sharding(actual: Vec<Vec<Option<CompositionSet>>>, expected: Vec<SetGroup>) {
    assert_eq!(
        expected.len(),
        actual.len(),
        "Not the number of set groups that were expected"
    );
    for (set_group_index, (actual_sets, expected_sets)) in
        actual.into_iter().zip(expected.into_iter()).enumerate()
    {
        assert_eq!(
            expected_sets.len(),
            actual_sets.len(),
            "Sets not matching for index {}, ",
            set_group_index
        );
        for (set_index, (expected_set_opt, actual_set_opt)) in expected_sets
            .into_iter()
            .zip(actual_sets.into_iter())
            .enumerate()
        {
            if expected_set_opt.is_none() {
                assert!(
                    actual_set_opt.is_none(),
                    "Expexted none, but found a set for index {}",
                    set_index
                );
                continue;
            }
            let mut expected_set = expected_set_opt.unwrap();
            let mut actual_set = actual_set_opt.unwrap();
            assert_eq!(expected_set.len(), actual_set.item_list.len());
            // sort both lists by item index, since that one should be unique, since we only have a single context
            expected_set.sort_by_key(|item| item.1);
            actual_set.item_list.sort_by_key(|item| item.1);
            // have two sorted lists, check that each item index is the expected one and that it has the correct key
            for ((expected_key, expected_index), (actual_key, actual_index, _)) in
                expected_set.into_iter().zip(actual_set.item_list)
            {
                assert_eq!(
                    expected_index, actual_index,
                    "for keys {}, {}",
                    expected_key, actual_key
                );
                assert_eq!(expected_key, actual_key);
            }
        }
    }
}

#[cfg(test)]
fn print_sharding(actual: &Vec<Vec<Option<CompositionSet>>>) {
    println!("Got sharding:");
    for inv_sets in actual.iter() {
        println!("[");
        for (set_idx, set) in inv_sets.iter().enumerate() {
            if set.is_none() {
                println!("  set {}: [None]", set_idx);
            } else {
                print!("  set {}: [ ", set_idx);
                for (key, itm, _) in set.as_ref().unwrap().item_list.iter() {
                    print!("({}, {}) ", key, itm);
                }
                println!("]");
            }
        }
        println!("]");
    }
}

#[test]
fn join_it_inner_test() {
    let sets = vec![
        Some((ShardingMode::Key, create_dummy_set(vec![3, 0, 1]))),
        Some((ShardingMode::Key, create_dummy_set(vec![1, 0, 1, 2]))),
    ];

    let join_order = vec![0, 1];
    let join_strategies = vec![JoinStrategy::Inner];

    let sharding = get_sharding(sets, join_order, join_strategies, 0);
    let expected = vec![
        vec![Some(vec![(0, 1)]), Some(vec![(0, 1)])],
        vec![Some(vec![(1, 2)]), Some(vec![(1, 0), (1, 2)])],
    ];

    print_sharding(&sharding);
    check_sharding(sharding, expected);
}

#[test]
fn join_it_left_test() {
    let sets = vec![
        Some((ShardingMode::Key, create_dummy_set(vec![0, 1, 2]))),
        Some((ShardingMode::Key, create_dummy_set(vec![3, 1, 0, 1]))),
    ];

    let join_order = vec![0, 1];
    let join_strategies = vec![JoinStrategy::Left];

    let sharding = get_sharding(sets, join_order, join_strategies, 0);
    let expected = vec![
        vec![Some(vec![(0, 0)]), Some(vec![(0, 2)])],
        vec![Some(vec![(1, 1)]), Some(vec![(1, 1), (1, 3)])],
        vec![Some(vec![(2, 2)]), None],
    ];

    print_sharding(&sharding);
    check_sharding(sharding, expected);
}

#[test]
fn join_it_right_test() {
    let sets = vec![
        Some((ShardingMode::Key, create_dummy_set(vec![3, 1, 0, 1]))),
        Some((ShardingMode::Key, create_dummy_set(vec![0, 1, 2]))),
    ];

    let join_order = vec![0, 1];
    let join_strategies = vec![JoinStrategy::Right];

    let sharding = get_sharding(sets, join_order, join_strategies, 0);
    let expected = vec![
        vec![Some(vec![(0, 2)]), Some(vec![(0, 0)])],
        vec![Some(vec![(1, 1), (1, 3)]), Some(vec![(1, 1)])],
        vec![None, Some(vec![(2, 2)])],
    ];

    print_sharding(&sharding);
    check_sharding(sharding, expected);
}

#[test]
fn join_it_outer_test() {
    let sets = vec![
        Some((ShardingMode::Key, create_dummy_set(vec![0, 1, 2]))),
        Some((ShardingMode::Key, create_dummy_set(vec![3, 1, 0, 1]))),
    ];

    let join_order = vec![0, 1];
    let join_strategies = vec![JoinStrategy::Outer];

    let sharding = get_sharding(sets, join_order, join_strategies, 0);
    let expected = vec![
        vec![Some(vec![(0, 0)]), Some(vec![(0, 2)])],
        vec![Some(vec![(1, 1)]), Some(vec![(1, 1), (1, 3)])],
        vec![Some(vec![(2, 2)]), None],
        vec![None, Some(vec![(3, 0)])],
    ];

    print_sharding(&sharding);
    check_sharding(sharding, expected);
}

#[test]
fn join_it_cross_test() {
    let sets = vec![
        Some((ShardingMode::Key, create_dummy_set(vec![0, 1, 2]))),
        Some((ShardingMode::Key, create_dummy_set(vec![3, 1, 0, 1]))),
    ];

    let join_order = vec![0, 1];
    let join_strategies = vec![JoinStrategy::Cross];

    let sharding = get_sharding(sets, join_order, join_strategies, 0);
    let expected = vec![
        vec![Some(vec![(0, 0)]), Some(vec![(0, 2)])],
        vec![Some(vec![(0, 0)]), Some(vec![(1, 1), (1, 3)])],
        vec![Some(vec![(0, 0)]), Some(vec![(3, 0)])],
        vec![Some(vec![(1, 1)]), Some(vec![(0, 2)])],
        vec![Some(vec![(1, 1)]), Some(vec![(1, 1), (1, 3)])],
        vec![Some(vec![(1, 1)]), Some(vec![(3, 0)])],
        vec![Some(vec![(2, 2)]), Some(vec![(0, 2)])],
        vec![Some(vec![(2, 2)]), Some(vec![(1, 1), (1, 3)])],
        vec![Some(vec![(2, 2)]), Some(vec![(3, 0)])],
    ];

    print_sharding(&sharding);
    check_sharding(sharding, expected);
}

#[test]
fn join_it_order_test() {
    let sets = vec![
        Some((ShardingMode::Key, create_dummy_set(vec![3, 1, 0, 1]))),
        Some((ShardingMode::Key, create_dummy_set(vec![0, 1, 2]))),
    ];

    let join_order = vec![1, 0];
    let join_strategies = vec![JoinStrategy::Left];

    let sharding = get_sharding(sets, join_order, join_strategies, 0);
    let expected = vec![
        vec![Some(vec![(0, 2)]), Some(vec![(0, 0)])],
        vec![Some(vec![(1, 1), (1, 3)]), Some(vec![(1, 1)])],
        vec![None, Some(vec![(2, 2)])],
    ];

    print_sharding(&sharding);
    check_sharding(sharding, expected);
}

#[test]
fn join_it_chain_test() {
    let sets = vec![
        Some((
            ShardingMode::Key,
            create_dummy_set(vec![1, 1234, 123, 124, 134]),
        )),
        Some((
            ShardingMode::Key,
            create_dummy_set(vec![2, 234, 1234, 123, 124]),
        )),
        Some((
            ShardingMode::Key,
            create_dummy_set(vec![3, 134, 234, 1234, 123]),
        )),
        Some((
            ShardingMode::Key,
            create_dummy_set(vec![4, 124, 134, 234, 1234]),
        )),
    ];

    let join_order = vec![0, 1, 2, 3];
    let join_strategies = vec![
        JoinStrategy::Outer,
        JoinStrategy::Outer,
        JoinStrategy::Outer,
    ];

    let sharding = get_sharding(sets, join_order, join_strategies, 0);
    let expected = vec![
        vec![Some(vec![(1, 0)]), None, None, None],
        vec![None, Some(vec![(2, 0)]), None, None],
        vec![None, None, Some(vec![(3, 0)]), None],
        vec![None, None, None, Some(vec![(4, 0)])],
        vec![
            Some(vec![(123, 2)]),
            Some(vec![(123, 3)]),
            Some(vec![(123, 4)]),
            None,
        ],
        vec![
            Some(vec![(124, 3)]),
            Some(vec![(124, 4)]),
            None,
            Some(vec![(124, 1)]),
        ],
        vec![
            Some(vec![(134, 4)]),
            None,
            Some(vec![(134, 1)]),
            Some(vec![(134, 2)]),
        ],
        vec![
            None,
            Some(vec![(234, 1)]),
            Some(vec![(234, 2)]),
            Some(vec![(234, 3)]),
        ],
        vec![
            Some(vec![(1234, 1)]),
            Some(vec![(1234, 2)]),
            Some(vec![(1234, 3)]),
            Some(vec![(1234, 4)]),
        ],
    ];

    print_sharding(&sharding);
    check_sharding(sharding, expected);
}

#[test]
fn join_it_any_simple_test() {
    let sets = vec![Some((
        ShardingMode::AnyEach,
        create_dummy_set(vec![0, 1, 2, 3]),
    ))];

    let join_order = vec![0];
    let join_strategies = vec![];

    let sharding = get_sharding(sets, join_order, join_strategies, 2);
    let expected = vec![
        vec![Some(vec![(0, 0), (1, 1)])],
        vec![Some(vec![(2, 2), (3, 3)])],
    ];

    print_sharding(&sharding);
    check_sharding(sharding, expected);
}

#[test]
fn join_it_any_joined_keys_test() {
    let sets = vec![
        Some((ShardingMode::AnyKey, create_dummy_set(vec![0, 1, 2, 3, 5]))),
        Some((ShardingMode::AnyKey, create_dummy_set(vec![0, 2, 3, 4, 5]))),
    ];

    let join_order = vec![0, 1];
    let join_strategies = vec![JoinStrategy::Inner];

    let sharding = get_sharding(sets, join_order, join_strategies, 2);
    let expected = vec![
        vec![Some(vec![(0, 0), (2, 2)]), Some(vec![(0, 0), (2, 1)])],
        vec![Some(vec![(3, 3), (5, 4)]), Some(vec![(3, 2), (5, 4)])],
    ];

    print_sharding(&sharding);
    check_sharding(sharding, expected);
}

#[test]
fn join_it_any_chain_test() {
    let sets = vec![
        Some((ShardingMode::AnyEach, create_dummy_set(vec![0, 0, 0]))),
        Some((ShardingMode::AnyKey, create_dummy_set(vec![0, 1, 2, 3, 5]))),
        Some((ShardingMode::AnyKey, create_dummy_set(vec![0, 2, 3, 4, 5]))),
    ];

    let join_order = vec![1, 2, 0];
    let join_strategies = vec![JoinStrategy::Inner, JoinStrategy::Cross];

    let sharding = get_sharding(sets, join_order, join_strategies, 2);
    let expected = vec![
        vec![
            Some(vec![(0, 0), (0, 1), (0, 2)]),
            Some(vec![(0, 0), (2, 2)]),
            Some(vec![(0, 0), (2, 1)]),
        ],
        vec![
            Some(vec![(0, 0), (0, 1), (0, 2)]),
            Some(vec![(3, 3), (5, 4)]),
            Some(vec![(3, 2), (5, 4)]),
        ],
    ];

    print_sharding(&sharding);
    check_sharding(sharding, expected);
}

#[test]
fn join_it_any_chain_test_1() {
    let sets = vec![
        Some((ShardingMode::AnyEach, create_dummy_set(vec![0, 1]))),
        Some((
            ShardingMode::AnyEach,
            create_dummy_set(vec![0, 1, 2, 3, 4, 5]),
        )),
        Some((ShardingMode::AnyEach, create_dummy_set(vec![0, 1, 2, 3]))),
    ];

    let join_order = vec![0, 1, 2];
    let join_strategies = vec![JoinStrategy::Cross, JoinStrategy::Cross];

    let sharding_6 = get_sharding(sets.clone(), join_order.clone(), join_strategies.clone(), 6);
    assert_eq!(sharding_6.len(), 6); // <- should get 6 shardings
    assert_eq!(sharding_6[0].len(), 3); // <- should still get 3 sets
    assert_eq!(sharding_6[0][0].as_ref().unwrap().len(), 2);
    assert_eq!(sharding_6[0][1].as_ref().unwrap().len(), 1); // <- parallelized set
    assert_eq!(sharding_6[0][2].as_ref().unwrap().len(), 4);

    let sharding_3 = get_sharding(sets.clone(), join_order.clone(), join_strategies.clone(), 3);
    assert_eq!(sharding_3.len(), 3); // <- should get 3 shardings
    assert_eq!(sharding_3[0].len(), 3); // <- should still get 3 sets
    assert_eq!(sharding_3[0][0].as_ref().unwrap().len(), 2);
    assert_eq!(sharding_3[0][1].as_ref().unwrap().len(), 2); // <- parallelized set
    assert_eq!(sharding_3[0][2].as_ref().unwrap().len(), 4);

    let sharding_4 = get_sharding(sets.clone(), join_order.clone(), join_strategies.clone(), 4);
    assert_eq!(sharding_4.len(), 4); // <- should get 4 shardings
    assert_eq!(sharding_4[0].len(), 3); // <- should still get 3 sets
    assert_eq!(sharding_4[0][0].as_ref().unwrap().len(), 2);
    assert_eq!(sharding_4[0][1].as_ref().unwrap().len(), 6);
    assert_eq!(sharding_4[0][2].as_ref().unwrap().len(), 1); // <- parallelized set
}

#[test]
fn join_it_any_chain_test_2() {
    let sets = vec![
        Some((ShardingMode::AnyEach, create_dummy_set(vec![0, 1, 2]))),
        Some((ShardingMode::AnyEach, create_dummy_set(vec![0, 1]))),
    ];

    let join_order = vec![0, 1];
    let join_strategies = vec![JoinStrategy::Cross];

    let sharding_2 = get_sharding(sets.clone(), join_order.clone(), join_strategies.clone(), 2);
    assert_eq!(sharding_2.len(), 2); // <- should get 2 shardings
    assert_eq!(sharding_2[0].len(), 2); // <- should still get 2 sets
    assert_eq!(sharding_2[0][0].as_ref().unwrap().len(), 3);
    assert_eq!(sharding_2[0][1].as_ref().unwrap().len(), 1); // <- parallelized set

    let sharding_3 = get_sharding(sets.clone(), join_order.clone(), join_strategies.clone(), 3);
    assert_eq!(sharding_3.len(), 3); // <- should get 3 shardings
    assert_eq!(sharding_3[0].len(), 2); // <- should still get 2 sets
    assert_eq!(sharding_3[0][0].as_ref().unwrap().len(), 1); // <- parallelized set
    assert_eq!(sharding_3[0][1].as_ref().unwrap().len(), 2);

    let sharding_6 = get_sharding(sets.clone(), join_order.clone(), join_strategies.clone(), 6);
    assert_eq!(sharding_6.len(), 6); // <- should get 6 shardings
    assert_eq!(sharding_6[0].len(), 2); // <- should still get 2 sets
    assert_eq!(sharding_6[0][0].as_ref().unwrap().len(), 1); // <- parallelized set
    assert_eq!(sharding_6[0][1].as_ref().unwrap().len(), 1); // <- parallelized set
}

#[test]
fn join_it_any_chain_test_3() {
    let sets = vec![
        Some((ShardingMode::Each, create_dummy_set(vec![0, 0]))),
        Some((ShardingMode::AnyEach, create_dummy_set(vec![0, 0, 0]))),
        Some((ShardingMode::AnyKey, create_dummy_set(vec![0, 1, 1]))),
    ];

    let join_order = vec![0, 1, 2];
    let join_strategies = vec![JoinStrategy::Cross, JoinStrategy::Cross];

    let sharding_2 = get_sharding(sets.clone(), join_order.clone(), join_strategies.clone(), 2);
    assert_eq!(sharding_2.len(), 2); // <- should get 2 shardings
    assert_eq!(sharding_2[0].len(), 3); // <- should still get 3 sets
    assert_eq!(sharding_2[0][0].as_ref().unwrap().len(), 1); // <- always parallelized (not any)
    assert_eq!(sharding_2[0][1].as_ref().unwrap().len(), 3);
    assert_eq!(sharding_2[0][2].as_ref().unwrap().len(), 3);

    let sharding_4 = get_sharding(sets.clone(), join_order.clone(), join_strategies.clone(), 4);
    assert_eq!(sharding_4.len(), 4); // <- should get 4 shardings
    assert_eq!(sharding_4[0].len(), 3); // <- should still get 3 sets
    assert_eq!(sharding_4[0][0].as_ref().unwrap().len(), 1); // <- always parallelized (not any)
    assert_eq!(sharding_4[0][1].as_ref().unwrap().len(), 3);
    assert_eq!(sharding_4[0][2].as_ref().unwrap().len(), 1); // <- parallelized any set

    let sharding_6 = get_sharding(sets.clone(), join_order.clone(), join_strategies.clone(), 6);
    assert_eq!(sharding_6.len(), 6); // <- should get 6 shardings
    assert_eq!(sharding_6[0].len(), 3); // <- should still get 3 sets
    assert_eq!(sharding_6[0][0].as_ref().unwrap().len(), 1); // <- always parallelized (not any)
    assert_eq!(sharding_6[0][1].as_ref().unwrap().len(), 1); // <- parallelized any set
    assert_eq!(sharding_6[0][2].as_ref().unwrap().len(), 3);
}

#[test]
fn join_it_any_chain_test_4() {
    let sets = vec![
        Some((ShardingMode::Each, create_dummy_set(vec![0, 0]))),
        Some((ShardingMode::AnyKey, create_dummy_set(vec![0, 2, 0, 1, 3]))),
        Some((ShardingMode::AnyKey, create_dummy_set(vec![0, 1, 1, 4]))),
    ];

    let join_order = vec![1, 2, 0];

    let join_strategies_inner = vec![JoinStrategy::Inner, JoinStrategy::Cross];
    let sharding_inner = get_sharding(
        sets.clone(),
        join_order.clone(),
        join_strategies_inner.clone(),
        10,
    );
    assert_eq!(sharding_inner.len(), 4); // <- should get 4 shardings
    assert_eq!(sharding_inner[0].len(), 3); // <- should still get 3 sets
    assert_eq!(sharding_inner[0][0].as_ref().unwrap().len(), 1); // <- always parallelized (not any)
    assert_eq!(sharding_inner[0][1].as_ref().unwrap().len(), 2); // <- parallelized any set
    assert_eq!(sharding_inner[0][2].as_ref().unwrap().len(), 1); // <- parallelized any set

    let join_strategies_left = vec![JoinStrategy::Left, JoinStrategy::Cross];
    let sharding_left = get_sharding(
        sets.clone(),
        join_order.clone(),
        join_strategies_left.clone(),
        10,
    );
    assert_eq!(sharding_left.len(), 8); // <- should get 2*4=8 shardings
    assert_eq!(sharding_left[0].len(), 3); // <- should still get 3 sets
    assert_eq!(sharding_left[0][0].as_ref().unwrap().len(), 1); // <- always parallelized (not any)
    assert_eq!(sharding_left[0][1].as_ref().unwrap().len(), 2); // <- parallelized any set
    assert_eq!(sharding_left[0][2].as_ref().unwrap().len(), 1); // <- parallelized any set

    let join_strategies_right = vec![JoinStrategy::Right, JoinStrategy::Cross];
    let sharding_right = get_sharding(
        sets.clone(),
        join_order.clone(),
        join_strategies_right.clone(),
        10,
    );
    assert_eq!(sharding_right.len(), 6); // <- should get 2*3=6 shardings
    assert_eq!(sharding_right[0].len(), 3); // <- should still get 3 sets
    assert_eq!(sharding_right[0][0].as_ref().unwrap().len(), 1); // <- always parallelized (not any)
    assert_eq!(sharding_right[0][1].as_ref().unwrap().len(), 2); // <- parallelized any set
    assert_eq!(sharding_right[0][2].as_ref().unwrap().len(), 1); // <- parallelized any set

    let join_strategies_outer = vec![JoinStrategy::Outer, JoinStrategy::Cross];
    let sharding_outer = get_sharding(
        sets.clone(),
        join_order.clone(),
        join_strategies_outer.clone(),
        10,
    );
    assert_eq!(sharding_outer.len(), 10); // <- should get 2*5=10 shardings
    assert_eq!(sharding_outer[0].len(), 3); // <- should still get 3 sets
    assert_eq!(sharding_outer[0][0].as_ref().unwrap().len(), 1); // <- always parallelized (not any)
    assert_eq!(sharding_outer[0][1].as_ref().unwrap().len(), 2); // <- parallelized any set
    assert_eq!(sharding_outer[0][2].as_ref().unwrap().len(), 1); // <- parallelized any set
}

#[test]
fn join_it_any_chain_test_5() {
    let sets = vec![Some((
        ShardingMode::AnyKey,
        create_dummy_set(vec![0, 2, 0, 1, 3]),
    ))];

    let join_order = vec![0];
    let join_strategies = vec![];

    let sharding_2 = get_sharding(sets.clone(), join_order.clone(), join_strategies.clone(), 2);
    assert_eq!(sharding_2.len(), 2); // <- should get 2 shardings
    assert_eq!(sharding_2[0].len(), 1); // <- should still get 1 set
    assert_eq!(sharding_2[0][0].as_ref().unwrap().len(), 3); // <- parallelized any set

    let sharding_4 = get_sharding(sets.clone(), join_order.clone(), join_strategies.clone(), 4);
    assert_eq!(sharding_4.len(), 4); // <- should get 4 shardings
    assert_eq!(sharding_4[0].len(), 1); // <- should still get 1 set
    assert_eq!(sharding_4[0][0].as_ref().unwrap().len(), 2); // <- parallelized any set
}

#[test]
fn join_it_any_chain_test_6() {
    let sets = vec![
        Some((ShardingMode::AnyKey, create_dummy_set(vec![0, 1, 4]))),
        Some((ShardingMode::AnyKey, create_dummy_set(vec![0, 2, 0, 3]))),
        Some((ShardingMode::AnyKey, create_dummy_set(vec![0, 1, 1]))),
        Some((ShardingMode::AnyEach, create_dummy_set(vec![5, 5]))),
    ];

    let join_order = vec![1, 2, 0, 3];
    let join_strategies = vec![
        JoinStrategy::Inner,
        JoinStrategy::Right,
        JoinStrategy::Cross,
    ];

    let sharding_2 = get_sharding(sets.clone(), join_order.clone(), join_strategies.clone(), 2);
    assert_eq!(sharding_2.len(), 2); // <- should get 2 shardings
    assert_eq!(sharding_2[0].len(), 4); // <- should still get 4 sets
    assert_eq!(sharding_2[0][0].as_ref().unwrap().len(), 3);
    assert_eq!(sharding_2[0][1].as_ref().unwrap().len(), 2);
    assert_eq!(sharding_2[0][2].as_ref().unwrap().len(), 1);
    assert_eq!(sharding_2[0][3].as_ref().unwrap().len(), 1); // <- parallelized any set
    assert_eq!(sharding_2[0][0].as_ref().unwrap().item_list[0].0, 0);
    assert_eq!(sharding_2[0][0].as_ref().unwrap().item_list[1].0, 1);
    assert_eq!(sharding_2[0][0].as_ref().unwrap().item_list[2].0, 4);
    assert_eq!(sharding_2[0][1].as_ref().unwrap().item_list[0].0, 0);
    assert_eq!(sharding_2[0][1].as_ref().unwrap().item_list[1].0, 0);
    assert_eq!(sharding_2[0][2].as_ref().unwrap().item_list[0].0, 0);
    assert_eq!(sharding_2[0][3].as_ref().unwrap().item_list[0].0, 5);

    let sharding_3 = get_sharding(sets.clone(), join_order.clone(), join_strategies.clone(), 3);
    assert_eq!(sharding_3.len(), 3); // <- should get 3 shardings
    assert_eq!(sharding_3[0].len(), 4); // <- should still get 4 sets
    assert_eq!(sharding_3[0][0].as_ref().unwrap().len(), 1); // <- parallelized any set
    assert_eq!(sharding_3[0][1].as_ref().unwrap().len(), 2); // <- parallelized any set
    assert_eq!(sharding_3[0][2].as_ref().unwrap().len(), 1); // <- parallelized any set
    assert_eq!(sharding_3[0][3].as_ref().unwrap().len(), 2);
    assert_eq!(sharding_3[0][0].as_ref().unwrap().item_list[0].0, 0);
    assert_eq!(sharding_3[0][1].as_ref().unwrap().item_list[0].0, 0);
    assert_eq!(sharding_3[0][1].as_ref().unwrap().item_list[1].0, 0);
    assert_eq!(sharding_3[0][2].as_ref().unwrap().item_list[0].0, 0);
    assert_eq!(sharding_3[0][3].as_ref().unwrap().item_list[0].0, 5);
    assert_eq!(sharding_3[0][3].as_ref().unwrap().item_list[1].0, 5);
}

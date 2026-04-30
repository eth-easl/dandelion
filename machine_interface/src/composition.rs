use crate::memory_domain::{Context, ContextTrait};
use dandelion_commons::{
    err_dandelion, DandelionError, DandelionResult, DispatcherError, FunctionId,
};
use itertools::Itertools;
use std::{
    cmp,
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
    vec,
};

#[cfg(test)]
use crate::memory_domain::read_only::ReadOnlyContext;

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

    pub fn sharded_len(&self, mode: ShardingMode) -> usize {
        match mode {
            ShardingMode::Each => self.len(),
            ShardingMode::Key => self
                .item_list
                .chunk_by(|(key_a, _, _), (key_b, _, _)| key_a == key_b)
                .count(),
            ShardingMode::All | _ => 1, // any are resource dependent so return 1
        }
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

    // TODO: we are just slicing a vec, should be able to do this via slice references or iters instead of Vecs
    pub fn shard(self, mode: ShardingMode) -> Vec<CompositionSet> {
        return match mode {
            ShardingMode::All => {
                vec![self]
            }
            ShardingMode::Key | ShardingMode::AnyKey => self
                .item_list
                .chunk_by(|(key_a, _, _), (key_b, _, _)| key_a == key_b)
                .map(|new_item_list| CompositionSet {
                    item_list: new_item_list.to_vec(),
                    set_index: self.set_index,
                })
                .collect(),
            ShardingMode::Each | ShardingMode::AnyEach => self
                .item_list
                .into_iter()
                .map(|item| CompositionSet {
                    item_list: vec![item],
                    set_index: self.set_index,
                })
                .collect(),
        };
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

    pub fn combine_keys_with_set(&self, other: &mut HashSet<u32>, intersect: bool) {
        if intersect {
            let mut new_set = HashSet::new();
            for (key, _, _) in self.item_list.iter() {
                if other.contains(key) {
                    new_set.insert(*key);
                }
            }
            *other = new_set;
        } else {
            for (key, _, _) in self.item_list.iter() {
                other.insert(*key);
            }
        }
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

// NOTE: assumes at least one element
fn compute_any_parallelism(
    sets: &Vec<Option<(ShardingMode, CompositionSet)>>,
    join_order: &Vec<usize>,
    join_strategies: &Vec<JoinStrategy>,
    target_parallelism: usize,
) -> HashMap<usize, usize> {
    let mut any_set_parallelism: HashMap<usize, usize> = HashMap::new();
    if target_parallelism == 0 {
        return any_set_parallelism;
    }

    debug_assert!(sets.len() > 0);
    let mut curr_join_keys: HashSet<u32> = HashSet::new();
    let mut curr_any_group: Option<Vec<usize>> = None;
    let mut any_groups: Vec<(Vec<usize>, usize)> = Vec::new();
    let mut fixed_parallelism = 1;
    for (i, set_index) in join_order.iter().enumerate() {
        let strategy = if i > 0 {
            join_strategies[i - 1]
        } else {
            JoinStrategy::Cross
        };
        if sets[*set_index].is_none() {
            match strategy {
                JoinStrategy::Left | JoinStrategy::Outer => {
                    // for left and outer joins this set may be none
                    if let Some(idx_list) = curr_any_group.as_mut() {
                        idx_list.push(*set_index);
                    }
                }
                _ => {
                    curr_any_group = None;
                    curr_join_keys.clear();
                }
            }
        }
        let (sharding, set) = sets[*set_index].as_ref().unwrap();
        match *sharding {
            ShardingMode::AnyEach => {
                // push current group or add parallelism of current join
                if let Some(idx_list) = curr_any_group.take() {
                    any_groups.push((idx_list, curr_join_keys.len()));
                    curr_join_keys.clear();
                } else if !curr_join_keys.is_empty() {
                    fixed_parallelism *= curr_join_keys.len();
                    curr_join_keys.clear();
                }

                // push this group (each cannot be joined)
                any_groups.push((vec![*set_index], set.len()));
            }
            ShardingMode::AnyKey => {
                match strategy {
                    JoinStrategy::Left => {
                        // only add the set index to the list
                        if let Some(idx_list) = curr_any_group.as_mut() {
                            idx_list.push(*set_index);
                        }
                    }
                    JoinStrategy::Right => {
                        // only use this set's keys
                        curr_join_keys.clear();
                        set.combine_keys_with_set(&mut curr_join_keys, false);
                        if let Some(idx_list) = curr_any_group.as_mut() {
                            idx_list.push(*set_index);
                        } else {
                            curr_any_group = Some(vec![*set_index]);
                        }
                    }
                    JoinStrategy::Inner => {
                        // use key intersection
                        set.combine_keys_with_set(&mut curr_join_keys, true);
                        if let Some(idx_list) = curr_any_group.as_mut() {
                            idx_list.push(*set_index);
                        }
                    }
                    JoinStrategy::Outer => {
                        // use key union
                        set.combine_keys_with_set(&mut curr_join_keys, false);
                        if let Some(idx_list) = curr_any_group.as_mut() {
                            idx_list.push(*set_index);
                        } else {
                            curr_any_group = Some(vec![*set_index]);
                        }
                    }
                    JoinStrategy::Cross => {
                        // push current group or add parallelism of current join
                        if let Some(idx_list) = curr_any_group.take() {
                            any_groups.push((idx_list, curr_join_keys.len()));
                            curr_join_keys.clear();
                        } else if !curr_join_keys.is_empty() {
                            fixed_parallelism *= curr_join_keys.len();
                            curr_join_keys.clear();
                        }
                        // create next group
                        curr_join_keys.clear();
                        set.combine_keys_with_set(&mut curr_join_keys, false);
                        curr_any_group = Some(vec![*set_index]);
                    }
                }
            }
            ShardingMode::Key => {
                // if an AnyKey is joined with a normal Key it becomes invalid except for cross joins
                if strategy == JoinStrategy::Cross {
                    if let Some(idx_list) = curr_any_group.take() {
                        any_groups.push((idx_list, curr_join_keys.len()));
                    }
                    curr_join_keys.clear();
                } else {
                    curr_any_group = None;
                    if strategy == JoinStrategy::Right {
                        curr_join_keys.clear(); // -> for right we only care about this set's keys
                    }
                    if strategy != JoinStrategy::Left {
                        set.combine_keys_with_set(
                            &mut curr_join_keys,
                            strategy != JoinStrategy::Outer,
                        );
                    }
                }
            }
            _ => {
                // push current group or add parallelism of current join
                if let Some(idx_list) = curr_any_group.take() {
                    any_groups.push((idx_list, curr_join_keys.len()));
                    curr_join_keys.clear();
                } else if !curr_join_keys.is_empty() {
                    fixed_parallelism *= curr_join_keys.len();
                    curr_join_keys.clear();
                }
                // add parallelism of this set
                fixed_parallelism *= set.sharded_len(*sharding);
            }
        }
    }

    // push current group or add parallelism of current join
    if let Some(idx_list) = curr_any_group.take() {
        any_groups.push((idx_list, curr_join_keys.len()));
        curr_join_keys.clear();
    } else if !curr_join_keys.is_empty() {
        fixed_parallelism *= curr_join_keys.len();
        curr_join_keys.clear();
    }

    // determine the parallelism of
    if fixed_parallelism < target_parallelism && !any_groups.is_empty() {
        let mut leftover_parallelism = target_parallelism / fixed_parallelism;
        loop {
            let mut best_idx = 0;
            let mut best_dist = 0.0;
            for (i, (_, max_p)) in any_groups.iter().enumerate() {
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

            // if the best set has fewer elements than the leftover parallelization we continue and
            // check if we can find another set to parallelize over in addition to the found one
            if best_dist >= 1.0 {
                for set_idx in any_groups[best_idx].0.iter() {
                    any_set_parallelism.insert(*set_idx, any_groups[best_idx].1);
                }
                leftover_parallelism /= any_groups[best_idx].1;
                if leftover_parallelism <= 1 {
                    break;
                }
            } else {
                for set_idx in any_groups[best_idx].0.iter() {
                    any_set_parallelism.insert(
                        *set_idx,
                        cmp::min(leftover_parallelism, any_groups[best_idx].1),
                    );
                }
                break;
            }
        }
    }
    any_set_parallelism
}

pub fn get_sharding(
    mut sets: Vec<Option<(ShardingMode, CompositionSet)>>,
    mut join_order: Vec<usize>,
    mut join_strategies: Vec<JoinStrategy>,
) -> Vec<Vec<Option<CompositionSet>>> {
    let set_num = sets.len();
    let mut final_sharding = Vec::new();

    if set_num == 0 {
        return final_sharding;
    }

    let target_parallelism = 10; // TODO: move to arg
    let any_set_parallelism =
        compute_any_parallelism(&sets, &join_order, &join_strategies, target_parallelism);

    // make sure every set is in the order and has a strategy
    let mut missing_sets: Vec<_> = (0..set_num).map(|index| Some(index)).collect();
    for index in join_order.iter() {
        missing_sets[*index] = None;
    }
    for missing_index in missing_sets {
        if let Some(missing) = missing_index {
            join_order.push(missing);
        }
    }
    join_strategies.resize(set_num - 1, JoinStrategy::Cross);

    // create the iterators later used to generate the final sharding
    let mut join_iter = None;
    // for (i, set_idx) in join_order.iter().enumerate() {
    let mut i = 0;
    while i < join_order.len() {
        let set_idx = join_order[i];
        if let Some((sharding, set)) = sets[set_idx].take() {
            match sharding {
                ShardingMode::All => {
                    join_iter = join_iterator::SetAllIterator::new(join_iter, set, set_idx);
                }
                ShardingMode::Each => {
                    join_iter = join_iterator::SetEachIterator::new(join_iter, set, set_idx);
                }
                ShardingMode::Key => {
                    let strategy = if i > 0 {
                        join_strategies[i - 1]
                    } else {
                        JoinStrategy::Cross
                    };
                    join_iter =
                        join_iterator::SetKeyIterator::new(join_iter, set, strategy, set_idx);
                }
                ShardingMode::AnyEach => {
                    if let Some(num_groups) = any_set_parallelism.get(&set_idx) {
                        join_iter = join_iterator::AnyIterator::new(
                            join_iter,
                            vec![set],
                            vec![],
                            vec![set_idx],
                            *num_groups,
                            sharding,
                        );
                    } else {
                        // if we got no specific parallelism for this set we assume parallelism is 1
                        join_iter = join_iterator::SetAllIterator::new(join_iter, set, set_idx);
                    }
                }
                ShardingMode::AnyKey => {
                    if let Some(num_groups) = any_set_parallelism.get(&set_idx) {
                        // get all sets that are joined together (i.e. find the next cross join)
                        let mut joined_sets = vec![set];
                        let mut joined_set_idcs = vec![set_idx];
                        let mut joined_strategies = vec![];
                        while i + 1 < join_order.len() {
                            let next_strategy = join_strategies[i + 1];
                            if next_strategy != JoinStrategy::Cross {
                                let next_set_idx = join_order[i + 1];
                                if let Some((_, set)) = sets[next_set_idx].take() {
                                    joined_sets.push(set);
                                    joined_set_idcs.push(next_set_idx);
                                    joined_strategies.push(next_strategy);
                                }
                                // NOTE: if the set is none we just skip it -> this allows for empty
                                //       optional sets
                                i += 1;
                            } else {
                                // a cross join breaks the chain
                                break;
                            }
                        }
                        join_iter = join_iterator::AnyIterator::new(
                            join_iter,
                            joined_sets,
                            joined_strategies,
                            joined_set_idcs,
                            *num_groups,
                            sharding,
                        );
                    } else {
                        // if we got no specific parallelism for this set we assume parallelism is 1
                        join_iter = join_iterator::SetAllIterator::new(join_iter, set, set_idx);
                    }
                }
            }
            i += 1;
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

// /// Structure to hold join iterator
// #[derive(Debug)]
// struct JoinIterator {
//     left: Option<Box<JoinIterator>>,
//     right: Vec<CompositionSet>,
//     /// Index of the current interator into it's compositon set vector
//     /// The current index points to the element set by the last successful advance call
//     right_index: usize,
//     write_index: usize,
//     mode: JoinStrategy,
//     key: u32,
// }

// impl JoinIterator {
//     fn new(
//         mode: JoinStrategy,
//         mut left_opt: Option<Box<Self>>,
//         right_opt: Option<(ShardingMode, CompositionSet)>,
//         write_index: usize,
//     ) -> Option<Box<Self>> {
//         if right_opt.is_none() || right_opt.as_ref().unwrap().1.is_empty() {
//             return left_opt;
//         }
//         let (set_mode, set) = right_opt.unwrap();
//         let right = set.shard(set_mode);

//         // we assume the keys of the sets are in descending order
//         debug_assert!(right.is_sorted_by_key(|set| set.item_list[0].0));

//         if right.is_empty() {
//             return left_opt;
//         }

//         let mut right_index = 0;
//         let mut key = right[0].item_list[0].0;

//         if let Some(left) = &mut left_opt {
//             match mode {
//                 JoinStrategy::Inner => {
//                     while right_index < right.len() && key != left.key {
//                         if key < left.key {
//                             right_index += 1;
//                             if right_index < right.len() {
//                                 key = right[right_index].item_list[0].0;
//                             }
//                         } else {
//                             if !left.advance() {
//                                 right_index = right.len();
//                             }
//                         }
//                     }
//                     if right_index == right.len() {
//                         return None;
//                     }
//                 }
//                 JoinStrategy::Left => {
//                     while right_index < right.len() && right[right_index].item_list[0].0 < left.key
//                     {
//                         right_index += 1;
//                     }
//                     key = left.key;
//                     if right_index == right.len() {
//                         return left_opt;
//                     }
//                 }
//                 JoinStrategy::Outer => {
//                     if left.key < key {
//                         key = left.key;
//                     }
//                     // else already has the correct key set
//                 }
//                 JoinStrategy::Right | JoinStrategy::Cross => (),
//             }
//         // there is not left iterator
//         } else {
//             match mode {
//                 JoinStrategy::Inner | JoinStrategy::Left => {
//                     return None;
//                 }
//                 _ => (),
//             }
//         }
//         Some(Box::new(Self {
//             left: left_opt,
//             right,
//             right_index: 0,
//             write_index,
//             mode,
//             key,
//         }))
//     }

//     fn fill_in(&mut self, to_fill: &mut Vec<Option<CompositionSet>>) {
//         let right_filled = self.right_index < self.right.len()
//             && self.key == self.right[self.right_index].item_list[0].0;
//         if right_filled {
//             to_fill[self.write_index] = Some(self.right[self.right_index].clone());
//         }
//         if let Some(left) = &mut self.left {
//             match self.mode {
//                 // modes for which always want left to fill in
//                 JoinStrategy::Cross | JoinStrategy::Left => left.fill_in(to_fill),
//                 // Only want to fill left if it is the one with the current key
//                 JoinStrategy::Outer => {
//                     if self.key == left.key {
//                         left.fill_in(to_fill)
//                     }
//                 }
//                 // Only want left to fill if right has filled something in
//                 JoinStrategy::Inner => {
//                     if right_filled {
//                         left.fill_in(to_fill)
//                     }
//                 }
//                 // Only want left to fill if right has filled and the keys match
//                 JoinStrategy::Right => {
//                     if right_filled && self.key == left.key {
//                         left.fill_in(to_fill)
//                     }
//                 }
//             }
//         };
//     }

//     /// Advance the iterator by one.
//     /// Another advance call after a advance that returned false always returns false
//     /// A fill_in call after a advance that called false is undefined behaviour.
//     fn advance(&mut self) -> bool {
//         // set this when there is no more adavnce calls to be had to shortcut evaluation
//         if self.right_index == self.right.len() {
//             return false;
//         }
//         let right = &mut self.right;
//         if let Some(left) = &mut self.left {
//             match self.mode {
//                 JoinStrategy::Inner => {
//                     // advance both at least once for inner
//                     // left is advanced on checking (after checking right can stil be advanced)
//                     // right is advanced after
//                     if self.right_index + 1 >= right.len() || !left.advance() {
//                         self.right_index = right.len();
//                         return false;
//                     }
//                     self.right_index += 1;
//                     self.key = right[self.right_index].item_list[0].0;
//                     while self.key != left.key {
//                         if self.key > left.key {
//                             if !left.advance() {
//                                 self.right_index = right.len();
//                                 return false;
//                             }
//                         // need to advance right and are able to do so
//                         } else if self.key < left.key {
//                             if self.right_index + 1 >= right.len() {
//                                 self.right_index = right.len();
//                                 return false;
//                             } else {
//                                 self.right_index += 1;
//                                 self.key = right[self.right_index].item_list[0].0;
//                             }
//                         }
//                     }
//                     true
//                 }
//                 JoinStrategy::Left => {
//                     // advance left and see if we can match
//                     if left.advance() {
//                         while self.right_index + 1 < right.len()
//                             && right[self.right_index].item_list[0].0 < left.key
//                         {
//                             self.right_index += 1;
//                         }
//                         // after this the key is guaranteed to be equal to the left key or bigger
//                         // so if the keys match that will be fine for copy in, otherwise this will be skipped
//                         // if the key already equal or bigger, it was not advanced
//                         self.key = left.key;
//                         true
//                     } else {
//                         self.right_index = right.len();
//                         false
//                     }
//                 }
//                 JoinStrategy::Right => {
//                     if self.right_index + 1 >= right.len() {
//                         self.right_index = right.len();
//                         return false;
//                     }
//                     self.right_index += 1;
//                     self.key = right[self.right_index].item_list[0].0;
//                     while self.key > left.key {
//                         if !left.advance() {
//                             break;
//                         }
//                     }
//                     true
//                 }
//                 JoinStrategy::Outer => {
//                     let current_self_key = right[self.right_index].item_list[0].0;
//                     let right_can_be_advanced = self.right_index + 1 < right.len();
//                     // check if one of the already known keys is bigger, if so we know we can adavance
//                     if self.key < left.key {
//                         // last key was set from right, since left is bigger
//                         debug_assert_eq!(self.key, current_self_key);
//                         // if right can be advance it should be advanced, otherwise just set key to left one
//                         if right_can_be_advanced {
//                             self.right_index += 1;
//                             // new right might still be smaller than left key
//                             self.key = u32::min(right[self.right_index].item_list[0].0, left.key);
//                         } else {
//                             self.key = left.key;
//                         }
//                         true
//                     } else if self.key < current_self_key {
//                         // the last key was set from left, since right is bigger
//                         debug_assert_eq!(self.key, left.key);
//                         // if left can be adnvanced it should be, if not move
//                         if left.advance() {
//                             // new left key might still be smaller than right
//                             self.key = u32::min(right[self.right_index].item_list[0].0, left.key);
//                         } else {
//                             //  left did not advance, so set key to current right key
//                             self.key = current_self_key;
//                         }
//                         true
//                     } else if self.key == left.key && self.key == current_self_key {
//                         // both keys are the same, so advance any that are possible to advance and take new key from there
//                         let left_advance_success = left.advance();
//                         if right_can_be_advanced {
//                             self.right_index += 1;
//                         }
//                         match (right_can_be_advanced, left_advance_success) {
//                             (true, true) => {
//                                 self.key =
//                                     u32::min(right[self.right_index].item_list[0].0, left.key);
//                                 true
//                             }
//                             (true, false) => {
//                                 self.key = right[self.right_index].item_list[0].0;
//                                 true
//                             }
//                             (false, true) => {
//                                 self.key = left.key;
//                                 true
//                             }
//                             (false, false) => {
//                                 self.right_index = right.len();
//                                 false
//                             }
//                         }
//                     } else {
//                         // the key is already set to the bigger of the two current keys, try to advance that one
//                         if current_self_key == left.key {
//                             let did_advance = left.advance();
//                             self.key = left.key;
//                             did_advance
//                         } else {
//                             if right_can_be_advanced {
//                                 self.right_index += 1;
//                                 self.key = right[self.right_index].item_list[0].0;
//                             }
//                             right_can_be_advanced
//                         }
//                     }
//                 }
//                 JoinStrategy::Cross => {
//                     if self.right_index + 1 < right.len() {
//                         self.right_index += 1;
//                         self.key = right[self.right_index].item_list[0].0;
//                         true
//                     } else if left.advance() {
//                         self.right_index = 0;
//                         self.key = right[0].item_list[0].0;
//                         true
//                     } else {
//                         // set right index to len so we can't accidentally copy something
//                         self.right_index = right.len();
//                         false
//                     }
//                 }
//             }
//         } else {
//             if self.right_index + 1 >= right.len() {
//                 self.right_index = right.len();
//                 false
//             } else {
//                 // advancing only makes sense for certain modes here
//                 if self.mode == JoinStrategy::Right
//                     || self.mode == JoinStrategy::Outer
//                     || self.mode == JoinStrategy::Cross
//                 {
//                     self.right_index += 1;
//                     self.key = right[self.right_index].item_list[0].0;
//                     true
//                 } else {
//                     panic!("Should never have join iterator with left or inner that has None for the left value");
//                 }
//             }
//         }
//     }
// }

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

    let sharding = get_sharding(sets, join_order, join_strategies);
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

    let sharding = get_sharding(sets, join_order, join_strategies);
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

    let sharding = get_sharding(sets, join_order, join_strategies);
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

    let sharding = get_sharding(sets, join_order, join_strategies);
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

    let sharding = get_sharding(sets, join_order, join_strategies);
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

    let sharding = get_sharding(sets, join_order, join_strategies);
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

    let sharding = get_sharding(sets, join_order, join_strategies);
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
fn any_parallelism_test_1() {
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

    let any_set_parallelism_6 = compute_any_parallelism(&sets, &join_order, &join_strategies, 6);
    assert_eq!(any_set_parallelism_6.len(), 1);
    assert!(any_set_parallelism_6.contains_key(&1));
    assert_eq!(any_set_parallelism_6[&1], 6);

    let any_set_parallelism_3 = compute_any_parallelism(&sets, &join_order, &join_strategies, 3);
    assert_eq!(any_set_parallelism_3.len(), 1);
    assert!(any_set_parallelism_3.contains_key(&1));
    assert_eq!(any_set_parallelism_3[&1], 3);

    let any_set_parallelism_4 = compute_any_parallelism(&sets, &join_order, &join_strategies, 4);
    assert_eq!(any_set_parallelism_4.len(), 1);
    assert!(any_set_parallelism_4.contains_key(&2));
    assert_eq!(any_set_parallelism_4[&2], 4);
}

#[test]
fn any_parallelism_test_2() {
    let sets = vec![
        Some((ShardingMode::AnyEach, create_dummy_set(vec![0, 1, 2]))),
        Some((ShardingMode::AnyEach, create_dummy_set(vec![0, 1]))),
    ];

    let join_order = vec![0, 1];
    let join_strategies = vec![JoinStrategy::Cross];

    let any_set_parallelism_2 = compute_any_parallelism(&sets, &join_order, &join_strategies, 2);
    assert_eq!(any_set_parallelism_2.len(), 1);
    assert!(any_set_parallelism_2.contains_key(&1));
    assert_eq!(any_set_parallelism_2[&1], 2);

    let any_set_parallelism_3 = compute_any_parallelism(&sets, &join_order, &join_strategies, 3);
    assert_eq!(any_set_parallelism_3.len(), 1);
    assert!(any_set_parallelism_3.contains_key(&0));
    assert_eq!(any_set_parallelism_3[&0], 3);

    let any_set_parallelism_6 = compute_any_parallelism(&sets, &join_order, &join_strategies, 6);
    assert_eq!(any_set_parallelism_6.len(), 2);
    assert!(any_set_parallelism_6.contains_key(&0));
    assert!(any_set_parallelism_6.contains_key(&1));
    assert_eq!(any_set_parallelism_6[&0], 3);
    assert_eq!(any_set_parallelism_6[&1], 2);
}

#[test]
fn any_parallelism_test_3() {
    let sets = vec![
        Some((ShardingMode::Each, create_dummy_set(vec![0, 0]))),
        Some((ShardingMode::AnyEach, create_dummy_set(vec![0, 0, 0]))),
        Some((ShardingMode::AnyKey, create_dummy_set(vec![0, 1, 1]))),
    ];

    let join_order = vec![0, 1, 2];
    let join_strategies = vec![JoinStrategy::Cross, JoinStrategy::Cross];

    let any_set_parallelism_2 = compute_any_parallelism(&sets, &join_order, &join_strategies, 2);
    assert_eq!(any_set_parallelism_2.len(), 0);

    let any_set_parallelism_4 = compute_any_parallelism(&sets, &join_order, &join_strategies, 4);
    assert_eq!(any_set_parallelism_4.len(), 1);
    assert!(any_set_parallelism_4.contains_key(&2));
    assert_eq!(any_set_parallelism_4[&2], 2);

    let any_set_parallelism_6 = compute_any_parallelism(&sets, &join_order, &join_strategies, 6);
    assert_eq!(any_set_parallelism_6.len(), 1);
    assert!(any_set_parallelism_6.contains_key(&1));
    assert_eq!(any_set_parallelism_6[&1], 3);
}

#[test]
fn any_parallelism_test_4() {
    let sets = vec![
        Some((ShardingMode::Each, create_dummy_set(vec![0, 0]))),
        Some((ShardingMode::AnyKey, create_dummy_set(vec![0, 2, 0, 1, 3]))),
        Some((ShardingMode::AnyKey, create_dummy_set(vec![0, 1, 1, 4]))),
    ];

    let join_order = vec![0, 1, 2];

    let join_strategies_inner = vec![JoinStrategy::Cross, JoinStrategy::Inner];
    let any_set_parallelism_inner =
        compute_any_parallelism(&sets, &join_order, &join_strategies_inner, 10);
    assert_eq!(any_set_parallelism_inner.len(), 2);
    assert!(any_set_parallelism_inner.contains_key(&1));
    assert!(any_set_parallelism_inner.contains_key(&2));
    assert_eq!(any_set_parallelism_inner[&1], 2);
    assert_eq!(any_set_parallelism_inner[&2], 2);

    let join_strategies_left = vec![JoinStrategy::Cross, JoinStrategy::Left];
    let any_set_parallelism_left =
        compute_any_parallelism(&sets, &join_order, &join_strategies_left, 10);
    assert_eq!(any_set_parallelism_left.len(), 2);
    assert!(any_set_parallelism_left.contains_key(&1));
    assert!(any_set_parallelism_left.contains_key(&2));
    assert_eq!(any_set_parallelism_left[&1], 4);
    assert_eq!(any_set_parallelism_left[&2], 4);

    let join_strategies_right = vec![JoinStrategy::Cross, JoinStrategy::Right];
    let any_set_parallelism_right =
        compute_any_parallelism(&sets, &join_order, &join_strategies_right, 10);
    assert_eq!(any_set_parallelism_right.len(), 2);
    assert!(any_set_parallelism_right.contains_key(&1));
    assert!(any_set_parallelism_right.contains_key(&2));
    assert_eq!(any_set_parallelism_right[&1], 3);
    assert_eq!(any_set_parallelism_right[&2], 3);

    let join_strategies_outer = vec![JoinStrategy::Cross, JoinStrategy::Outer];
    let any_set_parallelism_outer =
        compute_any_parallelism(&sets, &join_order, &join_strategies_outer, 10);
    assert_eq!(any_set_parallelism_outer.len(), 2);
    assert!(any_set_parallelism_outer.contains_key(&1));
    assert!(any_set_parallelism_outer.contains_key(&2));
    assert_eq!(any_set_parallelism_outer[&1], 5);
    assert_eq!(any_set_parallelism_outer[&2], 5);
}

#[test]
fn any_parallelism_test_5() {
    let sets = vec![
        Some((ShardingMode::AnyKey, create_dummy_set(vec![0, 2, 0, 1, 3]))),
        Some((ShardingMode::Key, create_dummy_set(vec![0, 1, 1, 4]))),
    ];

    let join_order = vec![0, 1];
    let join_strategies_inner = vec![JoinStrategy::Inner];

    // if an AnyKey and a Key are joined with a strategy other than cross it is no longer a valid
    // any set -> the output should be empty
    let any_set_parallelism_inner =
        compute_any_parallelism(&sets, &join_order, &join_strategies_inner, 10);
    assert_eq!(any_set_parallelism_inner.len(), 0);
}

#[test]
fn any_parallelism_test_6() {
    let sets = vec![Some((
        ShardingMode::AnyKey,
        create_dummy_set(vec![0, 2, 0, 1, 3]),
    ))];

    let join_order = vec![0];
    let join_strategies = vec![];

    let any_set_parallelism_2 = compute_any_parallelism(&sets, &join_order, &join_strategies, 2);
    assert_eq!(any_set_parallelism_2.len(), 1);
    assert!(any_set_parallelism_2.contains_key(&0));
    assert_eq!(any_set_parallelism_2[&0], 2);

    let any_set_parallelism_4 = compute_any_parallelism(&sets, &join_order, &join_strategies, 4);
    assert_eq!(any_set_parallelism_4.len(), 1);
    assert!(any_set_parallelism_4.contains_key(&0));
    assert_eq!(any_set_parallelism_4[&0], 4);
}

#[test]
fn any_parallelism_test_7() {
    let sets = vec![
        Some((ShardingMode::AnyKey, create_dummy_set(vec![0, 1, 4]))),
        Some((ShardingMode::AnyKey, create_dummy_set(vec![0, 2, 0, 3]))),
        Some((ShardingMode::AnyKey, create_dummy_set(vec![0, 1, 1]))),
        Some((ShardingMode::AnyEach, create_dummy_set(vec![0, 0]))),
    ];

    let join_order = vec![1, 2, 0, 3];
    let join_strategies = vec![
        JoinStrategy::Inner,
        JoinStrategy::Right,
        JoinStrategy::Cross,
    ];

    let any_set_parallelism_2 = compute_any_parallelism(&sets, &join_order, &join_strategies, 2);
    assert_eq!(any_set_parallelism_2.len(), 1);
    assert!(any_set_parallelism_2.contains_key(&3));
    assert_eq!(any_set_parallelism_2[&3], 2);

    let any_set_parallelism_3 = compute_any_parallelism(&sets, &join_order, &join_strategies, 3);
    assert_eq!(any_set_parallelism_3.len(), 3);
    assert!(any_set_parallelism_3.contains_key(&0));
    assert!(any_set_parallelism_3.contains_key(&1));
    assert!(any_set_parallelism_3.contains_key(&2));
    assert_eq!(any_set_parallelism_3[&0], 3);
    assert_eq!(any_set_parallelism_3[&1], 3);
    assert_eq!(any_set_parallelism_3[&2], 3);
}

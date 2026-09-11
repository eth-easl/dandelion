use std::{cmp, sync::Arc};

use dandelion_commons::data::DataSet;
use log::{debug, trace};

use crate::join_iterator::{
    AnyIterator, JoinIterator, SetAllIterator, SetEachIterator, SetKeyIterator,
};

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum JoinStrategy {
    Inner,
    Left,
    Right,
    Outer,
    Cross,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Sharding {
    All,
    Each,
    Keyed(JoinStrategy),
    AnyEach,
    AnyKeyed(JoinStrategy),
}

impl Sharding {
    pub fn is_blocking(&self) -> bool {
        match self {
            Sharding::Each | Sharding::AnyEach => false,
            _ => true,
        }
    }

    pub fn requires_sorting(&self) -> bool {
        match self {
            Sharding::Keyed(_) | Sharding::AnyKeyed(_) => true,
            _ => false,
        }
    }
}

// TODO: figure out where to put this
/// Contains system information used by the sharding policy.
pub struct SystemInfo {
    // /// The number of local compute cores in the system (as a watcher so we can update remote nodes
    // /// if the local core count changes).
    // pub num_local_cores_watcher: watch::Receiver<usize>,
    // pub num_local_cores_sender: watch::Sender<usize>,
    // /// The number of remote compute cores in the system.
    // pub num_remote_cores: AtomicUsize,
}

impl SystemInfo {
    pub fn local_cores(&self) -> usize {
        todo!("implement")
    }
    pub fn remote_cores(&self) -> usize {
        todo!("implement")
    }
}

pub struct AnyShardingParams {
    /// A reference to the current system information maintained by the queue.
    pub sys_info: Arc<SystemInfo>,
    /// A constant that estimates the offload overhead when determining the number of partitions.
    pub offload_const: usize,
}

pub enum AnyShardingMode {
    /// Use the maximum number of partiions (`AnyKey` becomes `Key`, `AnyEach` becomes `Each`).
    MaxSharding,
    /// Use a fixed target number of partitions.
    FixedSharding(usize),
    /// Compute an "optimal" target number of partitions.
    AutoSharding(AnyShardingParams),
}

#[derive(Debug)]
pub(crate) struct AnySetGroup {
    largest_set_size: usize,
    max_partitions: usize,
    target_partitions: usize,
    min_set_bytes: usize,
    processed: bool,
}

impl AnySetGroup {
    pub fn new(largest_set_size: usize, min_set_bytes: usize, max_partitions: usize) -> Self {
        // if the largest set size is zero it is considered unknown (e.g. contains system function
        // reference items) -> setting a target_partitions value of 0 leads to max sharding
        if largest_set_size == 0 {
            Self {
                largest_set_size,
                max_partitions,
                target_partitions: 0,
                min_set_bytes,
                processed: true,
            }
        } else {
            Self {
                largest_set_size,
                max_partitions,
                target_partitions: 1,
                min_set_bytes,
                processed: false,
            }
        }
    }

    pub fn target_partitions(&self) -> usize {
        self.target_partitions
    }
}

/// Computes the sharding for the given sets following the given join order and join strategies.
/// The `join_order` vector is expected to be of length `sets.len()`, the `join_strategies` vector
/// is expected to be of size `sets.len() - 1`.
///
/// Based on the any sharding mode the function uses the system information to determine a suitable
/// number of partitions and then tries to shard `AnyEach` and `AnyKey` sets accordingly if possible.
/// If an `AnyShardingMode::MaxSharding` is given it will create the maximum possible partitions
/// (i.e. `AnyKey` becomes `Key` and `AnyEach` becomes `Each`).
///
/// The `min_set_size` is used to create `any` set shards of at least that size and is ignored if
/// set to 0.
pub fn create_sharding_iter(
    mut sets: Vec<Option<(DataSet, Sharding)>>,
    join_order: &[usize],
    any_sharding_mode: &AnyShardingMode,
    min_set_bytes: &[usize],
) -> Option<Box<dyn JoinIterator>> {
    let set_num = sets.len();
    debug_assert_eq!(join_order.len(), set_num);

    trace!(
        "Computing sharding using sets: {:?}, join_order: {:?}.",
        sets,
        join_order,
    );
    if set_num == 0 {
        trace!("Found empty sharding.");
        return None;
    }

    // first create the iterators for keyed shardings
    let mut key_join_iter = None;
    let mut fixed_partitions = 1;
    let mut join_group_key_set: Vec<u32> = vec![];
    let mut i = 0;
    while i < set_num {
        let set_idx = join_order[i];
        if let Some((set, sharding)) = sets[set_idx].take() {
            if let Sharding::Keyed(strategy) = sharding {
                if strategy == JoinStrategy::Cross {
                    if join_group_key_set.len() > 0 {
                        fixed_partitions *= join_group_key_set.len();
                        join_group_key_set.clear();
                    }
                }
                key_join_iter = SetKeyIterator::new(
                    key_join_iter,
                    set,
                    strategy,
                    set_idx,
                    &mut join_group_key_set,
                );
            } else {
                sets[set_idx] = Some((set, sharding)); // put back into sets
                break; // continue building all other iterators in the second loop
            }
        }
        i += 1;
    }
    if join_group_key_set.len() > 0 {
        fixed_partitions *= join_group_key_set.len();
        join_group_key_set.clear();
    }

    // second create the iterators for all remaining shardings
    let mut any_set_groups = Vec::new();
    let mut join_iter = key_join_iter.map(|i| i as Box<dyn JoinIterator>);
    while i < set_num {
        let set_idx = join_order[i];
        if let Some((set, sharding)) = sets[set_idx].take() {
            match sharding {
                Sharding::All => {
                    join_iter = SetAllIterator::new(join_iter, set, set_idx);
                }
                Sharding::Each => {
                    let partitions;
                    (join_iter, partitions) = SetEachIterator::new(join_iter, set, set_idx);
                    fixed_partitions *= partitions;
                }
                Sharding::AnyEach => {
                    let (any_join_iter, largest_set_size, min_set_size, max_partitions) =
                        AnyIterator::new(
                            join_iter,
                            vec![set],
                            vec![],
                            vec![set_idx],
                            sharding,
                            &min_set_bytes[i..(i + 1)],
                        );
                    if max_partitions > 0 {
                        any_set_groups.push(AnySetGroup::new(
                            largest_set_size,
                            min_set_size,
                            max_partitions,
                        ));
                    }
                    join_iter = any_join_iter.map(|i| i as Box<dyn JoinIterator>);
                }
                Sharding::AnyKeyed(strategy) => {
                    debug_assert_eq!(strategy, JoinStrategy::Cross);

                    // get all sets that are joined together (i.e. find the next cross join)
                    let mut joined_sets = vec![set];
                    let mut joined_set_idcs = vec![set_idx];
                    let mut joined_strategies = vec![];
                    let start_idx = i;
                    while i + 1 < join_order.len() {
                        let next_set_idx = join_order[i + 1];
                        if let Some((_, Sharding::AnyKeyed(next_strategy))) = sets[next_set_idx] {
                            if next_strategy == JoinStrategy::Cross {
                                // a cross join breaks the chain
                                break;
                            }
                            let (next_set, _) = sets[next_set_idx].take().unwrap();
                            joined_sets.push(next_set);
                            joined_set_idcs.push(next_set_idx);
                            joined_strategies.push(next_strategy);
                            i += 1;
                        } else {
                            break;
                        }
                    }

                    let (any_join_iter, largest_set_size, min_set_size, max_partitions) =
                        AnyIterator::new(
                            join_iter,
                            joined_sets,
                            joined_strategies,
                            joined_set_idcs,
                            sharding,
                            &min_set_bytes[start_idx..(i + 1)],
                        );
                    if max_partitions > 0 {
                        any_set_groups.push(AnySetGroup::new(
                            largest_set_size,
                            min_set_size,
                            max_partitions,
                        ));
                    }
                    join_iter = any_join_iter.map(|i| i as Box<dyn JoinIterator>);
                }
                Sharding::Keyed(_) => {
                    panic!("Expecting key shardings to preceed all other shardings.");
                }
            }
        }
        i += 1;
    }

    // compute the target partitions
    let target_partitions = match any_sharding_mode {
        AnyShardingMode::MaxSharding => 0,
        AnyShardingMode::FixedSharding(n) => *n,
        AnyShardingMode::AutoSharding(params) => {
            let mut total_largest_any_set_sizes = 1;
            let mut total_min_set_sizes = 0;
            for any_group in any_set_groups.iter() {
                total_largest_any_set_sizes *= any_group.largest_set_size;
                total_min_set_sizes += any_group.min_set_bytes;
            }
            if total_largest_any_set_sizes == 0 {
                0
            } else {
                // use a minimal set size of at least 1 for this computation
                let s_min = cmp::max(total_min_set_sizes, 1);
                let c_local = params.sys_info.local_cores();
                let c_remote = params.sys_info.remote_cores();
                if total_largest_any_set_sizes > params.offload_const * s_min * c_local {
                    log::debug!(
                        "Any sets using at most local + remote cores = {}",
                        c_local + c_remote
                    );
                    c_local + c_remote
                } else {
                    log::debug!("Any sets using at most local cores = {}", c_local);
                    c_local
                }
            }
        }
    };

    // compute the partitions for the any shardings
    if fixed_partitions < target_partitions && !any_set_groups.is_empty() {
        let mut leftover_partitions = target_partitions / fixed_partitions;
        loop {
            // find the best suitable set to parallelize over
            let mut best_idx = 0;
            let mut best_dist = 0.0;
            for (i, any_set_group) in any_set_groups.iter().enumerate() {
                // check that we have not yet assigned a partitions to this any set
                if !any_set_group.processed {
                    let remainder = any_set_group.max_partitions % leftover_partitions;
                    if remainder == 0 {
                        best_idx = i;
                        best_dist = 0.0;
                        break;
                    }
                    let dist = (remainder) as f64 / (any_set_group.max_partitions) as f64;
                    if best_dist == 0.0 || dist < best_dist {
                        best_idx = i;
                        best_dist = dist;
                    }
                }
            }

            // if the best set has fewer elements than the leftover parallelization we continue and
            // check if we can find another set to parallelize over in addition to the found one
            if best_dist >= 1.0 {
                any_set_groups[best_idx].target_partitions =
                    any_set_groups[best_idx].max_partitions;
                leftover_partitions /= any_set_groups[best_idx].max_partitions;
                if leftover_partitions <= 1 {
                    break;
                }
                any_set_groups[best_idx].processed = true;
            } else {
                if !any_set_groups[best_idx].processed {
                    any_set_groups[best_idx].target_partitions =
                        cmp::min(leftover_partitions, any_set_groups[best_idx].max_partitions);
                }
                break;
            }
        }
    }
    debug!(
        "Found fixed_partitions: {} and any sets: {:?} (target_partitions: {})",
        fixed_partitions, any_set_groups, target_partitions,
    );
    if target_partitions > 0 {
        trace!("Using any partitions: {:?}", any_set_groups);
        if let Some(iter) = join_iter.as_mut() {
            iter.reduce_any_partitions(any_set_groups);
        }
    }

    join_iter
}

#[cfg(test)]
mod tests {
    use super::*;
    use dandelion_commons::data::{DataItem, Position};

    fn item(key: u32) -> Arc<DataItem> {
        Arc::new(DataItem {
            ident: format!("item-{key}"),
            data: Position { offset: 0, size: 0 },
            key,
        })
    }

    /// Builds a `DataSet` with one item per given key, sorted ascending (as
    /// `SetKeyIterator::new` requires: it groups by scanning for runs of equal keys).
    fn keyed_set(keys: &[u32]) -> DataSet {
        let mut keys = keys.to_vec();
        keys.sort();
        DataSet::from_items(Arc::new(keys.into_iter().map(item).collect()))
    }

    /// Like `keyed_set`, but with a non-zero item size. A size of 0 is a sentinel for "unknown
    /// size" (see `AnySetGroup::new`) that always forces max sharding, so any/fixed-sharding tests
    /// need sized items to actually exercise partition capping.
    fn sized_keyed_set(keys: &[u32], size: usize) -> DataSet {
        let mut keys = keys.to_vec();
        keys.sort();
        DataSet::from_items(Arc::new(
            keys.into_iter()
                .map(|key| {
                    Arc::new(DataItem {
                        ident: format!("item-{key}"),
                        data: Position { offset: 0, size },
                        key,
                    })
                })
                .collect(),
        ))
    }

    /// Runs a sharding iterator to completion and returns one `Vec<DataSet>` per produced group.
    fn collect_all(
        sets: Vec<Option<(DataSet, Sharding)>>,
        join_order: &[usize],
        any_sharding_mode: &AnyShardingMode,
        min_set_bytes: &[usize],
    ) -> Vec<Vec<DataSet>> {
        let num_sets = sets.len();
        let Some(mut iter) = create_sharding_iter(sets, join_order, any_sharding_mode, min_set_bytes)
        else {
            return vec![];
        };
        let mut result = Vec::new();
        loop {
            let mut buf = vec![DataSet::default(); num_sets];
            iter.fill_in(&mut buf);
            result.push(buf);
            if !iter.advance() {
                break;
            }
        }
        result
    }

    #[test]
    fn keyed_inner_join_keeps_only_matching_keys() {
        let a = keyed_set(&[1, 2, 3]);
        let b = keyed_set(&[2, 3, 4]);
        let sets = vec![
            // The first set's own strategy only matters in that it must allow standalone
            // advancing (Cross/Right/Outer); the actual join semantics come from the strategy
            // attached to the set(s) joined onto it.
            Some((a, Sharding::Keyed(JoinStrategy::Cross))),
            Some((b, Sharding::Keyed(JoinStrategy::Inner))),
        ];
        let result = collect_all(sets, &[0, 1], &AnyShardingMode::MaxSharding, &[0, 0]);

        let mut keys: Vec<_> = result
            .iter()
            .map(|group| {
                assert_eq!(group[0].items.len(), 1);
                assert_eq!(group[1].items.len(), 1);
                let key = group[0].items[0].key;
                assert_eq!(key, group[1].items[0].key, "inner join must match keys");
                key
            })
            .collect();
        keys.sort();
        assert_eq!(keys, vec![2, 3], "only keys present on both sides survive an inner join");
    }

    #[test]
    fn keyed_left_join_keeps_every_left_key_even_without_a_match() {
        let a = keyed_set(&[1, 2, 3]);
        let b = keyed_set(&[2, 3]);
        let sets = vec![
            Some((a, Sharding::Keyed(JoinStrategy::Cross))),
            Some((b, Sharding::Keyed(JoinStrategy::Left))),
        ];
        let result = collect_all(sets, &[0, 1], &AnyShardingMode::MaxSharding, &[0, 0]);

        assert_eq!(result.len(), 3, "one group per key on the left side");
        let mut left_keys: Vec<_> = result.iter().map(|g| g[0].items[0].key).collect();
        left_keys.sort();
        assert_eq!(left_keys, vec![1, 2, 3]);
        for group in &result {
            let left_key = group[0].items[0].key;
            if left_key == 1 {
                assert!(group[1].items.is_empty(), "key 1 has no match on the right");
            } else {
                assert_eq!(group[1].items[0].key, left_key);
            }
        }
    }

    #[test]
    fn keyed_outer_join_keeps_the_union_of_keys() {
        let a = keyed_set(&[1, 2]);
        let b = keyed_set(&[2, 3]);
        let sets = vec![
            Some((a, Sharding::Keyed(JoinStrategy::Cross))),
            Some((b, Sharding::Keyed(JoinStrategy::Outer))),
        ];
        let result = collect_all(sets, &[0, 1], &AnyShardingMode::MaxSharding, &[0, 0]);

        let mut seen_keys = std::collections::BTreeSet::new();
        for group in &result {
            let a_key = group[0].items.first().map(|i| i.key);
            let b_key = group[1].items.first().map(|i| i.key);
            if let (Some(ak), Some(bk)) = (a_key, b_key) {
                assert_eq!(ak, bk, "when both sides are present they must agree on the key");
            }
            seen_keys.insert(a_key.or(b_key).expect("at least one side must be present"));
        }
        assert_eq!(
            seen_keys,
            std::collections::BTreeSet::from([1, 2, 3]),
            "outer join must cover the union of both sides' keys"
        );
    }

    #[test]
    fn each_each_produces_the_cartesian_product() {
        let a = DataSet::from_items(Arc::new((0..2).map(item).collect()));
        let b = DataSet::from_items(Arc::new((10..13).map(item).collect()));
        let sets = vec![Some((a, Sharding::Each)), Some((b, Sharding::Each))];
        let result = collect_all(sets, &[0, 1], &AnyShardingMode::MaxSharding, &[0, 0]);

        assert_eq!(result.len(), 6, "2 x 3 items => 6 invocations");
        let mut pairs: Vec<_> = result
            .iter()
            .map(|g| (g[0].items[0].key, g[1].items[0].key))
            .collect();
        pairs.sort();
        let mut expected: Vec<_> = (0..2).flat_map(|x| (10..13).map(move |y| (x, y))).collect();
        expected.sort();
        assert_eq!(pairs, expected);
    }

    #[test]
    fn any_keyed_is_capped_to_the_requested_partition_count() {
        let a = sized_keyed_set(&[1, 2, 3, 4], 10);
        let sets = vec![Some((a, Sharding::AnyKeyed(JoinStrategy::Cross)))];
        let result = collect_all(sets, &[0], &AnyShardingMode::FixedSharding(2), &[0]);

        assert_eq!(result.len(), 2, "FixedSharding(2) should cap the 4 key groups into 2");
        let total_items: usize = result.iter().map(|g| g[0].items.len()).sum();
        assert_eq!(total_items, 4, "no items should be lost while regrouping");
    }
}

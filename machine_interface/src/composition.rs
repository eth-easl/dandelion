use crate::{
    composition::join_iterator::JoinIterator, memory_domain::Context, DataItem, DataSet, Position,
};
use core::fmt;
use dandelion_commons::FunctionId;
use log::{debug, trace};
use std::{
    cmp,
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    vec,
};
use tokio::sync::watch;

mod join_iterator;

#[cfg(test)]
mod composition_tests;

// TODO: determine suitable value here or come up with a better idea
pub const DEFAULT_AUTOSHARDING_OFFLOAD_CONST: usize = 2;

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

#[derive(Debug)]
struct AnySetGroup {
    largest_set_size: usize,
    max_partitions: usize,
    target_partitions: usize,
    min_set_bytes: usize,
    processed: bool,
}

impl AnySetGroup {
    fn new(largest_set_size: usize, min_set_bytes: usize, max_partitions: usize) -> Self {
        Self {
            largest_set_size,
            max_partitions,
            target_partitions: 1,
            min_set_bytes,
            processed: false,
        }
    }
}

// TODO: move this to the queue when refactoring the project parts structure
/// Contains system information used by the sharding policy.
pub struct SystemInfo {
    /// The number of local compute cores in the system (as a watcher so we can update remote nodes
    /// if the local core count changes).
    pub num_local_cores_watcher: watch::Receiver<usize>,
    pub num_local_cores_sender: watch::Sender<usize>,
    /// The number of remote compute cores in the system.
    pub num_remote_cores: AtomicUsize,
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

/// Struct that has all locations belonging to one set, that is potentially spread over multiple contexts.
/// Should only be constructed from a context, returning a list of all sets in the contexts content.
/// By construction, empty sets should not be allowed to exist, as they should return None instead on construction
#[derive(Clone, Debug)]
pub struct CompositionSet {
    /// Each tuple in the list contains the DataItem with the metadata and the Context in which the item is stored.
    /// The data: Position of the DataItem refers to offset and size within the Context.
    item_list: Vec<(DataItem, Arc<Context>)>,
    set_name: String,
}

impl CompositionSet {
    pub fn len(&self) -> usize {
        self.item_list.len()
    }

    pub fn size(&self) -> usize {
        self.item_list.iter().map(|(itm, _)| itm.data.size).sum()
    }

    pub fn get_name(&self) -> &String {
        &self.set_name
    }

    pub fn from_context(mut context: Context) -> Vec<Option<Self>> {
        // take the content from the context before putting it into an arc
        let sets = core::mem::take(&mut context.content);
        let context_arc = Arc::new(context);
        let mut composition_sets = Vec::with_capacity(sets.len());
        for set_option in sets.into_iter() {
            let composition_option = if let Some(set) = set_option {
                let DataSet { ident, buffers } = set;
                if buffers.len() == 0 {
                    None
                } else {
                    let mut item_list = Vec::with_capacity(buffers.len());
                    for item in buffers.into_iter() {
                        item_list.push((item, context_arc.clone()));
                    }
                    Some(CompositionSet {
                        item_list,
                        set_name: ident,
                    })
                }
            } else {
                None
            };
            composition_sets.push(composition_option);
        }
        composition_sets
    }

    // This is used on the frontend, to be depricated when we change function registration serialziation
    // DO NOT ADD USAGE
    pub fn from_byte_items(items: Vec<(String, Vec<u8>)>) -> Self {
        CompositionSet {
            item_list: items
                .into_iter()
                .map(|(name, data)| {
                    (
                        DataItem {
                            ident: name,
                            data: Position {
                                offset: 0,
                                size: data.len(),
                            },
                            key: 0,
                        },
                        Arc::new(
                            crate::memory_domain::read_only::ReadOnlyContext::new(
                                data.into_boxed_slice(),
                            )
                            .unwrap(),
                        ),
                    )
                })
                .collect(),
            // This is only used for static sets from function registration, which will get a name from the metadata
            set_name: String::new(),
        }
    }

    pub fn combine(&mut self, additional: CompositionSet) {
        self.item_list.extend(additional.item_list.into_iter());
        self.item_list.sort_unstable_by_key(|a| a.0.key);
    }
}

/// Iterator over a reference of the composition set, not taking ownership
impl<'origin> IntoIterator for &'origin CompositionSet {
    type Item = &'origin (DataItem, Arc<Context>);
    type IntoIter = std::slice::Iter<'origin, (DataItem, Arc<Context>)>;
    fn into_iter(self) -> Self::IntoIter {
        self.item_list.iter()
    }
}

/// Iterator taking ownership of the Compositon set
impl IntoIterator for CompositionSet {
    type Item = (DataItem, Arc<Context>);
    type IntoIter = std::vec::IntoIter<(DataItem, Arc<Context>)>;
    fn into_iter(self) -> Self::IntoIter {
        self.item_list.into_iter()
    }
}

/// A more concise display for the composition set that does not print the entire Context contents.
impl fmt::Display for CompositionSet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "items: [")?;
        for (item, ctx) in self.item_list.iter() {
            write!(
                f,
                "(item: {:?}, context: {{ type: {:?}, size: {}, state: {:?} }})",
                item, ctx.context, ctx.size, ctx.state
            )?;
        }
        write!(f, "]")
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
pub fn get_sharding(
    mut sets: Vec<Option<(ShardingMode, CompositionSet)>>,
    join_order: Vec<usize>,
    join_strategies: Vec<JoinStrategy>,
    any_sharding_mode: &AnyShardingMode,
    mut min_set_bytes: Vec<usize>,
) -> Vec<Vec<Option<CompositionSet>>> {
    let set_num = sets.len();
    debug_assert_eq!(join_order.len(), set_num);
    debug_assert_eq!(
        join_strategies.len(),
        if set_num == 0 { 0 } else { set_num - 1 }
    );
    min_set_bytes.resize(set_num, 0);

    trace!(
        "Computing sharding using sets: {:?}, join_order: {:?}, join_strategies: {:?}.",
        sets,
        join_order,
        join_strategies
    );
    let mut final_sharding = Vec::new();
    if set_num == 0 {
        trace!("Found empty sharding.");
        return final_sharding;
    }

    // first create the iterators for any keyed shardings
    let mut key_join_iter = None;
    let mut fixed_partitions = 1;
    let mut join_group_key_set: Vec<u32> = vec![];
    let mut i = 0;
    while i < set_num {
        let set_idx = join_order[i];
        if let Some((sharding, set)) = sets[set_idx].take() {
            if sharding != ShardingMode::Key {
                if join_group_key_set.len() > 0 {
                    fixed_partitions *= join_group_key_set.len();
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
                    fixed_partitions *= join_group_key_set.len();
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
    let mut any_set_groups = Vec::new();
    let mut join_iter = key_join_iter.map(|i| i as Box<dyn JoinIterator>);
    while i < set_num {
        let set_idx = join_order[i];
        if let Some((sharding, set)) = sets[set_idx].take() {
            match sharding {
                ShardingMode::All => {
                    join_iter = join_iterator::SetAllIterator::new(join_iter, set, set_idx);
                }
                ShardingMode::Each => {
                    let partitions;
                    (join_iter, partitions) =
                        join_iterator::SetEachIterator::new(join_iter, set, set_idx);
                    fixed_partitions *= partitions;
                }
                ShardingMode::AnyEach => {
                    let (any_join_iter, largest_set_size, min_set_size, max_partitions) =
                        join_iterator::AnyIterator::new(
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
                ShardingMode::AnyKey => {
                    // get all sets that are joined together (i.e. find the next cross join)
                    let mut joined_sets = vec![set];
                    let mut joined_set_idcs = vec![set_idx];
                    let mut joined_strategies = vec![];
                    let start_idx = i;
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

                    let (any_join_iter, largest_set_size, min_set_size, max_partitions) =
                        join_iterator::AnyIterator::new(
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
                ShardingMode::Key => {
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
                let c_local = { *params.sys_info.num_local_cores_watcher.borrow() };
                let c_remote = params.sys_info.num_remote_cores.load(Ordering::Acquire);
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

    trace!("Computed sharding: {:?}", final_sharding);
    final_sharding
}

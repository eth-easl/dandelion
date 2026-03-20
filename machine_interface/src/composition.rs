use crate::memory_domain::{Context, ContextTrait};
use dandelion_commons::{DandelionError, DandelionResult, DispatcherError, FunctionId};
use itertools::Itertools;
use std::{collections::BTreeMap, sync::Arc, vec};

#[cfg(test)]
use crate::memory_domain::bytes_context::BytesContext;
#[cfg(test)]
use libc::size_t;

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
}

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum JoinStrategy {
    Inner,
    Left,
    Right,
    Outer,
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

    pub fn shard(self, mode: ShardingMode) -> Vec<CompositionSet> {
        return match mode {
            ShardingMode::All => {
                vec![self]
            }
            ShardingMode::Key => {
                let CompositionSet {
                    mut item_list,
                    set_index,
                } = self;
                let mut keyed_vec = Vec::new();
                while !item_list.is_empty() {
                    let (last_key, _, _) = item_list.last().unwrap();
                    let mut position = item_list.len() - 1;
                    while position > 0 && item_list[position - 1].0 == *last_key {
                        position -= 1;
                    }
                    let new_list = item_list.split_off(position);
                    let new_composition = CompositionSet {
                        item_list: new_list,
                        set_index,
                    };
                    keyed_vec.push(new_composition);
                }
                keyed_vec
            }
            ShardingMode::Each => self
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
            return Err(DandelionError::Dispatcher(
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

    let mut join_iter_opt = JoinIterator::new(
        JoinStrategy::Outer,
        None,
        sets[join_order[0]].take(),
        join_order[0],
    );
    for (set_index, startegy) in join_order[1..].iter().zip_eq(join_strategies) {
        join_iter_opt =
            JoinIterator::new(startegy, join_iter_opt, sets[*set_index].take(), *set_index);
    }

    if let Some(mut join_iter) = join_iter_opt {
        let mut new_sets = Vec::with_capacity(set_num);
        new_sets.resize(set_num, None);
        join_iter.fill_in(&mut new_sets);
        final_sharding.push(new_sets);
        while join_iter.advance() {
            let mut advance_sets = Vec::with_capacity(set_num);
            advance_sets.resize(set_num, None);
            join_iter.fill_in(&mut advance_sets);
            final_sharding.push(advance_sets);
        }
    }

    final_sharding
}

struct JoinIterator {
    left: Option<Box<JoinIterator>>,
    right: Vec<CompositionSet>,
    right_index: usize,
    write_index: usize,
    mode: JoinStrategy,
    key: u32,
}

impl JoinIterator {
    fn new(
        mode: JoinStrategy,
        mut left_opt: Option<Box<Self>>,
        right_opt: Option<(ShardingMode, CompositionSet)>,
        write_index: usize,
    ) -> Option<Box<Self>> {
        if right_opt.is_none() || right_opt.as_ref().unwrap().1.is_empty() {
            return left_opt;
        }
        let (set_mode, set) = right_opt.unwrap();
        let right = set.shard(set_mode);
        if right.is_empty() {
            return left_opt;
        }

        let mut right_index = 0;
        let mut key = right[0].item_list[0].0;

        if let Some(left) = &mut left_opt {
            match mode {
                JoinStrategy::Inner => {
                    while right_index < right.len() && key != left.key {
                        if key < left.key {
                            right_index += 1;
                            if right_index < right.len() {
                                key = right[right_index].item_list[0].0;
                            }
                        } else {
                            if !left.advance() {
                                right_index = right.len();
                            }
                        }
                    }
                    if right_index == right.len() {
                        return None;
                    }
                }
                JoinStrategy::Left => {
                    while right_index < right.len() && right[right_index].item_list[0].0 < left.key
                    {
                        right_index += 1;
                    }
                    key = left.key;
                    if right_index == right.len() {
                        return left_opt;
                    }
                }
                JoinStrategy::Outer => {
                    if left.key < key {
                        key = left.key;
                    }
                    // else already has the correct key set
                }
                JoinStrategy::Right | JoinStrategy::Cross => (),
            }
        // there is not left iterator
        } else {
            match mode {
                JoinStrategy::Inner | JoinStrategy::Left => {
                    return None;
                }
                _ => (),
            }
        }
        Some(Box::new(Self {
            left: left_opt,
            right,
            right_index: 0,
            write_index,
            mode,
            key,
        }))
    }

    fn fill_in(&mut self, to_fill: &mut Vec<Option<CompositionSet>>) -> bool {
        let left_filled = if let Some(left) = &mut self.left {
            left.fill_in(to_fill)
        } else {
            false
        };
        let right_filled = if self.right_index < self.right.len()
            && self.key == self.right[self.right_index].item_list[0].0
        {
            to_fill[self.write_index] = Some(self.right[self.right_index].clone());
            true
        } else {
            false
        };
        left_filled || right_filled
    }

    fn advance(&mut self) -> bool {
        let right = &mut self.right;
        if let Some(left) = &mut self.left {
            match self.mode {
                JoinStrategy::Inner => {
                    // advance both at least once for inner
                    // left is advanced on checking (after checking right can stil be advanced)
                    // right is advanced after
                    if self.right_index >= right.len() || left.advance() {
                        return false;
                    }
                    self.right_index += 1;
                    self.key = right[self.right_index].item_list[0].0;
                    loop {
                        if self.key > left.key {
                            if !left.advance() {
                                return false;
                            }
                        } else if self.key < left.key {
                            self.right_index += 1;
                            if self.right_index < right.len() {
                                self.key = right[self.right_index].item_list[0].0;
                            } else {
                                return false;
                            }
                        } else {
                            return true;
                        }
                    }
                }
                JoinStrategy::Left => {
                    // advance left and see if we can match
                    if left.advance() {
                        while self.right_index < right.len()
                            && right[self.right_index].item_list[0].0 < left.key
                        {
                            self.right_index += 1;
                        }
                        // after this they key is guaranteed to be equal to the left key or bigger
                        // so if the keys match that will be fine for copy in, otherwise this will be skipped
                        // if the key already equal or bigger, it was not advanced
                        self.key = left.key;
                        true
                    } else {
                        self.right_index = right.len();
                        false
                    }
                }
                JoinStrategy::Right => {
                    if !(self.right_index < right.len()) {
                        return false;
                    }
                    self.right_index += 1;
                    if self.right_index < right.len() {
                        self.key = right[self.right_index].item_list[0].0;
                        while self.key > left.key {
                            if !left.advance() {
                                break;
                            }
                        }
                        true
                    } else {
                        false
                    }
                }
                JoinStrategy::Outer => {
                    // could be that right has no more items to contribute,
                    // but left still can advance
                    if !(self.right_index < right.len()) {
                        if left.advance() {
                            self.key = left.key;
                            return true;
                        } else {
                            return false;
                        }
                    }
                    // right still has items to contribute, check if both, right or left should be advanced
                    if left.key == right[self.right_index].item_list[0].0 {
                        let left_advance = left.advance();
                        self.right_index += 1;
                        match (self.right_index < right.len(), left_advance) {
                            (true, false) => {
                                self.key = right[self.right_index].item_list[0].0;
                                true
                            }
                            (false, true) => {
                                self.key = left.key;
                                true
                            }
                            (true, true) => {
                                let possible_key = right[self.right_index].item_list[0].0;
                                self.key = if possible_key < left.key {
                                    possible_key
                                } else {
                                    left.key
                                };
                                true
                            }
                            (false, false) => false,
                        }
                    } else {
                        false
                    }
                }
                JoinStrategy::Cross => {
                    if self.right_index + 1 < right.len() {
                        self.right_index += 1;
                        self.key = right[self.right_index].item_list[0].0;
                        true
                    } else if left.advance() {
                        self.right_index = 0;
                        self.key = right[0].item_list[0].0;
                        true
                    } else {
                        // set right index to len so we can't accidentally copy something
                        self.right_index = right.len();
                        false
                    }
                }
            }
        } else {
            if !(self.right_index < right.len()) {
                return false;
            }
            // advancing only makes sense for certain modes here
            if self.mode == JoinStrategy::Right
                || self.mode == JoinStrategy::Outer
                || self.mode == JoinStrategy::Cross
            {
                self.right_index += 1;
                if self.right_index < right.len() {
                    self.key = right[self.right_index].item_list[0].0;
                    true
                } else {
                    false
                }
            } else {
                panic!("Should never have join iterator with left or inner that has None for the left value");
            }
        }
    }
}

// tests

#[cfg(test)]
fn create_dummy_set(keys: Vec<u32>) -> CompositionSet {
    let dummy_context: Arc<Context> = Arc::new(Context::new(
        crate::memory_domain::ContextType::Bytes(Box::new(BytesContext::new(vec![]))),
        0,
    ));
    let items = keys
        .into_iter()
        .enumerate()
        .map(|(i, k)| (k, i, dummy_context.clone()))
        .collect();
    CompositionSet {
        item_list: items,
        set_index: 0,
    }
}

#[cfg(test)]
fn check_sharding(actual: &Vec<Vec<Option<CompositionSet>>>, expected: &Vec<Vec<Vec<size_t>>>) {
    assert!(actual.len() == expected.len());
    for inv_sets in actual.iter() {
        assert!(inv_sets.len() == expected[0].len());
        for (set_idx, set) in inv_sets.iter().enumerate() {
            assert!(set.is_some());
            let actual_items = &set.as_ref().unwrap().item_list;
            assert!(actual_items.len() > 0);
            let key = actual_items[0].0;
            let expected_items = &expected[key as size_t][set_idx];
            assert!(expected_items.len() == actual_items.len());
            assert!(actual_items
                .iter()
                .all(|(_, x, _)| expected_items.contains(x)));
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
        Some((ShardingMode::Key, create_dummy_set(vec![0]))),
        Some((ShardingMode::Key, create_dummy_set(vec![0]))),
    ];

    let join_order = vec![0, 1];
    let join_strategies = vec![JoinStrategy::Outer, JoinStrategy::Inner];

    let sharding = get_sharding(sets, join_order, join_strategies);
    let expected = vec![vec![vec![0], vec![0]]];

    print_sharding(&sharding);
    check_sharding(&sharding, &expected);
}

#[test]
fn join_it_inner_test2() {
    let sets = vec![
        Some((ShardingMode::Key, create_dummy_set(vec![0, 1]))),
        Some((ShardingMode::Key, create_dummy_set(vec![0, 1, 1, 2]))),
    ];

    let join_order = vec![0, 1];
    let join_strategies = vec![JoinStrategy::Outer, JoinStrategy::Inner];

    let sharding = get_sharding(sets, join_order, join_strategies);
    let expected = vec![vec![vec![0], vec![0]], vec![vec![1], vec![1, 2]]];

    print_sharding(&sharding);
    assert!(false);
    check_sharding(&sharding, &expected);
}

#[test]
fn join_it_left_test() {
    let sets = vec![
        Some((ShardingMode::Key, create_dummy_set(vec![0, 1, 2]))),
        Some((ShardingMode::Key, create_dummy_set(vec![0, 1, 1]))),
    ];

    let join_order = vec![0, 1];
    let join_strategies = vec![JoinStrategy::Outer, JoinStrategy::Left];

    let sharding = get_sharding(sets, join_order, join_strategies);
    let expected = vec![
        vec![vec![0], vec![0]],
        vec![vec![1], vec![1, 2]],
        vec![vec![2], vec![]],
    ];

    print_sharding(&sharding);
    check_sharding(&sharding, &expected);
}

#[test]
fn join_it_right_test() {
    let sets = vec![
        Some((ShardingMode::Key, create_dummy_set(vec![0, 1, 2]))),
        Some((ShardingMode::Key, create_dummy_set(vec![0, 1, 1, 3]))),
    ];

    let join_order = vec![0, 1];
    let join_strategies = vec![JoinStrategy::Outer, JoinStrategy::Right];

    let sharding = get_sharding(sets, join_order, join_strategies);
    let expected = vec![
        vec![vec![0], vec![0]],
        vec![vec![1], vec![1, 2]],
        vec![vec![], vec![3]],
    ];

    print_sharding(&sharding);
    check_sharding(&sharding, &expected);
}

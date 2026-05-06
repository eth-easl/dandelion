use std::ops::Range;

use crate::composition::{CompositionSet, JoinStrategy, ShardingMode};

pub(super) trait JoinIterator {
    /// Reduces the parallelism of all `AnyIterators` in the iterator chain by combining some sets.
    /// We expect this function is only called once, otherwise, we could end up with unevenly
    /// distributed or completely messed up groups.
    fn reduce_any_parallelism(&mut self, any_parallelisms: Vec<(usize, usize)>);

    /// Fills the given composition set vector with the current iterator state.
    /// This is undefined behaviour if the previous `advance` call returned `false`.
    fn fill_in(&mut self, to_fill: &mut Vec<Option<CompositionSet>>);

    /// Advances the iterator by one.
    /// An `advance` call following an `advance` that returned `false` always returns `false`.
    /// A `fill_in` call after an `advance` that called `false` is undefined behaviour.
    fn advance(&mut self) -> bool;
}

/// Implements the JoinIterator for the `all` sharding.
pub(super) struct SetAllIterator {
    left: Option<Box<dyn JoinIterator>>,
    set: CompositionSet,
    write_idx: usize,
}

impl SetAllIterator {
    pub(super) fn new(
        left: Option<Box<dyn JoinIterator>>,
        set: CompositionSet,
        write_idx: usize,
    ) -> Option<Box<dyn JoinIterator>> {
        if set.is_empty() {
            return left;
        }

        Some(Box::new(Self {
            left,
            set,
            write_idx,
        }))
    }
}

impl JoinIterator for SetAllIterator {
    fn reduce_any_parallelism(&mut self, any_parallelisms: Vec<(usize, usize)>) {
        if let Some(left) = self.left.as_mut() {
            left.reduce_any_parallelism(any_parallelisms);
        } else {
            debug_assert!(any_parallelisms.len() == 0);
        }
    }

    fn fill_in(&mut self, to_fill: &mut Vec<Option<CompositionSet>>) {
        to_fill[self.write_idx] = Some(self.set.clone());
        if let Some(left) = &mut self.left {
            left.fill_in(to_fill);
        }
    }

    fn advance(&mut self) -> bool {
        if let Some(left) = &mut self.left {
            left.advance()
        } else {
            false
        }
    }
}

/// Implements the JoinIterator for the `each` sharding.
pub(super) struct SetEachIterator {
    left: Option<Box<dyn JoinIterator>>,
    set: CompositionSet,
    item_idx: usize,
    write_idx: usize,
}

impl SetEachIterator {
    pub(super) fn new(
        left: Option<Box<dyn JoinIterator>>,
        set: CompositionSet,
        write_idx: usize,
    ) -> (Option<Box<dyn JoinIterator>>, usize) {
        if set.is_empty() {
            return (left, 1);
        }

        let num_items = set.len();
        let it: Option<Box<dyn JoinIterator>> = Some(Box::new(Self {
            left,
            set,
            item_idx: 0,
            write_idx,
        }));
        (it, num_items)
    }
}

impl JoinIterator for SetEachIterator {
    fn reduce_any_parallelism(&mut self, any_parallelisms: Vec<(usize, usize)>) {
        if let Some(left) = self.left.as_mut() {
            left.reduce_any_parallelism(any_parallelisms);
        } else {
            debug_assert!(any_parallelisms.len() == 0);
        }
    }

    fn fill_in(&mut self, to_fill: &mut Vec<Option<CompositionSet>>) {
        debug_assert!(self.item_idx < self.set.item_list.len());
        to_fill[self.write_idx] = Some(CompositionSet {
            item_list: vec![self.set.item_list[self.item_idx].clone()],
            set_index: self.set.set_index,
        });
        if let Some(left) = &mut self.left {
            left.fill_in(to_fill);
        }
    }

    fn advance(&mut self) -> bool {
        if self.item_idx + 1 < self.set.item_list.len() {
            self.item_idx += 1;
            true
        } else {
            if let Some(left) = &mut self.left {
                if left.advance() {
                    self.item_idx = 0;
                    true
                } else {
                    // set sets_idx to len so we can't accidentally copy something
                    self.item_idx = self.set.item_list.len();
                    false
                }
            } else {
                false
            }
        }
    }
}

/// Implements the JoinIterator for the `keyed` sharding.
///
/// NOTE: We assume all `key`/`anyKey` sharded sets to precede all other sharded sets in the join
///       order. As a result, the `SetKeyIterator` takes a reference another `SetKeyIterator` for
///       the left instead of a more general `JoinIterator`.
pub(super) struct SetKeyIterator {
    left: Option<Box<SetKeyIterator>>,
    set: CompositionSet,
    key_groups: Vec<(u32, Range<usize>)>,
    key_groups_idx: usize,
    key: u32,
    strategy: JoinStrategy,
    write_idx: usize,
}

fn key_set_intersect(curr_keys: &mut Vec<u32>, new_key_groups: &Vec<(u32, Range<usize>)>) {
    let mut write_idx = 0;
    let mut j = 0;

    for i in 0..curr_keys.len() {
        let val = curr_keys[i];
        while j < new_key_groups.len() && new_key_groups[j].0 < val {
            j += 1;
        }

        if j < new_key_groups.len() && new_key_groups[j].0 == val {
            curr_keys[write_idx] = val;
            write_idx += 1;
            j += 1;
        }
    }

    curr_keys.truncate(write_idx);
}

fn key_set_union(curr_keys: &mut Vec<u32>, new_key_groups: &Vec<(u32, Range<usize>)>) {
    if new_key_groups.len() == 0 {
        return;
    } else if curr_keys.len() == 0 {
        return *curr_keys = new_key_groups.iter().map(|(k, _)| *k).collect();
    }

    // using a new vector is more compute efficient but uses more memory than doing it in-place
    let mut result = Vec::with_capacity(curr_keys.len() + new_key_groups.len());
    let (mut i, mut j) = (0, 0);

    while i < curr_keys.len() && j < new_key_groups.len() {
        if curr_keys[i] < new_key_groups[j].0 {
            result.push(curr_keys[i]);
            i += 1;
        } else if curr_keys[i] > new_key_groups[j].0 {
            result.push(new_key_groups[j].0);
            j += 1;
        } else {
            result.push(curr_keys[i]);
            i += 1;
            j += 1;
        }
    }

    // push remaining elements from whichever vec isn't empty
    result.extend_from_slice(&curr_keys[i..]);
    result.extend(new_key_groups.iter().skip(j).map(|(k, _)| k));

    *curr_keys = result;
}

impl SetKeyIterator {
    /// Computes the key_groups (index ranges) that combine all items with the same key and sets the
    /// index to the first valid element based on the join strategy.
    pub(super) fn new(
        mut left: Option<Box<SetKeyIterator>>,
        set: CompositionSet,
        strategy: JoinStrategy,
        write_idx: usize,
        join_group_key_set: &mut Vec<u32>,
    ) -> Option<Box<SetKeyIterator>> {
        let mut key_groups = Vec::new();
        let mut start_idx = 0;
        for chunk in set.item_list.chunk_by(|a, b| a.0 == b.0) {
            let key = chunk[0].0;
            let end_idx = start_idx + chunk.len();
            key_groups.push((key, start_idx..end_idx));
            start_idx = end_idx;
        }

        // handle empty set
        if key_groups.is_empty() {
            match strategy {
                JoinStrategy::Left | JoinStrategy::Outer => (),
                _ => join_group_key_set.clear(),
            };
            return left;
        }

        let mut key_groups_idx = 0;
        let mut key = key_groups[0].0;
        if let Some(left_it) = &mut left {
            match strategy {
                JoinStrategy::Inner => {
                    key_set_intersect(join_group_key_set, &key_groups);

                    while key_groups_idx < key_groups.len() && key != left_it.key {
                        if key < left_it.key {
                            key_groups_idx += 1;
                            if key_groups_idx < key_groups.len() {
                                key = key_groups[key_groups_idx].0;
                            }
                        } else {
                            if !left_it.advance() {
                                key_groups_idx = key_groups.len();
                            }
                        }
                    }
                    if key_groups_idx == key_groups.len() {
                        return None;
                    }
                }
                JoinStrategy::Left => {
                    // -> join_group_key_set remains the same
                    while key_groups_idx < key_groups.len()
                        && key_groups[key_groups_idx].0 < left_it.key
                    {
                        key_groups_idx += 1;
                    }
                    key = left_it.key;
                    if key_groups_idx == key_groups.len() {
                        return left;
                    }
                }
                JoinStrategy::Outer => {
                    key_set_union(join_group_key_set, &key_groups);
                    if left_it.key < key {
                        key = left_it.key;
                    }
                }
                JoinStrategy::Right | JoinStrategy::Cross => {
                    *join_group_key_set = key_groups.iter().map(|(k, _)| *k).collect()
                    // -> already has the correct key set
                }
            }
        } else {
            match strategy {
                JoinStrategy::Inner | JoinStrategy::Left => {
                    join_group_key_set.clear();
                    return None;
                }
                _ => *join_group_key_set = key_groups.iter().map(|(k, _)| *k).collect(),
            }
        }

        Some(Box::new(Self {
            left,
            set,
            key_groups,
            key_groups_idx,
            key,
            strategy,
            write_idx,
        }))
    }
}

impl JoinIterator for SetKeyIterator {
    fn reduce_any_parallelism(&mut self, any_parallelisms: Vec<(usize, usize)>) {
        debug_assert!(any_parallelisms.len() == 0);
    }

    fn fill_in(&mut self, to_fill: &mut Vec<Option<CompositionSet>>) {
        // NOTE: for this iterator `self.key_groups_idx` may be equals to `self.key_groups.len()`
        //       (e.g. a right join might advance the left to this point if there is no matching
        //       left key for the right one)
        let fill_this_set = self.key_groups_idx < self.key_groups.len()
            && self.key == self.key_groups[self.key_groups_idx].0;
        if fill_this_set {
            to_fill[self.write_idx] = Some(CompositionSet {
                item_list: self.set.item_list[self.key_groups[self.key_groups_idx].1.clone()]
                    .to_vec(),
                set_index: self.set.set_index,
            });
        }
        if let Some(left) = &mut self.left {
            match self.strategy {
                // modes for which always want left to fill in
                JoinStrategy::Cross | JoinStrategy::Left => left.fill_in(to_fill),
                // only want to fill left if it is the one with the current key
                JoinStrategy::Outer => {
                    if self.key == left.key {
                        left.fill_in(to_fill)
                    }
                }
                // only want left to fill if this iterator has filled something in
                JoinStrategy::Inner => {
                    if fill_this_set {
                        left.fill_in(to_fill)
                    }
                }
                // only want left to fill if this iterator has filled and the keys match
                JoinStrategy::Right => {
                    if fill_this_set && self.key == left.key {
                        left.fill_in(to_fill)
                    }
                }
            }
        };
    }

    fn advance(&mut self) -> bool {
        if self.key_groups_idx >= self.key_groups.len() {
            return false;
        }

        if let Some(left) = &mut self.left {
            match self.strategy {
                JoinStrategy::Inner => {
                    // advance both at least once for inner
                    // left is advanced on checking (after checking right can stil be advanced)
                    // right is advanced after
                    self.key_groups_idx += 1;
                    if self.key_groups_idx >= self.key_groups.len() || !left.advance() {
                        self.key_groups_idx = self.key_groups.len();
                        return false;
                    }
                    self.key = self.key_groups[self.key_groups_idx].0;
                    while self.key != left.key {
                        if self.key > left.key {
                            if !left.advance() {
                                self.key_groups_idx = self.key_groups.len();
                                return false;
                            }
                        // need to advance right and are able to do so
                        } else if self.key < left.key {
                            self.key_groups_idx += 1;
                            if self.key_groups_idx >= self.key_groups.len() {
                                self.key_groups_idx = self.key_groups.len();
                                return false;
                            } else {
                                self.key = self.key_groups[self.key_groups_idx].0;
                            }
                        }
                    }
                    true
                }
                JoinStrategy::Left => {
                    // advance left and see if we can match
                    if left.advance() {
                        while self.key_groups_idx + 1 < self.key_groups.len()
                            && self.key_groups[self.key_groups_idx].0 < left.key
                        {
                            self.key_groups_idx += 1;
                        }
                        // after this the key is guaranteed to be equal to the left key or bigger
                        // so if the keys match that will be fine for copy in, otherwise this will be skipped
                        // if the key already equal or bigger, it was not advanced
                        self.key = left.key;
                        true
                    } else {
                        self.key_groups_idx = self.key_groups.len();
                        false
                    }
                }
                JoinStrategy::Right => {
                    self.key_groups_idx += 1;
                    if self.key_groups_idx >= self.key_groups.len() {
                        self.key_groups_idx = self.key_groups.len();
                        return false;
                    }
                    self.key = self.key_groups[self.key_groups_idx].0;
                    while self.key > left.key {
                        if !left.advance() {
                            break;
                        }
                    }
                    true
                }
                JoinStrategy::Outer => {
                    let current_self_key = self.key_groups[self.key_groups_idx].0;
                    let right_can_be_advanced = self.key_groups_idx + 1 < self.key_groups.len();
                    // check if one of the already known keys is bigger, if so we know we can adavance
                    if self.key < left.key {
                        // last key was set from right, since left is bigger
                        debug_assert_eq!(self.key, current_self_key);
                        // if right can be advance it should be advanced, otherwise just set key to left one
                        if right_can_be_advanced {
                            self.key_groups_idx += 1;
                            // new right might still be smaller than left key
                            self.key = u32::min(self.key_groups[self.key_groups_idx].0, left.key);
                        } else {
                            self.key = left.key;
                        }
                        true
                    } else if self.key < current_self_key {
                        // the last key was set from left, since right is bigger
                        debug_assert_eq!(self.key, left.key);
                        // if left can be advanced it should be, if not move
                        if left.advance() {
                            // new left key might still be smaller than right
                            self.key = u32::min(self.key_groups[self.key_groups_idx].0, left.key);
                        } else {
                            // left did not advance, so set key to current right key
                            self.key = current_self_key;
                            self.left = None; // asserts that left.advance() is not called again
                        }
                        true
                    } else if self.key == left.key && self.key == current_self_key {
                        // both keys are the same, so advance any that are possible to advance and take new key from there
                        let left_advance_success = left.advance();
                        if right_can_be_advanced {
                            self.key_groups_idx += 1;
                        }
                        match (right_can_be_advanced, left_advance_success) {
                            (true, true) => {
                                self.key =
                                    u32::min(self.key_groups[self.key_groups_idx].0, left.key);
                                true
                            }
                            (true, false) => {
                                self.key = self.key_groups[self.key_groups_idx].0;
                                true
                            }
                            (false, true) => {
                                self.key = left.key;
                                true
                            }
                            (false, false) => {
                                self.key_groups_idx = self.key_groups.len();
                                false
                            }
                        }
                    } else {
                        // the key is already set to the bigger of the two current keys, try to advance that one
                        if current_self_key == left.key {
                            let did_advance = left.advance();
                            self.key = left.key;
                            did_advance
                        } else {
                            if right_can_be_advanced {
                                self.key_groups_idx += 1;
                                self.key = self.key_groups[self.key_groups_idx].0;
                            }
                            right_can_be_advanced
                        }
                    }
                }
                JoinStrategy::Cross => {
                    if self.key_groups_idx + 1 < self.key_groups.len() {
                        self.key_groups_idx += 1;
                        self.key = self.key_groups[self.key_groups_idx].0;
                        true
                    } else if left.advance() {
                        self.key_groups_idx = 0;
                        self.key = self.key_groups[0].0;
                        true
                    } else {
                        // set right index to len so we can't accidentally copy something
                        self.key_groups_idx = self.key_groups.len();
                        false
                    }
                }
            }
        } else {
            if self.key_groups_idx + 1 >= self.key_groups.len() {
                self.key_groups_idx = self.key_groups.len();
                false
            } else {
                // advancing only makes sense for certain modes here
                if self.strategy == JoinStrategy::Right
                    || self.strategy == JoinStrategy::Outer
                    || self.strategy == JoinStrategy::Cross
                {
                    self.key_groups_idx += 1;
                    self.key = self.key_groups[self.key_groups_idx].0;
                    true
                } else {
                    panic!("Should never have join iterator with left or inner that has None for the left value");
                }
            }
        }
    }
}

/// Implements the JoinIterator for the `any` shardings. This could be a single `AnyEach` sharding
/// or a chain of joined `AnyKey` shardings.
///
/// Under the hood we first generate all sets using the `SetEachIterator` or `SetKeyIterators` and
/// save them in the `set_groups`. Once we know the degree to which this any sharding should be
/// parallelized we combine the sets into equally distributed groups using the
/// `reduce_any_parallelism` function.
///
/// NOTE: The `AnyIterator` assumes none of its sets that are cross-joined. For two cross-joined
///       `any` sets create two separate `AnyIterator` instances, one for each of the sets.
pub(super) struct AnyIterator {
    left: Option<Box<dyn JoinIterator>>,
    set_groups: Vec<Vec<Option<CompositionSet>>>,
    set_groups_idx: usize,
    write_idcs: Vec<usize>,
}

impl AnyIterator {
    pub(super) fn new(
        left: Option<Box<dyn JoinIterator>>,
        sets: Vec<CompositionSet>,
        strategies: Vec<JoinStrategy>,
        write_idcs: Vec<usize>,
        sharding: ShardingMode,
    ) -> (Option<Box<AnyIterator>>, usize) {
        let num_sets = sets.len();
        debug_assert!(num_sets > 0);

        // build the iterators to generate all sets so we can then group them
        let mut inner_join_it = None;
        if sharding == ShardingMode::AnyEach {
            debug_assert_eq!(num_sets, 1);
            (inner_join_it, _) =
                SetEachIterator::new(inner_join_it, sets.into_iter().next().unwrap(), 0);
        } else {
            debug_assert_eq!(sharding, ShardingMode::AnyKey);
            let mut inner_key_it = None;
            for (i, set) in sets.into_iter().enumerate() {
                let strategy = if i > 0 {
                    strategies[i - 1]
                } else {
                    JoinStrategy::Cross
                };
                let mut empty: Vec<u32> = vec![]; // can ignore join key set
                inner_key_it = SetKeyIterator::new(inner_key_it, set, strategy, i, &mut empty);
            }
            inner_join_it = inner_key_it.map(|i| i as Box<dyn JoinIterator>);
        }

        if let Some(mut it) = inner_join_it {
            let mut inner_sharding = Vec::new();

            // generate all sets
            let mut first_sets = Vec::with_capacity(num_sets);
            first_sets.resize(num_sets, None);
            it.fill_in(&mut first_sets);
            inner_sharding.push(first_sets);
            while it.advance() {
                let mut next_sets = Vec::with_capacity(num_sets);
                next_sets.resize(num_sets, None);
                it.fill_in(&mut next_sets);
                inner_sharding.push(next_sets);
            }
            let max_parallelism = inner_sharding.len();

            (
                Some(Box::new(Self {
                    left,
                    set_groups: inner_sharding,
                    set_groups_idx: 0,
                    write_idcs,
                })),
                max_parallelism,
            )
        } else {
            // we failed to build the inner join iterators...
            (None, 0)
        }
    }
}

impl JoinIterator for AnyIterator {
    fn reduce_any_parallelism(&mut self, mut any_parallelisms: Vec<(usize, usize)>) {
        let (_, parallelism) = any_parallelisms
            .pop()
            .expect("Ran out of any_parallelisms.");

        // can only group into less groups than we currently have
        debug_assert!(parallelism > 0);
        debug_assert!(parallelism <= self.set_groups.len());

        // split them into even groups
        let num_sets = self.set_groups[0].len();
        let mut new_set_groups = Vec::with_capacity(parallelism);
        let base_size = self.set_groups.len() / parallelism;
        let remainder = self.set_groups.len() % parallelism;
        let mut start_idx = 0;
        for group_idx in 0..parallelism {
            let extra = if group_idx < remainder { 1 } else { 0 };
            let end_idx = start_idx + base_size + extra;

            let mut set_group = Vec::with_capacity(num_sets);
            for set_idx in 0..num_sets {
                let mut curr_set: Option<CompositionSet> = None;
                for i in start_idx..end_idx {
                    if let Some(set) = &mut curr_set {
                        if let Some(next_set) = self.set_groups[i][set_idx].take() {
                            set.combine(next_set).unwrap();
                        }
                    } else {
                        curr_set = self.set_groups[i][set_idx].take();
                    }
                }
                set_group.push(curr_set);
            }
            new_set_groups.push(set_group);
            start_idx = end_idx;
        }
        self.set_groups = new_set_groups;

        if let Some(left) = self.left.as_mut() {
            left.reduce_any_parallelism(any_parallelisms);
        } else {
            debug_assert!(any_parallelisms.len() == 0);
        }
    }

    fn fill_in(&mut self, to_fill: &mut Vec<Option<CompositionSet>>) {
        debug_assert!(self.set_groups_idx < self.set_groups.len());
        for (i, write_idx) in self.write_idcs.iter().enumerate() {
            to_fill[*write_idx] = self.set_groups[self.set_groups_idx][i].clone();
        }
        if let Some(left) = &mut self.left {
            left.fill_in(to_fill);
        }
    }

    fn advance(&mut self) -> bool {
        self.set_groups_idx += 1;
        if self.set_groups_idx < self.set_groups.len() {
            true
        } else {
            if let Some(left) = &mut self.left {
                if left.advance() {
                    self.set_groups_idx = 0;
                    true
                } else {
                    // set key_groups_idx to len so we can't accidentally copy something
                    self.set_groups_idx = self.set_groups.len();
                    false
                }
            } else {
                false
            }
        }
    }
}

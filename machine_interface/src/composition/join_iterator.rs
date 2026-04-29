use crate::composition::{CompositionSet, JoinStrategy, ShardingMode};

trait JoinIterator {
    fn fill_in(&mut self, to_fill: &mut Vec<Option<CompositionSet>>);
    fn advance(&mut self) -> bool;
    fn get_key(&self) -> u32;
}

/// Implements the JoinIterator for the `all` sharding.
struct SetAllIterator {
    left: Option<Box<dyn JoinIterator>>,
    set: CompositionSet,
    write_idx: usize,
}

impl SetAllIterator {
    fn new(
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

    fn get_key(&self) -> u32 {
        panic!("get_key should not be called on SetAllIterator.");
    }
}

/// Implements the JoinIterator for the `each` sharding.
struct SetEachIterator {
    left: Option<Box<dyn JoinIterator>>,
    sets: Vec<CompositionSet>,
    sets_idx: usize,
    write_idx: usize,
}

impl SetEachIterator {
    fn new(
        left: Option<Box<dyn JoinIterator>>,
        set: CompositionSet,
        write_idx: usize,
    ) -> Option<Box<dyn JoinIterator>> {
        let sets = set.shard(ShardingMode::Each);
        if sets.is_empty() {
            return left;
        }

        Some(Box::new(Self {
            left,
            sets,
            sets_idx: 0,
            write_idx,
        }))
    }
}

impl JoinIterator for SetEachIterator {
    fn fill_in(&mut self, to_fill: &mut Vec<Option<CompositionSet>>) {
        if self.sets_idx < self.sets.len() {
            to_fill[self.write_idx] = Some(self.sets[self.sets_idx].clone());
        }
        if let Some(left) = &mut self.left {
            left.fill_in(to_fill);
        }
    }

    fn advance(&mut self) -> bool {
        if self.sets_idx + 1 < self.sets.len() {
            self.sets_idx += 1;
            true
        } else {
            if let Some(left) = &mut self.left {
                if left.advance() {
                    self.sets_idx = 0;
                    true
                } else {
                    // set sets_idx to len so we can't accidentally copy something
                    self.sets_idx = self.sets.len();
                    false
                }
            } else {
                false
            }
        }
    }

    fn get_key(&self) -> u32 {
        panic!("get_key should not be called on SetEachIterator.");
    }
}

/// Implements the JoinIterator for the `keyed` sharding.
struct SetKeyIterator {
    left: Option<Box<dyn JoinIterator>>,
    sets: Vec<CompositionSet>,
    sets_idx: usize,
    key: u32,
    strategy: JoinStrategy,
    write_idx: usize,
}

impl SetKeyIterator {
    fn new(
        mut left: Option<Box<dyn JoinIterator>>,
        set: CompositionSet,
        strategy: JoinStrategy,
        write_idx: usize,
    ) -> Option<Box<dyn JoinIterator>> {
        let sets = set.shard(ShardingMode::Key);
        if sets.is_empty() {
            return left;
        }

        let mut sets_idx = 0;
        let mut key = sets[0].item_list[0].0;
        if let Some(left_it) = &mut left {
            match strategy {
                JoinStrategy::Inner => {
                    while sets_idx < sets.len() && key != left_it.get_key() {
                        if key < left_it.get_key() {
                            sets_idx += 1;
                            if sets_idx < sets.len() {
                                key = sets[sets_idx].item_list[0].0;
                            }
                        } else {
                            if !left_it.advance() {
                                sets_idx = sets.len();
                            }
                        }
                    }
                    if sets_idx == sets.len() {
                        return None;
                    }
                }
                JoinStrategy::Left => {
                    while sets_idx < sets.len() && sets[sets_idx].item_list[0].0 < left_it.get_key()
                    {
                        sets_idx += 1;
                    }
                    key = left_it.get_key();
                    if sets_idx == sets.len() {
                        return left;
                    }
                }
                JoinStrategy::Outer => {
                    if left_it.get_key() < key {
                        key = left_it.get_key();
                    }
                    // else already has the correct key set
                }
                JoinStrategy::Right | JoinStrategy::Cross => (),
            }
        } else {
            match strategy {
                JoinStrategy::Inner | JoinStrategy::Left => {
                    return None;
                }
                _ => (),
            }
        }

        Some(Box::new(Self {
            left,
            sets,
            sets_idx,
            key,
            strategy,
            write_idx,
        }))
    }
}

impl JoinIterator for SetKeyIterator {
    fn fill_in(&mut self, to_fill: &mut Vec<Option<CompositionSet>>) {
        let has_filled =
            self.sets_idx < self.sets.len() && self.key == self.sets[self.sets_idx].item_list[0].0;
        if has_filled {
            to_fill[self.write_idx] = Some(self.sets[self.sets_idx].clone());
        }
        if let Some(left) = &mut self.left {
            match self.strategy {
                // modes for which always want left to fill in
                JoinStrategy::Cross | JoinStrategy::Left => left.fill_in(to_fill),
                // Only want to fill left if it is the one with the current key
                JoinStrategy::Outer => {
                    if self.key == left.get_key() {
                        left.fill_in(to_fill)
                    }
                }
                // Only want left to fill if this iterator has filled something in
                JoinStrategy::Inner => {
                    if has_filled {
                        left.fill_in(to_fill)
                    }
                }
                // Only want left to fill if this iterator has filled and the keys match
                JoinStrategy::Right => {
                    if has_filled && self.key == left.get_key() {
                        left.fill_in(to_fill)
                    }
                }
            }
        };
    }

    fn advance(&mut self) -> bool {
        if self.sets_idx == self.sets.len() {
            return false;
        }

        if let Some(left) = &mut self.left {
            match self.strategy {
                JoinStrategy::Inner => {
                    // advance both at least once for inner
                    // left is advanced on checking (after checking right can stil be advanced)
                    // right is advanced after
                    if self.sets_idx + 1 >= self.sets.len() || !left.advance() {
                        self.sets_idx = self.sets.len();
                        return false;
                    }
                    self.sets_idx += 1;
                    self.key = self.sets[self.sets_idx].item_list[0].0;
                    while self.key != left.get_key() {
                        if self.key > left.get_key() {
                            if !left.advance() {
                                self.sets_idx = self.sets.len();
                                return false;
                            }
                        // need to advance right and are able to do so
                        } else if self.key < left.get_key() {
                            if self.sets_idx + 1 >= self.sets.len() {
                                self.sets_idx = self.sets.len();
                                return false;
                            } else {
                                self.sets_idx += 1;
                                self.key = self.sets[self.sets_idx].item_list[0].0;
                            }
                        }
                    }
                    true
                }
                JoinStrategy::Left => {
                    // advance left and see if we can match
                    if left.advance() {
                        while self.sets_idx + 1 < self.sets.len()
                            && self.sets[self.sets_idx].item_list[0].0 < left.get_key()
                        {
                            self.sets_idx += 1;
                        }
                        // after this the key is guaranteed to be equal to the left key or bigger
                        // so if the keys match that will be fine for copy in, otherwise this will be skipped
                        // if the key already equal or bigger, it was not advanced
                        self.key = left.get_key();
                        true
                    } else {
                        self.sets_idx = self.sets.len();
                        false
                    }
                }
                JoinStrategy::Right => {
                    if self.sets_idx + 1 >= self.sets.len() {
                        self.sets_idx = self.sets.len();
                        return false;
                    }
                    self.sets_idx += 1;
                    self.key = self.sets[self.sets_idx].item_list[0].0;
                    while self.key > left.get_key() {
                        if !left.advance() {
                            break;
                        }
                    }
                    true
                }
                JoinStrategy::Outer => {
                    let current_self_key = self.sets[self.sets_idx].item_list[0].0;
                    let right_can_be_advanced = self.sets_idx + 1 < self.sets.len();
                    // check if one of the already known keys is bigger, if so we know we can adavance
                    if self.key < left.get_key() {
                        // last key was set from right, since left is bigger
                        debug_assert_eq!(self.key, current_self_key);
                        // if right can be advance it should be advanced, otherwise just set key to left one
                        if right_can_be_advanced {
                            self.sets_idx += 1;
                            // new right might still be smaller than left key
                            self.key =
                                u32::min(self.sets[self.sets_idx].item_list[0].0, left.get_key());
                        } else {
                            self.key = left.get_key();
                        }
                        true
                    } else if self.key < current_self_key {
                        // the last key was set from left, since right is bigger
                        debug_assert_eq!(self.key, left.get_key());
                        // if left can be adnvanced it should be, if not move
                        if left.advance() {
                            // new left key might still be smaller than right
                            self.key =
                                u32::min(self.sets[self.sets_idx].item_list[0].0, left.get_key());
                        } else {
                            //  left did not advance, so set key to current right key
                            self.key = current_self_key;
                        }
                        true
                    } else if self.key == left.get_key() && self.key == current_self_key {
                        // both keys are the same, so advance any that are possible to advance and take new key from there
                        let left_advance_success = left.advance();
                        if right_can_be_advanced {
                            self.sets_idx += 1;
                        }
                        match (right_can_be_advanced, left_advance_success) {
                            (true, true) => {
                                self.key = u32::min(
                                    self.sets[self.sets_idx].item_list[0].0,
                                    left.get_key(),
                                );
                                true
                            }
                            (true, false) => {
                                self.key = self.sets[self.sets_idx].item_list[0].0;
                                true
                            }
                            (false, true) => {
                                self.key = left.get_key();
                                true
                            }
                            (false, false) => {
                                self.sets_idx = self.sets.len();
                                false
                            }
                        }
                    } else {
                        // the key is already set to the bigger of the two current keys, try to advance that one
                        if current_self_key == left.get_key() {
                            let did_advance = left.advance();
                            self.key = left.get_key();
                            did_advance
                        } else {
                            if right_can_be_advanced {
                                self.sets_idx += 1;
                                self.key = self.sets[self.sets_idx].item_list[0].0;
                            }
                            right_can_be_advanced
                        }
                    }
                }
                JoinStrategy::Cross => {
                    if self.sets_idx + 1 < self.sets.len() {
                        self.sets_idx += 1;
                        self.key = self.sets[self.sets_idx].item_list[0].0;
                        true
                    } else if left.advance() {
                        self.sets_idx = 0;
                        self.key = self.sets[0].item_list[0].0;
                        true
                    } else {
                        // set right index to len so we can't accidentally copy something
                        self.sets_idx = self.sets.len();
                        false
                    }
                }
            }
        } else {
            if self.sets_idx + 1 >= self.sets.len() {
                self.sets_idx = self.sets.len();
                false
            } else {
                // advancing only makes sense for certain modes here
                if self.strategy == JoinStrategy::Right
                    || self.strategy == JoinStrategy::Outer
                    || self.strategy == JoinStrategy::Cross
                {
                    self.sets_idx += 1;
                    self.key = self.sets[self.sets_idx].item_list[0].0;
                    true
                } else {
                    panic!("Should never have join iterator with left or inner that has None for the left value");
                }
            }
        }
    }

    fn get_key(&self) -> u32 {
        self.key
    }
}

/// Implements the JoinIterator for the `any` shardings.
/// TODO
struct AnyIterator {
    left: Option<Box<dyn JoinIterator>>,
    set: CompositionSet,
    set_idx: usize,
    key: u32,
    write_index: usize,
}

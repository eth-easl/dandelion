use crate::function_registry::{FunctionDict, Metadata};
use dandelion_commons::{DandelionError, DandelionResult, DispatcherError, FunctionId};
use dparser;
use itertools::Itertools;
use machine_interface::memory_domain::Context;
use std::{
    collections::{btree_map::Entry, BTreeMap},
    sync::Arc,
};

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

impl Composition {
    /// For each composition the composition set indexes start enumerating the input sets from 0.
    /// The output sets are enumerated starting with the number directly after the highest input set index.
    /// For internal numbering there are no guarnatees.
    /// TODO: add validation for the function input / output set names against the locally registered meta data (or at least the number)
    pub fn from_module(
        module: &dparser::Module,
        function_ids: &mut FunctionDict,
    ) -> DandelionResult<Vec<(FunctionId, Self, Metadata)>> {
        // TODO: do we need module somewhere else outside? Otherwise take ownership to safe a lot of clones
        let mut known_functions = BTreeMap::new();
        let mut compositions = Vec::new();
        for item in module.0.iter() {
            match item {
                dparser::Item::FunctionDecl(fdecl) => {
                    known_functions.insert(
                        fdecl.v.name.clone(),
                        (
                            function_ids.lookup(&fdecl.v.name).ok_or(
                                DandelionError::CompositionContainsInvalidFunction(format!(
                                    "{}",
                                    &fdecl.v.name
                                )),
                            )?,
                            fdecl,
                        ),
                    );
                }
                dparser::Item::Composition(comp) => {
                    let composition_id = function_ids.insert_or_lookup(comp.v.name.clone());
                    let mut set_counter = 0usize;
                    let mut set_numbers = BTreeMap::new();
                    // add composition input sets
                    for input_set_name in comp.v.params.iter() {
                        match set_numbers.entry(input_set_name.clone()) {
                            Entry::Vacant(v) => v.insert(set_counter),
                            Entry::Occupied(_) => {
                                return Err(DandelionError::CompositionDuplicateSetName)
                            }
                        };
                        set_counter += 1;
                    }
                    let mut output_map = BTreeMap::new();
                    let output_sets_start = set_counter;
                    // add composition output sets
                    for (output_index, output_set_name) in comp.v.returns.iter().enumerate() {
                        match set_numbers.entry(output_set_name.clone()) {
                            Entry::Vacant(v) => {
                                v.insert(set_counter);
                                output_map.insert(set_counter, output_index);
                                set_counter += 1;
                            }
                            // output set is input set
                            Entry::Occupied(occupied) => {
                                output_map.insert(*occupied.get(), output_index);
                            }
                        };
                    }
                    let output_sets_end = set_counter;
                    // add all return sets from functions
                    let composition_set_identifiers = comp
                        .v
                        .statements
                        .iter()
                        .flat_map(|statement| match statement {
                            dparser::Statement::FunctionApplication(function_application) => {
                                function_application.v.rets.iter().map(|ret| ret.v.ident.clone())
                            }
                            dparser::Statement::Loop(_) =>
                                todo!("loop semantics need to be fleshed out and compositions extended to acoomodate them"),
                        });
                    for set_identifier in composition_set_identifiers {
                        match set_numbers.entry(set_identifier.clone()) {
                            Entry::Vacant(v) => {
                                v.insert(set_counter);
                                set_counter += 1;
                            }
                            Entry::Occupied(o) => {
                                if output_sets_start <= *o.get() && *o.get() < output_sets_end {
                                    continue;
                                } else {
                                    return Err(DandelionError::CompositionDuplicateSetName);
                                }
                            }
                        }
                    }

                    // have enumerated all set that are available so can start putting the composition together
                    let dependencies = comp
                        .v
                        .statements
                        .iter()
                        .map(|statement| match statement {
                            dparser::Statement::FunctionApplication(function_application) => {
                                let (function_id, function_decl) = known_functions
                                    .get(&function_application.v.name)
                                    .ok_or_else(|| DandelionError::CompositionContainsInvalidFunction(function_application.v.name.clone()))?;
                                if function_decl.v.params.len() < function_application.v.args.len()
                                    || function_decl.v.returns.len()
                                        < function_application.v.rets.len()
                                {
                                    return Err(DandelionError::CompositionContainsInvalidFunction(function_application.v.name.clone()));
                                }
                                // find the indeces of the sets in the function application by looking though the definition
                                let mut input_set_ids = Vec::new();
                                input_set_ids
                                    .try_reserve(function_decl.v.params.len()).map_err(|_| DandelionError::OutOfMemory)?;
                                input_set_ids.resize(function_decl.v.params.len(), None);
                                for argument in function_application.v.args.iter() {
                                    if let Some(index) =
                                        function_decl.v.params.iter().position(
                                            |param_name| argument.v.name == *param_name,
                                        )
                                    {
                                        let set_id = set_numbers.get(&argument.v.ident).ok_or_else(
                                            ||DandelionError::CompositionFunctionInvalidIdentifier(
                                                format!("Could not find compositon set for argument {} of function {}",
                                                argument.v.ident, function_application.v.name)
                                            ),
                                        )?;
                                        input_set_ids[index] = Some(InputSetDescriptor {
                                            composition_id: *set_id,
                                            sharding:
                                            ShardingMode::from_parser_sharding(
                                                &argument.v.sharding,
                                            ),
                                            optional: argument.v.optional }
                                        );
                                    } else {
                                        return Err(
                                            DandelionError::CompositionFunctionInvalidIdentifier(
                                                format!("could not find index for input set {} for function {}",
                                                argument.v.name, function_application.v.name)
                                            ),
                                        );
                                    }
                                }

                                // find the join order
                                let mut all_sets: Vec<_> =
                                    (0..function_decl.v.params.len())
                                    .map(|index| Some(index)).collect();
                                let mut join_set_order = Vec::new();
                                join_set_order.try_reserve(function_decl.v.params.len())
                                    .or(Err(DandelionError::OutOfMemory))?;
                                let mut join_strategies = Vec::new();
                                join_strategies.try_reserve(function_decl.v.params.len())
                                    .or(Err(DandelionError::OutOfMemory))?;
                                if let Some(strategy) = function_application.v.join_strategy.as_ref().and_then(|strategy| if strategy.join_strategy_order.is_empty() || strategy.join_strategies.is_empty() {None} else {Some(strategy)}) {
                                    for set_name in strategy.join_strategy_order.iter() {
                                        let set_index = function_decl.v.params.iter()
                                            .position(|param_name| *param_name == *set_name)
                                            .ok_or_else(|| DandelionError::CompositionFunctionInvalidIdentifier(
                                                format!("Join order for {} contains invalid set name: {}", function_application.v.name, set_name))
                                            )?;
                                        all_sets[set_index] = None;
                                        join_set_order.push(set_index);
                                    }
                                    for join_strategy in strategy.join_strategies.iter() {
                                        join_strategies.push(JoinStrategy::from_parser_strategy(&join_strategy))
                                    }
                                }

                                // find the index set index in the original definition for each return set in the application
                                let mut output_set_ids = Vec::new();
                                output_set_ids
                                    .try_reserve(function_decl.v.returns.len())
                                    .or(Err(DandelionError::OutOfMemory))?;
                                output_set_ids.resize(function_decl.v.returns.len(), None);
                                for return_set in function_application.v.rets.iter() {
                                    if let Some(index) =
                                        function_decl.v.returns.iter().position(
                                            |return_name| {
                                                return_set.v.name == *return_name
                                            },
                                        )
                                    {
                                        let set_id = set_numbers.get(&return_set.v.ident).ok_or(
                                            DandelionError::CompositionFunctionInvalidIdentifier(
                                                return_set.v.ident.clone(),
                                            ),
                                        )?;
                                        output_set_ids[index] = Some(*set_id);
                                    } else {
                                        return Err(
                                            DandelionError::CompositionFunctionInvalidIdentifier(
                                                return_set.v.ident.clone(),
                                            ),
                                        );
                                    }
                                }
                                Ok(FunctionDependencies {
                                    function: *function_id,
                                    input_set_ids,
                                    join_info: (join_set_order, join_strategies),
                                    output_set_ids,
                                })
                            }
                            dparser::Statement::Loop(_) => {
                                todo!("Need to implement loop support in compositions")
                            }
                        })
                        .collect::<DandelionResult<Vec<_>>>()?;
                    let metadata = Metadata {
                        input_sets: comp
                            .v
                            .params
                            .iter()
                            .map(|name| (name.clone(), None))
                            .collect_vec()
                            .into(),
                        output_sets: comp
                            .v
                            .returns
                            .iter()
                            .map(|name| name.clone())
                            .collect_vec()
                            .into(),
                    };
                    compositions.push((
                        composition_id,
                        Composition {
                            dependencies,
                            output_map,
                        },
                        metadata,
                    ));
                }
            }
        }

        Ok(compositions)
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

    // Needed to add this to get access in main to return output set contents for mcp
    pub fn get_item_list_ref(&self) -> &[(u32, usize, Arc<Context>)] {
        &self.item_list
    }
    
    pub fn is_empty(&self) -> bool {
        self.item_list.is_empty()
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

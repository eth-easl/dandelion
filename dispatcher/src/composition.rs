use crate::function_registry::{FunctionDict, Metadata};
use dandelion_commons::{DandelionError, DandelionResult, FunctionId};
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

#[derive(Clone, Debug)]
pub struct FunctionDependencies {
    pub function: FunctionId,
    /// composition set ids that the function needs to get ready and
    /// the mapping to local ids is given implicitly through the index in the vec
    /// if the id is none, that set is not provided by the compostion
    pub input_set_ids: Vec<Option<(usize, ShardingMode)>>,
    /// the composition ids for the output sets of the function,
    /// if the id is none, that set is not needed for the composition
    pub output_set_ids: Vec<Option<usize>>,
}

impl ShardingMode {
    pub fn from_parser_sharding(sharding: &dparser::Sharding) -> Self {
        return match sharding {
            dparser::Sharding::All => Self::All,
            dparser::Sharding::Keyed => Self::Key,
            dparser::Sharding::Each => Self::Each,
        };
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
                            function_ids
                                .lookup(&fdecl.v.name)
                                .ok_or(DandelionError::CompositionContainsInvalidFunction)?,
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
                            }
                            Entry::Occupied(_) => {
                                return Err(DandelionError::CompositionDuplicateSetName);
                            }
                        };
                        set_counter += 1;
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
                                    .ok_or(DandelionError::CompositionContainsInvalidFunction)?;
                                if function_decl.v.params.len() < function_application.v.args.len()
                                    || function_decl.v.returns.len()
                                        < function_application.v.rets.len()
                                {
                                    return Err(DandelionError::CompositionContainsInvalidFunction);
                                }
                                // find the indeces of the sets in the function application by looking though the definition
                                let mut input_set_ids = Vec::new();
                                input_set_ids
                                    .try_reserve(function_decl.v.params.len())
                                    .or(Err(DandelionError::OutOfMemory))?;
                                input_set_ids.resize(function_decl.v.params.len(), None);
                                for argument in function_application.v.args.iter() {
                                    if let Some((index, _)) =
                                        function_decl.v.params.iter().enumerate().find(
                                            |(_, &ref param_name)| argument.v.name == *param_name,
                                        )
                                    {
                                        let set_id = set_numbers.get(&argument.v.ident).ok_or(
                                            DandelionError::CompositionFunctionInvalidIdentifier(
                                                format!("Could not find compositon set for argument {} of function {}",
                                                argument.v.ident, function_application.v.name)
                                            ),
                                        )?;
                                        input_set_ids[index] = Some((
                                            *set_id,
                                            ShardingMode::from_parser_sharding(
                                                &argument.v.sharding,
                                            ),
                                        ));
                                    } else {
                                        return Err(
                                            DandelionError::CompositionFunctionInvalidIdentifier(
                                                format!("could not find index for input set {} for function {}",
                                                argument.v.name, function_application.v.name)
                                            ),
                                        );
                                    }
                                }
                                // find the index set index in the original definition for each return set in the application
                                let mut output_set_ids = Vec::new();
                                output_set_ids
                                    .try_reserve(function_decl.v.returns.len())
                                    .or(Err(DandelionError::OutOfMemory))?;
                                output_set_ids.resize(function_decl.v.returns.len(), None);
                                for return_set in function_application.v.rets.iter() {
                                    if let Some((index, _)) =
                                        function_decl.v.returns.iter().enumerate().find(
                                            |(_, &ref return_name)| {
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

/// Modes for the composition set iteratior to return sharding
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ShardingMode {
    All,
    Key,
    Each,
}

/// Struct that has all locations belonging to one set, that is potentially spread over multiple contexts.
#[derive(Clone, Debug)]
pub struct CompositionSet {
    /// list of all contexts and the set index in that context that belongs to the composition set and the buffer indexes which are present
    pub context_list: Vec<(Arc<Context>, core::ops::Range<usize>)>,
    /// the set side inside the contexts the composition set represents
    pub set_index: usize,
}

impl CompositionSet {
    /// index is only take for convenience outside
    pub fn shard(
        self,
        mode: ShardingMode,
        compositon_index: usize,
    ) -> Vec<(usize, CompositionSet)> {
        return match mode {
            ShardingMode::All => {
                vec![(compositon_index, self)]
            }
            ShardingMode::Key => {
                let mut key_map = BTreeMap::new();
                for (context, range) in self.context_list {
                    for index in range {
                        // TODO check / make sure all contexts actually have he set
                        if let Some(set) = &context.content[self.set_index] {
                            key_map
                                .entry(set.buffers[index].key)
                                .and_modify(
                                    |previous: &mut Vec<(
                                        Arc<Context>,
                                        core::ops::Range<usize>,
                                    )>| {
                                        // TODO find if we can extend ranges on consequtive matches
                                        // remeber that indexes might be consequtive, but different contexts
                                        previous.push((context.clone(), index..index + 1))
                                    },
                                )
                                .or_insert(vec![(context.clone(), index..index + 1)]);
                        }
                    }
                }
                key_map
                    .into_iter()
                    .map(|(_, context_list)| {
                        (
                            compositon_index,
                            CompositionSet {
                                context_list,
                                set_index: self.set_index,
                            },
                        )
                    })
                    .collect()
            }
            ShardingMode::Each => self
                .context_list
                .into_iter()
                .map(|(context, range)| {
                    range.into_iter().map(move |index| {
                        (
                            compositon_index,
                            CompositionSet {
                                context_list: vec![(context.clone(), index..index + 1)],
                                set_index: self.set_index,
                            },
                        )
                    })
                })
                .flatten()
                .collect(),
        };
    }
    pub fn combine(&mut self, additional: &mut CompositionSet) -> DandelionResult<()> {
        let CompositionSet {
            context_list,
            set_index,
        } = additional;
        if self.set_index != *set_index {
            return Err(DandelionError::DispatcherCompositionCombine);
        }
        self.context_list.append(context_list);
        return Ok(());
    }
}

impl From<(usize, Vec<Arc<Context>>)> for CompositionSet {
    fn from(pair: (usize, Vec<Arc<Context>>)) -> Self {
        let (set_index, context_vec) = pair;
        let context_list = context_vec
            .into_iter()
            .filter_map(|context| {
                if context.content.len() > set_index {
                    context.content[set_index].as_ref().and_then(|set| {
                        if set.buffers.len() > 0 {
                            Some((context.clone(), 0..set.buffers.len()))
                        } else {
                            None
                        }
                    })
                } else {
                    None
                }
            })
            .collect();
        return CompositionSet {
            context_list: context_list,
            set_index: set_index,
        };
    }
}

pub struct CompositionSetTransferIterator {
    /// which set in the contexts contains the buffer
    set: CompositionSet,
    current: Option<(Arc<Context>, core::ops::Range<usize>)>,
}

impl Iterator for CompositionSetTransferIterator {
    type Item = (usize, usize, Arc<Context>);

    fn next(&mut self) -> Option<Self::Item> {
        // initialization has skipped empty sets, so if current is Some can return aout of there
        // if current becomes empty, drop it and get next one
        // if we could guarantee the ranges always contain something could simplify this code
        if let Some((current_context, ref mut current_range)) = &mut self.current {
            if current_range.start + 1 == current_range.end {
                let next = loop {
                    if let Some(next) = self.set.context_list.pop() {
                        if next.1.is_empty() {
                            continue;
                        } else {
                            break Some(next);
                        }
                    } else {
                        break None;
                    }
                };
                let (taken_context, taken_range) = self.current.take().unwrap();
                self.current = next;
                return Some((self.set.set_index, taken_range.start, taken_context));
            } else {
                current_range.end -= 1;
                return Some((
                    self.set.set_index,
                    current_range.end,
                    current_context.clone(),
                ));
            }
        }
        return None;
    }
}

impl IntoIterator for CompositionSet {
    type Item = (usize, usize, Arc<Context>);
    type IntoIter = CompositionSetTransferIterator;
    fn into_iter(mut self) -> Self::IntoIter {
        loop {
            if let Some(current) = self.context_list.pop() {
                if current.1.is_empty() {
                    continue;
                } else {
                    return CompositionSetTransferIterator {
                        set: self,
                        current: Some(current),
                    };
                }
            } else {
                return CompositionSetTransferIterator {
                    set: self,
                    current: None,
                };
            }
        }
    }
}

impl IntoIterator for &CompositionSet {
    type Item = (usize, usize, Arc<Context>);
    type IntoIter = CompositionSetTransferIterator;
    fn into_iter(self) -> Self::IntoIter {
        let new_set = self.clone();
        return new_set.into_iter();
    }
}

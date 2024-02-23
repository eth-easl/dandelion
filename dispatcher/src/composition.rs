use crate::function_registry::{FunctionDict, Metadata};
use dandelion_commons::{DandelionError, DandelionResult, FunctionId};
use dparser;
use itertools::Itertools;
use machine_interface::memory_domain::Context;
use std::{collections::BTreeMap, sync::Arc};

/// A composition has a composition wide id space that maps ids of
/// the input and output sets to sets of individual functions to a unified
/// namespace. The ids in this namespace are used to find out which
/// functions have become ready.
#[derive(Clone, Debug)]
pub struct Composition {
    pub dependencies: Vec<FunctionDependencies>,
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
                    let mut set_counter = 1usize;
                    let mut set_numbers = BTreeMap::new();
                    // add composition input sets
                    for input_set_name in comp.v.params.iter() {
                        if (set_numbers.insert(input_set_name.clone(), set_counter)).is_some() {
                            // TODO: handle duplicate set names or guarantee the parser already errored on them
                        }
                        set_counter += 1;
                    }
                    // add all return sets from functions
                    let composition_set_identifiers = comp
                        .v
                        .statements
                        .iter()
                        .flat_map(|statement| match statement {
                            dparser::Statement::FunctionApplication(a) => {
                                a.v.rets.iter().map(|ret| ret.v.ident.clone())
                            }
                            dparser::Statement::Loop(_) =>
                                todo!("loop semantics need to be fleshed out and compositions extended to acoomodate them"),
                        });
                    for set_identifier in composition_set_identifiers {
                        if (set_numbers.insert(set_identifier, set_counter)).is_some() {
                            // TODO: handle duplicate set names or guarantee the parser already errored on them
                        }
                        set_counter += 1;
                    }
                    // have enumerated all set that are available so can start putting the composition together
                    let dependencies = comp
                        .v
                        .statements
                        .iter()
                        .map(|statement| match statement {
                            dparser::Statement::FunctionApplication(a) => {
                                let (function_id, function_decl) =
                                    known_functions.get(&a.v.name).ok_or(
                                        DandelionError::CompositionContainsInvalidFunction,
                                    )?;
                                if function_decl.v.params.len() < a.v.args.len()
                                    || function_decl.v.params.len() < a.v.rets.len()
                                {
                                    return Err(DandelionError::CompositionContainsInvalidFunction);
                                }
                                let mut input_set_ids = Vec::new();
                                input_set_ids
                                    .try_reserve(function_decl.v.params.len())
                                    .or(Err(DandelionError::OutOfMemory))?;
                                input_set_ids.resize(function_decl.v.params.len(), None);
                                for (index, param_name) in function_decl.v.params.iter().enumerate()
                                {
                                    if let Some(arg) =
                                        a.v.args.iter().find(|&arg| arg.v.name == *param_name)
                                    {
                                        let set_id = set_numbers.get(&arg.v.ident).ok_or(
                                            DandelionError::CompositionFunctionInvalidIdentifier,
                                        )?;
                                        input_set_ids[index] = Some((
                                            *set_id,
                                            ShardingMode::from_parser_sharding(&arg.v.sharding),
                                        ));
                                    }
                                }
                                let mut output_set_ids = Vec::new();
                                output_set_ids
                                    .try_reserve(function_decl.v.returns.len())
                                    .or(Err(DandelionError::OutOfMemory))?;
                                output_set_ids.resize(function_decl.v.returns.len(), None);
                                for (index, ret_name) in function_decl.v.returns.iter().enumerate()
                                {
                                    if a.v
                                        .rets
                                        .iter()
                                        .find(|&arg| arg.v.name == *ret_name)
                                        .is_some()
                                    {
                                        let set_id = set_numbers.get(ret_name).ok_or(
                                            DandelionError::CompositionContainsInvalidFunction,
                                        )?;
                                        output_set_ids[index] = Some(*set_id);
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

                    compositions.push((composition_id, Composition { dependencies }, metadata));
                }
            }
        }

        Ok(compositions)
    }
}

/// Modes for the composition set iteratior to return sharding
#[derive(Clone, Copy, Debug)]
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
}

impl Iterator for CompositionSetTransferIterator {
    type Item = (usize, usize, Arc<Context>);

    fn next(&mut self) -> Option<Self::Item> {
        // if we can guarnatee that there is always an item, we could simplify this to just taking the last one
        // would requeire ensuring that property for composition sets
        // TOOD: make composition set ranges private so the constructor can guarantee this
        while let Some((context, buffer_range)) = self.set.context_list.last_mut() {
            if buffer_range.end >= buffer_range.start + 1 {
                buffer_range.end = buffer_range.end - 1;
                return Some((self.set.set_index, buffer_range.end, context.clone()));
            } else {
                self.set.context_list.pop();
            }
        }
        return None;
    }
}

impl IntoIterator for CompositionSet {
    type Item = (usize, usize, Arc<Context>);
    type IntoIter = CompositionSetTransferIterator;
    fn into_iter(self) -> Self::IntoIter {
        return CompositionSetTransferIterator { set: self };
    }
}

impl IntoIterator for &CompositionSet {
    type Item = (usize, usize, Arc<Context>);
    type IntoIter = CompositionSetTransferIterator;
    fn into_iter(self) -> Self::IntoIter {
        return CompositionSetTransferIterator { set: self.clone() };
    }
}

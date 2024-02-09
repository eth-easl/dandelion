use dandelion_commons::{DandelionError, DandelionResult, FunctionId};
use machine_interface::memory_domain::Context;
use std::{collections::{BTreeMap, HashMap}, sync::Arc, vec};

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

impl Composition {
    pub fn from_module(module: &dparser::Module,
        function_ids: &mut crate::function_registry::FunctionDict) -> DandelionResult<Vec<(FunctionId, Self)>> {
        
        let mut known_functions = HashMap::new();
        // let mut known_collections = HashMap::new();

        for item in module.0.iter() {
            match item {
                dparser::Item::FunctionDecl(fdecl) => {
                    known_functions.insert(fdecl.v.name, (
                        function_ids.lookup(&fdecl.v.name).ok_or(DandelionError::InvalidComposition)?,
                        fdecl.v)
                    );
                }
                dparser::Item::Composition(comp) => {
                    
                }
            }
        }
        
        Ok(todo!())
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
                    context.content[set_index]
                        .as_ref()
                        .and_then(|set| Some((context.clone(), 0..set.buffers.len())))
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
        if let Some((context, buffer_range)) = self.set.context_list.last_mut() {
            let buffer_index = buffer_range.start;
            let ret_context = context.clone();
            if buffer_range.start + 1 == buffer_range.end {
                self.set.context_list.pop();
            } else {
                buffer_range.start = buffer_range.start + 1;
            }
            return Some((self.set.set_index, buffer_index, ret_context));
        } else {
            return None;
        }
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

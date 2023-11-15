use crate::{
    composition::{Composition, FunctionDependencies},
    execution_qs::EngineQueue,
    function_registry::{FunctionRegistry, FunctionType},
    resource_pool::ResourcePool,
};
use core::pin::Pin;
use dandelion_commons::{
    records::{Archive, RecordPoint, Recorder},
    ContextTypeId, DandelionError, DandelionResult, EngineTypeId, FunctionId,
};
use futures::{
    future::join_all,
    lock::Mutex,
    stream::{FuturesUnordered, StreamExt},
    Future,
};
use itertools::Itertools;
use machine_interface::{
    function_driver::FunctionConfig,
    memory_domain::{transer_data_item, Context, MemoryDomain},
};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::Arc,
    sync::Mutex as SyncMutex,
};

#[derive(Clone, Debug)]
pub enum ShardingMode {
    NoSharding,
    KeySharding(BTreeSet<u32>),
    // potentailly add an option to provide a map for specific items
}

/// Struct that has all locations belonging to one set, that is potentially spread over multiple contexts.
#[derive(Clone, Debug)]
pub struct CompositionSet {
    /// list of all contexts and the set index in that context that belongs to the composition set
    pub context_list: Vec<Arc<Context>>,
    pub set_index: usize,
    pub sharding_mode: ShardingMode,
}

impl CompositionSet {
    fn shard(self, index: usize) -> Vec<(usize, CompositionSet)> {
        let keys = match self.sharding_mode {
            ShardingMode::NoSharding => return vec![(index, self)],
            ShardingMode::KeySharding(keys) => keys,
        };
        return keys
            .into_iter()
            .map(|key| {
                let mut new_shard = BTreeSet::new();
                new_shard.insert(key);
                return (
                    index,
                    CompositionSet {
                        context_list: self.context_list.clone(),
                        set_index: self.set_index,
                        sharding_mode: ShardingMode::KeySharding(new_shard),
                    },
                );
            })
            .collect();
    }
    fn combine(&mut self, additional: &mut CompositionSet) -> DandelionResult<()> {
        let CompositionSet {
            context_list,
            set_index,
            sharding_mode,
        } = additional;
        if self.set_index != *set_index {
            return Err(DandelionError::DispatcherCompositionCombine);
        }
        match (&mut self.sharding_mode, sharding_mode) {
            (ShardingMode::KeySharding(current_keys), ShardingMode::KeySharding(foregin_keys)) => {
                current_keys.append(foregin_keys)
            }
            (ShardingMode::NoSharding, ShardingMode::NoSharding) => (),
            (_, _) => return Err(DandelionError::DispatcherCompositionCombine),
        }
        self.context_list.append(context_list);
        return Ok(());
    }
}

pub struct CompositionSetTransferIterator {
    set: CompositionSet,
    context_counter: usize,
    item_counter: usize,
}

impl IntoIterator for CompositionSet {
    type Item = (usize, usize, Arc<Context>);
    type IntoIter = CompositionSetTransferIterator;

    fn into_iter(self) -> Self::IntoIter {
        CompositionSetTransferIterator {
            set: self,
            context_counter: 0,
            item_counter: 0,
        }
    }
}

impl Iterator for CompositionSetTransferIterator {
    type Item = (usize, usize, Arc<Context>);

    fn next(&mut self) -> Option<Self::Item> {
        while self.set.context_list.len() > self.context_counter {
            let set_index = self.set.set_index;
            let context = self.set.context_list[self.context_counter].clone();
            if let Some(set) = &context.content[set_index] {
                while set.buffers.len() > self.item_counter {
                    let current_item = self.item_counter;
                    self.item_counter += 1;
                    match &self.set.sharding_mode {
                        ShardingMode::NoSharding => {
                            return Some((set_index, current_item, context));
                        }
                        ShardingMode::KeySharding(shard_set)
                            if shard_set.contains(&set.buffers[current_item].key) =>
                        {
                            return Some((set_index, current_item, context));
                        }
                        ShardingMode::KeySharding(_) => (),
                    }
                }
            }
            self.item_counter = 0;
            self.context_counter += 1;
        }
        return None;
    }
}

fn get_composition_set(
    contexts: &Vec<Arc<Context>>,
    function_set_index: usize,
) -> Option<CompositionSet> {
    let keys = contexts
        .iter()
        .filter_map(|context_ref| {
            if context_ref.content.len() <= function_set_index {
                return None;
            }
            let set_keys = context_ref.content[function_set_index]
                .as_ref()
                .and_then(|set| Some(set.buffers.iter().map(|buffer| buffer.key)));
            return set_keys;
        })
        .flatten()
        .collect();
    return Some(CompositionSet {
        set_index: function_set_index,
        sharding_mode: ShardingMode::KeySharding(keys),
        context_list: contexts.to_vec(),
    });
}

// TODO here and in registry can probably replace driver and loader function maps with fixed size arrays
// That have compile time size and static indexing
pub struct Dispatcher {
    domains: BTreeMap<ContextTypeId, Box<dyn MemoryDomain>>,
    engines: BTreeMap<EngineTypeId, EngineQueue>,
    type_map: BTreeMap<EngineTypeId, ContextTypeId>,
    function_registry: FunctionRegistry,
    pub archive: Arc<SyncMutex<Archive>>,
}

impl Dispatcher {
    pub fn init(
        domains: BTreeMap<ContextTypeId, Box<dyn MemoryDomain>>,
        type_map: BTreeMap<EngineTypeId, ContextTypeId>,
        function_registry: FunctionRegistry,
        mut resource_pool: ResourcePool,
    ) -> DandelionResult<Dispatcher> {
        let mut engines = BTreeMap::new();
        // Use up all engine resources to start with
        for (engine_id, driver) in function_registry.drivers.iter() {
            let mut engine_vec = Vec::new();
            while let Ok(Some(resource)) =
                resource_pool.sync_acquire_engine_resource(engine_id.clone())
            {
                if let Ok(engine) = driver.start_engine(vec![resource]) {
                    engine_vec.push(engine);
                }
            }
            let engine_queue = EngineQueue {
                internals: Mutex::new((engine_vec, VecDeque::new())),
            };
            engines.insert(engine_id.clone(), engine_queue);
        }
        let archive: Arc<SyncMutex<Archive>> = Arc::new(SyncMutex::new(Archive::new()));
        return Ok(Dispatcher {
            domains,
            engines,
            type_map,
            function_registry,
            archive,
        });
    }

    pub async fn queue_composition(
        &self,
        composition: Composition,
        mut inputs: BTreeMap<usize, CompositionSet>,
        output_sets: BTreeSet<usize>,
        non_caching: bool,
    ) -> DandelionResult<Vec<(usize, CompositionSet)>> {
        // build up ready sets
        let mut ready_sets = inputs.keys().cloned().collect::<BTreeSet<usize>>();
        let (mut ready_functions, mut non_ready_functions): (Vec<_>, Vec<_>) =
            composition.dependencies.into_iter().partition(
                |FunctionDependencies {
                     input_set_ids: in_ids,
                     output_set_ids: _,
                     function: _,
                 }| {
                    in_ids.iter().all(|index_opt| {
                        index_opt
                            .and_then(|index| Some(ready_sets.contains(&index)))
                            .unwrap_or(true)
                    })
                },
            );
        let mut running_functions: FuturesUnordered<_> = ready_functions
            .into_iter()
            .map(|dependencies| {
                let function_inputs = dependencies
                    .input_set_ids
                    .iter()
                    .enumerate()
                    .filter_map(|(function_index, composition_index_opt)| {
                        if let Some(composition_index) = composition_index_opt {
                            inputs.get(composition_index).and_then(|composition_set| {
                                Some((function_index, composition_set.clone()))
                            })
                        } else {
                            None
                        }
                    })
                    .collect();
                return self.queue_function_sharded(
                    dependencies.function,
                    function_inputs,
                    dependencies.output_set_ids,
                    non_caching,
                );
            })
            .collect();

        while let Some(Ok(new_compositions)) = running_functions.next().await {
            for (composition_set_index, composition_set) in new_compositions {
                inputs.insert(composition_set_index, composition_set);
                ready_sets.insert(composition_set_index);
            }
            // add newly ready ones
            (ready_functions, non_ready_functions) = non_ready_functions.into_iter().partition(
                |FunctionDependencies {
                     input_set_ids: in_ids,
                     output_set_ids: _,
                     function: _,
                 }| {
                    in_ids.iter().all(|index_opt| {
                        index_opt
                            .and_then(|index| Some(ready_sets.contains(&index)))
                            .unwrap_or(true)
                    })
                },
            );
            for ready_function in ready_functions {
                let function_inputs = ready_function
                    .input_set_ids
                    .iter()
                    .enumerate()
                    .filter_map(|(function_index, composition_index_opt)| {
                        if let Some(composition_index) = composition_index_opt {
                            inputs.get(composition_index).and_then(|composition_set| {
                                Some((function_index, composition_set.clone()))
                            })
                        } else {
                            None
                        }
                    })
                    .collect();
                running_functions.push(self.queue_function_sharded(
                    ready_function.function,
                    function_inputs,
                    ready_function.output_set_ids,
                    non_caching,
                ));
            }
        }
        return Ok(inputs
            .into_iter()
            .filter(|(set_index, _)| output_sets.contains(set_index))
            .collect::<Vec<_>>());
    }

    async fn queue_function_sharded<'context>(
        &self,
        function_id: FunctionId,
        input_sets: Vec<(usize, CompositionSet)>,
        output_mapping: Vec<Option<usize>>,
        non_caching: bool,
    ) -> DandelionResult<Vec<(usize, CompositionSet)>> {
        let results: Vec<_> = input_sets
            .into_iter()
            .map(|(index, composition_set)| composition_set.shard(index))
            .multi_cartesian_product()
            .map(|input_sets_local| {
                Box::pin(self.queue_function(
                    function_id,
                    input_sets_local,
                    output_mapping.clone(),
                    non_caching,
                ))
            })
            .collect();
        let composition_sets = join_all(results)
            .await
            .into_iter()
            .flatten()
            .flatten()
            .fold(BTreeMap::new(), |mut map, (set_id, mut composition_set)| {
                map.entry(set_id)
                    .and_modify(|previous: &mut CompositionSet| {
                        previous.combine(&mut composition_set);
                    })
                    .or_insert(composition_set);
                return map;
            })
            .into_iter()
            .collect::<Vec<(usize, _)>>();
        return Ok(composition_sets);
    }

    /// returns a vector of pairs of a index and a composition set
    /// the index describes which output set the composition belongs to.
    pub fn queue_function<'dispatcher>(
        &'dispatcher self,
        function_id: FunctionId,
        inputs: Vec<(usize, CompositionSet)>,
        output_mapping: Vec<Option<usize>>,
        non_caching: bool,
    ) -> Pin<
        Box<
            dyn Future<Output = DandelionResult<Vec<(usize, CompositionSet)>>> + 'dispatcher + Send,
        >,
    > {
        Box::pin(async move {
            let mut recorder = Recorder::new(self.archive.clone(), RecordPoint::Arrival);
            // start new record for the function
            // find an engine capable of running the function
            // TODO actual scheduling decisions
            let options = self.function_registry.get_options(function_id).await?;
            if let Some(alternative) = options.iter().next() {
                match &alternative.function_type {
                    FunctionType::Function(engine_id) => {
                        let (context, config, out_set_names) = self
                            .prepare_for_engine(
                                function_id,
                                *engine_id,
                                inputs,
                                non_caching,
                                recorder.clone(),
                            )
                            .await?;
                        let (result, context) = self
                            .run_on_engine(
                                *engine_id,
                                config,
                                out_set_names,
                                context,
                                recorder.clone(),
                            )
                            .await;
                        recorder.record(RecordPoint::FutureReturn)?;
                        result?;
                        let context_arc = Arc::new(context);
                        let composition_sets = output_mapping
                            .into_iter()
                            .enumerate()
                            .filter_map(|(function_set_id, composition_set_id_opt)| {
                                return composition_set_id_opt.and_then(|composition_set_id| {
                                    return get_composition_set(
                                        &vec![context_arc.clone()],
                                        function_set_id,
                                    )
                                    .and_then(|comp_set| Some((composition_set_id, comp_set)))
                                    .or(Some((
                                        composition_set_id,
                                        CompositionSet {
                                            set_index: function_set_id,
                                            context_list: vec![],
                                            sharding_mode: ShardingMode::KeySharding(
                                                BTreeSet::new(),
                                            ),
                                        },
                                    )));
                                });
                            })
                            .collect();
                        return Ok(composition_sets);
                    }
                    FunctionType::Composition(composition, out_sets) => {
                        // need to set the inner composition indexing to the outer composition indexing
                        let compositon_output = self
                            .queue_composition(
                                composition.clone(),
                                BTreeMap::from_iter(inputs),
                                out_sets.clone(),
                                non_caching,
                            )
                            .await;
                        return compositon_output;
                    }
                }
            } else {
                return Err(DandelionError::DispatcherUnavailableFunction);
            }
        })
    }

    async fn prepare_for_engine(
        &self,
        function_id: FunctionId,
        engine_type: EngineTypeId,
        inputs: Vec<(usize, CompositionSet)>,
        non_caching: bool,
        mut recorder: Recorder,
    ) -> DandelionResult<(Context, FunctionConfig, &Vec<String>)> {
        let (in_set_names, out_set_names) = self.function_registry.get_set_names(function_id)?;
        // get context and load static data
        let context_id = match self.type_map.get(&engine_type) {
            Some(id) => id,
            None => return Err(DandelionError::DispatcherConfigError),
        };
        let domain = match self.domains.get(context_id) {
            Some(d) => d,
            None => return Err(DandelionError::DispatcherConfigError),
        };
        // start doing transfers
        recorder.record(RecordPoint::TransferStart)?;
        let (mut function_context, function_config) = self
            .function_registry
            .load(function_id, engine_type, domain, non_caching)
            .await?;
        // make sure all input sets are there at the correct index
        for in_set_name in in_set_names {
            function_context
                .content
                .push(Some(machine_interface::DataSet {
                    ident: in_set_name.clone(),
                    buffers: vec![],
                }))
        }
        for (function_set, context_set) in inputs {
            let mut function_item = 0usize;
            for (subset, item, source_context) in context_set {
                // TODO get allignment information
                let set_name = &in_set_names[function_set];
                transer_data_item(
                    &mut function_context,
                    &source_context,
                    function_set,
                    128,
                    function_item,
                    set_name,
                    subset,
                    item,
                )?;
                function_item += 1;
            }
        }
        recorder.record(RecordPoint::TransferEnd)?;
        return Ok((function_context, function_config, out_set_names));
    }

    async fn run_on_engine(
        &self,
        engine_type: EngineTypeId,
        function_config: FunctionConfig,
        output_sets: &Vec<String>,
        function_context: Context,
        recorder: Recorder,
    ) -> (DandelionResult<()>, Context) {
        // preparation is done, get engine to receive engine
        let engine_queue = match self.engines.get(&engine_type) {
            Some(q) => q,
            None => return (Err(DandelionError::DispatcherConfigError), function_context),
        };
        return engine_queue
            .perform_single_run(&function_config, function_context, output_sets, recorder)
            .await;
    }
}

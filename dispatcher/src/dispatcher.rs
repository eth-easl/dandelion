use crate::{
    composition::{Composition, CompositionSet, FunctionDependencies, ShardingMode},
    execution_qs::EngineQueue,
    function_registry::{FunctionRegistry, FunctionType, Metadata},
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
                let engine = driver.start_engine(resource)?;
                engine_vec.push(engine);
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

    pub async fn update_func(
        &self,
        function_id: FunctionId,
        engine_type: EngineTypeId,
        ctx_size: usize,
        path: &str,
        metadata: Metadata,
    ) -> DandelionResult<()> {
        self.function_registry
            .insert_metadata(todo!("funciton name"), metadata)
            .await;
        return self
            .function_registry
            .add_local(function_id, engine_type, ctx_size, path)
            .await;
    }

    pub async fn queue_composition(
        &self,
        composition: Composition,
        mut inputs: BTreeMap<usize, CompositionSet>,
        output_sets: BTreeMap<usize, usize>,
        non_caching: bool,
    ) -> DandelionResult<BTreeMap<usize, CompositionSet>> {
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
                            .and_then(|(index, _)| Some(ready_sets.contains(&index)))
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
                        if let Some((composition_index, mode)) = composition_index_opt {
                            inputs.get(composition_index).and_then(|composition_set| {
                                Some((function_index, *mode, composition_set.clone()))
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

        while let Some(new_compositions_result) = running_functions.next().await {
            let new_compositions = new_compositions_result?;
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
                            .as_ref()
                            .and_then(|(index, _)| Some(ready_sets.contains(&index)))
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
                        if let Some((composition_index, mode)) = composition_index_opt {
                            inputs.get(composition_index).and_then(|composition_set| {
                                Some((function_index, *mode, composition_set.clone()))
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
            .filter_map(|(set_index, composition_set)| {
                output_sets
                    .get(&set_index)
                    .and_then(|output_index| Some((*output_index, composition_set)))
            })
            .collect::<BTreeMap<_, _>>());
    }

    async fn queue_function_sharded<'context>(
        &self,
        function_id: FunctionId,
        input_sets: Vec<(usize, ShardingMode, CompositionSet)>,
        output_mapping: Vec<Option<usize>>,
        non_caching: bool,
    ) -> DandelionResult<BTreeMap<usize, CompositionSet>> {
        let results: Vec<_> = input_sets
            .into_iter()
            .map(|(index, mode, composition_set)| composition_set.shard(mode, index))
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
        let composition_results: DandelionResult<Vec<_>> =
            join_all(results).await.into_iter().collect();
        let mut composition_set_maps = composition_results?.into_iter();
        if let Some(mut result_set_map) = composition_set_maps.next() {
            for additional_set_map in composition_set_maps {
                for (key, mut additional_set) in additional_set_map.into_iter() {
                    result_set_map
                        .entry(key)
                        .and_modify(|existing_set| {
                            existing_set
                                .combine(&mut additional_set)
                                .expect("Should not fail to combine");
                        })
                        .or_insert(additional_set);
                }
            }
            return Ok(result_set_map);
        } else {
            return Ok(BTreeMap::new());
        };
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
            dyn Future<Output = DandelionResult<BTreeMap<usize, CompositionSet>>>
                + 'dispatcher
                + Send,
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
                    FunctionType::Function(engine_id, ctx_size) => {
                        let (context, config, metadata) = self
                            .prepare_for_engine(
                                function_id,
                                *engine_id,
                                inputs,
                                *ctx_size,
                                non_caching,
                                recorder.clone(),
                            )
                            .await?;
                        let (result, context) = self
                            .run_on_engine(
                                *engine_id,
                                config,
                                &metadata.output_sets,
                                context,
                                recorder.clone(),
                            )
                            .await;
                        result?;
                        recorder.record(RecordPoint::FutureReturn)?;
                        let context_arc = Arc::new(context);
                        let composition_sets = output_mapping
                            .into_iter()
                            .enumerate()
                            .filter_map(|(function_set_id, composition_set_id_opt)| {
                                return composition_set_id_opt.and_then(|composition_set_id| {
                                    return Some(CompositionSet::from((
                                        function_set_id,
                                        vec![context_arc.clone()],
                                    )))
                                    .and_then(|comp_set| Some((composition_set_id, comp_set)))
                                    .or(Some((
                                        composition_set_id,
                                        CompositionSet {
                                            set_index: function_set_id,
                                            context_list: vec![],
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
                            .await?;
                        return Ok(compositon_output
                            .into_iter()
                            .filter_map(|(function_id, composition)| {
                                if output_mapping.len() < function_id {
                                    return None;
                                }
                                return output_mapping[function_id].and_then(|composition_id| {
                                    Some((composition_id, composition))
                                });
                            })
                            .collect());
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
        ctx_size: usize,
        non_caching: bool,
        mut recorder: Recorder,
    ) -> DandelionResult<(Context, FunctionConfig, Arc<Metadata>)> {
        let metadata = self.function_registry.get_metadata(function_id).await?;
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
        recorder.record(RecordPoint::LoadStart)?;
        let (mut function_context, function_config) = self
            .function_registry
            .load(function_id, engine_type, domain, ctx_size, non_caching)
            .await?;
        recorder.record(RecordPoint::TransferStart)?;
        // make sure all input sets are there at the correct index
        let mut static_sets = BTreeSet::new();
        for (function_set_index, (in_set_name, in_composition_set)) in
            metadata.input_sets.iter().enumerate()
        {
            function_context
                .content
                .push(Some(machine_interface::DataSet {
                    ident: in_set_name.clone(),
                    buffers: vec![],
                }));
            if let Some(composition_set) = in_composition_set {
                static_sets.insert(function_set_index);
                let mut function_buffer = 0usize;
                for (subset, item, source_context) in composition_set {
                    transer_data_item(
                        &mut function_context,
                        &source_context,
                        function_set_index,
                        128,
                        function_buffer,
                        in_set_name,
                        subset,
                        item,
                    )?;
                    function_buffer += 1;
                }
            }
        }
        for (function_set, context_set) in inputs {
            if static_sets.contains(&function_set) {
                continue;
            }
            let mut function_item = 0usize;
            for (subset, item, source_context) in context_set {
                // TODO get allignment information
                let set_name = &metadata.input_sets[function_set].0;
                transer_data_item(
                    &mut function_context,
                    &source_context,
                    function_set,
                    // TODO get allignment information from function
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
        return Ok((function_context, function_config, metadata));
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

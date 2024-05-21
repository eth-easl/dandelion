use crate::{
    composition::{Composition, CompositionSet, FunctionDependencies, ShardingMode},
    execution_qs::EngineQueue,
    function_registry::{FunctionRegistry, FunctionType, Metadata},
    resource_pool::ResourcePool,
};
use core::pin::Pin;
use dandelion_commons::{
    records::{RecordPoint, Recorder},
    DandelionError, DandelionResult, FunctionId,
};
use futures::{
    future::join_all,
    stream::{FuturesUnordered, StreamExt},
    Future,
};
use itertools::Itertools;
use machine_interface::{
    function_driver::{Driver, FunctionArguments, FunctionConfig, TransferArguments, WorkToDo},
    machine_config::{
        get_available_domains, get_available_drivers, get_compatibilty_table, DomainType,
        EngineType,
    },
    memory_domain::{Context, MemoryDomain},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

// TODO here and in registry can probably replace driver and loader function maps with fixed size arrays
// That have compile time size and static indexing
// TODO also here and in registry replace Arc Box with static references from leaked boxes for things we expect to be there for
// the entire execution time anyway
pub struct Dispatcher {
    domains: BTreeMap<DomainType, (&'static dyn MemoryDomain, Box<EngineQueue>)>,
    engine_queues: BTreeMap<EngineType, Box<EngineQueue>>,
    type_map: BTreeMap<EngineType, DomainType>,
    function_registry: FunctionRegistry,
}

impl Dispatcher {
    pub fn init(mut resource_pool: ResourcePool) -> DandelionResult<Dispatcher> {
        // get machine specific configurations
        let type_map = get_compatibilty_table();
        let domains = get_available_domains();
        let drivers = get_available_drivers();

        // Insert a work queue for each domain and use up all engine resource available
        let mut domain_map = BTreeMap::new();
        let mut engine_queues = BTreeMap::new();
        let mut registry_drivers: BTreeMap<EngineType, (&'static dyn Driver, Box<EngineQueue>)> =
            BTreeMap::new();
        for (engine_type, driver) in drivers.into_iter() {
            let work_queue = Box::new(EngineQueue::new());
            while let Ok(Some(resource)) = resource_pool.sync_acquire_engine_resource(engine_type) {
                driver.start_engine(resource, work_queue.clone())?;
            }
            let domain_type = type_map.get(&engine_type).unwrap();
            let domain = *domains.get(domain_type).unwrap();
            domain_map.insert(*domain_type, (domain, work_queue.clone()));
            engine_queues.insert(engine_type, work_queue.clone());
            registry_drivers.insert(
                engine_type,
                (driver as &'static dyn Driver, work_queue.clone()),
            );
        }
        let function_registry = FunctionRegistry::new(registry_drivers, &type_map, &domains);

        return Ok(Dispatcher {
            domains: domain_map,
            engine_queues,
            type_map,
            function_registry,
        });
    }

    pub async fn insert_func(
        &self,
        function_name: String,
        engine_type: EngineType,
        ctx_size: usize,
        path: String,
        metadata: Metadata,
    ) -> DandelionResult<FunctionId> {
        return self
            .function_registry
            .insert_function(function_name, engine_type, ctx_size, path, metadata)
            .await;
    }

    pub async fn insert_compositions(&self, compositions: String) -> DandelionResult<()> {
        return self
            .function_registry
            .insert_compositions(&compositions)
            .await;
    }

    pub async fn queue_function_by_name(
        &self,
        function_name: String,
        inputs: Vec<(usize, CompositionSet)>,
        output_mapping_option: Option<Vec<Option<usize>>>,
        non_caching: bool,
        recorder: Recorder,
    ) -> DandelionResult<BTreeMap<usize, CompositionSet>> {
        let function_id = self
            .function_registry
            .get_function_id(&function_name)
            .await
            .ok_or(DandelionError::DispatcherUnavailableFunction)?;
        let output_mapping = if let Some(mapping) = output_mapping_option {
            mapping
        } else {
            let metadata = self.function_registry.get_metadata(function_id).await?;
            let output_number = metadata.output_sets.len();
            let mut mapping = Vec::with_capacity(output_number);
            for index in 0..output_number {
                mapping.push(Some(index));
            }
            mapping
        };
        return self
            .queue_function(function_id, inputs, output_mapping, non_caching, recorder)
            .await;
    }

    pub async fn queue_composition(
        &self,
        composition: Composition,
        mut inputs: BTreeMap<usize, CompositionSet>,
        non_caching: bool,
        recorder: Recorder,
    ) -> DandelionResult<BTreeMap<usize, CompositionSet>> {
        // build up ready sets
        let (mut ready_functions, mut non_ready_functions): (Vec<_>, Vec<_>) =
            composition.dependencies.into_iter().partition(
                |FunctionDependencies {
                     input_set_ids: in_ids,
                     output_set_ids: _,
                     function: _,
                 }| {
                    in_ids.iter().all(|index_opt| {
                        index_opt
                            .and_then(|(index, _)| Some(inputs.contains_key(&index)))
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
                    recorder.get_sub_recorder().unwrap(),
                );
            })
            .collect();

        while let Some(new_compositions_result) = running_functions.next().await {
            let new_compositions = new_compositions_result?;
            for (composition_set_index, composition_set) in new_compositions {
                inputs.insert(composition_set_index, composition_set);
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
                            .and_then(|(index, _)| Some(inputs.contains_key(&index)))
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
                    recorder.get_sub_recorder().unwrap(),
                ));
            }
        }
        return Ok(inputs
            .into_iter()
            .filter_map(|(set_index, composition_set)| {
                composition
                    .output_map
                    .get(&set_index)
                    .and_then(|output_index| Some((*output_index, composition_set)))
            })
            .collect::<BTreeMap<_, _>>());
    }

    // TODO: Solve the composition. How does it work now??
    async fn queue_function_sharded<'context>(
        &self,
        function_id: FunctionId,
        input_sets: Vec<(usize, ShardingMode, CompositionSet)>,
        output_mapping: Vec<Option<usize>>,
        non_caching: bool,
        recorder: Recorder,
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
                    recorder.get_sub_recorder().unwrap(),
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
        mut recorder: Recorder,
    ) -> Pin<
        Box<
            dyn Future<Output = DandelionResult<BTreeMap<usize, CompositionSet>>>
                + 'dispatcher
                + Send,
        >,
    > {
        Box::pin(async move {
            // find an engine capable of running the function
            // TODO actual scheduling decisions
            let options = self.function_registry.get_options(function_id).await?;
            if let Some(alternative) = options.iter().next() {
                match &alternative.function_type {
                    FunctionType::Function(engine_id, ctx_size) => {
                        recorder.record(RecordPoint::PrepareEnvQueue)?;
                        let (context, config, metadata) = self
                            .prepare_for_engine(
                                function_id,
                                *engine_id,
                                inputs,
                                *ctx_size,
                                non_caching,
                                recorder.get_sub_recorder()?,
                            )
                            .await?;
                        recorder.record(RecordPoint::GetEngineQueue)?;
                        let context = self
                            .run_on_engine(
                                *engine_id,
                                config,
                                metadata.output_sets,
                                context,
                                recorder.get_sub_recorder()?,
                            )
                            .await?;
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
                    FunctionType::Composition(composition) => {
                        // need to set the inner composition indexing to the outer composition indexing
                        let compositon_output = self
                            .queue_composition(
                                composition.clone(),
                                BTreeMap::from_iter(inputs),
                                non_caching,
                                recorder,
                            )
                            .await?;
                        return Ok(compositon_output
                            .into_iter()
                            .filter_map(|(function_id, composition)| {
                                if output_mapping.len() <= function_id {
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
        engine_type: EngineType,
        inputs: Vec<(usize, CompositionSet)>,
        ctx_size: usize,
        non_caching: bool,
        mut recorder: Recorder,
    ) -> DandelionResult<(Context, FunctionConfig, Metadata)> {
        let metadata = self.function_registry.get_metadata(function_id).await?;
        // get context and load static data
        let context_id = match self.type_map.get(&engine_type) {
            Some(id) => id,
            None => return Err(DandelionError::DispatcherConfigError),
        };
        let (domain, transfer_queue) = match self.domains.get(context_id) {
            Some(d) => d,
            None => return Err(DandelionError::DispatcherConfigError),
        };
        // start doing transfers
        recorder.record(RecordPoint::LoadQueue)?;
        let (mut function_context, function_config) = self
            .function_registry
            .load(
                function_id,
                engine_type,
                *domain,
                ctx_size,
                non_caching,
                recorder.get_sub_recorder().unwrap(),
            )
            .await?;
        recorder.record(RecordPoint::LoadDequeue)?;
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
                    let args = WorkToDo::TransferArguments(TransferArguments {
                        destination: function_context,
                        source: source_context,
                        destination_set_index: function_set_index,
                        destination_allignment: 128,
                        destination_item_index: function_buffer,
                        destination_set_name: in_set_name.clone(),
                        source_set_index: subset,
                        source_item_index: item,
                        recorder: recorder.get_sub_recorder().unwrap(),
                    });
                    recorder.record(RecordPoint::TransferQueue)?;
                    function_context = transfer_queue.enqueu_work(args).await?.get_context();
                    recorder.record(RecordPoint::TransferDequeue)?;
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
                let args = WorkToDo::TransferArguments(TransferArguments {
                    destination: function_context,
                    source: source_context,
                    destination_set_index: function_set,
                    destination_allignment: 128,
                    destination_item_index: function_item,
                    destination_set_name: set_name.clone(),
                    source_set_index: subset,
                    source_item_index: item,
                    recorder: recorder.get_sub_recorder().unwrap(),
                });
                recorder.record(RecordPoint::TransferQueue).unwrap();
                function_context = transfer_queue.enqueu_work(args).await?.get_context();
                recorder.record(RecordPoint::TransferDequeue).unwrap();
                function_item += 1;
            }
        }
        return Ok((function_context, function_config, metadata));
    }

    async fn run_on_engine(
        &self,
        engine_type: EngineType,
        function_config: FunctionConfig,
        output_sets: Arc<Vec<String>>,
        function_context: Context,
        mut recorder: Recorder,
    ) -> DandelionResult<Context> {
        // preparation is done, get engine to receive engine
        let engine_queue = match self.engine_queues.get(&engine_type) {
            Some(q) => q,
            None => return Err(DandelionError::DispatcherConfigError),
        };
        let subrecoder = recorder.get_sub_recorder()?;
        let args = WorkToDo::FunctionArguments(FunctionArguments {
            config: function_config,
            context: function_context,
            output_sets,
            recorder: subrecoder,
        });
        recorder.record(RecordPoint::ExecutionQueue)?;
        let result = engine_queue.enqueu_work(args).await?.get_context();
        recorder.record(RecordPoint::FutureReturn)?;
        return Ok(result);
    }
}

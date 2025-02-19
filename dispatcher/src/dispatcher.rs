use crate::{
    composition::{Composition, CompositionSet, ShardingMode},
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
use log::trace;
use machine_interface::{
    function_driver::{Driver, FunctionConfig, WorkToDo},
    machine_config::{
        get_available_domains, get_available_drivers, get_compatibilty_table, DomainType,
        EngineType,
    },
    memory_domain::{Context, MemoryDomain, MemoryResource},
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
    domains: BTreeMap<DomainType, (Arc<Box<dyn MemoryDomain>>, Box<EngineQueue>)>,
    engine_queues: BTreeMap<EngineType, Box<EngineQueue>>,
    type_map: BTreeMap<EngineType, DomainType>,
    function_registry: FunctionRegistry,
}

impl Dispatcher {
    pub fn init(
        mut resource_pool: ResourcePool,
        memory_resources: BTreeMap<DomainType, MemoryResource>,
    ) -> DandelionResult<Dispatcher> {
        // get machine specific configurations
        let type_map = get_compatibilty_table();
        let domains = get_available_domains(memory_resources);
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
            let domain = domains.get(domain_type).unwrap().clone();
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
        inputs: Vec<Option<CompositionSet>>,
        non_caching: bool,
        recorder: Recorder,
    ) -> DandelionResult<Vec<Option<CompositionSet>>> {
        let function_id = self
            .function_registry
            .get_function_id(&function_name)
            .await
            .ok_or(DandelionError::DispatcherUnavailableFunction)?;
        return self
            .queue_function(function_id, inputs, non_caching, recorder)
            .await;
    }

    pub async fn queue_composition(
        &self,
        composition: Composition,
        inputs: Vec<Option<CompositionSet>>,
        non_caching: bool,
        recorder: Recorder,
    ) -> DandelionResult<Vec<Option<CompositionSet>>> {
        // build up ready sets
        trace!("queue composition");
        // local state of functions that still need to run
        struct FunctionArgs {
            function_id: FunctionId,
            inptut_sets: Vec<Option<(ShardingMode, CompositionSet)>>,
            output_mapping: Vec<Option<usize>>,
            missing_sets: BTreeMap<usize, (usize, ShardingMode)>,
        }

        // prepare output sets
        let output_number = composition.output_map.len();
        let mut output_sets = Vec::with_capacity(output_number);
        output_sets.resize(output_number, None);
        for (input_index, input_set) in inputs.iter().enumerate() {
            if let Some(out_index) = composition.output_map.get(&input_index) {
                output_sets[*out_index] = input_set.clone();
            }
        }

        let (ready_functions, mut non_ready_functions): (Vec<_>, Vec<_>) = composition
            .dependencies
            .into_iter()
            .map(|deps| {
                let mut missing_map = BTreeMap::new();
                let input_set_number: usize = deps.input_set_ids.len();
                let mut ready_inputs = Vec::with_capacity(input_set_number);
                ready_inputs.resize(deps.input_set_ids.len(), None);
                for (function_index, set) in deps.input_set_ids.iter().enumerate() {
                    if let Some((set_index, mode)) = set {
                        if *set_index < inputs.len() {
                            if let Some(comp_set) = &inputs[*set_index] {
                                ready_inputs[function_index] = Some((*mode, comp_set.clone()));
                            }
                        } else {
                            missing_map.insert(*set_index, (function_index, *mode));
                        }
                    }
                }
                FunctionArgs {
                    function_id: deps.function,
                    inptut_sets: ready_inputs,
                    output_mapping: deps.output_set_ids,
                    missing_sets: missing_map,
                }
            })
            .partition(|args| args.missing_sets.is_empty());
        let mut running_functions: FuturesUnordered<_> = ready_functions
            .into_iter()
            .map(|args| {
                return self.queue_function_sharded(
                    args.function_id,
                    args.inptut_sets,
                    args.output_mapping,
                    non_caching,
                    recorder.get_sub_recorder().unwrap(),
                );
            })
            .collect();
        trace!(
            "functions ready: {}, functions not ready: {}",
            running_functions.len(),
            non_ready_functions.len()
        );
        while let Some(new_compositions_result) = running_functions.next().await {
            let new_compositions = new_compositions_result?;
            for new_composition_set_option in new_compositions {
                if let Some((composition_set_index, composition_set)) = new_composition_set_option {
                    trace!(
                        "composition set {} arrived at dispatcher",
                        composition_set_index
                    );
                    if let Some(output_index) = composition.output_map.get(&composition_set_index) {
                        output_sets[*output_index] = Some(composition_set.clone());
                    }
                    non_ready_functions = non_ready_functions
                        .into_iter()
                        .filter_map(|mut args| {
                            if let Some((index, mode)) =
                                args.missing_sets.remove(&composition_set_index)
                            {
                                args.inptut_sets[index] = Some((mode, composition_set.clone()));
                            }
                            if args.missing_sets.is_empty() {
                                running_functions.push(self.queue_function_sharded(
                                    args.function_id,
                                    args.inptut_sets,
                                    args.output_mapping,
                                    non_caching,
                                    recorder.get_sub_recorder().unwrap(),
                                ));
                                None
                            } else {
                                Some(args)
                            }
                        })
                        .collect();
                }
            }
            trace!(
                "functions running: {}, functions not ready: {}",
                running_functions.len(),
                non_ready_functions.len()
            );
        }

        return Ok(output_sets);
    }

    /// Adapter between compositions and functions
    /// Keeps track of the composition set indexes so that when sets are returned to
    /// composition they have the corret index associated without the composition needing to track them.
    /// Also handles sharing of sets
    async fn queue_function_sharded<'context>(
        &self,
        function_id: FunctionId,
        input_sets: Vec<Option<(ShardingMode, CompositionSet)>>,
        output_mapping: Vec<Option<usize>>,
        non_caching: bool,
        recorder: Recorder,
    ) -> DandelionResult<Vec<Option<(usize, CompositionSet)>>> {
        trace!(
            "queue function {} shareded and input sets: {:?}",
            function_id,
            input_sets
        );
        let composition_results: DandelionResult<Vec<_>> = if input_sets.len() != 0 {
            let results: Vec<_> = input_sets
                .into_iter()
                .map(|composition_option| {
                    if let Some((mode, set)) = composition_option {
                        set.shard(mode)
                    } else {
                        vec![None]
                    }
                })
                .multi_cartesian_product()
                .map(|input_sets_local| {
                    Box::pin(self.queue_function(
                        function_id,
                        input_sets_local,
                        non_caching,
                        recorder.get_sub_recorder().unwrap(),
                    ))
                })
                .collect();
            join_all(results).await.into_iter().collect()
        // TODO this is added to support functions with all functions defined as static sets
        // might want to differentiate between those that have static sets and those that did not get input from predecessors
        } else {
            self.queue_function(function_id, vec![], non_caching, recorder)
                .await
                .and_then(|result| Ok(vec![result]))
        };

        // collect vec of vec of shards into vec of sets
        let output_lenght = output_mapping.len();
        let composition_set_vecs = composition_results?
            .into_iter()
            .reduce(|mut accumulator, elem_vec| {
                for (accumulator_set, elem_set) in accumulator.iter_mut().zip(elem_vec.into_iter())
                {
                    match (accumulator_set, elem_set) {
                        (_, None) => (),
                        (insert @ None, Some(set)) => {
                            *insert = Some(set);
                        }
                        (Some(old_set), Some(mut new_set)) => {
                            old_set
                                .combine(&mut new_set)
                                .expect("Should always be possible to combine");
                        }
                    }
                }
                accumulator
            })
            .unwrap_or_else(|| {
                let mut new_vec = Vec::with_capacity(output_lenght);
                new_vec.resize(output_lenght, None);
                new_vec
            });

        // assiociated sets with composition ids
        Ok(output_mapping
            .into_iter()
            .zip(composition_set_vecs.into_iter())
            .map(
                |(index_option, set_option)| match (index_option, set_option) {
                    (Some(index), Some(set)) => Some((index, set)),
                    (Some(index), None) => Some((
                        index,
                        CompositionSet {
                            context_list: Vec::new(),
                            set_index: 0,
                        },
                    )),
                    (None, _) => None,
                },
            )
            .collect())
    }

    /// returns a vector of pairs of a index and a composition set
    /// the index describes which output set the composition belongs to.
    pub fn queue_function<'dispatcher>(
        &'dispatcher self,
        function_id: FunctionId,
        inputs: Vec<Option<CompositionSet>>,
        non_caching: bool,
        mut recorder: Recorder,
    ) -> Pin<
        Box<dyn Future<Output = DandelionResult<Vec<Option<CompositionSet>>>> + 'dispatcher + Send>,
    > {
        trace!("queueing function with id: {}", function_id);
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
                        trace!("running function {} on {:?} type engine with input sets {:?} and output sets {:?}",
                            function_id,
                            *engine_id,
                            metadata.input_sets.iter().map(|(name, _)| name).collect_vec(),
                            metadata.output_sets);
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

                        let composition_sets = context_arc
                            .content
                            .iter()
                            .enumerate()
                            .map(|(function_set_id, data_option)| {
                                data_option.as_ref().and_then(|_| {
                                    Some(CompositionSet::from((
                                        function_set_id,
                                        vec![context_arc.clone()],
                                    )))
                                })
                            })
                            .collect();
                        return Ok(composition_sets);
                    }
                    FunctionType::Composition(composition) => {
                        return self
                            .queue_composition(composition.clone(), inputs, non_caching, recorder)
                            .await;
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
        inputs: Vec<Option<CompositionSet>>,
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
        let (mut function_context, function_config) = self
            .function_registry
            .load(
                function_id,
                engine_type,
                domain.clone(),
                ctx_size,
                non_caching,
                recorder.get_sub_recorder().unwrap(),
            )
            .await?;
        // make sure all input sets are there at the correct index
        let mut static_sets = BTreeSet::new();
        for (function_set_index, (in_set_name, metadata_set)) in
            metadata.input_sets.iter().enumerate()
        {
            function_context
                .content
                .push(Some(machine_interface::DataSet {
                    ident: in_set_name.clone(),
                    buffers: vec![],
                }));
            if let Some(composition_set) = metadata_set {
                trace!(
                    "preparing function {}: copying metadata set to function set {}",
                    function_id,
                    function_set_index
                );
                static_sets.insert(function_set_index);
                let mut function_buffer = 0usize;
                for (subset, item, source_context) in composition_set {
                    let args = WorkToDo::TransferArguments {
                        destination: function_context,
                        source: source_context,
                        destination_set_index: function_set_index,
                        destination_allignment: 128,
                        destination_item_index: function_buffer,
                        destination_set_name: in_set_name.clone(),
                        source_set_index: subset,
                        source_item_index: item,
                        recorder: recorder.get_sub_recorder().unwrap(),
                    };
                    recorder.record(RecordPoint::TransferQueue)?;
                    function_context = transfer_queue.enqueu_work(args).await?.get_context();
                    recorder.record(RecordPoint::TransferDequeue)?;
                    function_buffer += 1;
                }
            }
        }
        for (set_index, set_option) in inputs.iter().enumerate() {
            let set = if let Some(set) = set_option {
                set
            } else {
                continue;
            };

            if static_sets.contains(&set_index) {
                trace!(
                    "for function {} skipping input set {} from inputs because it was already in metadata",
                    function_id,
                    set_index,
                );
                continue;
            }
            trace!(
                "preparing function {}: copying composition set to function set {}",
                function_id,
                set_index,
            );
            let mut function_item = 0usize;
            for (subset, item, source_context) in set {
                // TODO get allignment information
                let set_name = source_context.content[subset]
                    .as_ref()
                    .unwrap()
                    .ident
                    .clone();
                let args = WorkToDo::TransferArguments {
                    destination: function_context,
                    source: source_context,
                    destination_set_index: set_index,
                    destination_allignment: 128,
                    destination_item_index: function_item,
                    destination_set_name: set_name,
                    source_set_index: subset,
                    source_item_index: item,
                    recorder: recorder.get_sub_recorder().unwrap(),
                };
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
        let args = WorkToDo::FunctionArguments {
            config: function_config,
            context: function_context,
            output_sets,
            recorder: subrecoder,
        };
        recorder.record(RecordPoint::ExecutionQueue)?;
        let result = engine_queue.enqueu_work(args).await?.get_context();
        recorder.record(RecordPoint::FutureReturn)?;
        return Ok(result);
    }
}

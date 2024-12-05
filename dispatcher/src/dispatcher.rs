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
    function_driver::{Driver, FunctionConfig, WorkToDo, ComputeResource},
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
    pub engine_queues: BTreeMap<EngineType, Box<EngineQueue>>,
    type_map: BTreeMap<EngineType, DomainType>,
    function_registry: FunctionRegistry,
}

impl Dispatcher {
    pub fn init(
        mut resource_pool: ResourcePool,
        mut cpu_core_map: BTreeMap<EngineType, Vec<u8>>,
        memory_resources: BTreeMap<DomainType, MemoryResource>,
    ) -> DandelionResult<(Dispatcher, ResourcePool, BTreeMap<EngineType, Vec<u8>>)> {
        // get machine specific configurations
        let type_map = get_compatibilty_table();
        let domains = get_available_domains(memory_resources);
        let drivers = get_available_drivers();

        // Insert a work queue for each domain and use up all engine resource available
        let mut domain_map = BTreeMap::new();
        let mut engine_queues = BTreeMap::new();
        let mut registry_drivers: BTreeMap<EngineType, (&'static dyn Driver, Box<EngineQueue>)> =
            BTreeMap::new();
        // let mut cpu_core_map: BTreeMap<EngineType, Vec<u8>> = BTreeMap::new();
        
        for (engine_type, driver) in drivers.into_iter() {
            let work_queue = Box::new(EngineQueue::new());
            cpu_core_map.insert(engine_type, Vec::new()); // Initialize the CPU core map
            while let Ok(Some(resource)) = resource_pool.sync_acquire_engine_resource(engine_type) {
                if let ComputeResource::CPU(core_id) = resource {
                    cpu_core_map.get_mut(&engine_type).unwrap().push(core_id);
                    driver.start_engine(resource, work_queue.clone())?;
                }
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

        return Ok((
            Dispatcher {
            domains: domain_map,
            engine_queues,
            type_map,
            function_registry,
        }, 
        resource_pool,
        cpu_core_map,
        ))
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
        inputs: BTreeMap<usize, CompositionSet>,
        non_caching: bool,
        recorder: Recorder,
    ) -> DandelionResult<BTreeMap<usize, CompositionSet>> {
        // build up ready sets
        trace!("queue composition");
        // local state of functions that still need to run
        struct FunctionArgs {
            function_id: FunctionId,
            inptut_sets: Vec<(usize, ShardingMode, CompositionSet)>,
            output_mapping: Vec<Option<usize>>,
            missibng_sets: BTreeMap<usize, (usize, ShardingMode)>,
        }

        // prepare output sets
        let mut outpu_sets = BTreeMap::new();
        let mut missing_outpus = composition.output_map;
        for (input_index, input_set) in &inputs {
            if let Some(ouput_index) = missing_outpus.remove(&input_index) {
                outpu_sets.insert(ouput_index, input_set.clone());
            }
        }

        let (ready_functions, mut non_ready_functions): (Vec<_>, Vec<_>) = composition
            .dependencies
            .into_iter()
            .map(|deps| {
                let mut missing_map = BTreeMap::new();
                let mut ready_inputs = Vec::with_capacity(deps.input_set_ids.len());
                for (function_index, set) in deps.input_set_ids.iter().enumerate() {
                    if let Some((set_index, mode)) = set {
                        if let Some(comp_set) = inputs.get(set_index) {
                            ready_inputs.push((function_index, *mode, comp_set.clone()));
                        } else {
                            missing_map.insert(*set_index, (function_index, *mode));
                        }
                    }
                }
                FunctionArgs {
                    function_id: deps.function,
                    inptut_sets: ready_inputs,
                    output_mapping: deps.output_set_ids,
                    missibng_sets: missing_map,
                }
            })
            .partition(|args| args.missibng_sets.is_empty());
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
            for (composition_set_index, composition_set) in &new_compositions {
                trace!(
                    "composition set {} arrived at dispatcher",
                    composition_set_index
                );
                if let Some(ouput_index) = missing_outpus.remove(&composition_set_index) {
                    outpu_sets.insert(ouput_index, composition_set.clone());
                }
            }
            non_ready_functions = non_ready_functions
                .into_iter()
                .filter_map(|mut args| {
                    for (composition_set_index, composition_set) in &new_compositions {
                        if let Some((index, mode)) =
                            args.missibng_sets.remove(composition_set_index)
                        {
                            args.inptut_sets
                                .push((index, mode, composition_set.clone()));
                        }
                    }
                    if args.missibng_sets.is_empty() {
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
            trace!(
                "functions running: {}, functions not ready: {}",
                running_functions.len(),
                non_ready_functions.len()
            );
        }

        return Ok(outpu_sets);
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
        trace!(
            "queue function {} shareded and input sets: {:?}",
            function_id,
            input_sets
        );
        // TODO this is added to support functions with all functions defined as static sets
        // might want to differentiate between those that have static sets and those that did not get input from predecessors
        let composition_results: DandelionResult<Vec<_>> = if input_sets.len() != 0 {
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
            join_all(results).await.into_iter().collect()
        } else {
            self.queue_function(function_id, vec![], output_mapping, non_caching, recorder)
                .await
                .and_then(|result| Ok(vec![result]))
        };
        // let : DandelionResult<Vec<_>> =
        let mut composition_set_maps = composition_results?.into_iter();
        trace!(
            "sharded function {} finished with {} composition results",
            function_id,
            composition_set_maps.len()
        );
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
                                        CompositionSet::from((function_set_id, vec![])),
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
                domain.clone(),
                ctx_size,
                non_caching,
                recorder.get_sub_recorder().unwrap(),
            )
            .await?;
        recorder.record(RecordPoint::LoadDequeue)?;
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
        for (function_set, context_set) in inputs {
            if static_sets.contains(&function_set) {
                trace!(
                    "for function {} skipping input set {} from inputs because it was already in metadata",
                    function_id,
                    function_set,
                );
                continue;
            }
            trace!(
                "preparing function {}: copying composition set {} to function set {}",
                function_id,
                context_set.set_index,
                function_set
            );
            let mut function_item = 0usize;
            for (subset, item, source_context) in context_set {
                // TODO get allignment information
                let set_name = source_context.content[subset]
                    .as_ref()
                    .unwrap()
                    .ident
                    .clone();
                let args = WorkToDo::TransferArguments {
                    destination: function_context,
                    source: source_context,
                    destination_set_index: function_set,
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

    pub fn get_total_tasks_lengths(&self) -> Vec<(EngineType, usize)> {
        self.engine_queues
            .iter()
            .map(|(engine_type, queue)| (*engine_type, queue.total_tasks_length()))
            .collect()
    }

    pub fn get_queue_lengths(&self) -> Vec<(EngineType, usize)> {
        self.engine_queues
            .iter()
            .map(|(engine_type, queue)| (*engine_type, queue.queue_length()))
            .collect()
    }
}

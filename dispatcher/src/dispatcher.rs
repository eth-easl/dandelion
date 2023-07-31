use crate::{
    composition::Composition, execution_qs::EngineQueue, function_registry::FunctionRegistry,
    resource_pool::ResourcePool,
};
use dandelion_commons::{
    records::{Archive, RecordPoint, Recorder},
    ContextTypeId, DandelionError, DandelionResult, EngineTypeId, FunctionId,
};
use futures::{
    lock::Mutex,
    stream::{FuturesUnordered, StreamExt},
};
use machine_interface::{
    function_driver::{DriverFunction, FunctionConfig},
    memory_domain::{transer_data_item, transfer_data_set, Context, MemoryDomain},
};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    rc::Rc,
    sync::Arc,
    sync::Mutex as SyncMutex,
    vec,
};

#[derive(Clone, Copy, Debug)]
pub struct ItemIndices {
    pub in_index: usize,
    pub out_index: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct TransferIndices {
    pub input_set_index: usize,
    pub output_set_index: usize,
    pub item_indices: Option<ItemIndices>,
}

struct PartiallyReadyFunction {
    function: FunctionId,
    waiting_map: BTreeMap<usize, Vec<TransferIndices>>,
    ready_ins: Vec<(Rc<Context>, Vec<TransferIndices>)>,
    output_id: usize,
}

// TODO here and in registry can probably replace driver and loader function maps with fixed size arrays
// That have compile time size and static indexing
pub struct Dispatcher {
    domains: BTreeMap<ContextTypeId, Box<dyn MemoryDomain>>,
    _drivers: BTreeMap<EngineTypeId, DriverFunction>,
    engines: BTreeMap<EngineTypeId, EngineQueue>,
    type_map: BTreeMap<EngineTypeId, ContextTypeId>,
    function_registry: FunctionRegistry,
    pub archive: Arc<SyncMutex<Archive>>,
}

impl Dispatcher {
    pub fn init(
        domains: BTreeMap<ContextTypeId, Box<dyn MemoryDomain>>,
        drivers: BTreeMap<EngineTypeId, DriverFunction>,
        type_map: BTreeMap<EngineTypeId, ContextTypeId>,
        function_registry: FunctionRegistry,
        mut resource_pool: ResourcePool,
    ) -> DandelionResult<Dispatcher> {
        let mut engines = BTreeMap::new();
        // Use up all engine resources to start with
        for (engine_id, driver) in drivers.iter() {
            let mut engine_vec = Vec::new();
            while let Ok(Some(resource)) =
                resource_pool.sync_acquire_engine_resource(engine_id.clone())
            {
                if let Ok(engine) = driver(vec![resource]) {
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
            _drivers: drivers,
            engines,
            type_map,
            function_registry,
            archive,
        });
    }

    pub async fn queue_composition(
        &self,
        composition: Composition,
        inputs: Vec<(usize, Rc<Context>)>,
        output_sets: BTreeSet<usize>,
        non_caching: bool,
    ) -> DandelionResult<Vec<(usize, Context)>> {
        // build up a structure with all ids in the composition namespace and the functions waiting for them
        // TODO preallocate here
        let mut partials = Vec::<Option<PartiallyReadyFunction>>::new();
        // map from context id to indexes in wating function vector
        let mut composition_waiting_map = BTreeMap::<usize, Vec<usize>>::new();
        for dependencies in composition.dependencies.into_iter() {
            let partial_index = partials.len();
            let function = dependencies.function;
            let output_id = dependencies.output_id;
            let mut waiting_map = BTreeMap::new();
            for (context_id, transfer_indices) in dependencies.input_ids {
                if waiting_map.insert(context_id, transfer_indices).is_some() {
                    return Err(DandelionError::DispatcherSetMissmatch);
                }
                composition_waiting_map
                    .entry(context_id)
                    .and_modify(|entry| entry.push(partial_index))
                    .or_insert(vec![partial_index]);
            }
            let partial = PartiallyReadyFunction {
                function,
                waiting_map,
                ready_ins: vec![],
                output_id,
            };
            partials.push(Some(partial));
        }
        // fill in the already available contexts
        let mut future_list = FuturesUnordered::new();
        for (input_id, input_context) in inputs {
            if let Some(waiting_list) = composition_waiting_map.remove(&input_id) {
                for waiting_func_index in waiting_list {
                    let mut is_empty = false;
                    if let Some(waiting_func) = partials[waiting_func_index].as_mut() {
                        if let Some(transfer_info) = waiting_func.waiting_map.remove(&input_id) {
                            waiting_func
                                .ready_ins
                                .push((input_context.clone(), transfer_info));
                        }
                        if waiting_func.waiting_map.is_empty() {
                            is_empty = true;
                        }
                    } else {
                        return Err(DandelionError::DispatcherDependencyError);
                    }
                    if is_empty {
                        if let Some(waiting) = partials[waiting_func_index].take() {
                            future_list.push(self.wrapped_queue_function(
                                waiting.function,
                                waiting.ready_ins,
                                non_caching,
                                waiting.output_id,
                            ));
                        }
                    }
                }
            }
        }
        // TODO replace with preallocated list (of options?) so do not need to mutatet in the loop, allowing to hand out refs instead of rcs
        let mut ready_contexts = Vec::new();
        let mut ready_context_ids = Vec::new();
        loop {
            let next_done = future_list.next().await;
            let (context_id, context_result) = match next_done {
                None => break,
                Some((trans_vec, res)) => (trans_vec, res?),
            };
            let rc_context = Rc::new(context_result);
            ready_contexts.push(rc_context.clone());
            if let Some(waiting_list) = composition_waiting_map.remove(&context_id) {
                for waiting_func_index in waiting_list {
                    let mut is_empty = false;
                    if let Some(waiting_func) = partials[waiting_func_index].as_mut() {
                        if let Some(transfer_info) = waiting_func.waiting_map.remove(&context_id) {
                            waiting_func
                                .ready_ins
                                .push((rc_context.clone(), transfer_info));
                        }
                        if waiting_func.waiting_map.is_empty() {
                            is_empty = true;
                        }
                    } else {
                        return Err(DandelionError::DispatcherDependencyError);
                    }
                    if is_empty {
                        if let Some(waiting) = partials[waiting_func_index].take() {
                            future_list.push(self.wrapped_queue_function(
                                waiting.function,
                                waiting.ready_ins,
                                non_caching,
                                waiting.output_id,
                            ));
                        }
                    }
                }
            }
            ready_context_ids.push(context_id);
        }
        let output_list = ready_contexts
            .into_iter()
            .zip(ready_context_ids.into_iter())
            .filter_map(|(rc_context, context_id)| {
                let context = Rc::try_unwrap(rc_context);
                return match (output_sets.contains(&context_id), context) {
                    (true, Ok(cont)) => Some((context_id, cont)),
                    (_, _) => None,
                };
            })
            .collect();
        return Ok(output_list);
    }

    async fn wrapped_queue_function(
        &self,
        function_id: FunctionId,
        inputs: Vec<(Rc<Context>, Vec<TransferIndices>)>,
        non_caching: bool,
        output_id: usize,
    ) -> (usize, DandelionResult<Context>) {
        let input_refs = inputs
            .iter()
            .map(|(rc_context, trans_vec)| (rc_context.as_ref(), trans_vec.to_owned()))
            .collect();
        return (
            output_id,
            self.queue_function(function_id, input_refs, non_caching)
                .await,
        );
    }

    pub async fn queue_function(
        &self,
        function_id: FunctionId,
        inputs: Vec<(&Context, Vec<TransferIndices>)>,
        non_caching: bool,
    ) -> DandelionResult<Context> {
        let mut recorder = Recorder::new(self.archive.clone(), RecordPoint::Arrival);
        // start new record for the function
        // find an engine capable of running the function
        // TODO actual scheduling decisions
        let engine_id;
        let options = self.function_registry.get_options(function_id).await?;
        if let Some(id) = options.0.iter().next() {
            engine_id = *id;
        } else {
            if let Some(load_id) = options.1.iter().next() {
                engine_id = *load_id;
            } else {
                return Err(DandelionError::DispatcherUnavailableFunction);
            }
        }
        let (context, config, out_set_names) = self
            .prepare_for_engine(
                function_id,
                engine_id,
                inputs,
                non_caching,
                recorder.clone(),
            )
            .await?;
        let (result, context) = self
            .run_on_engine(engine_id, config, out_set_names, context, recorder.clone())
            .await;
        recorder.record(RecordPoint::FutureReturn)?;
        return match result {
            Ok(()) => Ok(context),
            Err(err) => {
                let context_id = match self.type_map.get(&engine_id) {
                    Some(id) => id,
                    None => return Err(DandelionError::DispatcherConfigError),
                };
                let domain = match self.domains.get(context_id) {
                    Some(d) => d,
                    None => return Err(DandelionError::DispatcherConfigError),
                };
                let _release_result = domain.release_context(context);
                Err(err)
            }
        };
    }

    async fn prepare_for_engine(
        &self,
        function_id: FunctionId,
        engine_type: EngineTypeId,
        // vector with contexts that hold the inputs as well as assoziated tripples that say
        // the dynamic data index of the context, possible index into a set and index of the input in the new function
        inputs: Vec<(&Context, Vec<TransferIndices>)>,
        non_caching: bool,
        mut recorder: Recorder,
    ) -> DandelionResult<(Context, FunctionConfig, &Vec<String>)> {
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
        let (mut function_context, function_config, in_set_names, out_set_names) = self
            .function_registry
            .load(function_id, engine_type, domain, non_caching)
            .await?;
        for (input_context, index_map) in inputs {
            for transfer_info in index_map {
                // TODO get allignment information
                if transfer_info.output_set_index >= in_set_names.len() {
                    let _ = domain.release_context(function_context);
                    return Err(DandelionError::DispatcherSetMissmatch);
                }
                let set_name = &in_set_names[transfer_info.output_set_index];
                if let Some(item_indices) = transfer_info.item_indices {
                    transer_data_item(
                        &mut function_context,
                        input_context,
                        transfer_info.output_set_index,
                        128,
                        item_indices.out_index,
                        set_name,
                        transfer_info.input_set_index,
                        item_indices.in_index,
                    )?;
                } else {
                    transfer_data_set(
                        &mut function_context,
                        input_context,
                        transfer_info.output_set_index,
                        128,
                        set_name,
                        transfer_info.input_set_index,
                    )?;
                }
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

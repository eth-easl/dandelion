use crate::{
    function_registry::{FunctionRegistry, FunctionType},
    queue::{EngineQueue, WorkQueue},
    resource_pool::ResourcePool,
};
use composition::{AnyShardingMode, Composition, CompositionTemplate, Executor, ExpandSource, ExpansionTemplate};
use dandelion_commons::{
    err_dandelion,
    records::{RecordPoint, Recorder},
    DandelionError, DandelionResult, DispatcherError, FunctionId,
};
use futures::{
    future,
    stream::{FuturesUnordered, StreamExt},
};
use itertools::Itertools;
use log::{debug, trace};
use machine_interface::{
    function_driver::WorkToDo,
    machine_config::{get_available_domains, DomainType, EngineType, IntoEnumIterator},
};
use memory::{
    DataItem, DataSet, context::{ContextTrait, MemoryDomain, MemoryResource}, data::Metadata,
};
use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{atomic::AtomicUsize, Arc},
};

type InvocationResult = DandelionResult<(usize, ExecutionOutput)>;
type InvocationFuture<'a> = Pin<Box<dyn Future<Output = InvocationResult> + Send + 'a>>;

/// Enqueues the invocations of a composition directly into the work queue and keeps the futures
/// for their results, tagged with the index of the function in the composition.
struct CompositionExecutor<'a> {
    dispatcher: &'a Dispatcher,
    composition_id: usize,
    caching: bool,
    /// Recorder of the composition itself.
    recorder: Recorder,
    /// Recorders of all invocations created so far, indexed by the function index in the
    /// composition. Attached to the composition recorder as children once it finishes.
    children: Vec<Option<Vec<Recorder>>>,
    /// Results of all invocations that are still outstanding.
    awaited: FuturesUnordered<InvocationFuture<'a>>,
    /// current expansion_depth
    expansion_depth: usize,
}

impl<'a> CompositionExecutor<'a> {
    /// Creates the recorder for a new invocation and keeps track of it as child of the composition.
    fn new_child_recorder(&mut self, function_id: &FunctionId, composition_idx: usize) -> Recorder {
        let mut recorder = Recorder::new_from_parent(function_id.clone(), &self.recorder);
        recorder.record(RecordPoint::ShardingStart);
        self.children[composition_idx]
            .get_or_insert_with(Vec::new)
            .push(recorder.clone());
        recorder
    }
}

impl<'a> Executor for CompositionExecutor<'a> {
    fn enqueue_invocations<T: IntoIterator<Item = Vec<DataSet>>>(
        &mut self,
        function_id: FunctionId,
        composition_idx: usize,
        expansion: Option<Arc<ExpansionTemplate>>,
        inv_input_iter: T,
    ) {
        let dispatcher = self.dispatcher;
        let composition_id = self.composition_id;
        let caching = self.caching;

        match dispatcher
            .function_registry
            .get_function(&function_id)
            .expect("Should never have a lookup for non registered function")
        {
            FunctionType::Composition(comp) => {
                for input in inv_input_iter {
                    let recorder = self.new_child_recorder(&function_id, composition_idx);
                    let composition = Composition::from_template(
                        &comp.composition,
                        dispatcher.any_sharding_mode.clone(),
                        &dispatcher.function_registry,
                    );

                    let expansion_depth = self.expansion_depth;
                    self.awaited.push(Box::pin(async move {
                        dispatcher
                            .queue_composition(
                                composition_id,
                                composition,
                                input,
                                caching,
                                recorder,
                                expansion_depth,
                            )
                            .await
                            .map(|items| (composition_idx, items))
                    }));
                }
            }
            FunctionType::SystemFunction(sys_function) => {
                for input in inv_input_iter {
                    let mut recorder = self.new_child_recorder(&function_id, composition_idx);
                    let handle = tokio::spawn(async move {
                        recorder.record(RecordPoint::EngineStart);
                        let result =
                            machine_interface::function_driver::system_driver::convert_to_references(
                                sys_function,
                                input,
                            );
                        recorder.record(RecordPoint::EngineEnd);
                        recorder.record(RecordPoint::FutureReturn);
                        result
                    });
                    self.awaited.push(Box::pin(async move {
                        handle.await.unwrap().map(|items| (composition_idx, items))
                    }));
                }
            }
            FunctionType::Function(info) => {
                let function_alternatives = info.alternatives.read().unwrap().clone();
                
                // inputs after the function's own ones are forwarded sets, which only the expansion receives
                let num_own_inputs = info.metadata.input_sets.len();
                let mut recorders = Vec::new();
                let mut forwarded_sets = Vec::new();
                let work_iter = inv_input_iter.into_iter().map(|mut input| {
                    forwarded_sets.push(match expansion {
                        Some(_) => input.split_off(num_own_inputs),
                        None => Vec::new(),
                    });
                    let recorder = self.new_child_recorder(&function_id, composition_idx);
                    recorders.push(recorder.clone());
                    WorkToDo::FunctionArguments {
                        function_id: function_id.clone(),
                        function_alternatives: function_alternatives.clone(),
                        input_sets: input,
                        metadata: info.metadata.clone(),
                        caching,
                        recorder,
                    }
                });

                // enqueue all invocations at once to only take the queue lock once
                match dispatcher
                    .work_queue
                    .enqueue_many(work_iter, composition_id)
                {
                    Ok(promises) => {
                        let depth = self.expansion_depth;
                        for ((promise, mut recorder), forwarded) in promises.into_iter().zip(recorders).zip(forwarded_sets) {
                            let expansion = expansion.clone();
                            self.awaited.push(Box::pin(async move {
                                let outputs = promise.await?.get_data_items();
                                recorder.record(RecordPoint::FutureReturn);
                                let outputs = match expansion {
                                    None => outputs,
                                    Some(exp) => {
                                        // the actual expand step
                                        dispatcher
                                            .expand(composition_id, caching, depth, &exp, outputs, forwarded, recorder)
                                            .await?
                                    }
                                };
                                Ok((composition_idx, outputs))
                                
                            }));
                        }
                    }
                    // nothing was enqueued, report the error once the composition gets to it
                    Err(err) => self.awaited.push(Box::pin(future::ready(Err(err)))),
                }
            }
        };
    }
}

// TODO: Here and in the registry replace Arc<Box<...>> with static references from leaked boxes for
//       things we expect to be there for the entire execution time anyway.
pub struct Dispatcher {
    function_registry: FunctionRegistry,
    work_queue: WorkQueue,
    domains: Vec<Arc<Box<dyn MemoryDomain>>>,
    any_sharding_mode: AnyShardingMode,
    // TODO: The composition ids currently just go up from 0, there is no overflow handling.
    //       We are not expecting to overflow, but this should be handled for proper deployments.
    composition_id: AtomicUsize,
}

pub type ExecutionOutput = Vec<Arc<Vec<DataItem>>>;
/// Maximum number of nested expansions, guards against runaway recursion.
const MAX_EXPANSION_DEPTH: usize = 64;


impl Dispatcher {
    pub fn init(
        mut resource_pool: ResourcePool,
        memory_resources: BTreeMap<DomainType, MemoryResource>,
        work_queue: WorkQueue,
        any_sharding_mode: AnyShardingMode,
    ) -> DandelionResult<Self> {
        // get machine specific configurations
        let domains = get_available_domains(memory_resources);

        // create an engine queue wrapper of the work queue for each engine and use up all engine resource available
        for engine_type in EngineType::iter() {
            let engine_queue = EngineQueue::init(work_queue.clone(), engine_type);
            while let Ok(Some(resource)) = resource_pool.sync_acquire_engine_resource(engine_type) {
                engine_type.start_engine(resource, engine_queue.clone())?;
                if engine_type == EngineType::System {
                    continue;
                }
                work_queue.system_info.add_local_cores(1);
            }
        }

        // create the function registry
        let function_registry = FunctionRegistry::new();

        Ok(Dispatcher {
            function_registry,
            work_queue,
            domains,
            any_sharding_mode,
            composition_id: AtomicUsize::new(0),
        })
    }

    pub fn get_composition_id(&self) -> usize {
        self.composition_id
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
    }

    pub fn insert_function(
        &self,
        function_name: String,
        engine_type: EngineType,
        ctx_size: usize,
        path: String,
        metadata: Metadata,
    ) -> DandelionResult<()> {
        let function_id = Arc::new(function_name);
        let domain_type = engine_type.get_domain_type();
        self.function_registry.insert_function(
            function_id,
            engine_type,
            self.domains[domain_type as usize].clone(),
            ctx_size,
            path,
            metadata,
        )
    }

    pub fn insert_compositions(&self, composition_desc: String) -> DandelionResult<()> {
        let templates = CompositionTemplate::parse(&composition_desc, &self.function_registry)?;
        self.function_registry.insert_compositions(templates)
    }

    async fn resolve_results(
        &self,
        results: ExecutionOutput,
        composition_id: usize,
    ) -> DandelionResult<ExecutionOutput> {
        let results = if !results
            .iter()
            .all(|items| items.iter().all(|item| item.is_local()))
        {
            self.work_queue
                .enqueue(
                    WorkToDo::SetsToResolve {
                        input_sets: results.into_iter().map(DataSet::from_items).collect(),
                    },
                    composition_id,
                )?
                .await?
                .get_data_items()
        } else {
            results
        };
        Ok(results)
    }

    pub async fn queue_function_by_name(
        &self,
        function_id: Arc<String>,
        inputs: Vec<DataSet>,
        caching: bool,
        mut recorder: Recorder,
    ) -> DandelionResult<(ExecutionOutput, Recorder)> {
        debug!("Queuing function {}", function_id);
        recorder.record(RecordPoint::EnterDispatcher);

        let composition_id = self.get_composition_id();
        let results = self
            .queue_function(
                composition_id,
                function_id,
                inputs,
                caching,
                recorder.clone(),
            )
            .await?;

        let resolved_sets = self.resolve_results(results, composition_id).await?;
        Ok((resolved_sets, recorder))
    }

    pub async fn queue_unregistered_composition(
        &self,
        composition_desc: String,
        inputs: Vec<DataSet>,
        caching: bool,
        mut recorder: Recorder,
    ) -> DandelionResult<(ExecutionOutput, Recorder)> {
        debug!("Parsing single use composition");
        recorder.record(RecordPoint::EnterDispatcher);

        let mut templates = CompositionTemplate::parse(&composition_desc, &self.function_registry)?;
        let (composition_name, template, _metadata) = match templates.pop() {
            Some(parsed) if templates.is_empty() => parsed,
            _ => {
                debug!(
                    "Expected exactly one composition, but after popping {} were left over",
                    templates.len()
                );
                return err_dandelion!(DandelionError::Dispatcher(
                    DispatcherError::InvalidComposition,
                ));
            }
        };
        recorder.record(RecordPoint::DynamicParsingEnd);

        let composition_id = self.get_composition_id();
        debug!(
            "Queuing single use composition {} with id {}",
            composition_name, composition_id
        );
        let results = self
            .queue_composition(
                composition_id,
                Composition::from_template(
                    &template,
                    self.any_sharding_mode.clone(),
                    &self.function_registry,
                ),
                inputs,
                caching,
                recorder.clone(),
                0
            )
            .await?;

        let resolved_sets = self.resolve_results(results, composition_id).await?;

        Ok((resolved_sets, recorder))
    }

    async fn queue_composition(
        &self,
        composition_id: usize,
        mut composition: Composition,
        inputs: Vec<DataSet>,
        caching: bool,
        recorder: Recorder,
        depth: usize,
    ) -> DandelionResult<ExecutionOutput> {
        // build up ready sets
        trace!("queue composition");

        let mut ready_invocations = CompositionExecutor {
            dispatcher: self,
            composition_id,
            caching,
            recorder,
            children: vec![None; composition.num_functions()],
            awaited: FuturesUnordered::new(),
            expansion_depth: depth,
        };

        composition.start_execution(inputs, &mut ready_invocations);

        while let Some(set_result) = ready_invocations.awaited.next().await {
            let (composition_index, items) = set_result?;
            composition.push_invocation_output(items, composition_index, &mut ready_invocations);
        }

        let CompositionExecutor {
            mut recorder,
            children,
            ..
        } = ready_invocations;
        recorder.add_children(children);
        recorder.record(RecordPoint::FutureReturn);

        Ok(composition
            .collect()
            .into_iter()
            .map(|set| set.items)
            .collect())
    }

    /// returns a vector of pairs of a index and a composition set
    /// the index describes which output set the composition belongs to.
    pub async fn queue_function(
        &self,
        composition_id: usize,
        function_id: FunctionId,
        input_sets: Vec<DataSet>,
        caching: bool,
        mut recorder: Recorder,
    ) -> DandelionResult<Vec<Arc<Vec<DataItem>>>> {
        debug!("Queueing function with id: {}", function_id);
        // find an engine capable of running the function
        match self.function_registry.get_function(&function_id)? {
            // Defer actual execution of system functions (i.e. fetching),
            // by calling the system function to produce a composition set containing the reference to be resolved later
            FunctionType::SystemFunction(sys_function) => {
                recorder.record(RecordPoint::EngineStart);
                let result =
                    machine_interface::function_driver::system_driver::convert_to_references(
                        sys_function,
                        input_sets,
                    );
                recorder.record(RecordPoint::EngineEnd);
                result
            }
            FunctionType::Function(func_info) => {
                let function_alternatives = func_info
                    .alternatives
                    .read()
                    .expect("Function registry lock is poisoned!")
                    .clone();

                let metadata = func_info.metadata;
                #[cfg(feature = "log_function_stdio")]
                let stdio_index = metadata.output_sets.iter().position(|name| name == "stdio");
                // run on engine
                trace!(
                        "Running function {} with input sets {:?} and output sets {:?} and alternatives: {:?}",
                        function_id,
                        metadata
                            .input_sets
                            .iter()
                            .map(|(name, _)| name)
                            .collect_vec(),
                        metadata.output_sets,
                        function_alternatives
                    );
                let args = WorkToDo::FunctionArguments {
                    function_id: function_id.clone(),
                    function_alternatives,
                    input_sets,
                    metadata,
                    caching,
                    recorder: recorder.clone(),
                };

                let sets = self
                    .work_queue
                    .enqueue(args, composition_id)?
                    .await?
                    .get_data_items();
                recorder.record(RecordPoint::FutureReturn);

                #[cfg(feature = "log_function_stdio")]
                log_io_sets(&sets, stdio_index, &function_id)?;

                Ok(sets)
            }
            FunctionType::Composition(comp_info) => {
                self.queue_composition(
                    composition_id,
                    Composition::from_template(
                        &comp_info.composition,
                        self.any_sharding_mode.clone(),
                        &self.function_registry,
                    ),
                    input_sets,
                    caching,
                    recorder,
                    0
                )
                .await
            }
        }
    }

        /// Handles the outputs of one invocation of an expanding function. Without an expansion
    /// request the outputs are returned as they are (minus the `__expand` set). Otherwise the
    /// requested composition is run as nested composition and its outputs replace the ones of the
    /// invocation.
    async fn expand(
        &self,
        composition_id: usize,
        caching: bool,
        depth: usize,
        expansion: &ExpansionTemplate,
        mut outputs: ExecutionOutput,
        forwarded: Vec<DataSet>,
        recorder: Recorder,
    ) -> DandelionResult<ExecutionOutput> {
        // __expand is always the last output set of an expanding function
        let Some(expand_set) = outputs.pop() else {
            return Ok(outputs);
        };
        let target_item = match expand_set.as_slice() {
            // case: __expand set is empty -> the function does not expand. The normal function outputs are directly returned.
            [] => return Ok(outputs),
            // case: __expand contains directly one data item -> the function expands to the composition described by it.
            [item] => item.clone(),
            items => {
                return err_dandelion!(DandelionError::InvalidExpansion(format!(
                    "Function wrote {} items to __expand, at most one is allowed.",
                    items.len()
                )))
            }
        };
        if depth >= MAX_EXPANSION_DEPTH {
            return err_dandelion!(DandelionError::InvalidExpansion(format!(
                "Maximum expansion depth of {} exceeded.",
                MAX_EXPANSION_DEPTH
            )));
        }

        // read target composition from the __expand set entry
        let target_id: FunctionId =
            Arc::new(self.read_expansion_target(target_item, composition_id).await?);
        
        // check if target_id returned by the function at runtime is in allowed target compositions 
        // (the functions defined after the "into" part)
        if let Some(targets) = &expansion.targets {
            if !targets.contains(&target_id) {
                return err_dandelion!(DandelionError::InvalidExpansion(format!(
                    "Expansion target '{}' is not in the allowed targets {:?}.",
                    target_id, targets
                )));
            }
        }

        // extract composition from the registry by id
        let Ok(FunctionType::Composition(target)) = self.function_registry.get_function(&target_id)
        else {
            return err_dandelion!(DandelionError::InvalidExpansion(format!(
                "Expansion target '{}' is not a registered composition.",
                target_id
            )));
        };
        
        // the target takes the expands list and must produce the function's declared outputs
        if target.composition.num_params() != expansion.sources.len()
            || target.composition.num_returns() != outputs.len()
        {
            return err_dandelion!(DandelionError::InvalidExpansion(format!(
                "Expansion target '{}' takes {} and returns {} sets, but {} and {} are required.",
                target_id,
                target.composition.num_params(),
                target.composition.num_returns(),
                expansion.sources.len(),
                outputs.len()
            )));
        }

        let inputs = expansion
            .sources
            .iter()
            .map(|source| match source {
                ExpandSource::Output(idx) => DataSet::from_items(outputs[*idx].clone()),
                ExpandSource::Forwarded(idx) => forwarded[*idx].clone(),
            })
            .collect();
        let composition = Composition::from_template(
            &target.composition,
            self.any_sharding_mode.clone(),
            &self.function_registry,
        );
        let child_recorder = Recorder::new_from_parent(target_id, &recorder);
        // boxed, as queue_composition can lead back to expand
        Box::pin(self.queue_composition(
            composition_id,
            composition,
            inputs,
            caching,
            child_recorder,
            depth + 1,
        ))
        .await
    }

    /// Reads the name of the composition an invocation wants to expand into from its `__expand`
    /// item, fetching the item first if it is not local (e.g. the function ran on another node).
    async fn read_expansion_target(
        &self,
        item: DataItem,
        composition_id: usize,
    ) -> DandelionResult<String> {
        let item = if item.is_local() {
            item
        } else {
            let resolved = self
                .work_queue
                .enqueue(
                    WorkToDo::SetsToResolve {
                        input_sets: vec![DataSet::from_items(Arc::new(vec![item]))],
                    },
                    composition_id,
                )?
                .await?
                .get_data_items();
            match resolved.first().and_then(|set| set.first()) {
                Some(item) => item.clone(),
                None => {
                    return err_dandelion!(DandelionError::InvalidExpansion(
                        "Could not resolve the __expand item.".to_string()
                    ))
                }
            }
        };
        let local = item.local_ref()?;
        let mut buffer = vec![0u8; local.pos.size];
        local.ctx.read(local.pos.offset, &mut buffer)?;
        match String::from_utf8(buffer) {
            // tolerate trailing newlines, e.g. from writing the name with fprintf
            Ok(name) => Ok(name.trim().to_string()),
            Err(_) => err_dandelion!(DandelionError::InvalidExpansion(
                "Expansion target name is not valid UTF-8.".to_string()
            )),
        }
    }

}

#[cfg(feature = "log_function_stdio")]
fn log_io_sets(
    sets: &Vec<Arc<Vec<DataItem>>>,
    stdio_index: Option<usize>,
    function_id: &FunctionId,
) -> DandelionResult<()> {
    use memory::{context::ContextTrait, data::ItemData};

    if let Some(io_set) = stdio_index.and_then(|index| sets.get(index)) {
        for item in io_set.iter() {
            let local = match &item.data {
                ItemData::Local(local) => local,
                _ => {
                    debug!("Cannot print stdio data for non local items");
                    continue;
                }
            };
            if item.ident == "stderr" && local.pos.size > 0 {
                let mut stderr_output: Vec<u8> = vec![0; local.pos.size];
                local.ctx.read(local.pos.offset, &mut stderr_output)?;
                log::warn!(
                    "Function '{}' result contains stderr output:\n{}",
                    function_id,
                    std::str::from_utf8(stderr_output.as_slice()).expect("Invalid stderr buffer")
                );
            }
            if item.ident == "stdout" && local.pos.size > 0 {
                let mut stdout_output: Vec<u8> = vec![0; local.pos.size];
                local.ctx.read(local.pos.offset, &mut stdout_output)?;
                debug!(
                    "Function '{}' output:\n{}",
                    function_id,
                    std::str::from_utf8(stdout_output.as_slice()).expect("Invalid stdout buffer")
                );
            }
        }
    }
    Ok(())
}

use crate::{
    function_registry::{FunctionInfo, FunctionRegistry, FunctionType},
    meta_function::{input_metadata_set, output_metadata_set, read_composition},
    queue::{EngineQueue, WorkQueue},
    resource_pool::ResourcePool,
};
use composition::{AnyShardingMode, Composition, CompositionTemplate, Executor, MetaApplication};
use dandelion_commons::{
    err_dandelion,
    records::{RecordPoint, Recorder},
    DandelionError, DandelionResult, DispatcherError, FunctionId, FunctionRegistryError,
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
    context::{MemoryDomain, MemoryResource},
    data::Metadata,
    DataItem, DataSet,
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
    /// Metafunction context the composition runs in.
    meta: MetaContext,
}

/// Tracks how a composition is nested in metafunction plans, to bound their recursion.
#[derive(Clone, Copy, Debug, Default)]
struct MetaContext {
    /// Number of metafunction plans the composition is nested in, 0 for a top-level composition.
    depth: usize,
}

impl MetaContext {
    /// The context of the plan of a metafunction applied in this context.
    fn nested(self) -> Self {
        MetaContext {
            depth: self.depth + 1,
        }
    }
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
        inv_input_iter: T,
    ) {
        let dispatcher = self.dispatcher;
        let composition_id = self.composition_id;
        let caching = self.caching;
        let meta = self.meta;

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
                    // registered compositions do not add depth, but must not reset it either,
                    // otherwise wrapping a metafunction in one would bypass the limit
                    self.awaited.push(Box::pin(async move {
                        dispatcher
                            .queue_composition(
                                composition_id,
                                composition,
                                input,
                                caching,
                                recorder,
                                meta,
                            )
                            .await
                            .map(|items| (composition_idx, items))
                    }));
                }
            }
            FunctionType::MetaFunction(_) => {
                self.awaited
                    .push(Box::pin(future::ready(meta_invocation_error(&function_id))));
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
                // the recorders are needed again once the results arrive
                let mut recorders = Vec::new();
                let work_iter = inv_input_iter.into_iter().map(|input| {
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
                        for (promise, mut recorder) in promises.into_iter().zip(recorders) {
                            self.awaited.push(Box::pin(async move {
                                let result = promise.await;
                                recorder.record(RecordPoint::FutureReturn);
                                result.map(|work| (composition_idx, work.get_data_items()))
                            }));
                        }
                    }
                    // nothing was enqueued, report the error once the composition gets to it
                    Err(err) => self.awaited.push(Box::pin(future::ready(Err(err)))),
                }
            }
        };
    }

    fn enqueue_meta_invocations<T: IntoIterator<Item = Vec<DataSet>>>(
        &mut self,
        function_id: FunctionId,
        composition_idx: usize,
        application: Arc<MetaApplication>,
        inv_input_iter: T,
    ) {
        let dispatcher = self.dispatcher;
        let composition_id = self.composition_id;
        let caching = self.caching;

        let FunctionType::MetaFunction(planner) = dispatcher
            .function_registry
            .get_function(&function_id)
            .expect("Should never have a lookup for non registered function")
        else {
            self.awaited
                .push(Box::pin(future::ready(meta_invocation_error(&function_id))));
            return;
        };
        // checked before running any planner, so no engine time is spent on a plan that could
        // not be run anyway
        let meta = self.meta;
        if let Some(max_depth) = dispatcher.max_meta_depth {
            if meta.depth >= max_depth {
                self.awaited.push(Box::pin(future::ready(meta_depth_error(
                    &function_id,
                    max_depth,
                ))));
                return;
            }
        }
        for input in inv_input_iter {
            let recorder = self.new_child_recorder(&function_id, composition_idx);
            let function_id = function_id.clone();
            let planner = planner.clone();
            let application = application.clone();
            self.awaited.push(Box::pin(async move {
                dispatcher
                    .queue_meta_function(
                        composition_id,
                        function_id,
                        planner,
                        application,
                        input,
                        caching,
                        recorder,
                        meta,
                    )
                    .await
                    .map(|items| (composition_idx, items))
            }));
        }
    }
}

/// The error for applying a metafunction while already nested in the maximum number of plans.
fn meta_depth_error<T>(function_id: &FunctionId, max_depth: usize) -> DandelionResult<T> {
    err_dandelion!(DandelionError::FunctionRegistry(
        FunctionRegistryError::MetaRecursionLimit(format!(
            "Cannot apply metafunction {}: metafunction plans can be nested at most {} deep",
            function_id, max_depth
        )),
    ))
}

/// The error for invoking a function as metafunction that is none or vice versa.
fn meta_invocation_error<T>(function_id: &FunctionId) -> DandelionResult<T> {
    err_dandelion!(DandelionError::FunctionRegistry(
        FunctionRegistryError::InvalidMetaInvocation(format!(
            "{} can only be invoked as metafunction if it is one and only by a metafunction \
             application in a composition",
            function_id
        )),
    ))
}

// TODO: Here and in the registry replace Arc<Box<...>> with static references from leaked boxes for
//       things we expect to be there for the entire execution time anyway.
pub struct Dispatcher {
    function_registry: FunctionRegistry,
    work_queue: WorkQueue,
    domains: Vec<Arc<Box<dyn MemoryDomain>>>,
    any_sharding_mode: AnyShardingMode,
    /// Maximum number of metafunction plans a metafunction application can be nested in.
    /// None means unlimited, Some(0) disables metafunctions.
    max_meta_depth: Option<usize>,
    // TODO: The composition ids currently just go up from 0, there is no overflow handling.
    //       We are not expecting to overflow, but this should be handled for proper deployments.
    composition_id: AtomicUsize,
}

pub type ExecutionOutput = Vec<Arc<Vec<DataItem>>>;

impl Dispatcher {
    pub fn init(
        mut resource_pool: ResourcePool,
        memory_resources: BTreeMap<DomainType, MemoryResource>,
        work_queue: WorkQueue,
        any_sharding_mode: AnyShardingMode,
        max_meta_depth: Option<usize>,
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
            max_meta_depth,
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

    pub fn insert_meta_function(
        &self,
        function_name: String,
        engine_type: EngineType,
        ctx_size: usize,
        path: String,
        metadata: Metadata,
    ) -> DandelionResult<()> {
        let function_id = Arc::new(function_name);
        let domain_type = engine_type.get_domain_type();
        self.function_registry.insert_meta_function(
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
                MetaContext::default(),
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
        meta: MetaContext,
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
            meta,
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

    /// Runs a user defined function on an engine.
    async fn queue_compute_function(
        &self,
        composition_id: usize,
        function_id: FunctionId,
        func_info: FunctionInfo,
        input_sets: Vec<DataSet>,
        caching: bool,
        mut recorder: Recorder,
    ) -> DandelionResult<ExecutionOutput> {
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

    /// Runs a metafunction application: the planner on the planner inputs and the metadata of the
    /// data inputs and outputs, then the composition it returns on the data inputs.
    async fn queue_meta_function(
        &self,
        composition_id: usize,
        function_id: FunctionId,
        planner: FunctionInfo,
        application: Arc<MetaApplication>,
        mut input_sets: Vec<DataSet>,
        caching: bool,
        mut recorder: Recorder,
        meta: MetaContext,
    ) -> DandelionResult<ExecutionOutput> {
        let data_inputs = input_sets.split_off(application.num_planner_inputs);
        let mut planner_inputs = input_sets;
        planner_inputs.push(input_metadata_set(&application.input_names, &data_inputs)?);
        planner_inputs.push(output_metadata_set(&application.output_names)?);

        // planner is executed as normal function
        let planner_recorder = Recorder::new_from_parent(function_id.clone(), &recorder);
        let planner_output = self
            .queue_compute_function(
                composition_id,
                function_id.clone(),
                planner,
                planner_inputs,
                caching,
                planner_recorder.clone(),
            )
            .await?;

        // parse plan is a string of composition DSL
        // TODO: instead of using string and composition DSL, use own format to make it 
        // easier to write in SDK & save the parsing pass here
        let (plan_id, plan) = self.parse_plan(
            &function_id,
            &read_composition(&planner_output)?,
            &application,
        )?;
        debug!("Metafunction {} planned {}", function_id, plan_id);

        // the composition returned by the planner is executed normally via queue_composition
        let plan_recorder = Recorder::new_from_parent(plan_id, &recorder);
        let result = self
            .queue_composition(
                composition_id,
                Composition::from_template(
                    &plan,
                    self.any_sharding_mode.clone(),
                    &self.function_registry,
                ),
                data_inputs,
                caching,
                plan_recorder.clone(),
                meta.nested(),
            )
            .await;
        recorder.add_children(vec![
            Some(vec![planner_recorder]),
            Some(vec![plan_recorder]),
        ]);
        recorder.record(RecordPoint::FutureReturn);
        result
    }

    /// Parses the composition returned by the planner of a metafunction and checks that it takes
    /// the data inputs and produces the outputs of the application.
    fn parse_plan(
        &self,
        function_id: &FunctionId,
        plan: &str,
        application: &MetaApplication,
    ) -> DandelionResult<(FunctionId, CompositionTemplate)> {
        let invalid_plan = |reason: String| {
            err_dandelion!(DandelionError::FunctionRegistry(
                FunctionRegistryError::InvalidPlan(format!(
                    "Composition returned by the planner of {} {}",
                    function_id, reason
                )),
            ))
        };
        let mut templates = match CompositionTemplate::parse(plan, &self.function_registry) {
            Ok(templates) => templates,
            Err(err) => return invalid_plan(format!("failed to parse: {}", err.error)),
        };
        let num_compositions = templates.len();
        let (plan_id, template, metadata) = match templates.pop() {
            Some(parsed) if num_compositions == 1 => parsed,
            _ => {
                return invalid_plan(format!(
                    "has to contain exactly one composition, found {}",
                    num_compositions
                ))
            }
        };
        if metadata.input_sets.len() != application.input_names.len()
            || metadata.output_sets.len() != application.output_names.len()
        {
            return invalid_plan(format!(
                "takes {} input sets and returns {} output sets instead of {} and {}",
                metadata.input_sets.len(),
                metadata.output_sets.len(),
                application.input_names.len(),
                application.output_names.len()
            ));
        }
        Ok((plan_id, template))
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
    ) -> DandelionResult<ExecutionOutput> {
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
                self.queue_compute_function(
                    composition_id,
                    function_id,
                    func_info,
                    input_sets,
                    caching,
                    recorder,
                )
                .await
            }
            // cannot directly run a MetaFunction. To run a metafunction, there needs to be a MetaFunctionApplication.
            // This can currently only happen in a composition.
            FunctionType::MetaFunction(_) => meta_invocation_error(&function_id),
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
                    // invoked directly, not from within a composition
                    MetaContext::default(),
                )
                .await
            }
        }
    }
}

#[cfg(feature = "log_function_stdio")]
fn log_io_sets(
    sets: &ExecutionOutput,
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

use crate::{
    function_registry::{FunctionRegistry, FunctionType},
    queue::{EngineQueue, WorkQueue},
    resource_pool::ResourcePool,
};
use core::pin::Pin;
use dandelion_commons::{
    err_dandelion,
    records::{RecordPoint, Recorder},
    DandelionError, DandelionResult, DispatcherError, FunctionId,
};
use futures::{
    future::{join_all, ready, Either},
    stream::{FuturesUnordered, StreamExt},
    Future,
};
use itertools::Itertools;
#[cfg(feature = "log_function_stdio")]
use log::warn;
use log::{debug, trace};
#[cfg(feature = "log_function_stdio")]
use machine_interface::memory_domain::ContextTrait;
use machine_interface::{
    composition::{
        get_sharding, Composition, CompositionSet, InputSetDescriptor, JoinStrategy, ShardingMode,
    },
    function_driver::{Metadata, WorkToDo},
    machine_config::{get_available_domains, DomainType, EngineType, IntoEnumIterator},
    memory_domain::{MemoryDomain, MemoryResource},
};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Debug, Clone)]
pub enum DispatcherInput {
    None,
    Set(CompositionSet),
}

const MAX_QUEUE: usize = 4096;

// TODO also here and in registry replace Arc Box with static references from leaked boxes for things we expect to be there for
// the entire execution time anyway
pub struct Dispatcher {
    function_registry: FunctionRegistry,
    work_queue: WorkQueue,
    domains: Vec<Arc<Box<dyn MemoryDomain>>>,
}

impl Dispatcher {
    pub fn init(
        mut resource_pool: ResourcePool,
        memory_resources: BTreeMap<DomainType, MemoryResource>,
    ) -> DandelionResult<Dispatcher> {
        // get machine specific configurations
        let domains = get_available_domains(memory_resources);

        // TODO: get size from config?
        let work_queue = WorkQueue::init(MAX_QUEUE);

        // create an engine queue wrapper of the work queue for each engine and use up all engine resource available
        for engine_type in EngineType::iter() {
            let engine_queue = Box::new(EngineQueue::init(work_queue.clone(), engine_type));
            let driver = engine_type.get_driver();
            while let Ok(Some(resource)) = resource_pool.sync_acquire_engine_resource(engine_type) {
                driver.start_engine(resource, engine_queue.clone())?;
            }
        }

        // create the function registry
        let function_registry = FunctionRegistry::new(&domains);

        return Ok(Dispatcher {
            function_registry,
            work_queue,
            domains,
        });
    }

    pub fn get_work_queue(&self) -> WorkQueue {
        self.work_queue.clone()
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
        self.function_registry
            .insert_compositions(&composition_desc)
    }

    pub async fn queue_function_by_name(
        &self,
        function_id: Arc<String>,
        inputs: Vec<DispatcherInput>,
        caching: bool,
        mut recorder: Recorder,
    ) -> DandelionResult<(Vec<Option<CompositionSet>>, Recorder)> {
        debug!("Queuing function {}", function_id);
        recorder.record(RecordPoint::EnterDispatcher);

        let mut input_vec = Vec::with_capacity(inputs.len());
        input_vec.resize(inputs.len(), None);

        for (index, input) in inputs.into_iter().enumerate() {
            match input {
                DispatcherInput::None => (),
                DispatcherInput::Set(set) => {
                    input_vec[index] = Some(set);
                }
            }
        }

        let results = self
            .queue_function(function_id, input_vec, caching, recorder.get_sub_recorder())
            .await?;

        return Ok((results, recorder));
    }

    pub async fn queue_unregistered_composition(
        &self,
        composition_desc: String,
        inputs: Vec<DispatcherInput>,
        caching: bool,
        recorder: Recorder,
    ) -> DandelionResult<(Vec<Option<CompositionSet>>, Recorder)> {
        debug!("Parsing single use composition");
        let composition_meta_pairs = self
            .function_registry
            .parse_compositions(&composition_desc.as_str())?;
        if composition_meta_pairs.len() != 1 {
            debug!(
                "Expected exactly one composition got {}",
                composition_meta_pairs.len()
            );
            return err_dandelion!(DandelionError::Dispatcher(
                DispatcherError::InvalidComposition,
            ));
        }

        debug!(
            "Queuing single use composition {}",
            composition_meta_pairs[0].0
        );
        let mut input_vec = Vec::with_capacity(inputs.len());
        input_vec.resize(inputs.len(), None);
        for (index, input) in inputs.into_iter().enumerate() {
            match input {
                DispatcherInput::None => (),
                DispatcherInput::Set(set) => {
                    input_vec[index] = Some(set);
                }
            }
        }
        let results = self
            .queue_composition(
                composition_meta_pairs[0].1.clone(),
                input_vec,
                caching,
                recorder.get_sub_recorder(),
            )
            .await?;

        return Ok((results, recorder));
    }

    /// Queue a composition for execution.
    /// Returns a Vec of sets with the index corresponding to the set index in the composition definition
    ///
    /// # Arguments
    ///
    /// * `composition`
    /// * `inputs` vec of input set options, where the index in the vec is the input set number
    pub async fn queue_composition(
        &self,
        composition: Composition,
        inputs: Vec<Option<CompositionSet>>,
        caching: bool,
        mut recorder: Recorder,
    ) -> DandelionResult<Vec<Option<CompositionSet>>> {
        // build up ready sets
        trace!("queue composition");
        // local state of functions that still need to run
        // TODO: make nicer data structure to hold there and the compositions
        // Can use ? Sized, to create structs with fixed arrays where the size is known at runtime.
        // And potentially Rc / RefCell to make a proper graph
        struct FunctionArgs {
            function_id: FunctionId,
            inptut_sets: Vec<Option<(ShardingMode, CompositionSet)>>,
            join_info: (Vec<usize>, Vec<JoinStrategy>),
            output_mapping: Vec<Option<usize>>,
            missing_sets: BTreeMap<(usize, usize), (ShardingMode, bool)>,
        }

        // prepare output sets, that are also input sets
        let output_number = composition.output_map.len();
        let mut output_sets = Vec::with_capacity(output_number);
        output_sets.resize(output_number, None);
        for (input_index, input_set) in inputs.iter().enumerate() {
            if let Some(out_index) = composition.output_map.get(&input_index) {
                output_sets[*out_index] = input_set.clone();
            }
        }

        let mut awaited_sets = FuturesUnordered::new();

        let (ready_functions, mut non_ready_functions): (Vec<_>, Vec<_>) = composition
            .dependencies
            .into_iter()
            .filter_map(|deps| {
                let mut missing_map = BTreeMap::new();
                let input_set_number: usize = deps.input_set_ids.len();
                let mut ready_inputs = Vec::with_capacity(input_set_number);
                ready_inputs.resize(deps.input_set_ids.len(), None);
                for (function_index, in_set_decriptor) in deps.input_set_ids.iter().enumerate() {
                    if let Some(InputSetDescriptor {
                        composition_id,
                        sharding,
                        optional,
                    }) = in_set_decriptor
                    {
                        if let Some(comp_set) = inputs.get(*composition_id) {
                            // this means the a non optional set is empty, so we can skip it and directly queue all outputs are ready none
                            if !*optional
                                && (comp_set.is_none() || comp_set.as_ref().unwrap().is_empty())
                            {
                                let new_sets = deps
                                    .output_set_ids
                                    .iter()
                                    .filter_map(|index_opt| {
                                        index_opt.and_then(|index| Some((index, None)))
                                    })
                                    .collect();
                                awaited_sets.push(Either::Left(ready(Ok((new_sets, Vec::new())))));
                                return None;
                            }
                            if let Some(set) = comp_set {
                                ready_inputs[function_index] = Some((*sharding, set.clone()));
                            }
                        } else {
                            missing_map
                                .insert((*composition_id, function_index), (*sharding, *optional));
                        }
                    }
                }
                Some(FunctionArgs {
                    function_id: deps.function,
                    inptut_sets: ready_inputs,
                    join_info: deps.join_info,
                    output_mapping: deps.output_set_ids,
                    missing_sets: missing_map,
                })
            })
            .partition(|args| args.missing_sets.is_empty());

        // start all functions that are ready and insert their sets into the awaited ones
        for args in ready_functions.into_iter() {
            awaited_sets.push(Either::Right(self.queue_function_sharded(
                args.function_id,
                args.inptut_sets,
                args.join_info.0,
                args.join_info.1,
                args.output_mapping,
                caching,
                recorder.get_sub_recorder(),
            )));
        }
        let num_running_functions = awaited_sets.len();

        trace!(
            "functions ready: {}, functions not ready: {}",
            num_running_functions,
            non_ready_functions.len()
        );
        while let Some(new_compositions_result) = awaited_sets.next().await {
            let (new_compositions, new_recorders) = new_compositions_result?;
            recorder.add_children(new_recorders);
            for (composition_set_index, composition_set_option) in &new_compositions {
                trace!(
                    "composition set {:?} arrived at dispatcher is some: {}",
                    composition_set_index,
                    composition_set_option.is_some()
                );
                if let Some(output_index) = composition.output_map.get(&composition_set_index) {
                    output_sets[*output_index] = composition_set_option.clone();
                }
                non_ready_functions = non_ready_functions
                    .into_iter()
                    .filter_map(|mut args| {
                        let to_remove = args
                            .missing_sets
                            .range(
                                (*composition_set_index, 0)..(*composition_set_index, (usize::MAX)),
                            )
                            .map(|((comp_index, function_index), (mode, optional))| {
                                // if it was not optional skip executing and push all output sets
                                if !optional
                                    && (composition_set_option.is_none()
                                        || composition_set_option.as_ref().unwrap().is_empty())
                                {
                                    let new_sets = args
                                        .output_mapping
                                        .iter()
                                        .filter_map(|index_opt| {
                                            index_opt.and_then(|index| Some((index, None)))
                                        })
                                        .collect();
                                    awaited_sets
                                        .push(Either::Left(ready(Ok((new_sets, Vec::new())))));
                                    None
                                } else {
                                    args.inptut_sets[*function_index] = composition_set_option
                                        .clone()
                                        .and_then(|set| Some((*mode, set)));
                                    Some((*comp_index, *function_index))
                                }
                            })
                            .collect::<Vec<_>>();
                        // need to cancel the function if one of the non optional sets is empty
                        for key_opt in to_remove {
                            if let Some(key) = key_opt {
                                args.missing_sets.remove(&key);
                            } else {
                                return None;
                            }
                        }
                        if args.missing_sets.is_empty() {
                            awaited_sets.push(Either::Right(self.queue_function_sharded(
                                args.function_id,
                                args.inptut_sets,
                                args.join_info.0,
                                args.join_info.1,
                                args.output_mapping,
                                caching,
                                recorder.get_sub_recorder(),
                            )));
                            None
                        } else {
                            Some(args)
                        }
                    })
                    .collect();
            }
            trace!(
                "waiting for {} sets (running functions and global sets), functions not ready: {}",
                awaited_sets.len(),
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
        join_order: Vec<usize>,
        join_strategies: Vec<JoinStrategy>,
        output_mapping: Vec<Option<usize>>,
        caching: bool,
        recorder: Recorder,
    ) -> DandelionResult<(Vec<(usize, Option<CompositionSet>)>, Vec<Recorder>)> {
        trace!(
            "queue function {} sharded and input sets: {:?}",
            function_id,
            input_sets
        );
        let mut recorders;

        // check if there are no input sets or all of them are none, then don't need sharding,
        // but still want to run if we queued it.
        let is_sharded = input_sets.len() != 0
            && input_sets
                .iter()
                .any(|opt| opt.is_some() && !opt.as_ref().unwrap().1.is_empty());
        let composition_results: DandelionResult<Vec<_>> = if is_sharded {
            let sharded = get_sharding(input_sets, join_order, join_strategies);
            let size_hint = sharded.len();
            recorders = Vec::with_capacity(size_hint);
            let resutls: Vec<_> = sharded
                .into_iter()
                .map(|ins| {
                    let new_recorder = Recorder::new_from_parent(function_id.clone(), &recorder);
                    let future_box = Box::pin(self.queue_function(
                        function_id.clone(),
                        ins,
                        caching,
                        new_recorder.get_sub_recorder(),
                    ));
                    recorders.push(new_recorder);
                    future_box
                })
                .collect();
            join_all(resutls).await.into_iter().collect()
            // TODO this is added to support functions with all functions defined as static sets
            // might want to differentiate between those that have static sets and those that did not get input from predecessors
        } else {
            let new_recorder = Recorder::new_from_parent(function_id.clone(), &recorder);
            let future_box = self
                .queue_function(
                    function_id,
                    vec![],
                    caching,
                    new_recorder.get_sub_recorder(),
                )
                .await
                .and_then(|result| Ok(vec![result]));
            recorders = vec![new_recorder];
            future_box
        };

        // collect vec of vec of shards into vec of sets
        // TODO: collect vector of sets and rewrite combine to do it in one go instead of calling it over and over again
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
                        (Some(old_set), Some(new_set)) => {
                            old_set
                                .combine(new_set)
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
        Ok((
            output_mapping
                .into_iter()
                .zip(composition_set_vecs.into_iter())
                .filter_map(|(index_option, set_option)| {
                    index_option.and_then(|index| Some((index, set_option)))
                })
                .collect(),
            recorders,
        ))
    }

    /// returns a vector of pairs of a index and a composition set
    /// the index describes which output set the composition belongs to.
    pub fn queue_function<'dispatcher>(
        &'dispatcher self,
        function_id: FunctionId,
        input_sets: Vec<Option<CompositionSet>>,
        caching: bool,
        mut recorder: Recorder,
    ) -> Pin<
        Box<dyn Future<Output = DandelionResult<Vec<Option<CompositionSet>>>> + 'dispatcher + Send>,
    > {
        debug!("Queueing function with id: {}", function_id);
        Box::pin(async move {
            // find an engine capable of running the function
            // TODO: think about more distinctions, that allow pushing chains of functions which can be executed by single engine,
            // or potentially or potentially even compositions that still need to be split.
            match self.function_registry.get_function(&function_id)? {
                FunctionType::SystemFunction(func_info) | FunctionType::Function(func_info) => {
                    let function_alternatives = func_info
                        .alternatives
                        .read()
                        .expect("Function registry lock is poisoned!")
                        .clone();

                    let metadata = func_info.metadata;
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

                    let subrecoder = recorder.get_sub_recorder();
                    let args = WorkToDo::FunctionArguments {
                        function_id,
                        function_alternatives,
                        input_sets,
                        metadata,
                        caching,
                        recorder: subrecoder,
                    };
                    recorder.record(RecordPoint::ExecutionQueue);
                    let context = self.work_queue.do_work(args).await?.get_context();
                    recorder.record(RecordPoint::FutureReturn);

                    #[cfg(feature = "log_function_stdio")]
                    for opt in context.content.iter() {
                        if opt.as_ref().is_some_and(|s| s.ident == "stdio") {
                            for itm in opt.as_ref().unwrap().buffers.iter() {
                                if itm.ident == "stderr" && itm.data.size > 0 {
                                    let mut stderr_output: Vec<u8> = vec![0; itm.data.size];
                                    context.context.read(itm.data.offset, &mut stderr_output)?;
                                    warn!(
                                        "Function result contains stderr output:\n{}",
                                        std::str::from_utf8(stderr_output.as_slice())
                                            .expect("Invalid stderr buffer")
                                    );
                                }
                                if itm.ident == "stdout" && itm.data.size > 0 {
                                    let mut stdout_output: Vec<u8> = vec![0; itm.data.size];
                                    context.context.read(itm.data.offset, &mut stdout_output)?;
                                    debug!(
                                        "Function output:\n{}",
                                        std::str::from_utf8(stdout_output.as_slice())
                                            .expect("Invalid stdout buffer")
                                    );
                                }
                            }
                        }
                    }

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
                FunctionType::Composition(comp_info) => {
                    return self
                        .queue_composition(
                            (*comp_info.composition).clone(),
                            input_sets,
                            caching,
                            recorder,
                        )
                        .await;
                }
            };
        })
    }
}

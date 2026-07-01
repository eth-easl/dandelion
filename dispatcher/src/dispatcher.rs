use crate::{
    function_registry::{FunctionRegistry, FunctionType},
    queue::{EngineQueue, WorkQueue},
    resource_pool::ResourcePool,
};
use dandelion_commons::{
    err_dandelion,
    records::{RecordPoint, Recorder},
    DandelionError, DandelionResult, DispatcherError, FunctionId,
};
use futures::{
    future::{join_all, ready, Either},
    stream::{FuturesUnordered, StreamExt},
};
use itertools::Itertools;
#[cfg(feature = "log_function_stdio")]
use log::warn;
use log::{debug, trace};
#[cfg(feature = "log_function_stdio")]
use machine_interface::memory_domain::ContextTrait;
use machine_interface::{
    composition::{
        get_sharding, AnyShardingMode, Composition, CompositionSet, InputSetDescriptor,
        JoinStrategy, LocalCompositionSet, RemoteData, ShardingMode,
    },
    function_driver::{Metadata, WorkToDo},
    machine_config::{get_available_domains, DomainType, EngineType, IntoEnumIterator},
    memory_domain::{MemoryDomain, MemoryResource},
};
use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
    sync::Arc,
};

pub type RecoveredNodeOutputs = Arc<HashMap<usize, CompositionSet>>;

// TODO also here and in registry replace Arc Box with static references from leaked boxes for things we expect to be there for
// the entire execution time anyway
pub struct Dispatcher {
    function_registry: FunctionRegistry,
    work_queue: WorkQueue,
    domains: Vec<Arc<Box<dyn MemoryDomain>>>,
    any_sharding_mode: AnyShardingMode,
}

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
                work_queue.add_local_cores(1);
            }
        }

        // create the function registry
        let function_registry = FunctionRegistry::new();

        return Ok(Dispatcher {
            function_registry,
            work_queue,
            domains,
            any_sharding_mode,
        });
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

    pub fn load_or_enable_registry_persistence(
        &self,
        path: PathBuf,
    ) -> DandelionResult<(usize, usize)> {
        self.function_registry
            .load_or_enable_persistence(path, &self.domains)
    }

    pub async fn queue_function_by_name(
        &self,
        function_id: Arc<String>,
        inputs: Vec<Option<CompositionSet>>,
        caching: bool,
        mut recorder: Recorder,
        recovered_nodes: Option<RecoveredNodeOutputs>,
    ) -> DandelionResult<(Vec<Option<LocalCompositionSet>>, Recorder)> {
        debug!("Queuing function {}", function_id);
        recorder.record(RecordPoint::EnterDispatcher);

        let results = self
            .queue_function(
                function_id,
                inputs,
                caching,
                recorder.clone(),
                None,
                None,
                recovered_nodes,
            )
            .await?;

        // if any set is not local send them trough reference resolution
        let resolved_sets = if !(&results).into_iter().all(|set_option| {
            if let Some(set) = set_option {
                set.is_local()
            } else {
                true
            }
        }) {
            self.work_queue
                .do_work(WorkToDo::SetsToResolve {
                    input_sets: results,
                })
                .await?
                .get_composition()
        } else {
            results
        };

        let local_results = resolved_sets
            .into_iter()
            .map(|set_opt| set_opt.map(|set| set.into_local()))
            .collect();

        Ok((local_results, recorder))
    }

    pub async fn delete_remote_data(&self, remote_data: RemoteData) -> DandelionResult<()> {
        self.work_queue
            .do_work(WorkToDo::RemoteToDelete { remote_data })
            .await
            .map(|_| ())
    }

    pub async fn queue_unregistered_composition(
        &self,
        composition_desc: String,
        inputs: Vec<Option<CompositionSet>>,
        caching: bool,
        recorder: Recorder,
        recovered_nodes: Option<RecoveredNodeOutputs>,
    ) -> DandelionResult<(Vec<Option<LocalCompositionSet>>, Recorder)> {
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
        let results = self
            .queue_composition(
                composition_meta_pairs[0].1.clone(),
                None,
                inputs,
                caching,
                recorder.clone(),
                recovered_nodes,
            )
            .await?;

        // if any set is not local send them trough reference resolution
        let resolved_sets = if !(&results).into_iter().all(|set_option| {
            if let Some(set) = set_option {
                set.is_local()
            } else {
                true
            }
        }) {
            self.work_queue
                .do_work(WorkToDo::SetsToResolve {
                    input_sets: results,
                })
                .await?
                .get_composition()
        } else {
            results
        };
        let local_results = resolved_sets
            .into_iter()
            .map(|set_opt| set_opt.map(|set| set.into_local()))
            .collect();

        Ok((local_results, recorder))
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
        _composition_node_ids: Option<Arc<Vec<String>>>,
        inputs: Vec<Option<CompositionSet>>,
        caching: bool,
        mut recorder: Recorder,
        recovered_nodes: Option<RecoveredNodeOutputs>,
    ) -> DandelionResult<Vec<Option<CompositionSet>>> {
        // build up ready sets
        trace!("queue composition");
        // local state of functions that still need to run
        // TODO: make nicer data structure to hold there and the compositions
        // Can use ? Sized, to create structs with fixed arrays where the size is known at runtime.
        // And potentially Rc / RefCell to make a proper graph
        struct FunctionArgs {
            function_id: FunctionId,
            // Which index the function had in the function dependencies array.
            // used to differentiate between multiple functions with same ID in one invocation for debugging and tracing.
            function_index: usize,
            composition_node_id: Option<String>,
            input_sets: Vec<Option<(ShardingMode, CompositionSet)>>,
            join_info: (Vec<usize>, Vec<JoinStrategy>),
            output_mapping: Vec<Option<usize>>,
            missing_sets: BTreeMap<(usize, usize), (ShardingMode, bool)>,
        }

        let mut recorders = Vec::with_capacity(composition.dependencies.len());
        recorders.resize(composition.dependencies.len(), None);

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
            .enumerate()
            .filter_map(|(composition_index, deps)| {
                // Recovery is keyed by composition output set id. If every output set
                // produced by this node is already present, we can feed those recovered sets
                // back into the scheduler as if the node had just finished.
                let recovered_sets = deps
                    .output_set_ids
                    .iter()
                    .filter_map(|index_opt| {
                        index_opt.and_then(|index| {
                            recovered_nodes
                                .as_ref()
                                .and_then(|sets| sets.get(&index))
                                .cloned()
                                .map(|set| (index, Some(set)))
                        })
                    })
                    .collect::<Vec<_>>();
                let is_system_function = machine_interface::function_driver::system_driver::SYSTEM_FUNCTIONS
                    .iter()
                    .any(|system_function| deps.function.as_str() == Into::<&str>::into(system_function));
                let recovered_all_outputs = deps
                    .output_set_ids
                    .iter()
                    .filter_map(|index_opt| *index_opt)
                    .all(|index| {
                        recovered_nodes
                            .as_ref()
                            .is_some_and(|sets| sets.contains_key(&index))
                    });
                // Batched HTTP nodes can be only partially recovered item-by-item, so they still
                // go through the deferred IO path. For a single request item we can skip the node
                // entirely once both composition-visible outputs are present.
                let can_skip_system_function_dispatch = is_system_function
                    && deps
                        .input_set_ids
                        .iter()
                        .flatten()
                        .all(|descriptor| {
                            inputs
                                .get(descriptor.composition_id)
                                .and_then(|set| set.as_ref())
                                .is_some_and(|set| set.len() <= 1)
                        });
                if recovered_all_outputs && (!is_system_function || can_skip_system_function_dispatch) {
                    awaited_sets.push(Either::Left(ready(Ok((
                        recovered_sets,
                        composition_index,
                        Vec::new(),
                    )))));
                    return None;
                }
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
                            // this means the a non optional set None, so we can skip it and directly queue all outputs are ready None
                            if !*optional && comp_set.is_none() {
                                let new_sets = deps
                                    .output_set_ids
                                    .iter()
                                    .filter_map(|index_opt| {
                                        index_opt.and_then(|index| Some((index, None)))
                                    })
                                    .collect();
                                awaited_sets.push(Either::Left(ready(Ok((
                                    new_sets,
                                    function_index,
                                    Vec::new(),
                                )))));
                                return None;
                            }
                            if let Some(set) = comp_set {
                                debug_assert_ne!(
                                    0,
                                    set.len(),
                                    "Expect sets that are some to have at least one item"
                                );
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
                    function_index: composition_index,
                    composition_node_id: None,
                    input_sets: ready_inputs,
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
                args.function_index,
                args.composition_node_id,
                args.input_sets,
                args.join_info.0,
                args.join_info.1,
                args.output_mapping,
                caching,
                recorder.clone(),
                recovered_nodes.clone(),
            )));
        }
        let num_running_functions = awaited_sets.len();

        trace!(
            "functions ready: {}, functions not ready: {}",
            num_running_functions,
            non_ready_functions.len()
        );
        while let Some(new_compositions_result) = awaited_sets.next().await {
            let (new_compositions, function_index, new_recorders) = new_compositions_result?;
            recorders[function_index] = Some(new_recorders);
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
                                // TODO: for left, right and outer joins, some sets may also be pseudo optional.
                                // (i.e. an empty left set on a right join can still have functions that should run)
                                // Fix either by adding attributes to easily check here or move to check optional together with sharding.
                                if !optional && composition_set_option.is_none() {
                                    let new_sets = args
                                        .output_mapping
                                        .iter()
                                        .filter_map(|index_opt| {
                                            index_opt.and_then(|index| Some((index, None)))
                                        })
                                        .collect();
                                    awaited_sets.push(Either::Left(ready(Ok((
                                        new_sets,
                                        *function_index,
                                        Vec::new(),
                                    )))));
                                    None
                                } else {
                                    args.input_sets[*function_index] =
                                        composition_set_option.clone().and_then(|set| {
                                            debug_assert_ne!(
                                                0,
                                                set.len(),
                                                "Expect at least 1 item in composition set"
                                            );
                                            Some((*mode, set))
                                        });
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
                                args.function_index,
                                args.composition_node_id,
                                args.input_sets,
                                args.join_info.0,
                                args.join_info.1,
                                args.output_mapping,
                                caching,
                                recorder.clone(),
                                recovered_nodes.clone(),
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

        recorder.add_children(recorders);

        return Ok(output_sets);
    }

    /// Adapter between compositions and functions
    /// Keeps track of the composition set indexes so that when sets are returned to
    /// composition they have the corret index associated without the composition needing to track them.
    /// Also handles sharing of sets
    async fn queue_function_sharded<'context>(
        &self,
        function_id: FunctionId,
        // index of the function within the composition
        function_index: usize,
        composition_node_id: Option<String>,
        input_sets: Vec<Option<(ShardingMode, CompositionSet)>>,
        join_order: Vec<usize>,
        join_strategies: Vec<JoinStrategy>,
        output_mapping: Vec<Option<usize>>,
        caching: bool,
        recorder: Recorder,
        recovered_nodes: Option<RecoveredNodeOutputs>,
    ) -> DandelionResult<(Vec<(usize, Option<CompositionSet>)>, usize, Vec<Recorder>)> {
        trace!(
            "queue function {} sharded and input sets: {:?}",
            function_id,
            input_sets
        );
        let mut recorders;

        // check if there are no input sets or all of them are none, then don't need sharding,
        // but still want to run if we queued it.
        let is_sharded = input_sets.len() != 0 && input_sets.iter().any(|opt| opt.is_some());
        let composition_results: DandelionResult<Vec<_>> = if is_sharded {
            let min_set_bytes = self.function_registry.get_min_set_bytes(&function_id)?;
            let sharded = get_sharding(
                input_sets,
                join_order,
                join_strategies,
                &self.any_sharding_mode,
                min_set_bytes,
            );
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
                        new_recorder.clone(),
                        composition_node_id.clone(),
                        Some(output_mapping.clone()),
                        recovered_nodes.clone(),
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
                    new_recorder.clone(),
                    composition_node_id,
                    Some(output_mapping.clone()),
                    recovered_nodes,
                )
                .await
                .and_then(|result| Ok(vec![result]));
            recorders = vec![new_recorder];
            future_box
        };

        // collect vec of vec of shards into vec of sets
        let mut composition_set_vecs = Vec::with_capacity(output_mapping.len());
        composition_set_vecs.resize(output_mapping.len(), Vec::new());
        let mut set_names = Vec::with_capacity(output_mapping.len());
        set_names.resize(output_mapping.len(), None);
        for sets in composition_results? {
            for (index, set_option) in sets.into_iter().enumerate() {
                if let Some(set) = set_option {
                    set_names[index].get_or_insert_with(|| set.get_name().clone());
                    composition_set_vecs[index].extend(set.into_iter());
                }
            }
        }
        // assiociated sets with composition ids
        Ok((
            output_mapping
                .into_iter()
                .zip(composition_set_vecs.into_iter())
                .zip(set_names)
                .filter_map(|((index_option, sets), name)| {
                    index_option.map(|index| {
                        (
                            index,
                            name.and_then(|name| CompositionSet::from_item_list(name, sets)),
                        )
                    })
                })
                .collect(),
            function_index,
            recorders,
        ))
    }

    /// returns a vector of pairs of a index and a composition set
    /// the index describes which output set the composition belongs to.
    pub async fn queue_function<'dispatcher>(
        &'dispatcher self,
        function_id: FunctionId,
        input_sets: Vec<Option<CompositionSet>>,
        caching: bool,
        mut recorder: Recorder,
        _composition_node_id: Option<String>,
        composition_output_set_ids: Option<Vec<Option<usize>>>,
        recovered_nodes: Option<RecoveredNodeOutputs>,
    ) -> DandelionResult<Vec<Option<CompositionSet>>> {
        debug!("Queueing function with id: {}", function_id);
        // find an engine capable of running the function
        match self.function_registry.get_function(&function_id)? {
            // Defer actual execution of system functions (i.e. fetching),
            // by calling the system function to produce a composition set containing the reference to be resolved later
            FunctionType::SystemFunction(sys_function) => {
                machine_interface::function_driver::system_driver::convert_to_references(
                    sys_function,
                    recorder.invocation_id(),
                    composition_output_set_ids,
                    input_sets,
                )
            }
            FunctionType::Function(func_info) => {
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
                #[cfg(feature = "timestamp")]
                {
                    let (total_items, total_size) =
                        input_sets.iter().fold((0, 0), |(number, size), set| {
                            set.as_ref()
                                .map(|set| (number + set.len(), size + set.size()))
                                .unwrap_or((number, size))
                        });
                    recorder.record_input(total_items as u64, total_size as u64);
                }
                let args = WorkToDo::FunctionArguments {
                    invocation_id: recorder.invocation_id(),
                    function_id: function_id.clone(),
                    function_alternatives,
                    input_sets,
                    metadata,
                    caching,
                    recorder: recorder.clone(),
                };

                let sets = self.work_queue.do_work(args).await?.get_composition();
                recorder.record(RecordPoint::FutureReturn);

                #[cfg(feature = "log_function_stdio")]
                if let Some(io_set) = sets
                    .iter()
                    .filter_map(|set_option| set_option.as_ref())
                    .find(|set| set.get_name() == "stdio")
                {
                    for (item, data) in io_set {
                        use machine_interface::composition::ItemData;
                        let context = match data {
                            ItemData::LocalData(context) => context,
                            _ => {
                                debug!("Cannot print stdio data for non local items");
                                continue;
                            }
                        };
                        if item.ident == "stderr" && item.data.size > 0 {
                            let mut stderr_output: Vec<u8> = vec![0; item.data.size];
                            context.context.read(item.data.offset, &mut stderr_output)?;
                            warn!(
                                "Function '{}' result contains stderr output:\n{}",
                                function_id,
                                std::str::from_utf8(stderr_output.as_slice())
                                    .expect("Invalid stderr buffer")
                            );
                        }
                        if item.ident == "stdout" && item.data.size > 0 {
                            let mut stdout_output: Vec<u8> = vec![0; item.data.size];
                            context.context.read(item.data.offset, &mut stdout_output)?;
                            debug!(
                                "Function '{}' output:\n{}",
                                function_id,
                                std::str::from_utf8(stdout_output.as_slice())
                                    .expect("Invalid stdout buffer")
                            );
                        }
                    }
                }

                Ok(sets)
            }
            FunctionType::Composition(comp_info) => {
                self.queue_composition(
                    (*comp_info.composition).clone(),
                    Some(comp_info.composition_node_ids.clone()),
                    input_sets,
                    caching,
                    recorder,
                    recovered_nodes,
                )
                .await
            }
        }
    }
}

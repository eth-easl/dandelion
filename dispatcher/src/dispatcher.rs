use crate::{
    function_registry::{FunctionRegistry, FunctionType},
    queue::{EngineQueue, WorkQueue},
    resource_pool::ResourcePool,
};
use composition::{AnyShardingMode, Composition, CompositionTemplate, Invocation};
use dandelion_commons::{
    err_dandelion,
    records::{RecordPoint, Recorder},
    DandelionError, DandelionResult, DispatcherError, FunctionId,
};
use futures::{
    future::Either,
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
    sync::{atomic::AtomicUsize, Arc},
};

struct QueueingStruct<'dispatcher> {
    registry: &'dispatcher FunctionRegistry,
    function_invocations: Vec<(usize, WorkToDo)>,
    to_convert: Vec<Invocation>,
    compositons: Vec<Invocation>,
    // new_recorders: Vec<Recorder>,
    caching: bool,
    recorder: &'dispatcher Recorder,
}

impl<'dispatcher> Extend<Invocation> for QueueingStruct<'dispatcher> {
    // TODO: this assumes that a single extend invocation is all for the same function, verify.
    fn extend<T: IntoIterator<Item = Invocation>>(&mut self, iter: T) {
        let mut iter = iter.into_iter();
        if let Some(invocation) = iter.next() {
            match self
                .registry
                .get_function(&invocation.function_id)
                .expect("Should never have a lookup for non registered function")
            {
                FunctionType::Composition(_) => {
                    self.compositons.push(invocation);
                    self.compositons.extend(iter);
                }
                FunctionType::SystemFunction(_) => {
                    self.to_convert.push(invocation);
                    self.to_convert.extend(iter);
                }
                FunctionType::Function(info) => {
                    // TODO fix recorders
                    let recorder =
                        Recorder::new_from_parent(invocation.function_id.clone(), self.recorder);
                    self.function_invocations.push((
                        invocation.composition_idx,
                        WorkToDo::FunctionArguments {
                            function_id: invocation.function_id.clone(),
                            function_alternatives: info.alternatives.read().unwrap().clone(),
                            input_sets: invocation.input,
                            metadata: info.metadata.clone(),
                            caching: self.caching,
                            recorder,
                        },
                    ));
                    self.function_invocations.extend(iter.map(|invocation| {
                        let recorder = Recorder::new_from_parent(
                            invocation.function_id.clone(),
                            self.recorder,
                        );
                        (
                            invocation.composition_idx,
                            WorkToDo::FunctionArguments {
                                function_id: invocation.function_id.clone(),
                                function_alternatives: info.alternatives.read().unwrap().clone(),
                                input_sets: invocation.input,
                                metadata: info.metadata.clone(),
                                caching: self.caching,
                                recorder,
                            },
                        )
                    }));
                }
            };
        }
    }
}

// TODO also here and in registry replace Arc Box with static references from leaked boxes for things we expect to be there for
// the entire execution time anyway
// TODO: the composition ids currently just go up from 0, there is no overflow handling.
// We are not expecting to overflow, but this should be handled for proper deployments.
pub struct Dispatcher {
    function_registry: FunctionRegistry,
    work_queue: WorkQueue,
    domains: Vec<Arc<Box<dyn MemoryDomain>>>,
    any_sharding_mode: AnyShardingMode,
    composition_id: AtomicUsize,
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
            composition_id: AtomicUsize::new(0),
        });
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

    pub async fn queue_function_by_name(
        &self,
        function_id: Arc<String>,
        inputs: Vec<DataSet>,
        caching: bool,
        mut recorder: Recorder,
    ) -> DandelionResult<(Vec<Arc<Vec<DataItem>>>, Recorder)> {
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

        // if any set is not local send them trough reference resolution
        let resolved_sets = if !(&results)
            .iter()
            .all(|set| set.iter().all(|item| item.is_local()))
        {
            self.work_queue
                .do_work(
                    WorkToDo::SetsToResolve {
                        input_sets: results
                            .into_iter()
                            .map(|items| DataSet::from_items(items))
                            .collect(),
                    },
                    composition_id,
                )
                .await?
                .get_data_items()
        } else {
            results
        };

        Ok((resolved_sets, recorder))
    }

    pub async fn queue_unregistered_composition(
        &self,
        composition_desc: String,
        inputs: Vec<DataSet>,
        caching: bool,
        mut recorder: Recorder,
        // Possibly want to reintroduce a local type, for the way up, and also use it here.
    ) -> DandelionResult<(Vec<Arc<Vec<DataItem>>>, Recorder)> {
        debug!("Parsing single use composition");
        recorder.record(RecordPoint::EnterDispatcher);

        let mut templates = CompositionTemplate::parse(&composition_desc, &self.function_registry)?;
        let (composition_name, template, metadata) = match templates.pop() {
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
            )
            .await?;

        // if any set is not local send them trough reference resolution
        let resolved_sets = if !(&results)
            .into_iter()
            .all(|items| items.iter().all(|item| item.is_local()))
        {
            self.work_queue
                .do_work(
                    WorkToDo::SetsToResolve {
                        input_sets: results
                            .into_iter()
                            .map(|items| DataSet::from_items(items))
                            .collect(),
                    },
                    composition_id,
                )
                .await?
                .get_data_items()
        } else {
            results
        };

        Ok((resolved_sets, recorder))
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
        composition_id: usize,
        mut composition: Composition,
        inputs: Vec<DataSet>,
        caching: bool,
        mut recorder: Recorder,
    ) -> DandelionResult<Vec<Arc<Vec<DataItem>>>> {
        // build up ready sets
        trace!("queue composition");

        // TODO: check if we need to use VecDeq, to preserve invocation order
        let mut ready_invocations = QueueingStruct {
            registry: &self.function_registry,
            function_invocations: Vec::new(),
            to_convert: Vec::new(),
            compositons: Vec::new(),
            caching,
            recorder: &recorder,
        };
        let mut awaited_sets = FuturesUnordered::new();

        composition.start_execution(inputs, &mut ready_invocations);
        // enqueue all initial request
        loop {
            // enqueue all the function calls
            // TODO change do_work_many to push futures into the futures unordered we have here, so we can get the results individually
            while let Some((composition_index, invocation)) =
                ready_invocations.function_invocations.pop()
            {
                awaited_sets.push(Either::Left(async move {
                    self.work_queue
                        .do_work(invocation, composition_id)
                        .await
                        .map(|work| (composition_index, work.get_data_items()))
                }));
            }

            while let Some(sys_func) = ready_invocations.to_convert.pop() {
                awaited_sets.push(Either::Right(Either::Left(async move {
                    tokio::spawn(async move {
                        machine_interface::function_driver::system_driver::convert_to_references(
                            dandelion_commons::SystemFunction::HTTP,
                            sys_func.input,
                        )
                        .map(|items| (sys_func.composition_idx, items))
                    })
                    .await
                    .unwrap()
                })));
            }

            while let Some(composition) = ready_invocations.compositons.pop() {
                if let FunctionType::Composition(comp) = self
                    .function_registry
                    .get_function(&composition.function_id)?
                {
                    let new_recorder =
                        Recorder::new_from_parent(composition.function_id, &recorder);
                    // TODO: consider if this should run in a sperate spawn (at the moment can't because the future is not send)
                    awaited_sets.push(Either::Right(Either::Right(async move {
                        self.queue_composition(
                            composition_id,
                            Composition::from_template(
                                &comp.composition,
                                self.any_sharding_mode.clone(),
                                &self.function_registry,
                            ),
                            composition.input,
                            caching,
                            new_recorder,
                        )
                        .await
                        .map(|items| (composition.composition_idx, items))
                    })));
                } else {
                    unreachable!()
                }
            }

            if let Some(set_result) = awaited_sets.next().await {
                let (composition_index, items) = set_result?;
                composition.push_invocation_output(
                    items,
                    composition_index,
                    &mut ready_invocations,
                );
            } else {
                break;
            }
        }

        Ok(composition
            .collect()
            .into_iter()
            .map(|set| set.items)
            .collect())

        // local state of functions that still need to run
        // TODO: make nicer data structure to hold there and the compositions
        // Can use ? Sized, to create structs with fixed arrays where the size is known at runtime.
        // And potentially Rc / RefCell to make a proper graph
        // struct FunctionArgs {
        //     function_id: FunctionId,
        //     // Which index the function had in the function dependencies array.
        //     // used to differentiate between multiple functions with same ID in one invocation for debugging and tracing.
        //     function_index: usize,
        //     input_sets: Vec<Option<(ShardingMode, CompositionSet)>>,
        //     join_info: (Vec<usize>, Vec<JoinStrategy>),
        //     output_mapping: Vec<Option<usize>>,
        //     missing_sets: BTreeMap<(usize, usize), (ShardingMode, bool)>,
        // }

        // let mut recorders = Vec::with_capacity(composition.dependencies.len());
        // recorders.resize(composition.dependencies.len(), None);

        // // prepare output sets, that are also input sets
        // let output_number = composition.output_map.len();
        // let mut output_sets = Vec::with_capacity(output_number);
        // output_sets.resize(output_number, None);
        // for (input_index, input_set) in inputs.iter().enumerate() {
        //     if let Some(out_index) = composition.output_map.get(&input_index) {
        //         output_sets[*out_index] = input_set.clone();
        //     }
        // }

        // let mut awaited_sets = FuturesUnordered::new();

        // let (ready_functions, mut non_ready_functions): (Vec<_>, Vec<_>) = composition
        //     .dependencies
        //     .into_iter()
        //     .enumerate()
        //     .filter_map(|(composition_index, deps)| {
        //         let mut missing_map = BTreeMap::new();
        //         let input_set_number: usize = deps.input_set_ids.len();
        //         let mut ready_inputs = Vec::with_capacity(input_set_number);
        //         ready_inputs.resize(deps.input_set_ids.len(), None);
        //         for (function_index, in_set_decriptor) in deps.input_set_ids.iter().enumerate() {
        //             if let Some(InputSetDescriptor {
        //                 composition_id,
        //                 sharding,
        //                 optional,
        //             }) = in_set_decriptor
        //             {
        //                 if let Some(comp_set) = inputs.get(*composition_id) {
        //                     // this means the a non optional set None, so we can skip it and directly queue all outputs are ready None
        //                     if !*optional && comp_set.is_none() {
        //                         let new_sets = deps
        //                             .output_set_ids
        //                             .iter()
        //                             .filter_map(|index_opt| {
        //                                 index_opt.and_then(|index| Some((index, None)))
        //                             })
        //                             .collect();
        //                         awaited_sets.push(Either::Left(ready(Ok((
        //                             new_sets,
        //                             function_index,
        //                             Vec::new(),
        //                         )))));
        //                         return None;
        //                     }
        //                     if let Some(set) = comp_set {
        //                         debug_assert_ne!(
        //                             0,
        //                             set.len(),
        //                             "Expect sets that are some to have at least one item"
        //                         );
        //                         ready_inputs[function_index] = Some((*sharding, set.clone()));
        //                     }
        //                 } else {
        //                     missing_map
        //                         .insert((*composition_id, function_index), (*sharding, *optional));
        //                 }
        //             }
        //         }
        //         Some(FunctionArgs {
        //             function_id: deps.function,
        //             function_index: composition_index,
        //             input_sets: ready_inputs,
        //             join_info: deps.join_info,
        //             output_mapping: deps.output_set_ids,
        //             missing_sets: missing_map,
        //         })
        //     })
        //     .partition(|args| args.missing_sets.is_empty());

        // // start all functions that are ready and insert their sets into the awaited ones
        // for args in ready_functions.into_iter() {
        //     awaited_sets.push(Either::Right(self.queue_function_sharded(
        //         composition_id,
        //         args.function_id,
        //         args.function_index,
        //         args.input_sets,
        //         args.join_info.0,
        //         args.join_info.1,
        //         args.output_mapping,
        //         caching,
        //         recorder.clone(),
        //     )));
        // }
        // let num_running_functions = awaited_sets.len();

        // trace!(
        //     "functions ready: {}, functions not ready: {}",
        //     num_running_functions,
        //     non_ready_functions.len()
        // );
        // while let Some(new_compositions_result) = awaited_sets.next().await {
        //     let (new_compositions, function_index, new_recorders) = new_compositions_result?;
        //     recorders[function_index] = Some(new_recorders);
        //     for (composition_set_index, composition_set_option) in &new_compositions {
        //         trace!(
        //             "composition set {:?} arrived at dispatcher is some: {}",
        //             composition_set_index,
        //             composition_set_option.is_some()
        //         );
        //         if let Some(output_index) = composition.output_map.get(&composition_set_index) {
        //             output_sets[*output_index] = composition_set_option.clone();
        //         }
        //         non_ready_functions = non_ready_functions
        //             .into_iter()
        //             .filter_map(|mut args| {
        //                 let to_remove = args
        //                     .missing_sets
        //                     .range(
        //                         (*composition_set_index, 0)..(*composition_set_index, (usize::MAX)),
        //                     )
        //                     .map(|((comp_index, function_index), (mode, optional))| {
        //                         // if it was not optional skip executing and push all output sets
        //                         // TODO: for left, right and outer joins, some sets may also be pseudo optional.
        //                         // (i.e. an empty left set on a right join can still have functions that should run)
        //                         // Fix either by adding attributes to easily check here or move to check optional together with sharding.
        //                         if !optional && composition_set_option.is_none() {
        //                             let new_sets = args
        //                                 .output_mapping
        //                                 .iter()
        //                                 .filter_map(|index_opt| {
        //                                     index_opt.and_then(|index| Some((index, None)))
        //                                 })
        //                                 .collect();
        //                             awaited_sets.push(Either::Left(ready(Ok((
        //                                 new_sets,
        //                                 args.function_index,
        //                                 Vec::new(),
        //                             )))));
        //                             None
        //                         } else {
        //                             args.input_sets[*function_index] =
        //                                 composition_set_option.clone().and_then(|set| {
        //                                     debug_assert_ne!(
        //                                         0,
        //                                         set.len(),
        //                                         "Expect at least 1 item in composition set"
        //                                     );
        //                                     Some((*mode, set))
        //                                 });
        //                             Some((*comp_index, *function_index))
        //                         }
        //                     })
        //                     .collect::<Vec<_>>();
        //                 // need to cancel the function if one of the non optional sets is empty
        //                 for key_opt in to_remove {
        //                     if let Some(key) = key_opt {
        //                         args.missing_sets.remove(&key);
        //                     } else {
        //                         return None;
        //                     }
        //                 }
        //                 if args.missing_sets.is_empty() {
        //                     awaited_sets.push(Either::Right(self.queue_function_sharded(
        //                         composition_id,
        //                         args.function_id,
        //                         args.function_index,
        //                         args.input_sets,
        //                         args.join_info.0,
        //                         args.join_info.1,
        //                         args.output_mapping,
        //                         caching,
        //                         recorder.clone(),
        //                     )));
        //                     None
        //                 } else {
        //                     Some(args)
        //                 }
        //             })
        //             .collect();
        //     }
        //     trace!(
        //         "waiting for {} sets (running functions and global sets), functions not ready: {}",
        //         awaited_sets.len(),
        //         non_ready_functions.len()
        //     );
        // }

        // recorder.add_children(recorders);
        // recorder.record(RecordPoint::FutureReturn);
        // return Ok(output_sets);
    }

    /// Adapter between compositions and functions
    /// Keeps track of the composition set indexes so that when sets are returned to
    /// composition they have the corret index associated without the composition needing to track them.
    /// Also handles sharing of sets.
    // async fn queue_function_sharded<'context>(
    //     &self,
    //     composition_id: usize,
    //     function_id: FunctionId,
    //     // index of the function within the composition
    //     function_index: usize,
    //     input_sets: Vec<Option<(ShardingMode, CompositionSet)>>,
    //     join_order: Vec<usize>,
    //     join_strategies: Vec<JoinStrategy>,
    //     output_mapping: Vec<Option<usize>>,
    //     caching: bool,
    //     recorder: Recorder,
    // ) -> DandelionResult<(Vec<(usize, Option<CompositionSet>)>, usize, Vec<Recorder>)> {
    //     trace!(
    //         "queue function {} sharded and input sets: {:?}",
    //         function_id,
    //         input_sets
    //     );

    //     let (results, recorders) = match self.function_registry.get_function(&function_id)? {
    //         FunctionType::SystemFunction(sys_function) => {
    //             // check if there are no input sets or all of them are none, then don't need sharding,
    //             // but still want to run if we queued it.
    //             let mut new_recorder = Recorder::new_from_parent(function_id.clone(), &recorder);
    //             new_recorder.record(RecordPoint::EngineStart);
    //             let results = tokio::spawn(async move {
    //                 machine_interface::function_driver::system_driver::convert_to_references(
    //                     sys_function,
    //                     input_sets
    //                         .into_iter()
    //                         .map(|set_option| set_option.map(|(_, set)| set))
    //                         .collect(),
    //                 )
    //             })
    //             .await
    //             .unwrap()?;

    //             new_recorder.record(RecordPoint::EngineEnd);

    //             (results, vec![new_recorder])
    //         }
    //         FunctionType::Function(func_info) => {
    //             let function_alternatives = func_info
    //                 .alternatives
    //                 .read()
    //                 .expect("Function registry lock is poisoned!")
    //                 .clone();
    //             let metadata = func_info.metadata;

    //             // check if there are no input sets or all of them are none, then don't need sharding,
    //             // but still want to run if we queued it.
    //             let sharding_start = std::time::Instant::now();
    //             let mut recorders;
    //             let is_sharded =
    //                 input_sets.len() != 0 && input_sets.iter().any(|opt| opt.is_some());
    //             if is_sharded {
    //                 let min_set_bytes = self.function_registry.get_min_set_bytes(&function_id)?;
    //                 let sharded = get_sharding(
    //                     input_sets,
    //                     join_order,
    //                     join_strategies,
    //                     &self.any_sharding_mode,
    //                     min_set_bytes,
    //                 );
    //                 let size_hint = sharded.len();
    //                 recorders = Vec::with_capacity(size_hint);

    //                 let work_vec = sharded.into_iter().map(|input_sets| {
    //                     let mut new_recorder =
    //                         Recorder::new_from_parent(function_id.clone(), &recorder);
    //                     new_recorder.prerecorded(RecordPoint::ShardingStart, sharding_start);

    //                     let work_to_do = WorkToDo::FunctionArguments {
    //                         function_id: function_id.clone(),
    //                         function_alternatives: function_alternatives.clone(),
    //                         input_sets,
    //                         metadata: metadata.clone(),
    //                         caching,
    //                         recorder: new_recorder.clone(),
    //                     };
    //                     recorders.push(new_recorder);
    //                     work_to_do
    //                 });

    //                 let done_vec = self
    //                     .work_queue
    //                     .do_work_many(work_vec, composition_id)
    //                     .await?;

    //                 for new_recorder in recorders.iter_mut() {
    //                     new_recorder.record(RecordPoint::FutureReturn);
    //                 }

    //                 let mut composition_set_vecs = Vec::with_capacity(output_mapping.len());
    //                 composition_set_vecs.resize(output_mapping.len(), Vec::new());
    //                 for done_work in done_vec.into_iter() {
    //                     let sets = done_work.get_data_items();
    //                     #[cfg(feature = "log_function_stdio")]
    //                     log_io_sets(&sets, &function_id)?;

    //                     for (index, set_option) in sets.into_iter().enumerate() {
    //                         if let Some(set) = set_option {
    //                             composition_set_vecs[index].extend(set.into_iter());
    //                         }
    //                     }
    //                 }

    //                 let sets = composition_set_vecs
    //                     .into_iter()
    //                     .zip(metadata.output_sets.iter())
    //                     .map(|(sets, name)| CompositionSet::from_item_list(name.clone(), sets))
    //                     .collect();

    //                 (sets, recorders)
    //             } else {
    //                 // TODO this is added to support functions with all functions defined as static sets
    //                 // might want to differentiate between those that have static sets and those that did not get input from predecessors
    //                 let mut new_recorder =
    //                     Recorder::new_from_parent(function_id.clone(), &recorder);
    //                 new_recorder.prerecorded(RecordPoint::ShardingStart, sharding_start);

    //                 let args = WorkToDo::FunctionArguments {
    //                     function_id: function_id.clone(),
    //                     function_alternatives,
    //                     input_sets: vec![],
    //                     metadata,
    //                     caching,
    //                     recorder: new_recorder.clone(),
    //                 };

    //                 let sets = self
    //                     .work_queue
    //                     .do_work(args, composition_id)
    //                     .await?
    //                     .get_data_items();
    //                 new_recorder.record(RecordPoint::FutureReturn);

    //                 #[cfg(feature = "log_function_stdio")]
    //                 log_io_sets(&sets, &function_id)?;

    //                 (sets, vec![new_recorder])
    //             }
    //         }
    //         FunctionType::Composition(comp_info) => (
    //             self.queue_composition(
    //                 composition_id,
    //                 (*comp_info.composition).clone(),
    //                 input_sets
    //                     .into_iter()
    //                     .map(|opt| opt.map(|(_, set)| set))
    //                     .collect(),
    //                 caching,
    //                 recorder.clone(),
    //             )
    //             .await?,
    //             vec![recorder],
    //         ),
    //     };

    //     Ok((
    //         output_mapping
    //             .into_iter()
    //             .zip(results.into_iter())
    //             .filter_map(|(index_option, sets)| index_option.map(|index| (index, sets)))
    //             .collect(),
    //         function_index,
    //         recorders,
    //     ))
    // }

    /// returns a vector of pairs of a index and a composition set
    /// the index describes which output set the composition belongs to.
    pub async fn queue_function<'dispatcher>(
        &'dispatcher self,
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
                    .do_work(args, composition_id)
                    .await?
                    .get_data_items();
                recorder.record(RecordPoint::FutureReturn);

                #[cfg(feature = "log_function_stdio")]
                log_io_sets(&sets, &function_id)?;

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
                )
                .await
            }
        }
    }
}

#[cfg(feature = "log_function_stdio")]
fn log_io_sets(sets: &Vec<Arc<Vec<DataItem>>>, function_id: &FunctionId) -> DandelionResult<()> {
    use memory::context::ContextTrait;

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
                log::warn!(
                    "Function '{}' result contains stderr output:\n{}",
                    function_id,
                    std::str::from_utf8(stderr_output.as_slice()).expect("Invalid stderr buffer")
                );
            }
            if item.ident == "stdout" && item.data.size > 0 {
                let mut stdout_output: Vec<u8> = vec![0; item.data.size];
                context.context.read(item.data.offset, &mut stdout_output)?;
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

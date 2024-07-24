use dandelion_commons::{
    records::{RecordPoint, Recorder},
    DandelionError, DandelionResult, FunctionId,
};
use dparser::print_errors;
use futures::{lock::Mutex, Future, FutureExt};
use machine_interface::{
    function_driver::{
        system_driver::{get_system_function_input_sets, get_system_function_output_sets},
        Driver, Function, FunctionConfig,
    },
    machine_config::{get_system_functions, DomainType, EngineType},
    memory_domain::{Context, MemoryDomain},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    pin::Pin,
    sync::Arc,
};

use crate::{
    composition::{Composition, CompositionSet},
    execution_qs::EngineQueue,
};

#[derive(Clone, Debug)]
pub enum FunctionType {
    /// Function available on an engine holding the engine ID
    /// and the default size of the function context
    Function(EngineType, usize),
    /// Function available as composition, holding the composition graph
    /// and the set with the inidecs of the sets in the composition that are output sets
    Composition(Composition),
}

#[derive(Clone, Debug)]
pub struct Alternative {
    pub function_type: FunctionType,
    pub in_memory: bool, // can place more information the scheduler would need here later
}

/// Struct to describe meatadata about a function that is true accross all drivers
#[derive(Debug, Clone)]
pub struct Metadata {
    /// input set names and optionally a static composition that is to be used for that input
    /// if the static input set is defined, any new input to that set is to be ignored
    pub input_sets: Arc<Vec<(String, Option<CompositionSet>)>>,
    /// output set names
    pub output_sets: Arc<Vec<String>>,
}

pub struct FunctionDict {
    next_id: FunctionId,
    map: BTreeMap<String, FunctionId>,
}

impl FunctionDict {
    pub fn new() -> Self {
        // TODO free up ids as they are not used anymore?
        Self {
            next_id: 1,
            map: BTreeMap::new(),
        }
    }

    pub fn insert_or_lookup(&mut self, function_name: String) -> FunctionId {
        use std::collections::btree_map::Entry;
        log::debug!("Inserted function with name {}", &function_name);
        match self.map.entry(function_name) {
            Entry::Vacant(v) => {
                let new_id = self.next_id;
                v.insert(new_id);
                self.next_id += 1;
                new_id
            }
            Entry::Occupied(o) => *o.get(),
        }
    }

    pub fn lookup(&self, function_name: &str) -> Option<FunctionId> {
        self.map.get(function_name).cloned()
    }
}

/// Function to create a future that returns the loaded function
async fn load_local(
    static_domain: &'static dyn MemoryDomain,
    driver: &'static dyn Driver,
    mut recorder: Recorder,
    work_queue: Box<EngineQueue>,
    path: String,
) -> DandelionResult<Arc<Function>> {
    recorder.record(RecordPoint::ParsingQueueu).unwrap();
    let function = work_queue
        .enqueu_work(
            machine_interface::function_driver::WorkToDo::ParsingArguments {
                driver,
                path,
                static_domain,
                recorder: recorder.get_sub_recorder().unwrap(),
            },
        )
        .await?
        .get_function();
    recorder.record(RecordPoint::ParsingDequeu).unwrap();
    return Ok(Arc::new(function));
}

pub struct FunctionRegistry {
    /// List of engines available for each function
    engine_map: Mutex<BTreeMap<FunctionId, BTreeSet<EngineType>>>,
    /// Drivers for the engines to prepare function (get them from available to ready)
    pub(crate) drivers: BTreeMap<EngineType, (&'static dyn Driver, Box<EngineQueue>)>,
    /// map with list of all options for each function
    /// TODO: change structure to avoid copy on get_options
    options: Mutex<BTreeMap<FunctionId, Vec<Alternative>>>,
    /// Map with path to disk where function is located and an option to a in memory loaded version
    /// TOOD: decide if want to bake into alternative type
    loadable: Mutex<
        BTreeMap<
            (FunctionId, EngineType),
            (
                String,
                Option<
                    futures::future::Shared<
                        Pin<Box<dyn Future<Output = DandelionResult<Arc<Function>>> + Send>>,
                    >,
                >,
            ),
        >,
    >,
    /// map with input and output set names for functions
    metadata: Mutex<BTreeMap<FunctionId, Metadata>>,
    /// map name to function id
    function_dict: Mutex<FunctionDict>,
}

impl FunctionRegistry {
    // TODO: make sure that system functions can't be added later for other engines
    pub fn new(
        drivers: BTreeMap<EngineType, (&'static dyn Driver, Box<EngineQueue>)>,
        type_map: &BTreeMap<EngineType, DomainType>,
        domains: &BTreeMap<DomainType, &'static dyn MemoryDomain>,
    ) -> Self {
        // insert all system functons
        let mut engine_map = BTreeMap::new();
        let mut options = BTreeMap::new();
        let mut loadable = BTreeMap::new();
        let mut metadata = BTreeMap::new();
        let mut function_dict = FunctionDict::new();
        for (engine_type, (driver, _)) in drivers.iter() {
            let system_functions = get_system_functions(*engine_type);
            for (system_function, context_size) in system_functions {
                let function_id = function_dict.insert_or_lookup(system_function.to_string());
                // register engine for the function id of the system function
                engine_map
                    .entry(function_id)
                    .and_modify(|engine_set: &mut BTreeSet<EngineType>| {
                        engine_set.insert(*engine_type);
                    })
                    .or_insert(BTreeSet::from([*engine_type]));
                options
                    .entry(function_id)
                    .and_modify(|option_vec: &mut Vec<Alternative>| {
                        option_vec.push(Alternative {
                            function_type: FunctionType::Function(*engine_type, context_size),
                            in_memory: true,
                        })
                    })
                    .or_insert(vec![Alternative {
                        function_type: FunctionType::Function(*engine_type, context_size),
                        in_memory: true,
                    }]);
                // get the config from the parser
                let function_config = driver
                    .parse_function(
                        String::from(""),
                        *domains.get(type_map.get(engine_type).unwrap()).unwrap(),
                    )
                    .unwrap();
                match function_config.config {
                    FunctionConfig::SysConfig(_) => (),
                    _ => panic!("parsing system function did not return system config"),
                };
                loadable.insert(
                    (function_id, *engine_type),
                    (
                        String::new(),
                        Some(
                            (Box::pin(futures::future::ready(Ok(Arc::new(function_config))))
                                as Pin<Box<dyn Future<Output = DandelionResult<_>> + Send>>)
                                .shared(),
                        ),
                    ),
                );
                let function_metadata = Metadata {
                    input_sets: Arc::new(
                        get_system_function_input_sets(system_function)
                            .into_iter()
                            .map(|name| (name, None))
                            .collect(),
                    ),
                    output_sets: Arc::new(get_system_function_output_sets(system_function)),
                };
                metadata.entry(function_id).or_insert(function_metadata);
            }
        }
        // add metadata for the
        return FunctionRegistry {
            engine_map: Mutex::new(engine_map),
            drivers,
            options: Mutex::new(options),
            loadable: Mutex::new(loadable),
            metadata: Mutex::new(metadata),
            function_dict: Mutex::new(function_dict),
        };
    }

    pub async fn get_options(&self, function_id: FunctionId) -> DandelionResult<Vec<Alternative>> {
        // get the ones that are already loaded
        let lock_guard = self.options.lock().await;
        let alternatives = lock_guard.get(&function_id);
        return alternatives
            .and_then(|alt| Some(alt.to_vec()))
            .ok_or(DandelionError::DispatcherUnavailableFunction);
    }

    pub async fn get_metadata(&self, function_id: FunctionId) -> DandelionResult<Metadata> {
        return self
            .metadata
            .lock()
            .await
            .get(&function_id)
            .and_then(|meta| Some(meta.clone()))
            .ok_or(DandelionError::DispatcherUnavailableFunction);
    }

    /// TODO: find a better way to keep track of functions, so we can support updates to functions and compositions
    /// Also can allow adding the same function for multiple engines already without this
    pub async fn insert_function(
        &self,
        function_name: String,
        engine_id: EngineType,
        ctx_size: usize,
        path: String,
        metadata: Metadata,
    ) -> DandelionResult<FunctionId> {
        // check if function is already present, get ID if not
        let function_id;
        {
            let mut dict_lock = self.function_dict.lock().await;
            if dict_lock.lookup(&function_name).is_some() {
                return Err(DandelionError::DispatcherDuplicateFunction);
            }
            function_id = dict_lock.insert_or_lookup(function_name);
        }
        self.metadata.lock().await.insert(function_id, metadata);
        self.add_local(function_id, engine_id, ctx_size, path)
            .await?;
        return Ok(function_id);
    }

    /// TODO: for compositions that are already present the metadata is not overwritten
    pub async fn insert_compositions(&self, module: &str) -> DandelionResult<()> {
        // TODO actually handle the error in some sensible way
        // the error contains the parsing failure
        let mut dictlock = self.function_dict.lock().await;
        let composition_meta_pairs = {
            let module = dparser::parse(module).map_err(|parse_error| {
                print_errors(module, parse_error);
                DandelionError::CompositionParsingError
            })?;
            Composition::from_module(&module, &mut dictlock)?
        };
        for (function_id, composition, metadata) in composition_meta_pairs {
            self.metadata.lock().await.insert(function_id, metadata);
            self.add_composition(function_id, composition).await?;
        }
        return Ok(());
    }

    pub async fn get_function_id(&self, function_name: &str) -> Option<FunctionId> {
        return self.function_dict.lock().await.lookup(&function_name);
    }

    async fn add_composition(
        &self,
        function_id: FunctionId,
        composition: Composition,
    ) -> DandelionResult<()> {
        if !self.metadata.lock().await.contains_key(&function_id) {
            return Err(DandelionError::DispatcherMetaDataUnavailable);
        };
        self.options
            .lock()
            .await
            .entry(function_id)
            .and_modify(|option_vec| {
                option_vec.push(Alternative {
                    function_type: FunctionType::Composition(composition.clone()),
                    in_memory: true,
                })
            })
            .or_insert(vec![Alternative {
                function_type: FunctionType::Composition(composition),
                in_memory: true,
            }]);
        return Ok(());
    }

    pub async fn add_local(
        &self,
        function_id: FunctionId,
        engine_id: EngineType,
        ctx_size: usize,
        path: String,
    ) -> DandelionResult<()> {
        if !self.metadata.lock().await.contains_key(&function_id) {
            return Err(DandelionError::DispatcherMetaDataUnavailable);
        }
        self.loadable
            .lock()
            .await
            .insert((function_id, engine_id), (path, None));
        self.engine_map
            .lock()
            .await
            .entry(function_id)
            .and_modify(|set| {
                set.insert(engine_id);
            })
            .or_insert({
                let mut set = BTreeSet::new();
                set.insert(engine_id);
                set
            });
        self.options
            .lock()
            .await
            .entry(function_id)
            .and_modify(|current_alts| {
                current_alts.push(Alternative {
                    function_type: FunctionType::Function(engine_id, ctx_size),
                    in_memory: false,
                })
            })
            .or_insert(vec![Alternative {
                function_type: FunctionType::Function(engine_id, ctx_size),
                in_memory: false,
            }]);
        return Ok(());
    }

    pub async fn load(
        &self,
        function_id: FunctionId,
        engine_id: EngineType,
        domain: &'static dyn MemoryDomain,
        ctx_size: usize,
        non_caching: bool,
        mut recorder: Recorder,
    ) -> DandelionResult<(Context, FunctionConfig)> {
        // get loader
        let (driver, load_queue) = match self.drivers.get(&engine_id) {
            Some(l) => l,
            None => {
                return Err(DandelionError::DispatcherMissingLoader(format!(
                    "{:?}",
                    engine_id
                )))
            }
        };

        // check if function for the engine is in registry already
        // if it is not there enqueue the parsing
        // if it is cached insert a shared future
        let mut lock_guard = self.loadable.lock().await;
        let function_future =
            if let Some((path, future_option)) = lock_guard.get_mut(&(function_id, engine_id)) {
                if let Some(func_future) = future_option {
                    func_future.clone()
                } else {
                    let func_future = (Box::pin(load_local(
                        domain,
                        *driver,
                        recorder.get_sub_recorder()?,
                        load_queue.clone(),
                        path.clone(),
                    ))
                        as Pin<Box<dyn Future<Output = DandelionResult<_>> + Send>>)
                        .shared();
                    if !non_caching {
                        let _ = future_option.insert(func_future.clone());
                    }
                    func_future
                }
            } else {
                return Err(DandelionError::DispatcherUnavailableFunction);
            };
        drop(lock_guard);
        let function = function_future.await?;
        let function_config = function.config.clone();
        recorder.record(RecordPoint::LoadQueue)?;
        let context_work_done = load_queue
            .enqueu_work(
                machine_interface::function_driver::WorkToDo::LoadingArguments {
                    function,
                    domain,
                    recorder: recorder.get_sub_recorder()?,
                    ctx_size: ctx_size,
                },
            )
            .await;
        recorder.record(RecordPoint::LoadDequeue)?;
        let function_context = context_work_done?.get_context();
        return Ok((function_context, function_config));
    }
}

use dandelion_commons::{
    records::{RecordPoint, Recorder},
    DandelionError, DandelionResult, DispatcherError, FunctionId,
};
use dparser::print_errors;
use futures::lock::Mutex;
use log::trace;
use machine_interface::{
    function_driver::{
        system_driver::{get_system_function_input_sets, get_system_function_output_sets},
        Driver, Function, FunctionConfig,
    },
    machine_config::{get_system_functions, EngineType},
    memory_domain::{Context, MemoryDomain},
    DataRequirementList,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write,
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

#[derive(Clone)]
struct Loadable {
    path: String,
    requirements: DataRequirementList,
    config: FunctionConfig,
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
    /// TODO: decide if want to bake into alternative type.
    /// TODO: loading currently synchornous, as it is only mapping the file from disk, may want to make into shared future,
    ///         so multiple functions can wait on it and the loading can be async, if it becomes a bottleneck.
    loadable: Mutex<BTreeMap<(FunctionId, EngineType), (Loadable, Option<Arc<Function>>)>>,
    /// map with input and output set names for functions
    metadata: Mutex<BTreeMap<FunctionId, Metadata>>,
    /// map name to function id
    function_dict: Mutex<FunctionDict>,
    /// Directory where to store the binaries for the functions that are registered
    binary_dir: String,
}

impl FunctionRegistry {
    // TODO: make sure that system functions can't be added later for other engines
    pub fn new(
        drivers: BTreeMap<EngineType, (&'static dyn Driver, Box<EngineQueue>)>,
        binary_dir: String,
    ) -> Self {
        // insert all system functons
        let mut engine_map = BTreeMap::new();
        let mut options = BTreeMap::new();
        let mut loadable = BTreeMap::new();
        let mut metadata = BTreeMap::new();
        let mut function_dict = FunctionDict::new();
        for engine_type in drivers.keys() {
            let system_functions = get_system_functions(*engine_type);
            for (system_function, context_size) in system_functions {
                let function_id = function_dict.insert_or_lookup(system_function.to_string());
                trace!(
                    "Inserted system function: {} at index {}",
                    system_function.to_string(),
                    function_id
                );
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
                // create function config
                let function_config = Function {
                    requirements: machine_interface::DataRequirementList {
                        input_requirements: Vec::new(),
                        static_requirements: Vec::new(),
                    },
                    static_data: Vec::new(),
                    config: FunctionConfig::SysConfig(system_function),
                };
                loadable.insert(
                    (function_id, *engine_type),
                    (
                        Loadable {
                            config: FunctionConfig::SysConfig(system_function),
                            path: String::from(""),
                            requirements: machine_interface::DataRequirementList {
                                input_requirements: Vec::new(),
                                static_requirements: Vec::new(),
                            },
                        },
                        Some(Arc::new(function_config)),
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
            binary_dir,
        };
    }

    pub async fn get_options(&self, function_id: FunctionId) -> DandelionResult<Vec<Alternative>> {
        // get the ones that are already loaded
        let lock_guard = self.options.lock().await;
        let alternatives = lock_guard.get(&function_id);
        return alternatives
            .and_then(|alt| Some(alt.to_vec()))
            .ok_or(DandelionError::Dispatcher(
                DispatcherError::UnavailableFunction,
            ));
    }

    pub async fn get_metadata(&self, function_id: FunctionId) -> DandelionResult<Metadata> {
        return self
            .metadata
            .lock()
            .await
            .get(&function_id)
            .and_then(|meta| Some(meta.clone()))
            .ok_or(DandelionError::Dispatcher(
                DispatcherError::UnavailableFunction,
            ));
    }

    /// TODO: find a better way to keep track of functions, so we can support updates to functions and compositions
    /// Also can allow adding the same function for multiple engines already without this
    pub async fn insert_function(
        &self,
        function_name: String,
        engine_id: EngineType,
        ctx_size: usize,
        binary_data: Vec<u8>,
        metadata: Metadata,
    ) -> DandelionResult<FunctionId> {
        // check if function is already present, get ID if not
        let function_id;
        {
            let mut dict_lock = self.function_dict.lock().await;
            if dict_lock.lookup(&function_name).is_some() {
                return Err(DandelionError::Dispatcher(
                    DispatcherError::DuplicateFunction,
                ));
            }
            function_id = dict_lock.insert_or_lookup(function_name.clone());
        }
        self.metadata.lock().await.insert(function_id, metadata);
        let (driver, work_queue) = self.drivers.get(&engine_id).unwrap();
        let function = work_queue
            .enqueu_work(
                machine_interface::function_driver::WorkToDo::ParsingArguments {
                    driver: *driver,
                    binary_data,
                },
            )
            .await?
            .get_function();
        self.add_local(&function_name, function_id, engine_id, ctx_size, function)
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
            return Err(DandelionError::Dispatcher(
                DispatcherError::MetaDataUnavailable,
            ));
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
        function_name: &str,
        function_id: FunctionId,
        engine_id: EngineType,
        ctx_size: usize,
        function: Function,
    ) -> DandelionResult<()> {
        if !self.metadata.lock().await.contains_key(&function_id) {
            return Err(DandelionError::Dispatcher(
                DispatcherError::MetaDataUnavailable,
            ));
        }

        let Function {
            requirements,
            static_data,
            config,
        } = function;

        let mut path_buf = std::path::PathBuf::from(self.binary_dir.clone());
        path_buf.push(format!(
            "function_{}_id_{}_engine_{:?}",
            function_name, function_id, engine_id
        ));
        let mut function_file = std::fs::File::create(path_buf.clone())
            .expect("Should be able to create file to store function binary");
        function_file
            .write_all(&static_data)
            .expect("Should be able to read binary from file");
        let loadable = Loadable {
            requirements,
            config,
            path: path_buf.as_path().to_str().unwrap().to_string(),
        };

        self.loadable
            .lock()
            .await
            .insert((function_id, engine_id), (loadable, None));
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
        domain: Arc<Box<dyn MemoryDomain>>,
        ctx_size: usize,
        non_caching: bool,
        mut recorder: Recorder,
    ) -> DandelionResult<(Context, FunctionConfig)> {
        // get loader
        let (_, load_queue) = match self.drivers.get(&engine_id) {
            Some(l) => l,
            None => return Err(DandelionError::Dispatcher(DispatcherError::MissingLoader)),
        };

        // check if function for the engine is in registry already
        // if it is not there enqueue the parsing
        // if it is cached insert a shared future
        let mut lock_guard = self.loadable.lock().await;
        let function = if let Some((funtion_loadable, future_option)) =
            lock_guard.get_mut(&(function_id, engine_id))
        {
            if let Some(function) = future_option {
                function.clone()
            } else {
                let Loadable {
                    path,
                    requirements,
                    config,
                } = funtion_loadable.clone();
                let function_arc = Arc::new(Function {
                    static_data: std::fs::read(path).unwrap(),
                    requirements,
                    config,
                });
                if !non_caching {
                    let _ = future_option.insert(function_arc.clone());
                }
                function_arc
            }
        } else {
            return Err(DandelionError::Dispatcher(
                DispatcherError::UnavailableFunction,
            ));
        };
        drop(lock_guard);
        let function_config = function.config.clone();
        recorder.record(RecordPoint::LoadQueue);
        let context_work_done = load_queue
            .enqueu_work(
                machine_interface::function_driver::WorkToDo::LoadingArguments {
                    function,
                    domain,
                    recorder: recorder.get_sub_recorder(),
                    ctx_size: ctx_size,
                },
            )
            .await;
        recorder.record(RecordPoint::LoadDequeue);
        let function_context = context_work_done?.get_context();
        return Ok((function_context, function_config));
    }
}

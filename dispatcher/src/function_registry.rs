use dandelion_commons::{
    records::{RecordPoint, Recorder},
    CompositionError, DandelionError, DandelionResult, FunctionId, FunctionRegistryError,
};
use dparser::print_errors;
use futures::{future, lock::Mutex, Future, FutureExt};
use log::error;
use machine_interface::{
    function_driver::{
        system_driver::{get_system_function_input_sets, get_system_function_output_sets},
        Driver, Function, FunctionConfig, WorkToDo,
    },
    machine_config::{get_system_functions, DomainType, EngineType},
    memory_domain::{Context, MemoryDomain},
};
use std::{
    collections::BTreeMap,
    path::Path,
    pin::Pin,
    sync::{Arc, RwLock},
};

use crate::{
    composition::{Composition, CompositionSet},
    queue::WorkQueue,
};

/// Struct holding all information about an alternative engine to execute the function.
#[derive(Debug)]
pub struct FunctionAlternative {
    /// The engine type of the alternative.
    pub engine: EngineType,
    /// The default context size of this alternative.
    pub context_size: usize,
    /// Path to the function binary.
    pub path: String,
    /// Function object once the binary is loaded in memory.
    /// TODO: Could change it to create the future on insertion, as it only gets resolved on the
    ///       first await anyway. Might also want to implement some caching logic behind the future
    ///       at that point.
    pub function: Mutex<
        Option<
            future::Shared<Pin<Box<dyn Future<Output = DandelionResult<Arc<Function>>> + Send>>>,
        >,
    >,
}

/// Struct holding general function metadata that is true across all drivers.
#[derive(Debug)]
pub struct Metadata {
    /// The input set names with an optional static composition set. If the static set is set it will
    /// prioritized and any other input for that set is ignored.
    pub input_sets: Vec<(String, Option<CompositionSet>)>,
    /// The output set names.
    pub output_sets: Arc<Vec<String>>,
}

/// Struct holding all engine alternatives to run a function and the constant metadata. This struct
/// can be cloned cheaply and given to the scheduler for function execution.
#[derive(Debug, Clone)]
pub struct FunctionInfo {
    /// The engine alternatives to execute the functions.
    pub alternatives: Arc<RwLock<Vec<Arc<FunctionAlternative>>>>,
    /// The metadata that applies to all function alternatives.
    pub metadata: Arc<Metadata>,
}

impl FunctionInfo {
    /// Returns an atomic reference to the function alternative corresponding to the given engine type.
    pub fn get_alternative(&self, engine: EngineType) -> DandelionResult<Arc<FunctionAlternative>> {
        let alternatives_locked = self
            .alternatives
            .read()
            .expect("Function registry lock poisoned!");
        match alternatives_locked.iter().find(|alt| alt.engine == engine) {
            Some(alt) => Ok(alt.clone()),
            None => Err(DandelionError::FunctionRegistry(
                FunctionRegistryError::UnknownFunctionAlternative,
            )),
        }
    }
}

/// Struct holding the parsed composition and corresponding metadata. This struct
/// can be cloned cheaply and given to the scheduler for function execution.
#[derive(Debug, Clone)]
pub struct CompositionInfo {
    /// The engine alternatives to execute the functions.
    pub composition: Arc<Composition>,
    /// The metadata that applies to all function alternatives.
    pub metadata: Arc<Metadata>,
}

#[derive(Debug, Clone)]
pub enum FunctionType {
    /// A system function. Cannot add more alternatives for this type after initialization.
    SystemFunction(FunctionInfo),
    /// A user defined function.
    Function(FunctionInfo),
    /// A composition of functions.
    Composition(CompositionInfo),
}

/// A `BTreeMap` linking function identifiers to function types.
type FunctionMap = BTreeMap<String, FunctionType>;

// inserts the function into the function map
fn fmap_insert_function(
    fmap: &mut FunctionMap,
    key: FunctionId,
    func_alt: FunctionAlternative,
    func_meta: Metadata,
    is_system: bool,
) -> DandelionResult<()> {
    match fmap.get_mut(&(*key)) {
        Some(entry) => {
            let func_info = match entry {
                FunctionType::SystemFunction(info) => {
                    if !is_system {
                        return Err(DandelionError::FunctionRegistry(
                            FunctionRegistryError::InvalidSystemInsert((*key).clone()),
                        ));
                    }
                    info
                }
                FunctionType::Function(info) => {
                    if is_system {
                        return Err(DandelionError::FunctionRegistry(
                            FunctionRegistryError::InvalidUserInsert((*key).clone()),
                        ));
                    }
                    info
                }
                FunctionType::Composition(_) => {
                    return Err(DandelionError::FunctionRegistry(
                        FunctionRegistryError::TypeConflictInsert((*key).clone()),
                    ));
                }
            };

            // check if an alternative with this engine type already exists
            let mut lock_guard = func_info
                .alternatives
                .write()
                .expect("Function registry lock poisoned!");
            if lock_guard.iter().any(|alt| alt.engine == func_alt.engine) {
                return Err(DandelionError::FunctionRegistry(
                    FunctionRegistryError::DuplicateInsert((*key).clone()),
                ));
            }
            // TODO: check that metadata matches existing one
            lock_guard.push(Arc::new(func_alt));
        }
        None => {
            let func_info = FunctionInfo {
                alternatives: Arc::new(RwLock::new(vec![Arc::new(func_alt)])),
                metadata: Arc::new(func_meta),
            };
            if is_system {
                fmap.insert((*key).clone(), FunctionType::SystemFunction(func_info));
            } else {
                fmap.insert((*key).clone(), FunctionType::Function(func_info));
            }
        }
    };
    Ok(())
}

// inserts the function composition into the function map
fn fmap_insert_composition(
    fmap: &mut FunctionMap,
    key: FunctionId,
    composition: Composition,
    metadata: Metadata,
) -> DandelionResult<()> {
    match fmap.get(&(*key)) {
        Some(_) => {
            return Err(DandelionError::FunctionRegistry(
                FunctionRegistryError::DuplicateInsert((*key).clone()),
            ))
        }
        None => {
            let comp_info = CompositionInfo {
                composition: Arc::new(composition),
                metadata: Arc::new(metadata),
            };
            fmap.insert((*key).clone(), FunctionType::Composition(comp_info))
        }
    };
    Ok(())
}

// creates a future that returns the loaded function
async fn load_local(
    static_domain: Arc<Box<dyn MemoryDomain>>,
    driver: &'static dyn Driver,
    mut recorder: Recorder,
    work_queue: WorkQueue,
    engines: Vec<EngineType>,
    path: String,
) -> DandelionResult<Arc<Function>> {
    recorder.record(RecordPoint::ParsingQueue);
    let function = work_queue
        .do_work(
            WorkToDo::ParsingArguments {
                driver,
                path,
                static_domain,
                recorder: recorder.get_sub_recorder(),
            },
            engines,
        )
        .await?
        .get_function();
    recorder.record(RecordPoint::ParsingDequeue);
    return Ok(Arc::new(function));
}

/// The core function registry of dandelion.
///
/// The registration maps a function identifier (string) to a single function or composition of
/// functions. For single functions multiple engine alternatives may be registered that share the
/// same metadata.
#[derive(Debug)]
pub struct FunctionRegistry {
    /// The function map which links function ids to function types
    /// (functions with alternatives or compositions).
    function_map: RwLock<FunctionMap>,
    /// The work queue where function loading tasks may be inserted.
    work_queue: WorkQueue,
}

impl FunctionRegistry {
    /// Creates a new FunctionRegistry object.
    pub fn new(
        work_queue: WorkQueue,
        type_map: &BTreeMap<EngineType, DomainType>,
        drivers: &BTreeMap<EngineType, &'static dyn Driver>,
        domains: &BTreeMap<DomainType, Arc<Box<dyn MemoryDomain>>>,
    ) -> Self {
        let mut function_map = BTreeMap::new();

        // insert all system functons
        for (engine_type, driver) in drivers.iter() {
            let system_functions = get_system_functions(*engine_type);
            for (system_function, context_size) in system_functions {
                let func_id = Arc::new(system_function.to_string());

                // get the config from the parser
                let function_config = driver
                    .parse_function(
                        String::from(""),
                        domains.get(type_map.get(engine_type).unwrap()).unwrap(),
                    )
                    .unwrap();
                match function_config.config {
                    FunctionConfig::SysConfig(_) => (),
                    _ => panic!("parsing system function did not return system config"),
                };
                let func_alt = FunctionAlternative {
                    engine: *engine_type,
                    context_size,
                    path: String::new(),
                    function: Mutex::new(Some(
                        (Box::pin(futures::future::ready(Ok(Arc::new(function_config))))
                            as Pin<Box<dyn Future<Output = DandelionResult<_>> + Send>>)
                            .shared(),
                    )),
                };

                // get metadata
                let func_metadata = Metadata {
                    input_sets: get_system_function_input_sets(system_function)
                        .into_iter()
                        .map(|name| (name, None))
                        .collect(),
                    output_sets: Arc::new(get_system_function_output_sets(system_function)),
                };

                if let Err(err) =
                    fmap_insert_function(&mut function_map, func_id, func_alt, func_metadata, true)
                {
                    error!("Failed to insert system function: {:?}", err);
                    panic!("Function registry initialization failed!");
                }
            }
        }

        return FunctionRegistry {
            function_map: RwLock::new(function_map),
            work_queue,
        };
    }

    /// Returns the function corresponding to the given function identifier. The returned FunctionType
    /// object represents either a single function (SystemFunction, Function) or a composition of
    /// functions (Composition).
    pub fn get_function(&self, function_id: &FunctionId) -> DandelionResult<FunctionType> {
        let lock_guard = self
            .function_map
            .read()
            .expect("Function registry lock poisoned!");
        match lock_guard.get(&(**function_id)) {
            Some(x) => Ok(x.clone()),
            None => Err(DandelionError::FunctionRegistry(
                FunctionRegistryError::UnknownFunction((**function_id).clone()),
            )),
        }
    }

    /// Returns an atomic reference to the metadata of the given function identifier.
    pub fn get_metadata(&self, function_id: &FunctionId) -> DandelionResult<Arc<Metadata>> {
        let lock_guard = self
            .function_map
            .read()
            .expect("Function registry lock poisoned!");
        match lock_guard.get(&(**function_id)) {
            Some(func_type) => match func_type {
                FunctionType::SystemFunction(func_info) => Ok(func_info.metadata.clone()),
                FunctionType::Function(func_info) => Ok(func_info.metadata.clone()),
                FunctionType::Composition(comp_info) => Ok(comp_info.metadata.clone()),
            },
            None => Err(DandelionError::FunctionRegistry(
                FunctionRegistryError::UnknownFunction((**function_id).clone()),
            )),
        }
    }

    /// Inserts the function into the function registry. If the function identifier is already the
    /// metadata is expected to match the already existing one.
    pub fn insert_function(
        &self,
        function_id: FunctionId,
        engine_type: EngineType,
        context_size: usize,
        path: String,
        metadata: Metadata,
    ) -> DandelionResult<()> {
        // check that path exists
        if !Path::new(&path).exists() {
            return Err(DandelionError::FunctionRegistry(
                FunctionRegistryError::BinaryNotFound,
            ));
        }

        let func_alt = FunctionAlternative {
            engine: engine_type,
            context_size,
            path,
            function: Mutex::new(None),
        };
        let mut lock_guard = self
            .function_map
            .write()
            .expect("Function registry lock poisoned!");
        fmap_insert_function(&mut lock_guard, function_id, func_alt, metadata, false)
    }

    /// Inserts the composition into the function registry.
    pub fn insert_compositions(&self, composition_desc: &str) -> DandelionResult<()> {
        // TODO: might want to return the parsing issue back to the user in a better way
        let module = dparser::parse(composition_desc).map_err(|parse_error| {
            print_errors(composition_desc, parse_error);
            DandelionError::Composition(CompositionError::ParsingError)
        })?;
        let comp_vec = Composition::from_module(module, &self)?;
        let mut lock_guard = self
            .function_map
            .write()
            .expect("Function registry lock poisoned!");
        for (comp_name, composition, metadata) in comp_vec.into_iter() {
            fmap_insert_composition(&mut lock_guard, comp_name, composition, metadata)?;
        }
        Ok(())
    }

    /// Load the given function info of given engine type.
    /// TODO: should be independent of the registry and moved into the worker in the future
    pub async fn load_function(
        &self,
        function_alt: Arc<FunctionAlternative>,
        driver: &'static dyn Driver,
        memory_domain: Arc<Box<dyn MemoryDomain>>,
        caching: bool,
        mut recorder: Recorder,
    ) -> DandelionResult<(Context, FunctionConfig)> {
        // check if a function future already exists
        let mut lock_guard = function_alt.function.lock().await;
        let func_future = if lock_guard.is_some() {
            lock_guard.clone().unwrap()
        } else {
            let new_future = (Box::pin(load_local(
                memory_domain.clone(),
                driver,
                recorder.get_sub_recorder(),
                self.work_queue.clone(), // TODO: remove clone
                vec![function_alt.engine],
                function_alt.path.clone(),
            ))
                as Pin<Box<dyn Future<Output = DandelionResult<_>> + Send>>)
                .shared();
            if caching {
                *lock_guard = Some(new_future.clone());
            }
            new_future
        };
        drop(lock_guard);

        // load the function
        let function = func_future.await?;
        let function_config = function.config.clone();
        recorder.record(RecordPoint::LoadQueue);
        let context_work_done = self
            .work_queue
            .do_work(
                machine_interface::function_driver::WorkToDo::LoadingArguments {
                    function,
                    domain: memory_domain,
                    recorder: recorder.get_sub_recorder(),
                    ctx_size: function_alt.context_size,
                },
                vec![function_alt.engine],
            )
            .await;
        recorder.record(RecordPoint::LoadDequeue);
        let function_context = context_work_done?.get_context();
        return Ok((function_context, function_config));
    }

    /// Checks if a function identifier is registered in the function registry.
    pub fn exists_id(&self, function_id: &FunctionId) -> bool {
        let lock_guard = self
            .function_map
            .read()
            .expect("Function registry lock is poisoned!");
        lock_guard.contains_key(&(**function_id))
    }

    /// Checks if a function name is registered in the function registry.
    pub fn exists_name(&self, function_name: &String) -> bool {
        let lock_guard = self
            .function_map
            .read()
            .expect("Function registry lock is poisoned!");
        lock_guard.contains_key(function_name)
    }
}

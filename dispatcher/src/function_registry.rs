use crate::queue::WorkQueue;
use dandelion_commons::{
    records::{RecordPoint, Recorder},
    CompositionError, DandelionError, DandelionResult, FunctionId, FunctionRegistryError,
};
use dparser::print_errors;
use futures::{future, Future, FutureExt};
use itertools::Itertools;
use log::error;
use machine_interface::{
    composition::{
        Composition, FunctionDependencies, InputSetDescriptor, JoinStrategy, ShardingMode,
    },
    function_driver::{
        system_driver::{get_system_function_input_sets, get_system_function_output_sets},
        Driver, Function, FunctionConfig, Metadata, WorkToDo,
    },
    machine_config::{get_system_functions, DomainType, EngineType},
    memory_domain::MemoryDomain,
};
use std::{
    collections::{btree_map::Entry, BTreeMap, BTreeSet},
    path::Path,
    pin::Pin,
    sync::{Arc, RwLock},
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
    pub function:
        future::Shared<Pin<Box<dyn Future<Output = DandelionResult<Arc<Function>>> + Send>>>,
}

impl FunctionAlternative {
    /// Load the given function info of given engine type.
    pub async fn load_function(
        &self,
        driver: &'static dyn Driver,
        work_queue: WorkQueue,
        memory_domain: Arc<Box<dyn MemoryDomain>>,
        caching: bool,
        recorder: Recorder,
    ) -> DandelionResult<Arc<Function>> {
        // load the function
        if caching {
            self.function.clone().await
        } else {
            load_local(
                memory_domain.clone(),
                driver,
                recorder,
                work_queue,
                vec![self.engine],
                self.path.clone(),
            )
            .await
        }
    }
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
}

impl FunctionRegistry {
    /// Creates a new FunctionRegistry object.
    pub fn new(
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
                    function: (Box::pin(futures::future::ready(Ok(Arc::new(function_config))))
                        as Pin<Box<dyn Future<Output = DandelionResult<_>> + Send>>)
                        .shared(),
                };

                // get metadata
                let func_metadata = Metadata {
                    input_sets: get_system_function_input_sets(system_function)
                        .into_iter()
                        .map(|name| (name, None))
                        .collect(),
                    output_sets: get_system_function_output_sets(system_function),
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
        static_domain: Arc<Box<dyn MemoryDomain>>,
        driver: &'static dyn Driver,
        work_queue: WorkQueue,
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

        // TODO: check if timestamp is still useful this way
        let new_future = (Box::pin(load_local(
            static_domain,
            driver,
            Recorder::new(function_id.clone(), std::time::Instant::now()),
            work_queue,
            vec![engine_type],
            path.clone(),
        )) as Pin<Box<dyn Future<Output = DandelionResult<_>> + Send>>)
            .shared();

        let func_alt = FunctionAlternative {
            engine: engine_type,
            context_size,
            path,
            function: new_future,
        };
        let mut lock_guard = self
            .function_map
            .write()
            .expect("Function registry lock poisoned!");
        fmap_insert_function(&mut lock_guard, function_id, func_alt, metadata, false)
    }

    /// For each composition the composition set indexes start enumerating the input sets from 0.
    /// The output sets are enumerated starting with the number directly after the highest input set index.
    /// For internal numbering there are no guarnatees.
    /// TODO: add validation for the function input / output set names against the locally registered meta data (or at least the number)
    pub(super) fn composition_from_module(
        &self,
        module: dparser::Module,
    ) -> DandelionResult<Vec<(FunctionId, Composition, Metadata)>> {
        let mut composition_ids = BTreeSet::new();
        let mut known_functions = BTreeMap::new();
        let mut compositions = Vec::new();
        for item in module.0.iter() {
            match item {
                dparser::Item::FunctionDecl(fdecl) => {
                    if !self.exists_name(&fdecl.v.name) {
                        return Err(DandelionError::Composition(
                            CompositionError::ContainsInvalidFunction(fdecl.v.name.clone()),
                        ));
                    }
                    known_functions.insert(fdecl.v.name.clone(), fdecl);
                }
                dparser::Item::Composition(comp) => {
                    // check if composition name is already taken
                    if self.exists_name(&comp.v.name) || composition_ids.contains(&comp.v.name) {
                        return Err(DandelionError::Composition(
                            CompositionError::DuplicateIdentifier(comp.v.name.clone()),
                        ));
                    }
                    composition_ids.insert(comp.v.name.clone());

                    // add composition input sets
                    let mut set_counter = 0usize;
                    let mut set_numbers = BTreeMap::new();
                    for input_set_name in comp.v.params.iter() {
                        match set_numbers.entry(input_set_name.clone()) {
                            Entry::Vacant(v) => v.insert(set_counter),
                            Entry::Occupied(_) => {
                                return Err(DandelionError::Composition(
                                    CompositionError::DuplicateSetName,
                                ))
                            }
                        };
                        set_counter += 1;
                    }
                    let mut output_map = BTreeMap::new();
                    let output_sets_start = set_counter;
                    // add composition output sets
                    for (output_index, output_set_name) in comp.v.returns.iter().enumerate() {
                        match set_numbers.entry(output_set_name.clone()) {
                            Entry::Vacant(v) => {
                                v.insert(set_counter);
                                output_map.insert(set_counter, output_index);
                                set_counter += 1;
                            }
                            // output set is input set
                            Entry::Occupied(occupied) => {
                                output_map.insert(*occupied.get(), output_index);
                            }
                        };
                    }
                    let output_sets_end = set_counter;
                    // add all return sets from functions
                    let composition_set_identifiers = comp
                        .v
                        .statements
                        .iter()
                        .flat_map(|statement| match statement {
                            dparser::Statement::FunctionApplication(function_application) => {
                                function_application.v.rets.iter().map(|ret| ret.v.ident.clone())
                            }
                            dparser::Statement::Loop(_) =>
                                todo!("loop semantics need to be fleshed out and compositions extended to acoomodate them"),
                        });
                    for set_identifier in composition_set_identifiers {
                        match set_numbers.entry(set_identifier.clone()) {
                            Entry::Vacant(v) => {
                                v.insert(set_counter);
                                set_counter += 1;
                            }
                            Entry::Occupied(o) => {
                                if output_sets_start <= *o.get() && *o.get() < output_sets_end {
                                    continue;
                                } else {
                                    return Err(DandelionError::Composition(
                                        CompositionError::DuplicateSetName,
                                    ));
                                }
                            }
                        }
                    }

                    // have enumerated all set that are available so can start putting the composition together
                    let dependencies = comp
                        .v
                        .statements
                        .iter()
                        .map(|statement| match statement {
                            dparser::Statement::FunctionApplication(function_application) => {
                                let function_decl = known_functions
                                    .get(&function_application.v.name)
                                    .ok_or_else(|| DandelionError::Composition(CompositionError::ContainsInvalidFunction(function_application.v.name.clone())))?;
                                if function_decl.v.params.len() < function_application.v.args.len()
                                    || function_decl.v.returns.len()
                                        < function_application.v.rets.len()
                                {
                                    return Err(DandelionError::Composition(CompositionError::ContainsInvalidFunction(function_application.v.name.clone())));
                                }
                                // find the indeces of the sets in the function application by looking though the definition
                                let mut input_set_ids = Vec::new();
                                input_set_ids
                                    .try_reserve(function_decl.v.params.len()).map_err(|_| DandelionError::OutOfMemory)?;
                                input_set_ids.resize(function_decl.v.params.len(), None);
                                for argument in function_application.v.args.iter() {
                                    if let Some(index) =
                                        function_decl.v.params.iter().position(
                                            |param_name| argument.v.name == *param_name,
                                        )
                                    {
                                        let set_id = set_numbers.get(&argument.v.ident).ok_or_else(
                                            ||DandelionError::Composition(CompositionError::FunctionInvalidIdentifier(
                                                format!("Could not find compositon set for argument {} of function {}",
                                                argument.v.ident, function_application.v.name)
                                            )),
                                        )?;
                                        input_set_ids[index] = Some(InputSetDescriptor {
                                            composition_id: *set_id,
                                            sharding:
                                            ShardingMode::from_parser_sharding(
                                                &argument.v.sharding,
                                            ),
                                            optional: argument.v.optional }
                                        );
                                    } else {
                                        return Err(
                                            DandelionError::Composition(CompositionError::FunctionInvalidIdentifier(
                                                format!("could not find index for input set {} for function {}",
                                                argument.v.name, function_application.v.name)
                                            )),
                                        );
                                    }
                                }

                                // find the join order
                                let mut all_sets: Vec<_> =
                                    (0..function_decl.v.params.len())
                                    .map(|index| Some(index)).collect();
                                let mut join_set_order = Vec::new();
                                join_set_order.try_reserve(function_decl.v.params.len())
                                    .or(Err(DandelionError::OutOfMemory))?;
                                let mut join_strategies = Vec::new();
                                join_strategies.try_reserve(function_decl.v.params.len())
                                    .or(Err(DandelionError::OutOfMemory))?;
                                if let Some(strategy) = function_application.v.join_strategy.as_ref().and_then(|strategy| if strategy.join_strategy_order.is_empty() || strategy.join_strategies.is_empty() {None} else {Some(strategy)}) {
                                    for set_name in strategy.join_strategy_order.iter() {
                                        let set_index = function_decl.v.params.iter()
                                            .position(|param_name| *param_name == *set_name)
                                            .ok_or_else(|| DandelionError::Composition(CompositionError::FunctionInvalidIdentifier(
                                                format!("Join order for {} contains invalid set name: {}", function_application.v.name, set_name))
                                            ))?;
                                        all_sets[set_index] = None;
                                        join_set_order.push(set_index);
                                    }
                                    for join_strategy in strategy.join_strategies.iter() {
                                        join_strategies.push(JoinStrategy::from_parser_strategy(&join_strategy))
                                    }
                                }

                                // find the index set index in the original definition for each return set in the application
                                let mut output_set_ids = Vec::new();
                                output_set_ids
                                    .try_reserve(function_decl.v.returns.len())
                                    .or(Err(DandelionError::OutOfMemory))?;
                                output_set_ids.resize(function_decl.v.returns.len(), None);
                                for return_set in function_application.v.rets.iter() {
                                    if let Some(index) =
                                        function_decl.v.returns.iter().position(
                                            |return_name| {
                                                return_set.v.name == *return_name
                                            },
                                        )
                                    {
                                        let set_id = set_numbers.get(&return_set.v.ident).ok_or(
                                            DandelionError::Composition(CompositionError::FunctionInvalidIdentifier(
                                                return_set.v.ident.clone(),
                                            )),
                                        )?;
                                        output_set_ids[index] = Some(*set_id);
                                    } else {
                                        return Err(
                                            DandelionError::Composition(CompositionError::FunctionInvalidIdentifier(
                                                return_set.v.ident.clone(),
                                            )),
                                        );
                                    }
                                }
                                Ok(FunctionDependencies {
                                    function: Arc::new(function_decl.v.name.clone()),
                                    input_set_ids,
                                    join_info: (join_set_order, join_strategies),
                                    output_set_ids,
                                })
                            }
                            dparser::Statement::Loop(_) => {
                                todo!("Need to implement loop support in compositions")
                            }
                        })
                        .collect::<DandelionResult<Vec<_>>>()?;
                    let metadata = Metadata {
                        input_sets: comp
                            .v
                            .params
                            .iter()
                            .map(|name| (name.clone(), None))
                            .collect_vec()
                            .into(),
                        output_sets: comp
                            .v
                            .returns
                            .iter()
                            .map(|name| name.clone())
                            .collect_vec()
                            .into(),
                    };
                    compositions.push((
                        Arc::new(comp.v.name.clone()),
                        Composition {
                            dependencies,
                            output_map,
                        },
                        metadata,
                    ));
                }
            }
        }

        Ok(compositions)
    }

    /// Inserts the composition into the function registry.
    pub fn insert_compositions(&self, composition_desc: &str) -> DandelionResult<()> {
        // TODO: might want to return the parsing issue back to the user in a better way
        let module = dparser::parse(composition_desc).map_err(|parse_error| {
            print_errors(composition_desc, parse_error);
            DandelionError::Composition(CompositionError::ParsingError)
        })?;
        let comp_vec = self.composition_from_module(module)?;
        let mut lock_guard = self
            .function_map
            .write()
            .expect("Function registry lock poisoned!");
        for (comp_name, composition, metadata) in comp_vec.into_iter() {
            fmap_insert_composition(&mut lock_guard, comp_name, composition, metadata)?;
        }
        Ok(())
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

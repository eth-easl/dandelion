use dandelion_commons::{
    dandelion_err, err_dandelion, CompositionError, DandelionError, DandelionResult, FunctionId,
    FunctionRegistryError,
};
use dparser::print_errors;
use log::error;
use machine_interface::{
    composition::Composition,
    function_driver::{
        functions::{FunctionAlternative, FunctionConfig},
        system_driver::{
            get_system_function_input_sets, get_system_function_output_sets, SYSTEM_FUNCTIONS,
        },
        Metadata,
    },
    machine_config::EngineType,
    memory_domain::MemoryDomain,
};
use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, RwLock},
};

use crate::function_registry::composition_builder::CompositionBuilder;

mod composition_builder;

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
            None => err_dandelion!(DandelionError::FunctionRegistry(
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
                        return err_dandelion!(DandelionError::FunctionRegistry(
                            FunctionRegistryError::InvalidSystemInsert((*key).clone()),
                        ));
                    }
                    info
                }
                FunctionType::Function(info) => {
                    if is_system {
                        return err_dandelion!(DandelionError::FunctionRegistry(
                            FunctionRegistryError::InvalidUserInsert((*key).clone()),
                        ));
                    }
                    info
                }
                FunctionType::Composition(_) => {
                    return err_dandelion!(DandelionError::FunctionRegistry(
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
                return err_dandelion!(DandelionError::FunctionRegistry(
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
            return err_dandelion!(DandelionError::FunctionRegistry(
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
    pub fn new(domains: &Vec<Arc<Box<dyn MemoryDomain>>>) -> Self {
        let mut function_map = BTreeMap::new();

        // insert all system functons
        for &(engine_type, system_function, context_size) in SYSTEM_FUNCTIONS {
            let func_id = Arc::new(system_function.to_string());

            // get the config from the parser
            let function_config = engine_type
                .parse_function(
                    String::from(""),
                    &domains[engine_type.get_domain_type() as usize],
                )
                .unwrap();
            match function_config.config {
                FunctionConfig::SysConfig(_) => (),
                _ => panic!("parsing system function did not return system config"),
            };
            let func_alt = FunctionAlternative::new_loaded(
                engine_type,
                context_size,
                String::new(),
                domains[engine_type.get_domain_type() as usize].clone(),
                Arc::new(function_config),
            );

            // get metadata
            let func_metadata = Metadata {
                input_sets: get_system_function_input_sets(system_function)
                    .into_iter()
                    .map(|name| (name, None))
                    .collect(),
                output_sets: get_system_function_output_sets(system_function),
                min_set_size: 0,
            };

            if let Err(err) =
                fmap_insert_function(&mut function_map, func_id, func_alt, func_metadata, true)
            {
                error!("Failed to insert system function: {:?}", err);
                panic!("Function registry initialization failed!");
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
            None => err_dandelion!(DandelionError::FunctionRegistry(
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
            None => err_dandelion!(DandelionError::FunctionRegistry(
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
        context_size: usize,
        path: String,
        metadata: Metadata,
    ) -> DandelionResult<()> {
        // check that path exists
        if !Path::new(&path).exists() {
            return err_dandelion!(DandelionError::FunctionRegistry(
                FunctionRegistryError::BinaryNotFound,
            ));
        }

        log::trace!(
            "Inserting function with id: {} and path: {}",
            function_id,
            path
        );

        let func_alt = FunctionAlternative::new_unloaded(
            engine_type,
            context_size,
            path,
            static_domain.clone(),
        );

        let mut lock_guard = self
            .function_map
            .write()
            .expect("Function registry lock poisoned!");
        fmap_insert_function(&mut lock_guard, function_id, func_alt, metadata, false)
    }

    /// For each composition the composition set indexes start enumerating the input sets from 0.
    /// The output sets are enumerated starting with the number directly after the highest input set index.
    /// For internal numbering there are no guarnatees.
    pub(super) fn composition_from_module(
        &self,
        module: dparser::Module,
    ) -> DandelionResult<Vec<(FunctionId, Composition, Metadata)>> {
        let mut builder = CompositionBuilder::new(self);
        for item in module.0.iter() {
            match item {
                dparser::Item::FunctionDecl(fdecl) => {
                    builder.add_declaration(fdecl.clone())?;
                }
                dparser::Item::Composition(comp) => {
                    builder.add_composition(&comp.v)?;
                }
            }
        }
        Ok(builder.finish())
    }

    /// Inserts the composition into the function registry.
    pub fn insert_compositions(&self, composition_desc: &str) -> DandelionResult<()> {
        // TODO: might want to return the parsing issue back to the user in a better way
        let module = dparser::parse(composition_desc).map_err(|parse_error| {
            print_errors(composition_desc, parse_error);
            dandelion_err!(DandelionError::Composition(CompositionError::ParsingError))
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

    /// Parses the compositions without inserting it into the registry.
    pub fn parse_compositions(
        &self,
        composition_desc: &str,
    ) -> DandelionResult<Vec<(FunctionId, Composition, Metadata)>> {
        // TODO: might want to return the parsing issue back to the user in a better way
        let module = dparser::parse(composition_desc).map_err(|parse_error| {
            print_errors(composition_desc, parse_error);
            dandelion_err!(DandelionError::Composition(CompositionError::ParsingError))
        })?;
        self.composition_from_module(module)
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

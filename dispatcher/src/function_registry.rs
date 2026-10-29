use crate::dynamic::{SelectorInput, SELECTION_SET};
use composition::{CompositionTemplate, Registry};
use dandelion_commons::{
    dandelion_err, err_dandelion, CompositionError, DandelionError, DandelionResult, FunctionId,
    FunctionRegistryError, SystemFunction,
};
use itertools::Itertools;
use log::{error, trace};
use machine_interface::{
    function_driver::{
        functions::FunctionAlternative,
        system_driver::{
            get_system_function_input_sets, get_system_function_output_sets, SYSTEM_FUNCTIONS,
        },
    },
    machine_config::EngineType,
};
use memory::{context::MemoryDomain, data::Metadata};
use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, RwLock},
};

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

/// What a dynamic function adds on top of its selector, see [`crate::dynamic`].
#[derive(Debug)]
pub struct DynamicSignature {
    /// How each input set is passed to the selector, in input set order.
    pub selector_inputs: Vec<SelectorInput>,
    /// The output sets of the dynamic function, which the selected function has to produce.
    pub output_sets: Vec<String>,
}

/// Struct holding a dynamic function. This struct can be cloned cheaply and given to the
/// dispatcher for function execution.
#[derive(Debug, Clone)]
pub struct MetaFunctionInfo {
    /// The selector, its metadata holds the input sets of the dynamic function and the
    /// [`SELECTION_SET`] as the only output set.
    pub selector: FunctionInfo,
    pub signature: Arc<DynamicSignature>,
}

/// Struct holding the parsed composition and corresponding metadata. This struct
/// can be cloned cheaply and given to the scheduler for function execution.
#[derive(Debug, Clone)]
pub struct CompositionInfo {
    /// The engine alternatives to execute the functions.
    pub composition: Arc<CompositionTemplate>,
    /// The metadata that applies to all function alternatives.
    pub metadata: Arc<Metadata>,
}

#[derive(Debug, Clone)]
pub enum FunctionType {
    /// A system function.
    SystemFunction(SystemFunction),
    /// A user defined function.
    Function(FunctionInfo),
    /// A user defined function selecting the function to run on its inputs.
    MetaFunction(MetaFunctionInfo),
    /// A composition of functions.
    Composition(CompositionInfo),
}

/// A `BTreeMap` linking function identifiers to function types.
type FunctionMap = BTreeMap<String, FunctionType>;

// inserts the function into the function map, as dynamic function if a signature is given
fn fmap_insert_function(
    fmap: &mut FunctionMap,
    key: FunctionId,
    func_alt: FunctionAlternative,
    func_meta: Metadata,
    dynamic: Option<DynamicSignature>,
) -> DandelionResult<()> {
    match fmap.get(&(*key)) {
        Some(entry) => {
            let func_info = match (entry, &dynamic) {
                (FunctionType::Function(info), None) => info,
                (FunctionType::MetaFunction(info), Some(_)) => &info.selector,
                (FunctionType::SystemFunction(_), _) => {
                    return err_dandelion!(DandelionError::FunctionRegistry(
                        FunctionRegistryError::InvalidSystemInsert((*key).clone()),
                    ));
                }
                _ => {
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
            let entry = match dynamic {
                None => FunctionType::Function(func_info),
                Some(signature) => FunctionType::MetaFunction(MetaFunctionInfo {
                    selector: func_info,
                    signature: Arc::new(signature),
                }),
            };
            fmap.insert((*key).clone(), entry);
        }
    };
    Ok(())
}

// inserts the function composition into the function map
fn fmap_insert_composition(
    fmap: &mut FunctionMap,
    key: FunctionId,
    composition: CompositionTemplate,
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
    pub fn new() -> Self {
        let mut function_map = BTreeMap::new();

        // insert all system functons
        for &system_function in SYSTEM_FUNCTIONS {
            if let Some(previous) = function_map.insert(
                system_function.to_string(),
                FunctionType::SystemFunction(system_function),
            ) {
                error!(
                    "Failed to insert system function: {:?} already present: {:?}",
                    system_function.to_string(),
                    previous
                );
                panic!("Function registry initialization failed!");
            }
        }

        FunctionRegistry {
            function_map: RwLock::new(function_map),
        }
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

    /// Inserts the function into the function registry.
    /// If the function identifier is already the metadata is expected to match the already existing one.
    pub fn insert_function(
        &self,
        function_id: FunctionId,
        engine_type: EngineType,
        static_domain: Arc<Box<dyn MemoryDomain>>,
        context_size: usize,
        path: String,
        metadata: Metadata,
    ) -> DandelionResult<()> {
        self.insert_alternative(
            function_id,
            engine_type,
            static_domain,
            context_size,
            path,
            metadata,
            None,
        )
    }

    /// Inserts a dynamic function into the function registry, see [`crate::dynamic`].
    /// The metadata describes the dynamic function, the selector binary gets the sets named in
    /// `metadata_sets` replaced by their metadata and returns the [`SELECTION_SET`] instead.
    pub fn insert_dynamic_function(
        &self,
        function_id: FunctionId,
        engine_type: EngineType,
        static_domain: Arc<Box<dyn MemoryDomain>>,
        context_size: usize,
        path: String,
        mut metadata: Metadata,
        metadata_sets: &[String],
    ) -> DandelionResult<()> {
        if let Some(unknown) = metadata_sets
            .iter()
            .find(|name| !metadata.input_sets.iter().any(|(input, _)| input == *name))
        {
            return err_dandelion!(DandelionError::FunctionRegistry(
                FunctionRegistryError::InvalidDynamicInsert(format!(
                    "Metadata set {} is not an input set of {}",
                    unknown, function_id
                )),
            ));
        }
        let selector_inputs = metadata
            .input_sets
            .iter()
            .map(|(name, static_set)| match metadata_sets.contains(name) {
                // a static set always replaces the input, so there is no input to describe
                true if static_set.is_some() => err_dandelion!(DandelionError::FunctionRegistry(
                    FunctionRegistryError::InvalidDynamicInsert(format!(
                        "Metadata set {} of {} has static data",
                        name, function_id
                    )),
                )),
                true => Ok(SelectorInput::Metadata),
                false => Ok(SelectorInput::Data),
            })
            .collect::<DandelionResult<_>>()?;
        let output_sets =
            std::mem::replace(&mut metadata.output_sets, vec![SELECTION_SET.to_string()]);
        self.insert_alternative(
            function_id,
            engine_type,
            static_domain,
            context_size,
            path,
            metadata,
            Some(DynamicSignature {
                selector_inputs,
                output_sets,
            }),
        )
    }

    /// Checks that the function selected by a dynamic function can be run in its place / has
    /// the same input and output sets.
    pub fn check_selection(
        &self,
        selection: &str,
        dynamic_info: &MetaFunctionInfo,
    ) -> DandelionResult<()> {
        let params = dynamic_info
            .selector
            .metadata
            .input_sets
            .iter()
            .map(|(name, _)| name.as_str())
            .collect_vec();
        let returns = dynamic_info
            .signature
            .output_sets
            .iter()
            .map(String::as_str)
            .collect_vec();
        self.check_declaration(selection, &params, &returns)
            .map_err(|err| {
                dandelion_err!(DandelionError::FunctionRegistry(
                    FunctionRegistryError::InvalidSelection(format!(
                        "Selected function {} cannot replace the dynamic function: {}",
                        selection, err
                    ))
                ))
            })
    }

    fn insert_alternative(
        &self,
        function_id: FunctionId,
        engine_type: EngineType,
        static_domain: Arc<Box<dyn MemoryDomain>>,
        context_size: usize,
        path: String,
        metadata: Metadata,
        dynamic: Option<DynamicSignature>,
    ) -> DandelionResult<()> {
        // check that path exists
        if !Path::new(&path).exists() {
            return err_dandelion!(DandelionError::FunctionRegistry(
                FunctionRegistryError::BinaryNotFound,
            ));
        }

        trace!(
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
        fmap_insert_function(&mut lock_guard, function_id, func_alt, metadata, dynamic)
    }

    /// Inserts the composition into the function registry.
    pub fn insert_compositions(
        &self,
        compositions: Vec<(FunctionId, CompositionTemplate, Metadata)>,
    ) -> DandelionResult<()> {
        let mut lock_guard = self
            .function_map
            .write()
            .expect("Function registry lock poisoned!");
        for (id, templ, meta) in compositions.into_iter() {
            trace!("Inserting composition with id: {}", id);
            fmap_insert_composition(&mut lock_guard, id, templ, meta)?;
        }
        Ok(())
    }
}

impl composition::Registry for FunctionRegistry {
    /// Confirms the declared function is registered with matching params and returns.
    fn check_declaration(
        &self,
        id: &str,
        params: &[&str],
        returns: &[&str],
    ) -> DandelionResult<()> {
        let lock_guard = self
            .function_map
            .read()
            .expect("Function registry lock poisoned!");
        let (input_sets, output_sets) = match lock_guard.get(id) {
            // TODO: refactor to work on iterators of string references, so we don't need to create a vec every time
            Some(func_type) => match func_type {
                FunctionType::SystemFunction(sys_function) => (
                    &get_system_function_input_sets(*sys_function),
                    &get_system_function_output_sets(*sys_function),
                ),
                FunctionType::Function(func_info) => (
                    &func_info.metadata.input_sets,
                    &func_info.metadata.output_sets,
                ),
                FunctionType::MetaFunction(dynamic_info) => (
                    &dynamic_info.selector.metadata.input_sets,
                    &dynamic_info.signature.output_sets,
                ),
                FunctionType::Composition(comp_info) => (
                    &comp_info.metadata.input_sets,
                    &comp_info.metadata.output_sets,
                ),
            },
            None => {
                return err_dandelion!(DandelionError::Composition(
                    CompositionError::InvalidFunctionDeclaration(format!(
                        "Unknown function {}",
                        id
                    )),
                ))
            }
        };

        // validate function arguments
        if params.len() != input_sets.len()
            || params
                .iter()
                .zip_eq(input_sets.iter())
                .any(|(decl_name, (metadata_name, _))| *decl_name != *metadata_name)
        {
            return err_dandelion!(DandelionError::Composition(
                CompositionError::InvalidFunctionDeclaration(format!(
                    "Function arguments do not match registration for function {}.",
                    id
                )),
            ));
        }

        // validated function returns
        if returns.len() != output_sets.len()
            || returns
                .iter()
                .zip_eq(output_sets.iter())
                .any(|(decl_name, metadata_name)| *decl_name != *metadata_name)
        {
            return err_dandelion!(DandelionError::Composition(
                CompositionError::InvalidFunctionDeclaration(format!(
                    "Function returns do not match registration for function {}.",
                    id
                )),
            ));
        }

        Ok(())
    }

    /// Simple lookup whether an identifier is already registered.
    fn id_exists(&self, id: &str) -> bool {
        let lock_guard = self
            .function_map
            .read()
            .expect("Function registry lock is poisoned!");
        lock_guard.contains_key(id)
    }

    /// Get min_set_bytes for a function.
    fn get_min_set_bytes(&self, id: &FunctionId) -> DandelionResult<Vec<usize>> {
        let lock_guard = self
            .function_map
            .read()
            .expect("Function registry lock poisoned!");
        match lock_guard.get(&(**id)) {
            Some(func_type) => match func_type {
                FunctionType::Function(func_info) => Ok(func_info.metadata.min_set_bytes.clone()),
                FunctionType::MetaFunction(dynamic_info) => {
                    Ok(dynamic_info.selector.metadata.min_set_bytes.clone())
                }
                FunctionType::Composition(comp_info) => {
                    Ok(comp_info.metadata.min_set_bytes.clone())
                }
                FunctionType::SystemFunction(_) => Ok(vec![]),
            },
            None => err_dandelion!(DandelionError::FunctionRegistry(
                FunctionRegistryError::UnknownFunction(id.to_string()),
            )),
        }
    }
}

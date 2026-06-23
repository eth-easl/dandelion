use dandelion_commons::{
    dandelion_err, err_dandelion, CompositionError, DandelionError, DandelionResult, FunctionId,
    FrontendError, FunctionRegistryError,
};
use dparser::print_errors;
use log::{debug, error, info};
use machine_interface::{
    composition::Composition,
    function_driver::{
        functions::{FunctionAlternative, SystemFunction},
        system_driver::SYSTEM_FUNCTIONS,
        Metadata,
    },
    machine_config::EngineType,
    memory_domain::MemoryDomain,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
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
    /// Stable ids for each composition node in parser order.
    pub composition_node_ids: Arc<Vec<String>>,
    /// Stable hash of the composition definition.
    pub composition_sha256: Arc<String>,
}

#[derive(Debug, Clone)]
pub enum FunctionType {
    /// A system function.
    SystemFunction(SystemFunction),
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
) -> DandelionResult<()> {
    match fmap.get_mut(&(*key)) {
        Some(entry) => {
            let func_info = match entry {
                FunctionType::Function(info) => info,
                FunctionType::SystemFunction(_) => {
                    return err_dandelion!(DandelionError::FunctionRegistry(
                        FunctionRegistryError::InvalidSystemInsert((*key).clone()),
                    ));
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
            fmap.insert((*key).clone(), FunctionType::Function(func_info));
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
    composition_node_ids: Vec<String>,
    composition_sha256: String,
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
                composition_node_ids: Arc::new(composition_node_ids),
                composition_sha256: Arc::new(composition_sha256),
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
    persistence: RwLock<Option<RegistryPersistence>>,
}

#[derive(Debug, Clone)]
struct RegistryPersistence {
    path: PathBuf,
    state: PersistedRegistry,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct PersistedRegistry {
    version: u32,
    #[serde(default)]
    functions: Vec<PersistedFunctionRegistration>,
    #[serde(default)]
    compositions: Vec<PersistedCompositionRegistration>,
    #[serde(default)]
    composition_sources: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedFunctionRegistration {
    name: String,
    engine_type: String,
    context_size: usize,
    path: String,
    binary_sha256: String,
    metadata: PersistedMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedCompositionRegistration {
    name: String,
    composition_sha256: String,
    metadata: PersistedMetadata,
    composition_node_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedMetadata {
    input_sets: Vec<String>,
    output_sets: Vec<String>,
    min_set_bytes: Vec<usize>,
}

fn metadata_snapshot(metadata: &Metadata) -> PersistedMetadata {
    PersistedMetadata {
        input_sets: metadata
            .input_sets
            .iter()
            .map(|(name, _)| name.clone())
            .collect(),
        output_sets: metadata.output_sets.clone(),
        min_set_bytes: metadata.min_set_bytes.clone(),
    }
}

fn metadata_from_persisted(metadata: PersistedMetadata) -> Metadata {
    Metadata {
        input_sets: metadata.input_sets.into_iter().map(|name| (name, None)).collect(),
        output_sets: metadata.output_sets,
        min_set_bytes: metadata.min_set_bytes,
    }
}

fn engine_type_name(engine_type: EngineType) -> &'static str {
    match engine_type {
        EngineType::System => "System",
        #[cfg(feature = "mmu")]
        EngineType::Process => "Process",
        #[cfg(feature = "kvm")]
        EngineType::Kvm => "Kvm",
        #[cfg(feature = "cheri")]
        EngineType::Cheri => "Cheri",
    }
}

fn parse_engine_type(engine_type: &str) -> DandelionResult<EngineType> {
    match engine_type {
        "System" => Ok(EngineType::System),
        #[cfg(feature = "mmu")]
        "Process" => Ok(EngineType::Process),
        #[cfg(feature = "kvm")]
        "Kvm" => Ok(EngineType::Kvm),
        #[cfg(feature = "cheri")]
        "Cheri" => Ok(EngineType::Cheri),
        _ => err_dandelion!(DandelionError::RequestError(FrontendError::InternalError(
            format!("Unknown engine type in registry snapshot: {engine_type}"),
        ))),
    }
}

fn hash_file(path: &Path) -> DandelionResult<String> {
    let bytes = fs::read(path).map_err(|_| {
        dandelion_err!(DandelionError::FunctionRegistry(
            FunctionRegistryError::BinaryNotFound,
        ))
    })?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn sha256_hex(bytes: impl AsRef<[u8]>) -> String {
    format!("{:x}", Sha256::digest(bytes.as_ref()))
}

fn join_strategy_name(strategy: &dparser::JoinFilterStrategy) -> &'static str {
    match strategy {
        dparser::JoinFilterStrategy::Cross => "cross",
        dparser::JoinFilterStrategy::Inner => "inner",
        dparser::JoinFilterStrategy::Left => "left",
        dparser::JoinFilterStrategy::Right => "right",
        dparser::JoinFilterStrategy::Full => "full",
    }
}


// collect all material needed to uniquely identify a composition node
fn composition_node_material(
    composition: &dparser::Composition,
    statement_index: usize,
    function_application: &dparser::FunctionApplication,
) -> String {
    let mut material = format!(
        "composition={}\nstatement_index={}\nfunction={}\n",
        composition.name, statement_index, function_application.name
    );
    for input in &function_application.args {
        material.push_str(&format!(
            "input:{}:{}:{}:{}\n",
            input.v.name,
            input.v.ident,
            input.v.optional,
            match input.v.sharding {
                dparser::Sharding::All => "all",
                dparser::Sharding::Keyed => "keyed",
                dparser::Sharding::Each => "each",
                dparser::Sharding::AnyKeyed => "anyKeyed",
                dparser::Sharding::AnyEach => "anyEach",
            }
        ));
    }
    for output in &function_application.rets {
        material.push_str(&format!("output:{}:{}\n", output.v.ident, output.v.name));
    }
    if let Some(join_strategy) = &function_application.join_strategy {
        for (name, strategy) in join_strategy
            .join_strategy_order
            .iter()
            .zip(join_strategy.join_strategies.iter())
        {
            material.push_str(&format!("join:{}:{}\n", name, join_strategy_name(strategy)));
        }
        if join_strategy.join_strategy_order.len() > join_strategy.join_strategies.len() {
            let last_name = &join_strategy.join_strategy_order[join_strategy.join_strategies.len()];
            material.push_str(&format!("join_terminal:{}\n", last_name));
        }
    }
    material
}

// Returns a stable hash of the composition node material
fn composition_node_id(
    composition: &dparser::Composition,
    statement_index: usize,
    function_application: &dparser::FunctionApplication,
) -> String {
    sha256_hex(composition_node_material(
        composition,
        statement_index,
        function_application,
    ))
}

fn persisted_metadata_from_composition(composition: &dparser::Composition) -> PersistedMetadata {
    PersistedMetadata {
        input_sets: composition.params.clone(),
        output_sets: composition.returns.clone(),
        min_set_bytes: vec![],
    }
}

fn persisted_composition_registration(
    composition: &dparser::Composition,
) -> PersistedCompositionRegistration {
    let composition_node_ids = composition
        .statements
        .iter()
        .enumerate()
        .map(|(statement_index, statement)| match statement {
            dparser::Statement::FunctionApplication(function_application) => {
                composition_node_id(composition, statement_index, &function_application.v)
            }
            dparser::Statement::Loop(_) => {
                unimplemented!("loop semantics need to be fleshed out before persistence")
            }
        })
        .collect::<Vec<_>>();

    let mut composition_material = format!(
        "name={}\nparams={:?}\nreturns={:?}\n",
        composition.name, composition.params, composition.returns
    );
    for node_id in &composition_node_ids {
        composition_material.push_str("node_id=");
        composition_material.push_str(node_id);
        composition_material.push('\n');
    }

    PersistedCompositionRegistration {
        name: composition.name.clone(),
        composition_sha256: sha256_hex(composition_material),
        metadata: persisted_metadata_from_composition(composition),
        composition_node_ids,
    }
}

fn persisted_composition_registrations(
    module: &dparser::Module,
) -> Vec<PersistedCompositionRegistration> {
    module
        .0
        .iter()
        .filter_map(|item| match item {
            dparser::Item::Composition(composition) => {
                Some(persisted_composition_registration(&composition.v))
            }
            dparser::Item::FunctionDecl(_) => None,
        })
        .collect()
}

fn write_registry_snapshot(path: &Path, state: &PersistedRegistry) -> DandelionResult<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|_| {
            dandelion_err!(DandelionError::RequestError(FrontendError::InternalError(
                format!("Failed to create registry snapshot directory {}", parent.display()),
            )))
        })?;
    }
    let tmp_path = path.with_extension("json.tmp");
    let serialized = serde_json::to_vec_pretty(state).map_err(|_| {
        dandelion_err!(DandelionError::RequestError(FrontendError::InternalError(
            "Failed to serialize registry snapshot".to_string(),
        )))
    })?;
    fs::write(&tmp_path, serialized).map_err(|_| {
        dandelion_err!(DandelionError::RequestError(FrontendError::InternalError(
            format!("Failed to write registry snapshot {}", tmp_path.display()),
        )))
    })?;
    fs::rename(&tmp_path, path).map_err(|_| {
        dandelion_err!(DandelionError::RequestError(FrontendError::InternalError(
            format!(
                "Failed to atomically replace registry snapshot {}",
                path.display()
            ),
        )))
    })?;
    debug!(
        "Wrote registry snapshot to {} (functions={}, compositions={}, sources={})",
        path.display(),
        state.functions.len(),
        state.compositions.len(),
        state.composition_sources.len()
    );
    Ok(())
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

        return FunctionRegistry {
            function_map: RwLock::new(function_map),
            persistence: RwLock::new(None),
        };
    }

    pub fn load_or_enable_persistence(
        &self,
        path: PathBuf,
        domains: &[Arc<Box<dyn MemoryDomain>>],
    ) -> DandelionResult<(usize, usize)> {
        if path.exists() {
            let restored = self.restore_from_snapshot(&path, domains)?;
            debug!(
                "Loaded registry snapshot from {} (functions={}, compositions={})",
                path.display(),
                restored.0,
                restored.1
            );
            Ok(restored)
        } else {
            self.enable_persistence(path)?;
            Ok((0, 0))
        }
    }

    pub fn enable_persistence(&self, path: PathBuf) -> DandelionResult<()> {
        let mut lock_guard = self
            .persistence
            .write()
            .expect("Function registry persistence lock poisoned!");
        let state = PersistedRegistry {
            version: 2,
            ..PersistedRegistry::default()
        };
        write_registry_snapshot(&path, &state)?;
        *lock_guard = Some(RegistryPersistence { path, state });
        Ok(())
    }

    fn persist_function_registration(
        &self,
        registration: PersistedFunctionRegistration,
    ) -> DandelionResult<()> {
        let mut lock_guard = self
            .persistence
            .write()
            .expect("Function registry persistence lock poisoned!");
        let Some(persistence) = lock_guard.as_mut() else {
            return Ok(());
        };
        debug!(
            "Persisting function registration {} to {}",
            registration.name,
            persistence.path.display()
        );
        persistence.state.functions.push(registration);
        write_registry_snapshot(&persistence.path, &persistence.state)
    }

    fn persist_composition_source(&self, composition_desc: &str) -> DandelionResult<()> {
        let mut lock_guard = self
            .persistence
            .write()
            .expect("Function registry persistence lock poisoned!");
        let Some(persistence) = lock_guard.as_mut() else {
            return Ok(());
        };
        debug!(
            "Persisting composition source to {}",
            persistence.path.display()
        );
        persistence
            .state
            .composition_sources
            .push(composition_desc.to_string());
        write_registry_snapshot(&persistence.path, &persistence.state)
    }

    fn persist_composition_registrations(
        &self,
        registrations: Vec<PersistedCompositionRegistration>,
    ) -> DandelionResult<()> {
        let mut lock_guard = self
            .persistence
            .write()
            .expect("Function registry persistence lock poisoned!");
        let Some(persistence) = lock_guard.as_mut() else {
            return Ok(());
        };
        debug!(
            "Persisting {} composition registrations to {}",
            registrations.len(),
            persistence.path.display()
        );
        persistence.state.compositions.extend(registrations);
        write_registry_snapshot(&persistence.path, &persistence.state)
    }

    // validates the snapshot file and restores the function registry from it
    fn restore_from_snapshot(
        &self,
        path: &Path,
        domains: &[Arc<Box<dyn MemoryDomain>>],
    ) -> DandelionResult<(usize, usize)> {
        let bytes = fs::read(path).map_err(|_| {
            dandelion_err!(DandelionError::RequestError(FrontendError::InternalError(
                format!("Failed to read registry snapshot {}", path.display()),
            )))
        })?;
        let state: PersistedRegistry = serde_json::from_slice(&bytes).map_err(|_| {
            dandelion_err!(DandelionError::RequestError(FrontendError::InternalError(
                format!("Failed to parse registry snapshot {}", path.display()),
            )))
        })?;
        // check that the snapshot version is supported
        if !(1..=2).contains(&state.version) {
            return err_dandelion!(DandelionError::RequestError(FrontendError::InternalError(
                format!(
                    "Unsupported registry snapshot version {} at {}",
                    state.version,
                    path.display()
                ),
            )));
        }

        // restore the functions from the snapshot
        {
            let mut lock_guard = self
                .function_map
                .write()
                .expect("Function registry lock poisoned!");

            // check that the binary hash matches the hash in the snapshot
            for registration in &state.functions {
                let actual_hash = hash_file(Path::new(&registration.path))?;
                if actual_hash != registration.binary_sha256 {
                    return err_dandelion!(DandelionError::RequestError(
                        FrontendError::InternalError(format!(
                            "Binary hash mismatch for {} at {}",
                            registration.name, registration.path
                        )),
                    ));
                }

                // parse the engine type and get the domain type
                let engine_type = parse_engine_type(&registration.engine_type)?;
                let domain_type = engine_type.get_domain_type();

                // create a new function alternative
                let func_alt = FunctionAlternative::new_unloaded(
                    engine_type,
                    registration.context_size,
                    registration.path.clone(),
                    domains[domain_type as usize].clone(),
                );

                // insert the function into the function map
                fmap_insert_function(
                    &mut lock_guard,
                    Arc::new(registration.name.clone()),
                    func_alt,
                    metadata_from_persisted(registration.metadata.clone()),
                )?;
            }
        }

        // restore the compositions from the snapshot
        let mut restored_compositions = 0usize;
        let mut composition_iter = state.compositions.iter();
        for composition_desc in &state.composition_sources {
            // parse the composition description
            let module = dparser::parse(composition_desc).map_err(|parse_error| {
                print_errors(composition_desc, parse_error);
                dandelion_err!(DandelionError::Composition(CompositionError::ParsingError))
            })?;

            // get the computed persisted composition registrations
            let computed_persisted = persisted_composition_registrations(&module);

            // get the composition vector from the module
            let comp_vec = self.composition_from_module(module)?;

            // insert the compositions into the function map
            let mut lock_guard = self
                .function_map
                .write()
                .expect("Function registry lock poisoned!");
            for ((comp_name, composition, metadata), computed) in
                comp_vec.into_iter().zip(computed_persisted.into_iter())
            {
                // if the snapshot version is 2 or higher, check that the persisted composition metadata matches the computed metadata
                // version 2 adds the composition sha256 and node ids to the persisted metadata
                if state.version >= 2 {
                    let Some(persisted) = composition_iter.next() else {
                        return err_dandelion!(DandelionError::RequestError(
                            FrontendError::InternalError(
                                "Registry snapshot is missing persisted composition metadata"
                                    .to_string(),
                            ),
                        ));
                    };
                    if persisted.name != computed.name
                        || persisted.composition_sha256 != computed.composition_sha256
                        || persisted.composition_node_ids != computed.composition_node_ids
                    {
                        return err_dandelion!(DandelionError::RequestError(
                            FrontendError::InternalError(format!(
                                "Persisted composition metadata mismatch for {}",
                                computed.name
                            )),
                        ));
                    }
                    // insert the persisted composition into the function map
                    fmap_insert_composition(
                        &mut lock_guard,
                        comp_name,
                        composition,
                        metadata,
                        persisted.composition_node_ids.clone(),
                        persisted.composition_sha256.clone(),
                    )?;
                } else {
                    // insert the computed composition into the function map
                    fmap_insert_composition(
                        &mut lock_guard,
                        comp_name,
                        composition,
                        metadata,
                        computed.composition_node_ids,
                        computed.composition_sha256,
                    )?;
                }
                restored_compositions += 1;
            }
        }
        if state.version >= 2 && composition_iter.next().is_some() {
            return err_dandelion!(DandelionError::RequestError(FrontendError::InternalError(
                "Registry snapshot contains more persisted compositions than composition sources"
                    .to_string(),
            )));
        }

        // update the persistence lock
        let mut persistence_lock = self
            .persistence
            .write()
            .expect("Function registry persistence lock poisoned!");
        *persistence_lock = Some(RegistryPersistence {
            path: path.to_path_buf(),
            state,
        });

        info!(
            "Restored registry snapshot from {} (functions={}, compositions={})",
            path.display(),
            persistence_lock
                .as_ref()
                .map(|p| p.state.functions.len())
                .unwrap_or(0),
            restored_compositions
        );
        Ok((
            persistence_lock
                .as_ref()
                .map(|p| p.state.functions.len())
                .unwrap_or(0),
            restored_compositions,
        ))
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
    pub fn get_min_set_bytes(&self, function_id: &FunctionId) -> DandelionResult<Vec<usize>> {
        let lock_guard = self
            .function_map
            .read()
            .expect("Function registry lock poisoned!");
        match lock_guard.get(&(**function_id)) {
            Some(func_type) => match func_type {
                FunctionType::Function(func_info) => Ok(func_info.metadata.min_set_bytes.clone()),
                FunctionType::Composition(comp_info) => {
                    Ok(comp_info.metadata.min_set_bytes.clone())
                }
                FunctionType::SystemFunction(_) => Ok(vec![]),
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

        let persisted_registration = PersistedFunctionRegistration {
            name: (*function_id).clone(),
            engine_type: engine_type_name(engine_type).to_string(),
            context_size,
            path: path.clone(),
            binary_sha256: hash_file(Path::new(&path))?,
            metadata: metadata_snapshot(&metadata),
        };

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
        fmap_insert_function(&mut lock_guard, function_id, func_alt, metadata)?;
        drop(lock_guard);
        self.persist_function_registration(persisted_registration)
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
        let persisted_compositions = persisted_composition_registrations(&module);
        let comp_vec = self.composition_from_module(module)?;
        let mut lock_guard = self
            .function_map
            .write()
            .expect("Function registry lock poisoned!");
        for ((comp_name, composition, metadata), persisted) in
            comp_vec.into_iter().zip(persisted_compositions.iter())
        {
            debug_assert_eq!(&*comp_name, &persisted.name);
            fmap_insert_composition(
                &mut lock_guard,
                comp_name,
                composition,
                metadata,
                persisted.composition_node_ids.clone(),
                persisted.composition_sha256.clone(),
            )?;
        }
        drop(lock_guard);
        self.persist_composition_source(composition_desc)?;
        self.persist_composition_registrations(persisted_compositions)
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

#[cfg(test)]
mod tests {
    use super::{FunctionRegistry, FunctionType};
    use machine_interface::{
        function_driver::Metadata,
        memory_domain::{malloc::MallocMemoryDomain, MemoryDomain, MemoryResource},
    };
    use serde_json::Value;
    use std::{
        fs,
        path::PathBuf,
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    };

    fn snapshot_path(label: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("dandelion_{label}_{unique}.json"))
    }

    #[test]
    fn persists_composition_sources() {
        let registry = FunctionRegistry::new();
        let path = snapshot_path("registry_composition");
        registry.enable_persistence(path.clone()).unwrap();
        let composition = r#"
            function HTTP (requests) => (headers, bodies);
            composition Composition (comp_requests) => (comp_headers, comp_bodies) {
                HTTP (requests = all comp_requests) => (comp_headers = headers, comp_bodies = bodies);
            }
        "#;
        registry.insert_compositions(composition).unwrap();

        let snapshot: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(2, snapshot["version"].as_u64().unwrap());
        assert_eq!(0, snapshot["functions"].as_array().unwrap().len());
        assert_eq!(1, snapshot["compositions"].as_array().unwrap().len());
        assert_eq!(1, snapshot["composition_sources"].as_array().unwrap().len());
        assert_eq!(
            composition,
            snapshot["composition_sources"][0].as_str().unwrap()
        );
        assert_eq!(
            "Composition",
            snapshot["compositions"][0]["name"].as_str().unwrap()
        );
        assert_eq!(
            1,
            snapshot["compositions"][0]["composition_node_ids"]
                .as_array()
                .unwrap()
                .len()
        );
        assert!(!snapshot["compositions"][0]["composition_sha256"]
            .as_str()
            .unwrap()
            .is_empty());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn persists_stable_composition_node_ids() {
        let registry = FunctionRegistry::new();
        let path = snapshot_path("registry_node_ids");
        registry.enable_persistence(path.clone()).unwrap();
        let composition = r#"
            function HTTP (requests) => (headers, bodies);
            composition Composition (comp_requests) => (comp_headers, comp_bodies) {
                HTTP (requests = all comp_requests) => (comp_headers = headers, comp_bodies = bodies);
            }
        "#;

        registry.insert_compositions(composition).unwrap();
        let first_snapshot: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        let first_node_id = first_snapshot["compositions"][0]["composition_node_ids"][0]
            .as_str()
            .unwrap()
            .to_string();
        let first_hash = first_snapshot["compositions"][0]["composition_sha256"]
            .as_str()
            .unwrap()
            .to_string();

        let second_path = snapshot_path("registry_node_ids_second");
        let second_registry = FunctionRegistry::new();
        second_registry
            .enable_persistence(second_path.clone())
            .unwrap();
        second_registry.insert_compositions(composition).unwrap();
        let second_snapshot: Value =
            serde_json::from_slice(&fs::read(&second_path).unwrap()).unwrap();
        let second_node_id = second_snapshot["compositions"][0]["composition_node_ids"][0]
            .as_str()
            .unwrap();
        let second_hash = second_snapshot["compositions"][0]["composition_sha256"]
            .as_str()
            .unwrap();

        assert_eq!(first_node_id, second_node_id);
        assert_eq!(first_hash, second_hash);

        let _ = fs::remove_file(path);
        let _ = fs::remove_file(second_path);
    }

    #[test]
    fn reloads_persisted_compositions() {
        let registry = FunctionRegistry::new();
        let path = snapshot_path("registry_reload_composition");
        registry.enable_persistence(path.clone()).unwrap();
        let composition = r#"
            function HTTP (requests) => (headers, bodies);
            composition Composition (comp_requests) => (comp_headers, comp_bodies) {
                HTTP (requests = all comp_requests) => (comp_headers = headers, comp_bodies = bodies);
            }
        "#;
        registry.insert_compositions(composition).unwrap();

        let reloaded = FunctionRegistry::new();
        let domains: Vec<Arc<Box<dyn MemoryDomain>>> = vec![];
        let (functions, compositions) = reloaded
            .load_or_enable_persistence(path.clone(), &domains)
            .unwrap();
        assert_eq!(0, functions);
        assert_eq!(1, compositions);

        match reloaded
            .get_function(&Arc::new(String::from("Composition")))
            .unwrap()
        {
            FunctionType::Composition(comp) => {
                assert_eq!(1, comp.composition_node_ids.len());
                assert!(!comp.composition_sha256.is_empty());
            }
            other => panic!("Expected composition, got {other:?}"),
        }

        let _ = fs::remove_file(path);
    }

    #[cfg(any(feature = "kvm", feature = "mmu", feature = "cheri"))]
    #[test]
    fn persists_function_registration_with_hash() {
        use machine_interface::machine_config::EngineType;

        let registry = FunctionRegistry::new();
        let path = snapshot_path("registry_function");
        registry.enable_persistence(path.clone()).unwrap();

        let domain = Arc::new(MallocMemoryDomain::init(MemoryResource::None).unwrap());
        let mut binary_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        binary_path.pop();
        binary_path.push("machine_interface/tests/data");
        #[cfg(feature = "kvm")]
        {
            binary_path.push(format!("test_elf_kvm_{}_basic", std::env::consts::ARCH));
        }
        #[cfg(all(not(feature = "kvm"), feature = "mmu"))]
        {
            binary_path.push(format!("test_elf_mmu_{}_basic", std::env::consts::ARCH));
        }
        #[cfg(all(not(feature = "kvm"), not(feature = "mmu"), feature = "cheri"))]
        {
            binary_path.push("test_elf_cheri_basic");
        }

        let metadata = Metadata {
            input_sets: vec![(String::from("in"), None)],
            output_sets: vec![String::from("out")],
            min_set_bytes: vec![64],
        };
        registry
            .insert_function(
                Arc::new(String::from("persisted_function")),
                {
                    #[cfg(feature = "kvm")]
                    {
                        EngineType::Kvm
                    }
                    #[cfg(all(not(feature = "kvm"), feature = "mmu"))]
                    {
                        EngineType::Process
                    }
                    #[cfg(all(not(feature = "kvm"), not(feature = "mmu"), feature = "cheri"))]
                    {
                        EngineType::Cheri
                    }
                },
                domain,
                4096,
                binary_path.to_str().unwrap().to_string(),
                metadata,
            )
            .unwrap();

        let snapshot: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        let functions = snapshot["functions"].as_array().unwrap();
        assert_eq!(1, functions.len());
        assert_eq!("persisted_function", functions[0]["name"].as_str().unwrap());
        assert_eq!(4096, functions[0]["context_size"].as_u64().unwrap());
        assert!(!functions[0]["binary_sha256"].as_str().unwrap().is_empty());
        let _ = fs::remove_file(path);
    }
}

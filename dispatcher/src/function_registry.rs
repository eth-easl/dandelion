use dandelion_commons::{
    dandelion_err, err_dandelion, CompositionError, DandelionError, DandelionResult, FrontendError,
    FunctionId, FunctionRegistryError,
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
    fs::{self, File},
    io::Write,
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExportedMetadata {
    pub input_sets: Vec<String>,
    pub output_sets: Vec<String>,
    pub min_set_bytes: Vec<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExportedFunctionAlternative {
    pub engine_type: String,
    pub context_size: usize,
    pub binary: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExportedFunctionRegistration {
    pub name: String,
    pub metadata: ExportedMetadata,
    pub alternatives: Vec<ExportedFunctionAlternative>,
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
    functions: Vec<PersistedFunctionRegistration>,
    compositions: Vec<PersistedCompositionSource>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedFunctionRegistration {
    name: String,
    engine_type: String,
    context_size: usize,
    path: String,
    binary_sha256: String,
    metadata: ExportedMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedCompositionSource {
    source: String,
}

fn metadata_snapshot(metadata: &Metadata) -> ExportedMetadata {
    ExportedMetadata {
        input_sets: metadata
            .input_sets
            .iter()
            .map(|(name, _)| name.clone())
            .collect(),
        output_sets: metadata.output_sets.clone(),
        min_set_bytes: metadata.min_set_bytes.clone(),
    }
}

fn metadata_from_persisted(metadata: ExportedMetadata) -> Metadata {
    Metadata {
        input_sets: metadata
            .input_sets
            .into_iter()
            .map(|name| (name, None))
            .collect(),
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

fn write_registry_snapshot(path: &Path, state: &PersistedRegistry) -> DandelionResult<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|_| {
            dandelion_err!(DandelionError::RequestError(FrontendError::InternalError(
                format!(
                    "Failed to create registry snapshot directory {}",
                    parent.display()
                ),
            )))
        })?;
    }
    let tmp_path = path.with_extension("json.tmp");
    let serialized = serde_json::to_vec_pretty(state).map_err(|_| {
        dandelion_err!(DandelionError::RequestError(FrontendError::InternalError(
            "Failed to serialize registry snapshot".to_string(),
        )))
    })?;
    let mut tmp_file = File::create(&tmp_path).map_err(|_| {
        dandelion_err!(DandelionError::RequestError(FrontendError::InternalError(
            format!("Failed to write registry snapshot {}", tmp_path.display()),
        )))
    })?;
    tmp_file.write_all(&serialized).map_err(|_| {
        dandelion_err!(DandelionError::RequestError(FrontendError::InternalError(
            format!("Failed to write registry snapshot {}", tmp_path.display()),
        )))
    })?;
    tmp_file.sync_all().map_err(|_| {
        dandelion_err!(DandelionError::RequestError(FrontendError::InternalError(
            format!("Failed to sync registry snapshot {}", tmp_path.display()),
        )))
    })?;
    drop(tmp_file);
    fs::rename(&tmp_path, path).map_err(|_| {
        dandelion_err!(DandelionError::RequestError(FrontendError::InternalError(
            format!(
                "Failed to atomically replace registry snapshot {}",
                path.display()
            ),
        )))
    })?;
    if let Some(parent) = path.parent() {
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|_| {
                dandelion_err!(DandelionError::RequestError(FrontendError::InternalError(
                    format!(
                        "Failed to sync registry snapshot directory {}",
                        parent.display()
                    ),
                )))
            })?;
    }
    debug!(
        "Wrote registry snapshot to {} (functions={}, composition sources={})",
        path.display(),
        state.functions.len(),
        state.compositions.len()
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
            version: 1,
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
        let mut next_state = persistence.state.clone();
        next_state.functions.push(registration);
        write_registry_snapshot(&persistence.path, &next_state)?;
        persistence.state = next_state;
        Ok(())
    }

    fn persist_compositions(&self, source: PersistedCompositionSource) -> DandelionResult<()> {
        let mut lock_guard = self
            .persistence
            .write()
            .expect("Function registry persistence lock poisoned!");
        let Some(persistence) = lock_guard.as_mut() else {
            return Ok(());
        };
        debug!("Persisting compositions to {}", persistence.path.display());
        let mut next_state = persistence.state.clone();
        next_state.compositions.push(source);
        write_registry_snapshot(&persistence.path, &next_state)?;
        persistence.state = next_state;
        Ok(())
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
        // Only the current snapshot format is supported.
        if state.version != 1 {
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
        for persisted_source in &state.compositions {
            // parse the composition description
            let module = dparser::parse(&persisted_source.source).map_err(|parse_error| {
                print_errors(&persisted_source.source, parse_error);
                dandelion_err!(DandelionError::Composition(CompositionError::ParsingError))
            })?;

            // get the composition vector from the module
            let comp_vec = self.composition_from_module(module)?;

            // insert the compositions into the function map
            let mut lock_guard = self
                .function_map
                .write()
                .expect("Function registry lock poisoned!");
            for (comp_name, composition, metadata) in comp_vec {
                fmap_insert_composition(&mut lock_guard, comp_name, composition, metadata)?;
                restored_compositions += 1;
            }
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

    pub fn export_function(
        &self,
        function_id: &FunctionId,
    ) -> DandelionResult<ExportedFunctionRegistration> {
        let lock_guard = self
            .function_map
            .read()
            .expect("Function registry lock poisoned!");
        match lock_guard.get(&(**function_id)) {
            Some(FunctionType::Function(func_info)) => {
                let alternatives_locked = func_info
                    .alternatives
                    .read()
                    .expect("Function registry lock poisoned!");
                let mut alternatives = Vec::with_capacity(alternatives_locked.len());
                for alternative in alternatives_locked.iter() {
                    alternatives.push(ExportedFunctionAlternative {
                        engine_type: engine_type_name(alternative.engine).to_string(),
                        context_size: alternative.context_size,
                        binary: fs::read(&alternative.path).map_err(|_| {
                            dandelion_err!(DandelionError::FunctionRegistry(
                                FunctionRegistryError::BinaryNotFound,
                            ))
                        })?,
                    });
                }
                Ok(ExportedFunctionRegistration {
                    name: (**function_id).clone(),
                    metadata: metadata_snapshot(&func_info.metadata),
                    alternatives,
                })
            }
            Some(_) => err_dandelion!(DandelionError::RequestError(FrontendError::InvalidRequest(
                format!("Function {} is not a user-defined function", function_id)
            ),)),
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
        let comp_vec = self.composition_from_module(module)?;
        let persisted_source = PersistedCompositionSource {
            source: composition_desc.to_string(),
        };
        let mut lock_guard = self
            .function_map
            .write()
            .expect("Function registry lock poisoned!");
        for (comp_name, composition, metadata) in comp_vec {
            fmap_insert_composition(&mut lock_guard, comp_name, composition, metadata)?;
        }
        drop(lock_guard);
        self.persist_compositions(persisted_source)
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

use dandelion_commons::{DandelionError, DandelionResult, EngineTypeId, FunctionId};
use futures::lock::Mutex;
use machine_interface::{
    function_driver::{Driver, Function, FunctionConfig},
    memory_domain::{malloc::MallocMemoryDomain, Context, MemoryDomain},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc, 
};

use crate::composition::{Composition, CompositionSet};

#[derive(Clone, Debug)]
pub enum FunctionType {
    /// Function available on an engine holding the engine ID
    Function(EngineTypeId),
    /// Function available as composition, holding the composition graph
    /// and the set with the inidecs of the sets in the composition that are output sets
    Composition(Composition, BTreeMap<usize, usize>),
}

#[derive(Clone, Debug)]
pub struct Alternative {
    pub function_type: FunctionType,
    pub in_memory: bool, // can place more information the scheduler would need here later
}

/// Struct to describe meatadata about a function that is true accross all drivers
#[derive(Debug)]
pub struct Metadata {
    /// input set names and optionally a static composition that is to be used for that input
    /// if the static input set is defined, any new input to that set is to be ignored
    pub input_sets: Vec<(String, Option<CompositionSet>)>,
    /// output set names
    pub output_sets: Vec<String>,
}

pub struct FunctionRegistry {
    /// List of engines available for each function
    engine_map: Mutex<BTreeMap<FunctionId, BTreeSet<EngineTypeId>>>,
    /// Drivers for the engines to prepare function (get them from available to ready)
    pub(crate) drivers: BTreeMap<EngineTypeId, Box<dyn Driver>>,
    /// map with list of all options for each function
    /// TODO: change structure to avoid copy on get_options
    options: Mutex<BTreeMap<FunctionId, Vec<Alternative>>>,
    /// map with function information for functions that are available in memory
    in_memory: Mutex<BTreeMap<(FunctionId, EngineTypeId), Function>>,
    /// map with file paths for functions for on disk available functons
    on_disk: Mutex<BTreeMap<(FunctionId, EngineTypeId), String>>,
    /// map with input and output set names for functions
    metadata: Mutex<BTreeMap<FunctionId, Arc<Metadata>>>,
}

impl FunctionRegistry {
    // TODO registr all system function on creation and make sure that no other method can add to their entries
    pub fn new(drivers: BTreeMap<EngineTypeId, Box<dyn Driver>>) -> Self {
        return FunctionRegistry {
            engine_map: Mutex::new(BTreeMap::new()),
            drivers,
            options: Mutex::new(BTreeMap::new()),
            in_memory: Mutex::new(BTreeMap::new()),
            on_disk: Mutex::new(BTreeMap::new()),
            metadata: Mutex::new(BTreeMap::new()),
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

    /// function that tries to insert metadata, returns true if metadata was successfully inserted
    /// or false if there was already metadata present
    pub async fn insert_metadata(&self, function_id: FunctionId, metadata: Metadata) -> () {
        self.metadata.lock().await.insert(function_id, Arc::new(metadata));
        return;
    }

    pub async fn get_metadata(&self, function_id: FunctionId) -> DandelionResult<Arc<Metadata>> {
        return self
            .metadata
            .lock()
            .await
            .get(&function_id)
            .and_then(|meta| Some(meta.clone()))
            .ok_or(DandelionError::DispatcherUnavailableFunction);
    }

    pub fn add_composition(
        &mut self,
        function_id: FunctionId,
        composition: Composition,
        output_set_map: BTreeMap<usize, usize>,
    ) -> DandelionResult<()> {
        if !self.metadata.get_mut().contains_key(&function_id) {
            return Err(DandelionError::DispatcherMetaDataUnavailable);
        };
        self.options
            .get_mut()
            .entry(function_id)
            .and_modify(|option_vec| {
                option_vec.push(Alternative {
                    function_type: FunctionType::Composition(
                        composition.clone(),
                        output_set_map.clone(),
                    ),
                    in_memory: true,
                })
            })
            .or_insert(vec![Alternative {
                function_type: FunctionType::Composition(composition, output_set_map),
                in_memory: true,
            }]);
        return Ok(());
    }

    pub fn add_system(
        &mut self,
        function_id: FunctionId,
        engine_id: EngineTypeId,
    ) -> DandelionResult<()> {
        if !self.metadata.get_mut().contains_key(&function_id) {
            return Err(DandelionError::DispatcherMetaDataUnavailable);
        }
        let driver = self
            .drivers
            .get(&engine_id)
            .ok_or(DandelionError::DispatcherMissingLoader(engine_id))?;
        // domain for the static context, expected to not be used
        let malloc_domain = Box::new(MallocMemoryDomain {});
        let function_config =
            driver.parse_function(String::new(), &(malloc_domain as Box<dyn MemoryDomain>))?;
        match function_config.config {
            FunctionConfig::SysConfig(_) => (),
            _ => return Err(DandelionError::DispatcherConfigError),
        };
        self.in_memory
            .get_mut()
            .insert((function_id, engine_id), function_config);
        self.engine_map.get_mut()
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
            .get_mut()
            .entry(function_id)
            .and_modify(|option_vec| {
                option_vec.push(Alternative {
                    function_type: FunctionType::Function(engine_id),
                    in_memory: true,
                })
            })
            .or_insert(vec![Alternative {
                function_type: FunctionType::Function(engine_id),
                in_memory: true,
            }]);
        return Ok(());
    }

    pub async fn add_local(
        &self,
        function_id: FunctionId,
        engine_id: EngineTypeId,
        path: &str,
    ) -> DandelionResult<()> {
        if !self.metadata.lock().await.contains_key(&function_id) {
            return Err(DandelionError::DispatcherMetaDataUnavailable);
        }
        self.on_disk
            .lock().await
            .insert((function_id, engine_id), path.to_string());
        self.engine_map
            .lock().await.entry(function_id)
            .and_modify(|set| {
                set.insert(engine_id);
            })
            .or_insert({
                let mut set = BTreeSet::new();
                set.insert(engine_id);
                set
            });
        self.options
            .lock().await
            .entry(function_id)
            .and_modify(|current_alts| {
                current_alts.push(Alternative {
                    function_type: FunctionType::Function(engine_id),
                    in_memory: false,
                })
            })
            .or_insert(vec![Alternative {
                function_type: FunctionType::Function(engine_id),
                in_memory: false,
            }]);
        return Ok(());
    }

    async fn load_local(
        &self,
        function_id: FunctionId,
        engine_id: EngineTypeId,
        domain: &Box<dyn MemoryDomain>,
    ) -> DandelionResult<Function> {
        // get loader
        let driver = match self.drivers.get(&engine_id) {
            Some(l) => l,
            None => return Err(DandelionError::DispatcherMissingLoader(engine_id)),
        };
        // get function code
        let path = 
        // TODO replace by queueing of pre added composition to fetch code by id
        {
            let disk_lock = self.on_disk.lock().await;
            match disk_lock.get(&(function_id, engine_id)) {
                Some(s) => s.clone(),
                None => return Err(DandelionError::DispatcherUnavailableFunction),
            }
        };
        let tripple = driver.parse_function(path, domain)?;
        return Ok(tripple);
    }

    pub async fn load(
        &self,
        function_id: FunctionId,
        engine_id: EngineTypeId,
        domain: &Box<dyn MemoryDomain>,
        non_caching: bool,
    ) -> DandelionResult<(Context, FunctionConfig)> {
        // check if function for the engine is in registry already
        {
            let lock_guard = self.in_memory.lock().await;
            if let Some(function) = lock_guard.get(&(function_id, engine_id)) {
                let function_context = function.load(domain)?;
                return Ok((function_context, function.config.clone()));
            }
        }

        // if it is not in memory or disk we return the error from loading as it is not available
        let function = self.load_local(function_id, engine_id, domain).await?;
        let function_context = function.load(domain)?;
        let function_config = function.config.clone();
        if !non_caching {
            if self
                .in_memory
                .lock()
                .await
                .insert((function_id, engine_id), function)
                .is_some()
            {
                panic!("Function not in registry even after Ok from loading");
            };
        }
        return Ok((function_context, function_config));
    }
}

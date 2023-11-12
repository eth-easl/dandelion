use dandelion_commons::{DandelionError, DandelionResult, EngineTypeId, FunctionId};
use futures::lock::Mutex;
use machine_interface::{
    function_driver::{
        load_utils::load_u8_from_file,
        system_driver::{get_system_function_input_sets, get_system_function_output_sets},
        Driver, Function, FunctionConfig,
    },
    memory_domain::{malloc::MallocMemoryDomain, Context, MemoryDomain},
};
use std::collections::{BTreeMap, BTreeSet};

use crate::composition::Composition;

#[derive(Clone, Debug)]
pub enum FunctionType {
    Function(EngineTypeId),
    Composition(Composition, BTreeSet<usize>),
}

#[derive(Clone, Debug)]
pub struct Alternative {
    pub function_type: FunctionType,
    pub in_memory: bool, // can place more information the scheduler would need here later
}

pub struct FunctionRegistry {
    /// List of engines available for each function
    engine_map: BTreeMap<FunctionId, BTreeSet<EngineTypeId>>,
    /// Drivers for the engines to prepare function (get them from available to ready)
    pub(crate) drivers: BTreeMap<EngineTypeId, Box<dyn Driver>>,
    // TODO replace with futures compatible RW lock if it becomes a bottleneck
    /// map with list of all options for each function
    options: Mutex<BTreeMap<FunctionId, Vec<Alternative>>>,
    /// map with function information for functions that are available in memory
    in_memory: Mutex<BTreeMap<(FunctionId, EngineTypeId), Function>>,
    /// map with file paths for functions for on disk available functons
    on_disk: BTreeMap<(FunctionId, EngineTypeId), String>,
    /// map with input and output set names for functions
    set_names: BTreeMap<FunctionId, (Vec<String>, Vec<String>)>,
}

impl FunctionRegistry {
    pub fn new(drivers: BTreeMap<EngineTypeId, Box<dyn Driver>>) -> Self {
        return FunctionRegistry {
            engine_map: BTreeMap::new(),
            drivers,
            options: Mutex::new(BTreeMap::new()),
            in_memory: Mutex::new(BTreeMap::new()),
            on_disk: BTreeMap::new(),
            set_names: BTreeMap::new(),
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

    pub fn get_set_names(
        &self,
        function_id: FunctionId,
    ) -> DandelionResult<&(Vec<String>, Vec<String>)> {
        return self
            .set_names
            .get(&function_id)
            .ok_or(DandelionError::DispatcherUnavailableFunction);
    }

    pub fn add_system(
        &mut self,
        function_id: FunctionId,
        engine_id: EngineTypeId,
    ) -> DandelionResult<()> {
        let driver = self
            .drivers
            .get(&engine_id)
            .ok_or(DandelionError::DispatcherMissingLoader(engine_id))?;
        // domain for the static context, expected to not be used
        let malloc_domain = Box::new(MallocMemoryDomain {});
        let function_config =
            driver.parse_function(vec![], &(malloc_domain as Box<dyn MemoryDomain>))?;
        let system_function = match function_config.config {
            FunctionConfig::SysConfig(sys) => sys,
            _ => return Err(DandelionError::DispatcherConfigError),
        };
        self.in_memory
            .get_mut()
            .insert((function_id, engine_id), function_config);
        self.engine_map
            .entry(function_id)
            .and_modify(|set| {
                set.insert(engine_id);
            })
            .or_insert({
                let mut set = BTreeSet::new();
                set.insert(engine_id);
                set
            });
        let in_set_names = get_system_function_input_sets(system_function);
        let out_set_names = get_system_function_output_sets(system_function);
        // TODO: default now is overwriting, might want to have different modes
        self.set_names
            .insert(function_id, (in_set_names, out_set_names));
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

    pub fn add_local(
        &mut self,
        function_id: FunctionId,
        engine_id: EngineTypeId,
        path: &str,
        input_sets: Vec<String>,
        output_sets: Vec<String>,
    ) {
        self.on_disk
            .insert((function_id, engine_id), path.to_string());
        self.engine_map
            .entry(function_id)
            .and_modify(|set| {
                set.insert(engine_id);
            })
            .or_insert({
                let mut set = BTreeSet::new();
                set.insert(engine_id);
                set
            });
        // TODO: default now is overwriting, might want to have different modes
        self.set_names
            .insert(function_id, (input_sets, output_sets));
        self.options
            .get_mut()
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
    }

    fn load_local(
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
        // TODO replace by queueing of pre added composition to fetch code by id
        let path = match self.on_disk.get(&(function_id, engine_id)) {
            Some(s) => s,
            None => return Err(DandelionError::DispatcherUnavailableFunction),
        };
        let function_buffer = load_u8_from_file(path.to_string())?;
        let tripple = driver.parse_function(function_buffer, domain)?;
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
        let mut lock_guard = self.in_memory.lock().await;
        if let Some(function) = lock_guard.get(&(function_id, engine_id)) {
            let function_context = function.load(domain)?;
            return Ok((function_context, function.config.clone()));
        }

        // if it is not in memory or disk we return the error from loading as it is not available
        let function = self.load_local(function_id, engine_id, domain)?;
        let function_context = function.load(domain)?;
        let function_config = function.config.clone();
        if !non_caching {
            if lock_guard
                .insert((function_id, engine_id), function)
                .is_some()
            {
                panic!("Function not in registry even after Ok from loading");
            };
        }
        return Ok((function_context, function_config));
    }
}

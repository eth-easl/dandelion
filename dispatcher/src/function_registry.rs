use crate::{EngineTypeId, FunctionId};
use core::panic;
use dandelion_commons::{DandelionError, DandelionResult};
use futures::lock::Mutex;
use machine_interface::{
    function_lib::{
        util::{load_static, load_u8_from_file},
        FunctionConfig, LoaderFunction,
    },
    memory_domain::{Context, MemoryDomain},
    DataRequirementList,
};
use std::collections::{HashMap, HashSet};

pub struct FunctionRegistry {
    engine_map: HashMap<FunctionId, HashSet<EngineTypeId>>,
    loaders: HashMap<EngineTypeId, &'static LoaderFunction>,
    // TODO replace with futures compatible RW lock if it becomes a bottleneck
    registry:
        Mutex<HashMap<(FunctionId, EngineTypeId), (DataRequirementList, Context, FunctionConfig)>>,
    local_available: HashMap<(FunctionId, EngineTypeId), String>,
}

impl FunctionRegistry {
    pub fn new() -> Self {
        return FunctionRegistry {
            engine_map: HashMap::new(),
            loaders: HashMap::new(),
            registry: Mutex::new(HashMap::new()),
            local_available: HashMap::new(),
        };
    }
    pub async fn get_options(
        &self,
        function_id: FunctionId,
    ) -> DandelionResult<(HashSet<EngineTypeId>, &HashSet<EngineTypeId>)> {
        // get the ones that are already loaded
        let lock_guard = self.registry.lock().await;
        let loaded = lock_guard.keys().filter_map(|(function, engine)| {
            if *function == function_id {
                Some(*engine)
            } else {
                None
            }
        });
        let local_ret_set = match self.engine_map.get(&function_id) {
            Some(set) => set,
            None => return Err(DandelionError::DispatcherUnavailableFunction),
        };
        return Ok((loaded.collect(), local_ret_set));
    }
    pub fn add_local(&mut self, function_id: FunctionId, engine_id: EngineTypeId, path: &str) {
        self.local_available
            .insert((function_id, engine_id), path.to_string());
        self.engine_map
            .entry(function_id)
            .and_modify(|set| {
                set.insert(engine_id);
            })
            .or_insert({
                let mut set = HashSet::new();
                set.insert(engine_id);
                set
            });
    }
    async fn load_local(
        &self,
        function_id: FunctionId,
        engine_id: EngineTypeId,
        domain: &Box<dyn MemoryDomain>,
    ) -> DandelionResult<()> {
        // get loader
        let loader = match self.loaders.get(&engine_id) {
            Some(l) => l,
            None => return Err(DandelionError::DispatcherConfigError),
        };
        // get function code
        // TODO replace by queueing of pre added composition to fetch code by id
        let path = match self.local_available.get(&(function_id, engine_id)) {
            Some(s) => s,
            None => return Err(DandelionError::DispatcherUnavailableFunction),
        };
        let function_buffer = load_u8_from_file(path.to_string())?;
        let tripple = loader(function_buffer, domain)?;
        // panic on none since we checked before and was not available
        let mut lock_guard = self.registry.lock().await;
        if let Some(_) = lock_guard.insert((function_id, engine_id), tripple) {
            panic!("Found value on insert that should not have been there");
        }
        return Ok(());
    }
    pub async fn load(
        &self,
        function_id: FunctionId,
        engine_id: EngineTypeId,
        domain: &Box<dyn MemoryDomain>,
    ) -> DandelionResult<(Context, FunctionConfig)> {
        // check if function for the engine is in registry already
        let lock_guard = self.registry.lock().await;
        let tripple = match lock_guard.get(&(function_id, engine_id)) {
            Some(t) => t,
            None => {
                self.load_local(function_id, engine_id, domain).await?;
                match lock_guard.get(&(function_id, engine_id)) {
                    Some(t) => t,
                    None => panic!("Function not in registry even after Ok from loading"),
                }
            }
        };
        let function_context = load_static(domain, &tripple.1, &tripple.0)?;
        return Ok((function_context, tripple.2));
    }
}

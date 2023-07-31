use dandelion_commons::{DandelionError, DandelionResult, EngineTypeId, FunctionId};
use futures::lock::Mutex;
use machine_interface::{
    function_driver::{
        util::{load_static, load_u8_from_file},
        FunctionConfig, LoaderFunction,
    },
    memory_domain::{Context, MemoryDomain},
    DataRequirementList,
};
use std::collections::{BTreeMap, BTreeSet};

pub struct FunctionRegistry {
    engine_map: BTreeMap<FunctionId, BTreeSet<EngineTypeId>>,
    loaders: BTreeMap<EngineTypeId, LoaderFunction>,
    // TODO replace with futures compatible RW lock if it becomes a bottleneck
    registry:
        Mutex<BTreeMap<(FunctionId, EngineTypeId), (DataRequirementList, Context, FunctionConfig)>>,
    local_available: BTreeMap<(FunctionId, EngineTypeId), String>,
    set_names: BTreeMap<FunctionId, (Vec<String>, Vec<String>)>,
}

impl FunctionRegistry {
    pub fn new(loaders: BTreeMap<EngineTypeId, LoaderFunction>) -> Self {
        return FunctionRegistry {
            engine_map: BTreeMap::new(),
            loaders,
            registry: Mutex::new(BTreeMap::new()),
            local_available: BTreeMap::new(),
            set_names: BTreeMap::new(),
        };
    }
    pub async fn get_options(
        &self,
        function_id: FunctionId,
    ) -> DandelionResult<(BTreeSet<EngineTypeId>, &BTreeSet<EngineTypeId>)> {
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
    pub fn add_local(
        &mut self,
        function_id: FunctionId,
        engine_id: EngineTypeId,
        path: &str,
        input_sets: Vec<String>,
        output_sets: Vec<String>,
    ) {
        self.local_available
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
        self.set_names
            .insert(function_id, (input_sets, output_sets));
    }
    fn load_local(
        &self,
        function_id: FunctionId,
        engine_id: EngineTypeId,
        domain: &Box<dyn MemoryDomain>,
    ) -> DandelionResult<(DataRequirementList, Context, FunctionConfig)> {
        // get loader
        let loader = match self.loaders.get(&engine_id) {
            Some(l) => l,
            None => return Err(DandelionError::DispatcherMissingLoader(engine_id)),
        };
        // get function code
        // TODO replace by queueing of pre added composition to fetch code by id
        let path = match self.local_available.get(&(function_id, engine_id)) {
            Some(s) => s,
            None => return Err(DandelionError::DispatcherUnavailableFunction),
        };
        let function_buffer = load_u8_from_file(path.to_string())?;
        let tripple = loader(function_buffer, domain)?;
        return Ok(tripple);
    }
    pub async fn load(
        &self,
        function_id: FunctionId,
        engine_id: EngineTypeId,
        domain: &Box<dyn MemoryDomain>,
        non_caching: bool,
    ) -> DandelionResult<(Context, FunctionConfig, &Vec<String>, &Vec<String>)> {
        // get input and output set names
        let (in_set_names, out_set_names) = self
            .set_names
            .get(&function_id)
            .ok_or(DandelionError::DispatcherUnavailableFunction)?;
        // check if function for the engine is in registry already
        let mut lock_guard = self.registry.lock().await;
        if let Some(tripple) = lock_guard.get(&(function_id, engine_id)) {
            let function_context = load_static(domain, &tripple.1, &tripple.0)?;
            return Ok((function_context, tripple.2, in_set_names, out_set_names));
        }

        let tripple = self.load_local(function_id, engine_id, domain)?;
        let function_context = load_static(domain, &tripple.1, &tripple.0)?;
        let function_config = tripple.2;
        if !non_caching {
            if lock_guard
                .insert((function_id, engine_id), tripple)
                .is_some()
            {
                panic!("Function not in registry even after Ok from loading");
            };
        }
        return Ok((
            function_context,
            function_config,
            in_set_names,
            out_set_names,
        ));
    }
}

use dandelion_commons::{DandelionError, DandelionResult, EngineTypeId, FunctionId};
use futures::lock::Mutex;
use machine_interface::{
    function_driver::{
        util::{load_static, load_u8_from_file},
        Driver, Function, FunctionConfig,
    },
    memory_domain::{Context, MemoryDomain},
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
    engine_map: BTreeMap<FunctionId, BTreeSet<EngineTypeId>>,
    pub(crate) drivers: BTreeMap<EngineTypeId, Box<dyn Driver>>,
    // TODO replace with futures compatible RW lock if it becomes a bottleneck
    options: Mutex<BTreeMap<FunctionId, Vec<Alternative>>>,
    in_memory: Mutex<BTreeMap<(FunctionId, EngineTypeId), Function>>,
    on_disk: BTreeMap<(FunctionId, EngineTypeId), String>,
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
        if let Some(Function {
            requirements,
            context,
            config,
        }) = lock_guard.get(&(function_id, engine_id))
        {
            let function_context = load_static(domain, &context, &requirements)?;
            return Ok((function_context, *config));
        }

        let function = self.load_local(function_id, engine_id, domain)?;
        let function_context = load_static(domain, &function.context, &function.requirements)?;
        let function_config = function.config;
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

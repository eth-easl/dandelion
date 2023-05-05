use crate::{function_registry::FunctionRegistry, resource_pool::ResourcePool};
use dandelion_commons::{ContextTypeId, DandelionError, DandelionResult, EngineTypeId, FunctionId};
use futures::{channel::oneshot, lock::Mutex};
use machine_interface::{
    function_lib::{DriverFunction, Engine, FunctionConfig},
    memory_domain::{transer_data_item, Context, MemoryDomain},
};
use std::collections::{HashMap, VecDeque};

struct SchedulerQueue {
    internals: Mutex<(
        Vec<Box<dyn Engine>>,
        VecDeque<oneshot::Sender<Box<dyn Engine>>>,
    )>,
}

impl SchedulerQueue {
    async fn get_engine(&self) -> DandelionResult<oneshot::Receiver<Box<dyn Engine>>> {
        let mut mux_guard = self.internals.lock().await;
        // create new lock
        let (sender, receiver) = oneshot::channel();
        if let Some(engine) = mux_guard.0.pop() {
            let sender_result: Result<(), Box<dyn Engine>> = sender.send(engine);
            if let Err(engine) = sender_result {
                mux_guard.0.push(engine);
                return Err(DandelionError::DispatcherChannelError);
            }
        } else {
            mux_guard.1.push_back(sender);
        }
        return Ok(receiver);
    }
    async fn yield_engine(&self, engine: Box<dyn Engine>) -> DandelionResult<()> {
        let mut mux_guard = self.internals.lock().await;
        if let Some(sender) = mux_guard.1.pop_front() {
            match sender.send(engine) {
                Ok(()) => (),
                Err(_) => return Err(DandelionError::DispatcherChannelError),
            }
        } else {
            mux_guard.0.push(engine);
        }
        Ok(())
    }
}

// TODO here and in registry can probably replace driver and loader function maps with fixed size arrays
// That have compile time size and static indexing
pub struct Dispatcher {
    domains: HashMap<ContextTypeId, Box<dyn MemoryDomain>>,
    _drivers: HashMap<EngineTypeId, DriverFunction>,
    engines: HashMap<EngineTypeId, SchedulerQueue>,
    type_map: HashMap<EngineTypeId, ContextTypeId>,
    function_registry: FunctionRegistry,
    _resource_pool: ResourcePool,
}

impl Dispatcher {
    pub fn init(
        domains: HashMap<ContextTypeId, Box<dyn MemoryDomain>>,
        drivers: HashMap<EngineTypeId, DriverFunction>,
        type_map: HashMap<EngineTypeId, ContextTypeId>,
        function_registry: FunctionRegistry,
        mut resource_pool: ResourcePool,
    ) -> DandelionResult<Dispatcher> {
        let mut engines = HashMap::with_capacity(drivers.len());
        // Use up all engine resources to start with
        for (engine_id, driver) in drivers.iter() {
            let mut engine_vec = Vec::new();
            while let Ok(Some(resource)) =
                resource_pool.sync_acquire_engine_resource(engine_id.clone())
            {
                if let Ok(engine) = driver(vec![resource]) {
                    engine_vec.push(engine);
                }
            }
            let engine_queue = SchedulerQueue {
                internals: Mutex::new((engine_vec, VecDeque::new())),
            };
            engines.insert(engine_id.clone(), engine_queue);
        }
        return Ok(Dispatcher {
            domains,
            _drivers: drivers,
            engines,
            type_map,
            function_registry,
            _resource_pool: resource_pool,
        });
    }
    pub async fn queue_function(
        &self,
        function_id: FunctionId,
        inputs: Vec<(&Context, Vec<(usize, Option<usize>, usize)>)>,
    ) -> DandelionResult<Context> {
        // find an engine capable of running the function
        // TODO actual scheduling decisions
        let engine_id;
        let options = self.function_registry.get_options(function_id).await?;
        if let Some(id) = options.0.iter().next() {
            engine_id = *id;
        } else {
            if let Some(load_id) = options.1.iter().next() {
                engine_id = *load_id;
            } else {
                return Err(DandelionError::DispatcherUnavailableFunction);
            }
        }
        let (context, config) = self
            .prepare_for_engine(function_id, engine_id, inputs)
            .await?;
        let (result, context) = self.run_on_engine(engine_id, config, context).await;
        match result {
            Ok(()) => Ok(context),
            Err(err) => {
                let context_id = match self.type_map.get(&engine_id) {
                    Some(id) => id,
                    None => return Err(DandelionError::DispatcherConfigError),
                };
                let domain = match self.domains.get(context_id) {
                    Some(d) => d,
                    None => return Err(DandelionError::DispatcherConfigError),
                };
                let _release_result = domain.release_context(context);
                Err(err)
            }
        }
        // Err(DandelionError::NotImplemented)
    }
    async fn prepare_for_engine(
        &self,
        function_id: FunctionId,
        engine_type: EngineTypeId,
        // vector with contexts that hold the inputs as well as assoziated tripples that say
        // the dynamic data index of the context, possible index into a set and index of the input in the new function
        inputs: Vec<(&Context, Vec<(usize, Option<usize>, usize)>)>,
    ) -> DandelionResult<(Context, FunctionConfig)> {
        // get context and load static data
        let context_id = match self.type_map.get(&engine_type) {
            Some(id) => id,
            None => return Err(DandelionError::DispatcherConfigError),
        };
        let domain = match self.domains.get(context_id) {
            Some(d) => d,
            None => return Err(DandelionError::DispatcherConfigError),
        };
        // start doing transfers
        let (mut function_context, function_config) = self
            .function_registry
            .load(function_id, engine_type, domain)
            .await?;
        for (input_context, index_map) in inputs {
            for (input_index, input_sub_index, function_index) in index_map {
                // TODO get allignment information
                transer_data_item(
                    &mut function_context,
                    input_context,
                    function_index,
                    128,
                    input_index,
                    input_sub_index,
                )?;
            }
        }
        return Ok((function_context, function_config));
    }

    async fn run_on_engine(
        &self,
        engine_type: EngineTypeId,
        function_config: FunctionConfig,
        function_context: Context,
    ) -> (DandelionResult<()>, Context) {
        // preparation is done, get engine to receive engine
        let engine_queue = match self.engines.get(&engine_type) {
            Some(q) => q,
            None => return (Err(DandelionError::DispatcherConfigError), function_context),
        };
        let mut engine = match engine_queue.get_engine().await {
            Ok(res) => match res.await {
                Ok(eng) => eng,
                Err(_) => {
                    return (
                        Err(DandelionError::DispatcherChannelError),
                        function_context,
                    )
                }
            },
            Err(_) => {
                return (
                    Err(DandelionError::DispatcherChannelError),
                    function_context,
                )
            }
        };
        let (result, output_context) = engine.run(&function_config, function_context).await;
        let end_result = match (result, engine_queue.yield_engine(engine).await) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(err), _) | (Ok(()), Err(err)) => Err(err),
        };
        return (end_result, output_context);
    }
}

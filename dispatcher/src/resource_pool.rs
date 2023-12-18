use dandelion_commons::{DandelionError, DandelionResult, EngineTypeId};
use futures::lock::Mutex;
use machine_interface::function_driver::ComputeResource;
use std::collections::BTreeMap;

// Struct to potentially trade off resource as runtime
// mainly for GPUs or FPGAs where we can have different sizes of slicing
pub struct ResourcePool {
    // TODO write init and make this private
    pub engine_pool: Mutex<BTreeMap<EngineTypeId, Vec<ComputeResource>>>,
}

impl ResourcePool {
    pub fn sync_acquire_engine_resource(
        &mut self,
        engine_id: EngineTypeId,
    ) -> DandelionResult<Option<ComputeResource>> {
        let pool_guard = self.engine_pool.get_mut();
        let pool = pool_guard.get_mut(&engine_id).and_then(|x| x.pop());
        Ok(pool)
    }
    pub async fn acquire_engine_resource(
        &self,
        engine_id: EngineTypeId,
    ) -> DandelionResult<Option<ComputeResource>> {
        let mut pool_guard = self.engine_pool.lock().await;
        let pool = pool_guard.get_mut(&engine_id).and_then(|x| x.pop());
        Ok(pool)
    }
    pub async fn release_engine_resource(
        &self,
        engine_id: EngineTypeId,
        resource: ComputeResource,
    ) -> DandelionResult<()> {
        let mut pool_guard = self.engine_pool.lock().await;
        match pool_guard.get_mut(&engine_id) {
            Some(pool) => {
                pool.push(resource);
                Ok(())
            }
            None => Err(DandelionError::DispatcherConfigError),
        }
    }
}

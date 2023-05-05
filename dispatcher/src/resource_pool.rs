use dandelion_commons::{DandelionError, DandelionResult, EngineTypeId};
use futures::lock::Mutex;
use std::collections::HashMap;

pub struct ResourcePool {
    // TODO write init and make this private
    pub engine_pool: Mutex<HashMap<EngineTypeId, Vec<u8>>>,
}

impl ResourcePool {
    pub fn sync_acquire_engine_resource(
        &mut self,
        engine_id: EngineTypeId,
    ) -> DandelionResult<Option<u8>> {
        let pool_guard = self.engine_pool.get_mut();
        let pool = pool_guard.get_mut(&engine_id).and_then(|x| x.pop());
        Ok(pool)
    }
    pub async fn acquire_engine_resource(
        &self,
        engine_id: EngineTypeId,
    ) -> DandelionResult<Option<u8>> {
        let mut pool_guard = self.engine_pool.lock().await;
        let pool = pool_guard.get_mut(&engine_id).and_then(|x| x.pop());
        Ok(pool)
    }
    pub async fn release_engine_resource(
        &self,
        engine_id: EngineTypeId,
        resource: u8,
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

use dispatcher::{
    dispatcher::Dispatcher,
    resource_pool::ResourcePool,
};

use machine_interface::{
    machine_config::{get_available_drivers, EngineType},
    function_driver::{WorkToDo, ComputeResource},
};

use tokio::time::{sleep, Duration};

use std::collections::BTreeMap;

pub struct Controller {
    pub resource_pool: &'static mut ResourcePool,
    pub dispatcher: &'static Dispatcher,
    pub cpu_core_map: &'static mut BTreeMap<EngineType, Vec<u8>>,
}

impl Controller {
    pub fn new(
        resource_pool: &'static mut ResourcePool,
        dispatcher: &'static Dispatcher,
        cpu_core_map: &'static mut BTreeMap<EngineType, Vec<u8>>,
    ) -> Self {
        Controller {
            resource_pool,
            dispatcher,
            cpu_core_map,
        }
    }

    pub async fn monitor_and_allocate(&mut self) {
        // Monitor the resource pool and allocate resources
        loop {
            let queue_lengths = self.dispatcher.get_queue_lengths();
            let mut need_more_cores = None;

            // print cpu core map
            // println!("CPU core map: {:?}", self.cpu_core_map);

            for (engine_type, length) in &queue_lengths {
                if *length > 1000 {
                    need_more_cores = Some(*engine_type);
                    break;
                }
            }

            if let Some(engine_type_to_expand) = need_more_cores {
                let deallocated = self.deallocate_cores_from_other_engines(engine_type_to_expand, &queue_lengths).await;

                if deallocated {
                    self.allocate_more_cores(engine_type_to_expand).await;
                }
            }
            //print resource pool status
            // let pool_guard = self.resource_pool.engine_pool.lock().await;
            // println!("Engine pool: {:?}", pool_guard);
            
            sleep(Duration::from_millis(200)).await;
        }
    }

    async fn allocate_more_cores(&mut self, engine_type: EngineType) {

        if let Ok(Some(resource)) = self.resource_pool.sync_acquire_engine_resource(engine_type){
            if let ComputeResource::CPU(core_id) = resource {
                if let Some(core_list) = self.cpu_core_map.get_mut(&engine_type) {
                    core_list.push(core_id);
                } else {
                    // If this engine type has no entries in the map, create a new entry
                    self.cpu_core_map.insert(engine_type, vec![core_id]);
                }

                let drivers = get_available_drivers();
                if let Some(driver) = drivers.get(&engine_type){
                    let work_queue = self.dispatcher.engine_queues.get(&engine_type).unwrap().clone();
                    let _ = driver.start_engine(resource, work_queue);
                }
            }
        }
    }

    async fn deallocate_cores_from_other_engines(
        &mut self, 
        target_engine: EngineType,
        queue_lengths: &[(EngineType, usize)],
    ) -> bool {
        // Iterate over the engine types to find one to deallocate
        for (engine_type, length) in queue_lengths{
            if *engine_type != target_engine && *length < 10 {
                // println!("Deallocating core from {:?} to allocate to {:?}", engine_type, target_engine);
                
                // Get the cores allocated to this engine type
                if let Some(cores_in_use) = self.cpu_core_map.get(engine_type) {
                    if cores_in_use.len() == 1 {
                        break;
                    } 
                    if let Some(&core_id) = cores_in_use.first() {
                        // Stop the thread running on this core
                        // println!("Stopping engine on core {}", core_id);
                        let shutdown_task = WorkToDo::Shutdown();
                        let engine_queue = self.dispatcher.engine_queues.get(engine_type).unwrap().clone();
                        let _ = engine_queue.enqueu_work(shutdown_task).await;

                        // Remove the core from the cpu_core_map
                        if let Some(core_list) = self.cpu_core_map.get_mut(engine_type) {
                            core_list.retain(|&core| core != core_id);
                        }

                        println!("CPU core map after dellocation: {:?}", self.cpu_core_map);

                        // Return the core to the resource pool
                        if let Err(e) = self.resource_pool.release_engine_resource(target_engine, ComputeResource::CPU(core_id)).await {
                            println!("Error releasing core {} back to the resource pool: {:?}", core_id, e);
                            continue;
                        }
                        return true; // Core deallocation successful
                    }
                }
            }
        }
        false // No cores deallocated
    }
}
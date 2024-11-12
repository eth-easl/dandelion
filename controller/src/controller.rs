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
    pub delta: usize,
    pub loop_duration: u64, 
}

impl Controller {
    pub fn new(
        resource_pool: &'static mut ResourcePool,
        dispatcher: &'static Dispatcher,
        cpu_core_map: &'static mut BTreeMap<EngineType, Vec<u8>>,
        delta: usize,
        loop_duration: u64,
    ) -> Self {
        Controller {
            resource_pool,
            dispatcher,
            cpu_core_map,
            delta,
            loop_duration,
        }
    }

    /// Print the number of cores allocated to each engine type and the length of each queue
    fn print_core_info(&self, queue_lengths: &[(EngineType, usize)]) {
        let mut print_str = String::new();
        for (engine_type, cores) in self.cpu_core_map.iter() {
            print_str.push_str(&format!("Engine type: {:?}, Cores: {:?}; ", engine_type, cores.len()));
        }
        println!("{}", print_str);

        print_str = String::new();
        for (engine_type, length) in queue_lengths {
            print_str.push_str(&format!("Engine type: {:?}, Queue length: {:?}; ", engine_type, length));
        }
        println!("{}", print_str);
    }

    /// Monitor the resource pool and allocate resources
    pub async fn monitor_and_allocate(&mut self) {

        loop {
            let queue_lengths = self.dispatcher.get_queue_lengths();
            let mut need_more_cores = None;

            // Print how many cores are allocated to each engine type
            self.print_core_info(&queue_lengths);

            let avg_load = queue_lengths.iter().map(|(_, length)| length).sum::<usize>() / queue_lengths.len();
            let most_overloaded_queue = queue_lengths.iter().max_by_key(|(_, length)| *length);
            if let Some((engine_type, length)) = most_overloaded_queue {
                if *length > avg_load + self.delta {
                    need_more_cores = Some(*engine_type);
                }
            }

            if let Some(engine_type_to_expand) = need_more_cores {
                let deallocated = self.deallocate_cores_from_other_engines(engine_type_to_expand, &queue_lengths, avg_load).await;

                if deallocated {
                    self.allocate_more_cores(engine_type_to_expand).await;
                }
            }

            sleep(Duration::from_millis(self.loop_duration)).await
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
        avg_load: usize,
    ) -> bool {
        // Iterate over the engine types to find one to deallocate
        for (engine_type, length) in queue_lengths{
            if *engine_type == target_engine && *length > avg_load {
                continue;
            }
            // println!("Deallocating core from {:?} to allocate to {:?}", engine_type, target_engine);
            
            // Get the cores allocated to this engine type
            if let Some(cores_in_use) = self.cpu_core_map.get(engine_type) {
                if let Some(&core_id) = cores_in_use.first() {
                    // Stop the thread running on this core
                    let shutdown_task = WorkToDo::Shutdown();
                    let engine_queue = self.dispatcher.engine_queues.get(engine_type).unwrap().clone();
                    if cores_in_use.len() == 1 && engine_queue.queue_length() > 0 {
                        continue;
                    }

                    let _ = engine_queue.enqueu_work(shutdown_task).await;

                    // Remove the core from the cpu_core_map
                    if let Some(core_list) = self.cpu_core_map.get_mut(engine_type) {
                        core_list.retain(|&core| core != core_id);
                    }

                    // Return the core to the resource pool
                    if let Err(e) = self.resource_pool.release_engine_resource(target_engine, ComputeResource::CPU(core_id)).await {
                        println!("Error releasing core {} back to the resource pool: {:?}", core_id, e);
                        continue;
                    }
                    return true; // Core deallocation successful
                }
            }
        }
        false // No cores deallocated
    }
}
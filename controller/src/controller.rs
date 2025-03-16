use dispatcher::{dispatcher::Dispatcher, resource_pool::ResourcePool};
use machine_interface::function_driver::WorkDone;
use machine_interface::{
    function_driver::{ComputeResource, WorkToDo},
    machine_config::{get_available_drivers, EngineType},
};
use std::collections::BTreeMap;
use tokio::time::{sleep, Duration};

/// Do not perform re-allocations if the absolute error is less than this value
const EPSILON: f64 = 1.0;

pub struct Controller {
    pub resource_pool: &'static mut ResourcePool,
    pub dispatcher: &'static Dispatcher,
    pub cpu_core_map: &'static mut BTreeMap<EngineType, Vec<u8>>,

    pub control_kp: f64,
    pub control_ki: f64,
    pub control_kd: f64,
    pub control_tu: f64,
    pub loop_duration: u64,

    threads_per_core: usize,
    cpu_pinning: bool,
    compute_range: (usize, usize),

    prev_tasks_lengths: BTreeMap<EngineType, usize>,
    prev_error: BTreeMap<EngineType, f64>,
    prev_integral: BTreeMap<EngineType, f64>,
}

impl Controller {
    pub fn new(
        resource_pool: &'static mut ResourcePool,
        dispatcher: &'static Dispatcher,
        cpu_core_map: &'static mut BTreeMap<EngineType, Vec<u8>>,
        control_ku: f64,
        control_tu: f64,
        loop_duration: u64,
        threads_per_core: usize,
        cpu_pinning: bool,
        compute_range: (usize, usize),
    ) -> Self {
        let control_kp = 0.45 * control_ku;
        let control_ki = 0.54 * control_ku / control_tu;
        let control_kd = 0.075 * control_ku * control_tu;

        println!("[CTRL] Control parameters: kp: {}, ki: {}, kd: {}", control_kp, control_ki, control_kd);

        Controller {
            resource_pool,
            dispatcher,
            cpu_core_map,
            control_kp,
            control_ki,
            control_kd,
            control_tu,
            loop_duration,
            threads_per_core,
            cpu_pinning,
            compute_range,
            prev_tasks_lengths: BTreeMap::new(),
            prev_error: BTreeMap::new(),
            prev_integral: BTreeMap::new(),
        }
    }

    /// Log queue length and number of allocated cores
    fn log_core_info(&self) {
        let queue_lengths = self.dispatcher.get_queue_lengths();
        let tasks_lengths = self.dispatcher.get_total_tasks_lengths();

        let mut print_str = String::new();
        for (engine_type, cores) in self.cpu_core_map.iter() {
            print_str.push_str(&format!(
                "Engine type: {:?}, Cores: {:?}; ",
                engine_type,
                cores.len()
            ));
        }
        println!("[CTRL] {}", print_str);

        print_str = String::new();
        for (engine_type, length) in queue_lengths {
            print_str.push_str(&format!(
                "Engine type: {:?}, Queue length: {:?}; ",
                engine_type, length
            ));
        }
        println!("[CTRL] {}", print_str);

        print_str = String::new();
        for (engine_type, length) in tasks_lengths {
            print_str.push_str(&format!(
                "Engine type: {:?}, Tasks: {:?}; ",
                engine_type, length
            ));
        }
        println!("[CTRL] {}", print_str);
    }

    /// Monitor the resource pool and allocate resources
    pub async fn monitor_and_allocate(&mut self) {
        loop {
            let tasks_lengths = self.dispatcher.get_total_tasks_lengths();

            self.log_core_info();

            let need_more_cores = self.get_engine_type_to_expand(&tasks_lengths);

            let mut deallocated = false;
            if let Some(engine_type_to_expand) = need_more_cores {
                deallocated = self
                    .deallocate_cores_from_other_engines(engine_type_to_expand, &tasks_lengths)
                    .await;

                if deallocated {
                    self.allocate_more_cores(engine_type_to_expand).await;
                }
            }

            let wait_interval = if deallocated {
                10 * self.loop_duration
            } else {
                self.loop_duration
            };
            sleep(Duration::from_millis(wait_interval)).await; //
        }
    }

    /// Return the engine type that needs more cores
    fn get_engine_type_to_expand(
        &mut self,
        tasks_lengths: &Vec<(EngineType, usize)>,
    ) -> Option<EngineType> {
        // Calculate the growth rate of each engine type
        let mut max_growth_rate: f64 = 0.0;
        let mut min_growth_rate: f64 = 100.0;
        let mut engine_type_to_expand = None;

        // Calculate tasks logarithmic growth rates
        for (engine_type, length) in tasks_lengths {
            let prev_length = *self.prev_tasks_lengths.get(engine_type).unwrap_or(&0);
            self.prev_tasks_lengths.insert(*engine_type, *length);

            let growth_rate = (1.0 + (*length as f64)).log2() - (1.0 + (prev_length as f64)).log2();

            if growth_rate < min_growth_rate {
                min_growth_rate = growth_rate;
            }

            if growth_rate > max_growth_rate {
                max_growth_rate = growth_rate;
                engine_type_to_expand = Some(*engine_type);
            }
        }

        if engine_type_to_expand.is_none() {
            return None;
        }

        // Calculate error as the difference between the max and min growth rates
        let error = max_growth_rate - min_growth_rate;
        let target_engine = engine_type_to_expand.unwrap();
        let prev_error = *self.prev_error.get(&target_engine).unwrap_or(&0.0);
        let prev_integral = *self.prev_integral.get(&target_engine).unwrap_or(&0.0);
        
        let pid_signal = self.control_kp * error
            + self.control_ki * prev_integral;

        println!("[CTRL] min: {}, max: {}, error: {}, prev_error: {}, prev_integral: {}, pid_signal: {}", min_growth_rate, max_growth_rate, error, prev_error, prev_integral, pid_signal);

        // Update previous error and integral for each engine type
        for (engine_type, _) in tasks_lengths {
            self.prev_error.insert(*engine_type, if engine_type == &target_engine { error } else { -error });
            let new_integral = self.prev_integral.get(engine_type).unwrap_or(&0.0) + (if engine_type == &target_engine { error } else { -error });
            self.prev_integral.insert(*engine_type, new_integral);
        }

        if pid_signal < EPSILON {
            return None;
        }
        
        Some(target_engine)
    }

    /// Check if a core can be deallocated from the engine type
    fn check_can_deallocate(&self, engine_type: EngineType) -> bool {
        if let Some(cores) = self.cpu_core_map.get(&engine_type) {
            if cores.len() <= 1 {
                return false;
            }

            return true;
        }
        false
    }

    /// Allocate a core to a target engine type
    async fn allocate_more_cores(&mut self, engine_type: EngineType) {
        if let Ok(Some(resource)) = self.resource_pool.sync_acquire_engine_resource(engine_type) {
            if let ComputeResource::CPU(core_id) = resource {
                if let Some(core_list) = self.cpu_core_map.get_mut(&engine_type) {
                    core_list.push(core_id);
                } else {
                    // If this engine type has no entries in the map, create a new entry
                    self.cpu_core_map.insert(engine_type, vec![core_id]);
                }

                let drivers = get_available_drivers();
                if let Some(driver) = drivers.get(&engine_type) {
                    let work_queue = self
                        .dispatcher
                        .engine_queues
                        .get(&engine_type)
                        .unwrap()
                        .clone();
                    let tasks_length = work_queue.total_tasks_length();
                    match driver.start_engine(
                        resource,
                        work_queue,
                        self.threads_per_core,
                        self.cpu_pinning,
                        self.compute_range,
                    ) {
                        Ok(_) => println!(
                            "[CTRL] Allocated core {} to engine type {:?} with {} tasks",
                            core_id, engine_type, tasks_length
                        ),
                        Err(e) => log::error!(
                            "[CTRL] Error starting engine for core {}: {:?}",
                            core_id,
                            e
                        ),
                    };
                }
            }
        }
    }

    /// Deallocate a core from other engines to allocate to a target engine
    async fn deallocate_cores_from_other_engines(
        &mut self,
        target_engine: EngineType,
        tasks_lengths: &[(EngineType, usize)],
    ) -> bool {
        // Iterate over the engine types to find one to deallocate
        for (engine_type, _length) in tasks_lengths {
            if *engine_type == target_engine || !self.check_can_deallocate(*engine_type) {
                continue;
            }

            // Get the cores allocated to this engine type
            if let Some(cores_in_use) = self.cpu_core_map.get(engine_type) {
                // Stop the thread running on this core
                let shutdown_task = WorkToDo::Shutdown();
                let engine_queue = self
                    .dispatcher
                    .engine_queues
                    .get(engine_type)
                    .unwrap()
                    .clone();
                if cores_in_use.len() <= 1 {
                    continue;
                }

                let core_id = match engine_queue.enqueu_work(shutdown_task).await {
                    Ok(WorkDone::Resources(resources)) => {
                        if let ComputeResource::CPU(core_id) = resources[0] {
                            core_id
                        } else {
                            continue;
                        }
                    }
                    _ => continue,
                };

                // Remove the core from the cpu_core_map
                if let Some(core_list) = self.cpu_core_map.get_mut(engine_type) {
                    core_list.retain(|&core| core != core_id);
                }

                // Return the core to the resource pool
                if let Err(e) = self
                    .resource_pool
                    .release_engine_resource(target_engine, ComputeResource::CPU(core_id))
                    .await
                {
                    log::error!(
                        "[CTRL] Error releasing core {} back to the resource pool: {:?}",
                        core_id,
                        e
                    );
                    continue;
                }
                println!(
                    "[CTRL] Deallocated core {} from engine type {:?} with {} tasks",
                    core_id,
                    engine_type,
                    engine_queue.total_tasks_length()
                );

                // Core deallocation successful
                return true;
            }
        }
        false // No cores deallocated
    }
}

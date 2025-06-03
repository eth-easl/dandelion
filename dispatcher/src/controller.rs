use crate::{
    controller,
    execution_qs::EngineQueue,
    resource_pool::{self, ResourcePool},
};
use crossbeam::queue;
use futures::{
    channel::mpsc::{channel, Receiver, Sender},
    lock::Mutex,
    StreamExt,
};
use itertools::min;
use log::{debug, trace};
use machine_interface::{
    function_driver::{ComputeResource, ComputeResourceType, Driver, RESOURCE_TYPES},
    machine_config::{get_available_drivers, EngineType, ENGINE_RESOURCE_MAP, ENGINE_TYPES},
};
use smol::{process::driver, Executor};
use std::{
    collections::BTreeMap,
    isize,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Weak,
    },
    time::Instant,
};

const WINDOW_SIZE: usize = 16;
/// Number of microseconds after which a rebalancing check should be performed
const MIN_UPDATE_INTERVAL: u128 = 30_000;

/// Controller parameter to set proportional control reaction
const PROPORTIONAL: f32 = 0.008;
const INTEGRAL: f32 = 0.002;

// assert that there cannot be overflow on the total
const _: () = assert!(WINDOW_SIZE
    .checked_mul(crate::execution_qs::MAX_QUEUE)
    .is_some(),
    "Controller window size combined with set max exeuction queue length could lead to overflow in controller statistics total");

struct QueueStatistics {
    previous_lengths: [(Instant, usize); WINDOW_SIZE],
    head: usize,
    total: usize,
}

struct QueueDescriptor {
    e_type: EngineType,
    queue: EngineQueue,
    engines: AtomicUsize,
}

/// Controller that manages resources between queues
/// Owns all the engine queues
/// The lock on the statisics is both for updating them and for rebalancing
pub struct Controller {
    resource_pool: Vec<Mutex<Vec<ComputeResource>>>,
    queues: Box<[QueueDescriptor]>,
    statistics: Mutex<Box<[QueueStatistics]>>,
    drivers: &'static Vec<&'static dyn Driver>,
    runtime: Executor<'static>,
    rebalance_waker: Mutex<(Instant, Sender<()>)>,
}

impl Controller {
    pub fn new(
        resources: Vec<ComputeResource>,
    ) -> (Arc<Self>, BTreeMap<EngineType, Box<EngineQueue>>) {
        let mut return_queues = BTreeMap::new();

        // prepare resource pool
        let mut sorted_resources = vec![Vec::new(); RESOURCE_TYPES];
        for resource in resources.into_iter() {
            let index = Into::<usize>::into(&resource);
            debug!(
                "inserting respurce for type {:?} at index {}",
                resource, index
            );
            sorted_resources[index].push(resource);
        }
        let resource_pool = sorted_resources
            .into_iter()
            .map(|resources| Mutex::new(resources))
            .collect();

        debug!("resource_pool: {:?}", resource_pool);

        let now = Instant::now();
        let (rebalance_waker, rebalance_receiver) = channel(1);
        let new_controller = Arc::new_cyclic(|weak_controller| {
            let mut controller_queues = Vec::with_capacity(ENGINE_RESOURCE_MAP.len());
            let mut statistics = Vec::with_capacity(ENGINE_RESOURCE_MAP.len());
            // set up separate engine queues
            for (q_index, engine_type) in ENGINE_TYPES.iter().enumerate() {
                let new_queue = EngineQueue::new(weak_controller.clone(), q_index);
                return_queues.insert(*engine_type, Box::new(new_queue.clone()));
                controller_queues.push(QueueDescriptor {
                    e_type: *engine_type,
                    queue: new_queue,
                    engines: AtomicUsize::new(0),
                });
                statistics.push(QueueStatistics {
                    previous_lengths: [(now, 0); WINDOW_SIZE],
                    head: 0,
                    total: 0,
                });
            }

            // prepare executor
            let runtime = Executor::new();

            Controller {
                resource_pool,
                queues: controller_queues.into_boxed_slice(),
                drivers: get_available_drivers(),
                statistics: Mutex::new(statistics.into_boxed_slice()),
                runtime,
                rebalance_waker: Mutex::new((Instant::now(), rebalance_waker)),
            }
        });

        new_controller
            .runtime
            .spawn(Controller::rebalance(
                Arc::downgrade(&new_controller),
                rebalance_receiver,
            ))
            .detach();

        (new_controller, return_queues)
    }

    async fn rebalance(weak_controller: Weak<Controller>, mut receiver: Receiver<()>) {
        while let Some(()) = receiver.next().await {
            trace!("Starting to rebalance");
            let controller = weak_controller.upgrade().unwrap();

            // update statistics
            let mut guard = controller.statistics.lock().await;
            let now = Instant::now();

            // data to find queues that need engine most and that could give up engine
            let mut max_growth = f32::MIN;
            let mut max_current = 0;
            let mut max_index = guard.len();
            let mut min_current = 0;
            let mut min_growth = f32::MAX;
            let mut min_index = guard.len();

            for (engine_type, statistic) in guard.iter_mut().enumerate() {
                let current_queue_length = controller.queues[engine_type].queue.get_occupied();
                let tail = statistic.previous_lengths[statistic.head];
                let before_head = statistic.head.wrapping_sub(1) % WINDOW_SIZE;
                let previous = statistic.previous_lengths[before_head];
                // update statistic
                statistic.previous_lengths[statistic.head] = (now, current_queue_length);
                statistic.head = (statistic.head + 1) % WINDOW_SIZE;
                statistic.total = statistic.total - tail.1 + current_queue_length;

                // prepare data to find queues that need more resources
                // first check if the queue went from 0 to any and there is no engine to work on it
                if previous.1 == 0
                    && current_queue_length > 0
                    && controller.queues[engine_type]
                        .engines
                        .load(Ordering::Acquire)
                        == 0
                {
                    max_growth = f32::MAX;
                    max_current = current_queue_length;
                    max_index = engine_type;
                } else {
                    let current_growth = (current_queue_length as f32 - previous.1 as f32)
                        / (now.duration_since(previous.0).as_micros() as f32);
                    if current_growth > max_growth {
                        max_growth = current_growth;
                        max_current = current_queue_length;
                        max_index = engine_type;
                    } else if current_growth < min_growth {
                        min_growth = current_growth;
                        min_current = current_queue_length;
                        min_index = engine_type;
                    }
                }
            }
            // check if there was a queue that grew and we have spare resources to allocate anyway
            if max_growth > 0.0 {
                let needed_resource = ENGINE_RESOURCE_MAP[max_index] as usize;
                let mut resource_lock = controller.resource_pool[needed_resource].lock().await;
                if resource_lock.len() > 0 {
                    let spare_resource = resource_lock.pop().unwrap();
                    controller.drivers[max_index]
                        .start_engine(
                            spare_resource,
                            Box::new(controller.queues[max_index].queue.clone()),
                        )
                        .expect("Expect engine start to be successful");
                    controller.queues[max_index]
                        .engines
                        .fetch_add(1, Ordering::AcqRel);
                }
                continue;
            }

            // TODO maybe we want to deallocate resources from empty queues premeptively

            let pid_signal = PROPORTIONAL * (max_current as f32 - min_current as f32)
                + INTEGRAL * (max_growth - min_growth);

            // rebalance if it is bigger than 1,
            // might want to think about scaling to do multiple moves if the number is bigger
            if pid_signal > 1.0 {
                let new_queue = Box::new(controller.queues[max_index].queue.clone());
                let queue = controller.queues[min_index].queue.clone();
                let driver = controller.drivers[max_index];
                controller
                    .runtime
                    .spawn(async move {
                        let resource = queue.shutdown_engine().await;
                        driver.start_engine(resource[0], new_queue).unwrap();
                    })
                    .detach();
            }

            trace!(
                "Engines after rebalancing: {:?}",
                controller
                    .queues
                    .iter()
                    .map(|queue| queue.engines.load(Ordering::Acquire))
                    .collect::<Vec<_>>()
            );
        }
    }

    /// Actions to take when when new tasks are added to a workqueue
    /// Currently used to allocate engines for tasks if there are spare resources,
    /// that can be commited right away
    pub async fn on_work_enqueue(self: Arc<Self>, engine_index: usize) {
        trace!(
            "work enqueue executing, with {} jobs in queue",
            self.queues[0].queue.get_occupied()
        );
        if let Some(mut guard) = self.rebalance_waker.try_lock() {
            let now = Instant::now();
            if self.queues[engine_index].engines.load(Ordering::Acquire) == 0
                || now.duration_since(guard.0).as_micros() > MIN_UPDATE_INTERVAL
            {
                let _ = guard.1.try_send(());
            }
        }
        while self.runtime.try_tick() {}
        trace!("work enqueue finished");
    }

    pub fn on_get_args(self: Arc<Self>) {
        trace!("on get args executing");
        // check when the statstics where updated last
        if let Some(mut guard) = self.rebalance_waker.try_lock() {
            let now = Instant::now();
            if now.duration_since(guard.0).as_micros() > MIN_UPDATE_INTERVAL {
                let _ = guard.1.try_send(());
            }
        }
        while self.runtime.try_tick() {}
    }
}

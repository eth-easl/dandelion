use std::{cmp, sync::atomic::Ordering};

use machine_interface::composition::{CompositionSet, ShardingMode};

use crate::dispatcher::Dispatcher;

pub fn get_max_parallelism(
    dispatcher: &Dispatcher,
    input_sets: &Vec<Option<(ShardingMode, CompositionSet)>>,
    min_set_size: usize,
) -> usize {
    let mut max_any_set_size = 0;
    for set in input_sets.iter() {
        if let Some((sharding, set)) = set {
            match sharding {
                ShardingMode::AnyKey | ShardingMode::AnyEach => {
                    max_any_set_size = cmp::max(max_any_set_size, set.size());
                }
                _ => (),
            }
        }
    }

    if max_any_set_size > 0 {
        if max_any_set_size
            > dispatcher.system_info.offload_const
                * min_set_size
                * dispatcher.system_info.num_local_cores
        {
            dispatcher.system_info.num_local_cores
        } else {
            dispatcher.system_info.num_local_cores
                + dispatcher
                    .system_info
                    .num_remote_cores
                    .load(Ordering::Acquire)
        }
    } else {
        0
    }
}

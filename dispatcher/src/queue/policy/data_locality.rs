use dandelion_commons::records::RecordPoint;
use machine_interface::{composition::ItemData, function_driver::WorkToDo, promise::Debt};
use std::collections::{btree_map::Entry, BTreeMap, LinkedList};
use tokio::sync::mpsc;

use super::super::{ComputeQueueElement, IoQueueElement};

pub const PREFETCH_PER_CORE: usize = 1;

pub struct IOElementData {
    pub remote_data: BTreeMap<u64, usize>,
    pub total_input_size: usize,
}

/// Prepares the policy-specific element data for a new IO queue element.
/// Returns `None` if the work was offloaded to a remote node (consumed).
/// Returns `Some((work, debt, data))` if the work should be queued locally.
pub fn prepare_io_element(
    work: WorkToDo,
    debt: Debt,
    try_offload: bool,
    composition_id: usize,
    remote_nodes: &std::sync::Mutex<BTreeMap<u64, mpsc::UnboundedSender<(WorkToDo, Debt, usize)>>>,
) -> Option<(WorkToDo, Debt, IOElementData)> {
    let (remote_data, total_input_size) =
        if let WorkToDo::FunctionArguments { ref input_sets, .. } = work {
            let mut ref_map = BTreeMap::new();
            let mut total_input_size = 0usize;
            for (item, data) in input_sets
                .iter()
                .filter_map(|s| s.as_ref().map(|s| s.into_iter()))
                .flatten()
            {
                total_input_size += item.data.size;
                if let ItemData::RemoteData(remote_data) = data {
                    if item.data.size > 0 {
                        match ref_map.entry(remote_data.node_id) {
                            Entry::Occupied(mut value) => *value.get_mut() += item.data.size,
                            Entry::Vacant(value) => {
                                value.insert(item.data.size);
                            }
                        }
                    }
                }
            }
            (ref_map, total_input_size)
        } else {
            (BTreeMap::new(), 0)
        };

    // TODO: think about if there is a better place to do this
    if try_offload {
        if let Some((node_id, max_size)) = remote_data.iter().max() {
            // if more than half of the data is on one specific node try to offload to that one
            if max_size * 2 > total_input_size {
                let maybe_sender = remote_nodes.lock().unwrap().get(node_id).cloned();
                if let Some(node_sender) = maybe_sender {
                    node_sender.send((work, debt, composition_id)).unwrap();
                    return None;
                }
            }
        }
    }

    Some((
        work,
        debt,
        IOElementData {
            remote_data,
            total_input_size,
        },
    ))
}

/// Decides whether an IO queue element (FunctionArguments) should be taken by an IO worker.
pub fn should_io_take(
    element_data: &IOElementData,
    compute_pending: usize,
    active_fetch_count: usize,
    local_cores: usize,
    idle_compute_cores: usize,
) -> bool {
    // additionally want to prevent fetching, if there is remote data and no local core is idle
    // always take it if there are idle cores, only prefetch if it is prefetching via IO, not from other nodes
    idle_compute_cores > 0
        || (compute_pending + active_fetch_count < PREFETCH_PER_CORE * local_cores
            && !element_data.remote_data.is_empty())
}

/// Selects work items from the local queues to hand off to a remote node,
/// preferring items whose data majority resides on that node.
pub fn get_work_for_remote(
    io_queue: &mut LinkedList<IoQueueElement>,
    compute_queue: &mut LinkedList<ComputeQueueElement>,
    engine_flags: u32,
    node_id: u64,
    number_of_functions: usize,
) -> Vec<(WorkToDo, Debt, usize)> {
    let mut functions = Vec::with_capacity(number_of_functions);
    // go through all the input sets and find those with the most data already on the node asking for work
    functions.extend(
        io_queue
            .extract_if(|queue_element| {
                if let WorkToDo::FunctionArguments { recorder, .. } = &mut queue_element.work {
                    let node_has_most_data = queue_element
                        .policy_data
                        .remote_data
                        .iter()
                        .max_by_key(|(_, v)| **v)
                        .map(|(max_id, _)| node_id == *max_id)
                        .unwrap_or(false);
                    if queue_element.flags & engine_flags != 0 && node_has_most_data {
                        recorder.record(RecordPoint::IOQueueEnd);
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            })
            .map(|queue_element| {
                (
                    queue_element.work,
                    queue_element.debt,
                    queue_element.composition_id,
                )
            })
            .take(number_of_functions),
    );

    // did not find enough work where the node has the majority of data, so take anything left over
    if functions.len() < number_of_functions {
        let still_needed = number_of_functions - functions.len();
        functions.extend(
            io_queue
                .extract_if(|queue_element| {
                    if let WorkToDo::FunctionArguments { recorder, .. } = &mut queue_element.work {
                        if queue_element.flags & engine_flags != 0 {
                            recorder.record(RecordPoint::IOQueueEnd);
                            true
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                })
                .map(|queue_element| {
                    (
                        queue_element.work,
                        queue_element.debt,
                        queue_element.composition_id,
                    )
                })
                .take(still_needed),
        );
    }

    // did not find enough work in the io_queue so check compute queue
    if functions.len() < number_of_functions {
        let still_needed = number_of_functions - functions.len();
        functions.extend(
            compute_queue
                .extract_if(|queue_element| {
                    if let WorkToDo::FunctionArguments { recorder, .. } = &mut queue_element.work {
                        if queue_element.flags & engine_flags != 0 {
                            recorder.record(RecordPoint::ComputeQueueEnd);
                            // Don't need to wake IO cores to do more prefetching,
                            // since those queues must be empty if we are taking from here.
                            true
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                })
                .map(|queue_element| {
                    (
                        queue_element.work,
                        queue_element.debt,
                        queue_element.composition_id,
                    )
                })
                .take(still_needed),
        );
    }

    functions
}

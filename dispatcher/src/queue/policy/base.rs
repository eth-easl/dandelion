use dandelion_commons::records::RecordPoint;
use machine_interface::{function_driver::WorkToDo, promise::Debt};
use std::collections::{BTreeMap, LinkedList};
use tokio::sync::mpsc;

use super::super::{ComputeQueueElement, IoQueueElement};

pub const PREFETCH_PER_CORE: usize = 1;

/// Empty struct since the base policy does not use additional information for scheduling decisions.
pub struct IOElementData;

/// Prepares the policy-specific element data for a new IO queue element.
/// Returns `None` if the work was offloaded to a remote node (consumed).
/// Returns `Some((work, debt, data))` if the work should be queued locally.
pub fn prepare_io_element(
    work: WorkToDo,
    debt: Debt,
    _try_offload: bool,
    _composition_id: usize,
    _remote_nodes: &std::sync::Mutex<BTreeMap<u64, mpsc::UnboundedSender<(WorkToDo, Debt, usize)>>>,
) -> Option<(WorkToDo, Debt, IOElementData)> {
    Some((work, debt, IOElementData))
}

/// Decides whether an IO queue element (FunctionArguments) should be taken by an IO worker.
pub fn should_io_take(
    _element_data: &IOElementData,
    compute_pending: usize,
    active_fetch_count: usize,
    local_cores: usize,
    _idle_compute_cores: usize,
) -> bool {
    compute_pending + active_fetch_count < PREFETCH_PER_CORE * local_cores
}

/// Selects work items from the local queues to hand off to a remote node.
pub fn get_work_for_remote(
    io_queue: &mut LinkedList<IoQueueElement>,
    compute_queue: &mut LinkedList<ComputeQueueElement>,
    engine_flags: u32,
    _node_id: u64,
    number_of_functions: usize,
    queue_state_decrease: &impl Fn(),
) -> Vec<(WorkToDo, Debt, usize)> {
    let mut functions = Vec::with_capacity(number_of_functions);
    // first check the queue with unresolved references, since those are easier to steal
    functions.extend(
        io_queue
            .extract_if(|queue_element| {
                if let WorkToDo::FunctionArguments { recorder, .. } = &mut queue_element.work {
                    if queue_element.flags & engine_flags != 0 {
                        queue_state_decrease();
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
    // did not find enough work in the io_queue so check compute queue
    if functions.len() < number_of_functions {
        let still_needed = number_of_functions - functions.len();
        functions.extend(
            compute_queue
                .extract_if(|queue_element| {
                    if let WorkToDo::FunctionArguments { recorder, .. } = &mut queue_element.work {
                        if queue_element.flags & engine_flags != 0 {
                            recorder.record(RecordPoint::ComputeQueueEnd);
                            queue_state_decrease();
                            // don't need to poke IO cores to do more prefetching,
                            // since those queues are empty if we are taking from here
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

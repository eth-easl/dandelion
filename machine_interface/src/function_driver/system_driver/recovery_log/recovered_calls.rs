use super::*;
use crate::{
    composition::{CompositionSet, ItemData},
    function_driver::system_driver::get_system_function_output_sets,
    DataItem, Position,
};

type CallOutputs = Vec<Option<CompositionSet>>;
type RecoveredCalls = HashMap<IoPath, CallOutputs>;
static RECOVERED_CALLS: OnceLock<Mutex<HashMap<RunId, RecoveredCalls>>> = OnceLock::new();

fn recovered_calls() -> &'static Mutex<HashMap<RunId, RecoveredCalls>> {
    RECOVERED_CALLS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Called only at recovery start, with the item records remaining after checkpoint validation.
/// Partial calls retain the existing per-item replay path.
pub async fn install_recovered_io_calls(
    run_id: RunId,
    records: &[IoCompletionRecord],
) -> DandelionResult<()> {
    let sizes = {
        let run_lock = run_log_lock(run_id);
        let mut state = run_lock.lock().await;
        ensure_run_log_loaded(&run_log_path(run_id)?, &mut state).await?;
        state.call_sizes.clone()
    };
    let calls = rebuild_calls(run_id, &sizes, records)?;
    recovered_calls()
        .lock()
        .expect("Recovered IO call map poisoned")
        .insert(run_id, calls);
    Ok(())
}

pub fn recovered_io_call(run_id: RunId, io_path: &IoPath) -> Option<CallOutputs> {
    recovered_calls()
        .lock()
        .expect("Recovered IO call map poisoned")
        .get(&run_id)?
        .get(io_path)
        .cloned()
}

pub(super) fn clear(run_id: RunId) {
    recovered_calls()
        .lock()
        .expect("Recovered IO call map poisoned")
        .remove(&run_id);
}

fn rebuild_calls(
    run_id: RunId,
    sizes: &HashMap<IoPath, (SystemFunction, usize)>,
    records: &[IoCompletionRecord],
) -> DandelionResult<RecoveredCalls> {
    let mut items_by_call: HashMap<&IoPath, HashMap<usize, &IoCompletionRecord>> = HashMap::new();
    let mut conflicting = HashSet::new();
    for record in records.iter().filter(|record| record.id.run_id == run_id) {
        let items = items_by_call.entry(&record.id.io_path).or_default();
        if let Some(previous) = items.insert(record.id.request_item_index, record) {
           
            if previous.function != record.function
                || encode_io_completion_payload(&previous.outputs)?
                    != encode_io_completion_payload(&record.outputs)?
            {
                conflicting.insert(&record.id.io_path);
            }
        }
    }

    let mut restored = HashMap::new();
    for (path, &(function, n)) in sizes {
        let empty = HashMap::new();
        let items = items_by_call.get(path).unwrap_or(&empty);
        let names = get_system_function_output_sets(function);
        if conflicting.contains(path)
            || items.len() != n
            || items
                .iter()
                .any(|(&index, record)| index >= n || !complete_item(record, function, &names))
        {
            continue;
        }

        let mut output_items = vec![Vec::with_capacity(n); names.len()];
        // Enumerate request positions
        for index in 0..n {
            let record = items[&index];
            for output in &record.outputs {
                let item = &output.items[0];
                let (size, data) = match &item.location {
                    IoCompletionData::Inline(bytes) => (
                        bytes.len(),
                        ItemData::LocalData(Arc::new(ReadOnlyContext::from_boxed(
                            bytes.clone().into_boxed_slice(),
                        )?)),
                    ),
                    IoCompletionData::Remote {
                        node_id,
                        data_id,
                        size,
                    } => (
                        *size,
                        ItemData::RemoteData(RemoteData::durable(*node_id, *data_id)),
                    ),
                };
                output_items[output.set_index].push((
                    DataItem {
                        ident: item.identifier.clone(),
                        key: item.key as u32, // Checked by complete_item.
                        data: Position { offset: 0, size },
                    },
                    data,
                ));
            }
        }
        let outputs = names
            .into_iter()
            .zip(output_items)
            .map(|(name, items)| CompositionSet::from_item_list(name, items))
            .collect();
        restored.insert(path.clone(), outputs);
    }
    Ok(restored)
}

fn complete_item(record: &IoCompletionRecord, function: SystemFunction, names: &[String]) -> bool {
    if record.function != function
        || record.outputs.len() != names.len()
        || record.request_id().is_err()
    {
        return false;
    }
    let mut seen = vec![false; names.len()];
    for output in &record.outputs {
        if output.set_index >= names.len()
            || seen[output.set_index]
            || output.set_name != names[output.set_index]
            || output.items.len() != 1
            || u32::try_from(output.items[0].key).is_err()
        {
            return false;
        }
        seen[output.set_index] = true;
    }
    true
}

#[cfg(test)]
mod tests;

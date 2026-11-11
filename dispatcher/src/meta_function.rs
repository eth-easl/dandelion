use dandelion_commons::{err_dandelion, DandelionError, DandelionResult, FunctionRegistryError};
use memory::{context::ContextTrait, DataItem, DataSet};
use std::sync::Arc;

/// Input set of a planner with one item per data input of the application, in application order.
/// Each item is named after the set in the applying composition, has its position as key and holds
/// the number of items in the set followed by their summed size in bytes, both as little endian u64.
pub const INPUT_METADATA_SET: &str = "__dandelion_metadata_in";
/// Input set of a planner with one empty item per output of the application, in application order.
/// Each item is named after the set in the applying composition and has its position as key.
pub const OUTPUT_METADATA_SET: &str = "__dandelion_metadata_out";
/// Name of the single output set of a planner, holding one item with the source of the composition
/// to run in place of the metafunction.
pub const COMPOSITION_SET: &str = "composition";

/// Builds the input metadata set for the given data inputs. Only uses what the platform already
/// knows about the sets, so remote items are not resolved.
pub fn input_metadata_set(names: &[String], sets: &[DataSet]) -> DandelionResult<DataSet> {
    DataSet::from_keyed_bytes(
        names
            .iter()
            .zip(sets)
            .enumerate()
            .map(|(key, (name, set))| {
                let mut data = (set.len() as u64).to_le_bytes().to_vec();
                data.extend_from_slice(&(set.total_size as u64).to_le_bytes());
                (name.clone(), key as u32, data)
            }),
    )
}

/// Builds the output metadata set for the given outputs.
pub fn output_metadata_set(names: &[String]) -> DandelionResult<DataSet> {
    DataSet::from_keyed_bytes(
        names
            .iter()
            .enumerate()
            .map(|(key, name)| (name.clone(), key as u32, vec![])),
    )
}

/// Reads the composition source from the output sets of a planner.
pub fn read_composition(planner_output: &[Arc<Vec<DataItem>>]) -> DandelionResult<String> {
    let item = match planner_output.first().map(|set| set.as_slice()) {
        Some([item]) => item,
        _ => {
            return err_dandelion!(DandelionError::FunctionRegistry(
                FunctionRegistryError::InvalidPlan(format!(
                    "Planner needs to return exactly one item in its '{}' set",
                    COMPOSITION_SET
                )),
            ))
        }
    };
    let local = item.local_ref()?;
    let mut bytes = vec![0u8; local.pos.size];
    local.ctx.read(local.pos.offset, &mut bytes)?;
    match String::from_utf8(bytes) {
        Ok(composition) => Ok(composition),
        Err(_) => err_dandelion!(DandelionError::FunctionRegistry(
            FunctionRegistryError::InvalidPlan(String::from(
                "Composition returned by the planner is not valid UTF-8"
            )),
        )),
    }
}

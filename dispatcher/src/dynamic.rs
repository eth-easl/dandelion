//! The interface between the platform and the selector of a dynamic function.
//!
//! A dynamic function is registered with the input and output sets of the function it stands for.
//! Instead of computing the outputs itself, its binary (the selector) gets the input sets, with the
//! sets registered as metadata sets replaced by a description of them, and returns the identifier
//! of the function that is run on the original input sets to produce the outputs.

use dandelion_commons::{err_dandelion, DandelionError, DandelionResult, FunctionRegistryError};
use memory::{context::ContextTrait, DataItem, DataSet};
use std::sync::Arc;

/// Name of the single output set of a selector, holding one item with the selected function id.
pub const SELECTION_SET: &str = "function_id";

/// Metadata item holding the number of items in the described set, as little endian u64.
pub const NUM_ITEMS_ITEM: &str = "num_items";
/// Metadata item holding the summed size in bytes of all items in the described set, as little
/// endian u64.
pub const TOTAL_SIZE_ITEM: &str = "total_size";

/// How an input set of a dynamic function is passed to its selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectorInput {
    /// The selector gets the set itself.
    Data,
    /// The selector gets a set describing the set, see [`metadata_set`].
    Metadata,
}

/// Builds the set describing `set`, with one item per property named after the property.
/// Only uses what the platform already knows about the set, so remote items are not resolved.
pub fn metadata_set(set: &DataSet) -> DandelionResult<DataSet> {
    DataSet::from_bytes([
        (
            NUM_ITEMS_ITEM.to_string(),
            (set.len() as u64).to_le_bytes().to_vec(),
        ),
        (
            TOTAL_SIZE_ITEM.to_string(),
            (set.total_size as u64).to_le_bytes().to_vec(),
        ),
    ])
}

/// Reads the selected function id from the output sets of a selector.
pub fn read_selection(selector_output: &[Arc<Vec<DataItem>>]) -> DandelionResult<String> {
    let item = match selector_output.first().map(|set| set.as_slice()) {
        Some([item]) => item,
        _ => {
            return err_dandelion!(DandelionError::FunctionRegistry(
                FunctionRegistryError::InvalidSelection(format!(
                    "Selector needs to return exactly one item in its '{}' set",
                    SELECTION_SET
                )),
            ))
        }
    };
    let local = item.local_ref()?;
    let mut bytes = vec![0u8; local.pos.size];
    local.ctx.read(local.pos.offset, &mut bytes)?;
    match String::from_utf8(bytes) {
        Ok(id) => Ok(id.trim().to_string()),
        Err(_) => err_dandelion!(DandelionError::FunctionRegistry(
            FunctionRegistryError::InvalidSelection(String::from(
                "Selected function id is not valid UTF-8"
            )),
        )),
    }
}

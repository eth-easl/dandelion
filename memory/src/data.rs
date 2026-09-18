use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::context::{Context, ContextDataSet};
use dandelion_commons::{DandelionResult, FunctionId};

//---------------------------------------------------
// ItemData
//---------------------------------------------------

pub type ResolvedItemData = Pin<Box<dyn Future<Output = DandelionResult<LocalItemData>> + Send>>;
pub trait ItemData: Debug + Send + Sync {
    fn size(&self) -> usize;
    fn is_local(&self) -> bool;
    fn resolve(self: Arc<Self>) -> ResolvedItemData;
    fn data(&self) -> DandelionResult<(Arc<Context>, Position)>;
}

#[derive(Clone, Copy, Debug)]
pub struct Position {
    pub offset: usize,
    pub size: usize,
}

/// Data that is available locally on the node to use for computation.
#[derive(Clone, Debug)]
pub struct LocalItemData {
    pub ctx: Arc<Context>,
    pub pos: Position,
}

impl ItemData for LocalItemData {
    fn size(&self) -> usize {
        self.pos.size
    }
    fn is_local(&self) -> bool {
        true
    }
    fn resolve(self: Arc<Self>) -> ResolvedItemData {
        Box::pin(async move { Ok((*self).clone()) })
    }
    fn data(&self) -> DandelionResult<(Arc<Context>, Position)> {
        Ok((self.ctx.clone(), self.pos))
    }
}

//---------------------------------------------------
// DataItem
//---------------------------------------------------

#[derive(Clone, Debug)]
pub struct DataItem {
    pub ident: String,
    pub key: u32,
    pub data: Arc<dyn ItemData>,
}

impl DataItem {
    pub fn new_local(ident: String, key: u32, ctx: Arc<Context>, pos: Position) -> Self {
        DataItem {
            ident,
            key,
            data: Arc::new(LocalItemData { ctx, pos }),
        }
    }

    pub fn is_local(&self) -> bool {
        self.data.is_local()
    }
}

//---------------------------------------------------
// DataSet
//---------------------------------------------------

#[derive(Clone, Debug, Default)]
pub struct DataSet {
    /// Share items among DataSet clones so sets that are frequently read by multiple consumers
    /// (e.g. streaming consumers) are efficient.
    pub items: Arc<Vec<Arc<DataItem>>>,
    pub num_unresolved: usize,
    pub total_size: usize,
}

impl DataSet {
    pub fn from_items(items: Arc<Vec<Arc<DataItem>>>) -> DataSet {
        let mut num_unresolved = 0;
        let mut total_size = 0;
        for item in items.iter() {
            if !item.is_local() {
                num_unresolved += 1;
            }
            total_size += item.data.size();
        }
        DataSet {
            items,
            num_unresolved,
            total_size,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_local(&self) -> bool {
        self.num_unresolved == 0
    }

    pub fn combine(mut sets: Vec<DataSet>) -> DataSet {
        let mut out_set = match sets.pop() {
            Some(set) => set,
            None => return DataSet::default(),
        };
        let mut items = (*out_set.items).clone();
        while let Some(next_set) = sets.pop() {
            items.extend(next_set.items.iter().cloned());
            out_set.num_unresolved += next_set.num_unresolved;
            out_set.total_size += next_set.total_size;
        }
        items.sort_by_key(|a| a.key);
        out_set.items = Arc::new(items);
        out_set
    }

    pub fn from_context(mut ctx: Context) -> Vec<Option<DataSet>> {
        // take the content from the context before putting it into an arc
        let sets = core::mem::take(&mut ctx.content);
        let context_arc = Arc::new(ctx);
        sets.into_iter()
            .map(|set_option| {
                if let Some(set) = set_option {
                    let ContextDataSet { items, total_size } = set;
                    if items.is_empty() {
                        None
                    } else {
                        let set_items = items
                            .into_iter()
                            .map(|ctx_itm| {
                                Arc::new(DataItem::new_local(
                                    ctx_itm.ident,
                                    ctx_itm.key,
                                    context_arc.clone(),
                                    ctx_itm.data,
                                ))
                            })
                            .collect();
                        Some(DataSet {
                            items: Arc::new(set_items),
                            num_unresolved: 0,
                            total_size,
                        })
                    }
                } else {
                    None
                }
            })
            .collect()
    }
}

/// Iterator over a reference of the composition set, not taking ownership.
impl<'origin> IntoIterator for &'origin DataSet {
    type Item = &'origin Arc<DataItem>;
    type IntoIter = std::slice::Iter<'origin, Arc<DataItem>>;
    fn into_iter(self) -> Self::IntoIter {
        self.items.iter()
    }
}

//---------------------------------------------------
// DataSetAccumulator
//---------------------------------------------------

#[derive(Debug)]
pub struct DataSetAccumulator {
    items: Vec<Arc<DataItem>>,
    num_unresolved: usize,
    total_size: usize,
}

impl DataSetAccumulator {
    pub fn new() -> Self {
        DataSetAccumulator {
            items: Vec::new(),
            num_unresolved: 0,
            total_size: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn push_items(&mut self, items: &[Arc<DataItem>]) {
        for item in items.iter() {
            if !item.is_local() {
                self.num_unresolved += 1;
            }
            self.total_size += item.data.size();
            self.items.push(item.clone());
        }
    }

    pub fn push_set(&mut self, set: DataSet) {
        self.items.extend(set.items.iter().cloned());
        self.num_unresolved += set.num_unresolved;
        self.total_size += set.total_size;
    }

    /// Returns the current (unsorted) set.
    pub fn clone_set(&self) -> DataSet {
        DataSet {
            items: Arc::new(self.items.clone()),
            num_unresolved: self.num_unresolved,
            total_size: self.total_size,
        }
    }

    /// Consumes the accumulator returning the set sorted by key.
    pub fn collect(mut self) -> DataSet {
        self.items.sort_by_key(|itm| itm.key);
        self.collect_unsorted()
    }

    /// Consumes the accumulator returning the (unsorted) set.
    pub fn collect_unsorted(self) -> DataSet {
        DataSet {
            items: Arc::new(self.items),
            num_unresolved: self.num_unresolved,
            total_size: self.total_size,
        }
    }
}

// TODO: move to better file?
//---------------------------------------------------
// Invocation
//---------------------------------------------------

#[derive(Debug)]
pub struct Invocation {
    pub function_id: FunctionId,
    pub composition_idx: usize,
    pub input: Vec<DataSet>,
}

/// Struct holding general function metadata that is true across all drivers.
#[derive(Debug)]
pub struct Metadata {
    /// The input set names with an optional static composition set. If the static set is set it will
    /// prioritized and any other input for that set is ignored.
    pub input_sets: Vec<(String, Option<DataSet>)>,
    /// The output set names.
    /// TODO these strings need to be clone around a few times, should think about also putting them in arcs.
    /// May be worth a general consireations, if that should be the default for Strings, also may want to check we only propagate them when they are actually necessary.
    pub output_sets: Vec<String>,
    /// The minimum size in bytes the largest set of a group of any sets should have. If given (i.e.
    /// has a value of > 0) the JoinIterator will combine any sets to achieve this size best-effort.
    pub min_set_bytes: Vec<usize>,
}

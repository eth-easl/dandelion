use std::{fmt::Debug, ops::Deref, sync::Arc};

use crate::context::{read_only::ReadOnlyContext, Context, ContextDataSet};
use dandelion_commons::DandelionResult;
use log::error;
use tokio::sync::{mpsc, OnceCell};

#[derive(Debug, Clone)]
pub enum ItemData {
    Local(LocalData),
    Remote(RemoteData),
    Io(IoData),
}

impl ItemData {
    fn size(&self) -> usize {
        match self {
            ItemData::Local(LocalData { pos, .. }) => pos.size,
            ItemData::Remote(remote_data) => remote_data.size,
            // NOTE: The size of IO data is only known once it is resolved.
            ItemData::Io(_) => 0,
        }
    }
    fn is_local(&self) -> bool {
        matches!(self, ItemData::Local(_))
    }
}

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
pub struct Position {
    pub offset: usize,
    pub size: usize,
}

/// Data that is available locally on the node to use for computation.
#[derive(Clone, Debug)]
pub struct LocalData {
    pub ctx: Arc<Context>,
    pub pos: Position,
}

/// A reference to data that lives on another node.
#[derive(Clone, Debug)]
pub struct RemoteData {
    inner: Arc<RemoteDataInner>,
}

/// TODO: for the ones we create locally we are going through the sender too,
/// think if it would be easier to have an enum in the delete sender to perform local drop directly.
#[derive(Debug)]
pub struct RemoteDataInner {
    pub node_id: u64,
    pub data_id: u64,
    pub size: usize,
    delete_sender: Option<mpsc::UnboundedSender<(u64, u64)>>,
}

impl RemoteData {
    pub fn new(node_id: u64, data_id: u64, size: usize) -> Self {
        Self {
            inner: Arc::new(RemoteDataInner {
                node_id,
                data_id,
                size,
                delete_sender: None,
            }),
        }
    }

    pub fn delete_on_drop(
        node_id: u64,
        data_id: u64,
        size: usize,
        delete_sender: mpsc::UnboundedSender<(u64, u64)>,
    ) -> Self {
        Self {
            inner: Arc::new(RemoteDataInner {
                node_id,
                data_id,
                size,
                delete_sender: Some(delete_sender),
            }),
        }
    }
}

impl Deref for RemoteData {
    type Target = RemoteDataInner;

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref()
    }
}

impl Drop for RemoteDataInner {
    fn drop(&mut self) {
        if let Some(delete_sender) = &self.delete_sender {
            if let Err(err) = delete_sender.send((self.node_id, self.data_id)) {
                error!(
                    "Failed to send remote data deletion message for node_id {}, data_id {}: {}",
                    self.node_id, self.data_id, err
                );
            }
        }
    }
}

/// Data that needs to be fetched using an IO function.
#[derive(Debug, Clone)]
pub struct IoData {
    pub original_data: Box<ItemData>,
    // A vec with the resolved outputs for this IO request
    // one entry for each output set of the function.
    // The output item starts at 0 in the context and goes until the end of the context.
    pub resolved: Arc<OnceCell<DandelionResult<Vec<Arc<Context>>>>>,
    pub function: dandelion_commons::SystemFunction,
    pub set_index: usize,
}

#[derive(Debug, Clone)]
pub struct DataItem {
    inner: Arc<DataItemInner>,
}

impl core::ops::Deref for DataItem {
    type Target = DataItemInner;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Clone, Debug)]
pub struct DataItemInner {
    pub ident: String,
    pub key: u32,
    pub data: ItemData,
}

impl DataItem {
    pub fn new(ident: String, key: u32, data: ItemData) -> Self {
        DataItem {
            inner: Arc::new(DataItemInner { ident, key, data }),
        }
    }

    pub fn new_local(ident: String, key: u32, local: LocalData) -> Self {
        DataItem {
            inner: Arc::new(DataItemInner {
                ident,
                key,
                data: ItemData::Local(local),
            }),
        }
    }

    pub fn new_io(ident: String, key: u32, io_data: IoData) -> Self {
        DataItem {
            inner: Arc::new(DataItemInner {
                ident,
                key,
                data: ItemData::Io(io_data),
            }),
        }
    }

    pub fn size(&self) -> usize {
        self.data.size()
    }

    pub fn is_local(&self) -> bool {
        self.data.is_local()
    }
    pub fn local_ref(&self) -> DandelionResult<&LocalData> {
        if let ItemData::Local(local) = &self.data {
            Ok(local)
        } else {
            // TODO make specific error to make it easer to recognize
            dandelion_commons::err_dandelion!(dandelion_commons::DandelionError::InvalidRead)
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct DataSet {
    /// Share items among DataSet clones so sets that are frequently read by multiple consumers
    /// (e.g. streaming consumers) are efficient.
    pub items: Arc<Vec<DataItem>>,
    pub num_unresolved: usize,
    pub total_size: usize,
}

impl DataSet {
    // TODO this does construct an unsorted data set, meaning we cannot rely on datasets always being sorted.
    // Should check if we need this or it would make sense to only construct datasets that are sorted (or never sorted, and leave that to the composition sets?)
    pub fn from_items(items: Arc<Vec<DataItem>>) -> DataSet {
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

    /// Builds a set out of named byte buffers, each backed by its own read only context.
    pub fn from_bytes<I: IntoIterator<Item = (String, Vec<u8>)>>(
        items: I,
    ) -> DandelionResult<DataSet> {
        let items = items
            .into_iter()
            .map(|(ident, data)| {
                let size = data.len();
                let ctx = ReadOnlyContext::from_boxed(data.into_boxed_slice())?;
                Ok(DataItem::new_local(
                    ident,
                    0,
                    LocalData {
                        ctx: Arc::new(ctx),
                        pos: Position { offset: 0, size },
                    },
                ))
            })
            .collect::<DandelionResult<Vec<_>>>()?;
        Ok(DataSet::from_items(Arc::new(items)))
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

    pub fn from_context(mut ctx: Context) -> Vec<DataSet> {
        // take the content from the context before putting it into an arc
        let sets = core::mem::take(&mut ctx.content);
        let context_arc = Arc::new(ctx);
        sets.into_iter()
            .map(|set_option| {
                if let Some(set) = set_option {
                    let ContextDataSet { items, total_size } = set;
                    if items.is_empty() {
                        DataSet {
                            items: Arc::new(Vec::new()),
                            num_unresolved: 0,
                            total_size: 0,
                        }
                    } else {
                        let set_items = items
                            .into_iter()
                            .map(|ctx_itm| {
                                DataItem::new_local(
                                    ctx_itm.ident,
                                    ctx_itm.key,
                                    LocalData {
                                        ctx: context_arc.clone(),
                                        pos: ctx_itm.data,
                                    },
                                )
                            })
                            .collect();
                        DataSet {
                            items: Arc::new(set_items),
                            num_unresolved: 0,
                            total_size,
                        }
                    }
                } else {
                    DataSet {
                        items: Arc::new(Vec::new()),
                        num_unresolved: 0,
                        total_size: 0,
                    }
                }
            })
            .collect()
    }
}

/// Iterator over a reference of the composition set, not taking ownership.
impl<'origin> IntoIterator for &'origin DataSet {
    type Item = &'origin DataItem;
    type IntoIter = std::slice::Iter<'origin, DataItem>;
    fn into_iter(self) -> Self::IntoIter {
        self.items.iter()
    }
}

#[derive(Debug)]
pub struct DataSetAccumulator {
    items: Vec<DataItem>,
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

    pub fn push_items(&mut self, items: &[DataItem]) {
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

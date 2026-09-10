use std::sync::Arc;

use crate::FunctionId;

#[derive(Clone, Copy, Debug)]
pub struct Position {
    pub offset: usize,
    pub size: usize,
}

#[derive(Clone, Debug)]
pub struct DataItem {
    pub ident: String,
    pub data: Position,
    pub key: u32,
}

impl DataItem {
    pub fn is_local(&self) -> bool {
        todo!("implement");
    }
}

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
            total_size += item.data.size;
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
}

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
            self.total_size += item.data.size;
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

#[derive(Debug)]
pub struct Invocation {
    pub function_id: FunctionId,
    pub composition_idx: usize,
    pub input: Vec<DataSet>,
}

// #[derive(Clone, Debug)]
// pub enum ItemData {
//     /// Data that is available locally on the node to use for computation.
//     LocalData(Arc<Context>),
//     /// Data that is already on another node and can be fetched from there.
//     RemoteData(RemoteData),
//     /// Data that needs to be fetched using an IO function.
//     IoData(IoData),
// }

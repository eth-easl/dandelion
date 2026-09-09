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
    pub items: Vec<Arc<DataItem>>,
    pub num_unresolved: usize,
    pub total_size: usize,
}

impl DataSet {
    pub fn from_items(items: Vec<Arc<DataItem>>) -> DataSet {
        let mut num_unresolved = 0;
        let mut total_size = 0;
        for item in items.iter() {
            if !item.is_local() {
                num_unresolved += 1;
            }
            total_size += item.data.size;
        }
        DataSet {
            items: items,
            num_unresolved,
            total_size,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn combine(mut sets: Vec<DataSet>) -> DataSet {
        // TODO: can this be written more efficiently?
        let mut out_set = match sets.pop() {
            Some(set) => set,
            None => return DataSet::default(),
        };
        while let Some(next_set) = sets.pop() {
            let DataSet {
                items,
                num_unresolved,
                total_size,
            } = next_set;
            out_set.items.extend(items);
            out_set.num_unresolved += num_unresolved;
            out_set.total_size += total_size;
        }
        out_set.items.sort_by_key(|a| a.key);
        out_set
    }
}

pub struct DataSetAccumulator {
    set: DataSet,
}

impl DataSetAccumulator {
    pub fn new() -> Self {
        DataSetAccumulator {
            set: DataSet::default(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.set.is_empty()
    }

    pub fn push_items(&mut self, items: &[Arc<DataItem>]) {
        for item in items.iter() {
            if !item.is_local() {
                self.set.num_unresolved += 1;
            }
            self.set.total_size += item.data.size;
            self.set.items.push(item.clone());
        }
    }

    pub fn push_set(&mut self, set: DataSet) {
        let DataSet {
            items,
            num_unresolved,
            total_size,
        } = set;
        self.set.items.extend(items);
        self.set.num_unresolved += num_unresolved;
        self.set.total_size += total_size;
    }

    /// Returns the current (unsorted) set.
    pub fn clone_set(&self) -> DataSet {
        self.set.clone()
    }

    /// Consumes the accumulator returning the set sorted by key.
    pub fn collect(mut self) -> DataSet {
        self.set.items.sort_by_key(|itm| itm.key);
        self.set
    }

    /// Consumes the accumulator returning the (unsorted) set.
    pub fn collect_unsorted(self) -> DataSet {
        self.set
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

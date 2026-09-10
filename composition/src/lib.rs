mod function;
mod join_iterator;
mod set;
mod sharding;

use std::sync::{Arc, Weak};

use crate::{function::Function, set::CompositionSet, sharding::AnyShardingMode};
use dandelion_commons::data::{DataSet, Invocation};

pub struct Composition {
    functions: Vec<Arc<Function>>,
    any_sharding_mode: AnyShardingMode,
    input_sets: Vec<Weak<CompositionSet>>,
    output_sets: Vec<Arc<CompositionSet>>,
}

impl Composition {
    pub fn parse(raw: &str) -> Self {
        todo!("implement composition creation (from parser directly)")
    }

    pub fn start_execution(&self, composition_inputs: Vec<DataSet>) -> Vec<Invocation> {
        debug_assert_eq!(composition_inputs.len(), self.input_sets.len());

        for (i, in_set) in composition_inputs.into_iter().enumerate() {
            self.input_sets[i]
                .upgrade()
                .expect("Composition input set is gone.")
                .set_composition_input(in_set);
        }

        let mut initial_invocations = Vec::new();
        for f in self.functions.iter() {
            initial_invocations.extend(f.in_set_complete(&self.any_sharding_mode));
        }
        initial_invocations
    }

    pub fn push_invocation_output(
        &self,
        output: Vec<DataSet>,
        composition_idx: usize,
    ) -> Vec<Invocation> {
        self.functions[composition_idx].add_invocation_output(output, &self.any_sharding_mode)
    }

    pub fn collect(self) -> Vec<DataSet> {
        self.output_sets
            .iter()
            .map(|out_set| out_set.get_set(None))
            .collect()
    }
}

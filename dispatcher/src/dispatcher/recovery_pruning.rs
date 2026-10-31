use super::*;
use std::collections::{HashMap, HashSet};

pub(super) struct ReplayPlan {
    pub needed: Vec<bool>,
    pub restored: BTreeMap<usize, Vec<Option<CompositionSet>>>,
}

impl ReplayPlan {
    pub fn discover(composition: &Composition, policy: &impl IoReferencePolicy) -> Option<Self> {
        let mut restored: BTreeMap<_, _> = composition
            .dependencies
            .iter()
            .enumerate()
            .filter_map(|(index, _)| {
                policy
                    .nested(index)
                    .restored_call()
                    .map(|outputs| (index, outputs))
            })
            .collect();
        if restored.is_empty() {
            // In particular, first attempts retain the existing scheduling behaviour.
            return None;
        }

        let mut producers: HashMap<usize, Vec<usize>> = HashMap::new();
        for (index, dependency) in composition.dependencies.iter().enumerate() {
            for set in dependency.output_set_ids.iter().flatten() {
                producers.entry(*set).or_default().push(index);
            }
        }
        let mut needed = vec![false; composition.dependencies.len()];
        let mut pending: Vec<_> = composition.output_map.keys().copied().collect();
        let mut visited = HashSet::new();
        while let Some(set) = pending.pop() {
            if !visited.insert(set) {
                continue;
            }
            for &index in producers.get(&set).into_iter().flatten() {
                if needed[index] {
                    continue;
                }
                needed[index] = true;
                if !restored.contains_key(&index) {
                    // Nested compositions conservatively require every input at this level.
                    // They prune their own bodies when queue_composition recurses into them.
                    pending.extend(
                        composition.dependencies[index]
                            .input_set_ids
                            .iter()
                            .flatten()
                            .map(|input| input.composition_id),
                    );
                }
            }
        }
        restored.retain(|index, _| needed[*index]);
        Some(Self { needed, restored })
    }
}

#[cfg(test)]
mod tests;

//! Shared helpers for the black-box `composition` tests.
//!
//! Everything here only goes through `composition`'s public API (`CompositionTemplate::parse`,
//! `Composition`, `Registry`), the same way a real embedder of the crate would.
//!
//! This file is compiled separately into each `tests/*/main.rs` binary, so any one binary may
//! only use a subset of it.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;

use composition::{Composition, CompositionTemplate, Registry};
use dandelion_commons::{
    dandelion_err, err_dandelion, DandelionError, DandelionResult, FunctionId,
    FunctionRegistryError,
};
use memory::{
    data::{DataItem, DataSet, Position},
    Context,
};

/// A small real context for tests; the composition layer only tracks items, it never reads their
/// bytes, so any context of sufficient size will do.
pub fn test_context() -> Arc<Context> {
    use memory::context::{malloc::MallocMemoryDomain, MemoryDomain, MemoryResource};
    let domain = MallocMemoryDomain::init(MemoryResource::None).expect("malloc domain");
    Arc::new(domain.acquire_context(4096).expect("context"))
}

/// A hand-rolled [`Registry`] for tests: a function must be registered with
/// [`TestRegistry::with_function`] before composition source referencing it can be parsed.
#[derive(Default)]
pub struct TestRegistry {
    functions: HashMap<String, (Vec<String>, Vec<String>)>,
}

impl TestRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a function with the given parameter and return names, builder-style.
    pub fn with_function(mut self, name: &str, params: &[&str], returns: &[&str]) -> Self {
        self.functions.insert(
            name.to_string(),
            (
                params.iter().map(|s| s.to_string()).collect(),
                returns.iter().map(|s| s.to_string()).collect(),
            ),
        );
        self
    }
}

impl Registry for TestRegistry {
    fn check_declaration(&self, id: &str, params: &[&str], rets: &[&str]) -> DandelionResult<()> {
        let (reg_params, reg_rets) = self.functions.get(id).ok_or_else(|| {
            dandelion_err!(DandelionError::Parsing(format!(
                "unknown function '{id}' in test registry"
            )))
        })?;
        let params_match = reg_params
            .iter()
            .map(String::as_str)
            .eq(params.iter().copied());
        let rets_match = reg_rets.iter().map(String::as_str).eq(rets.iter().copied());
        if params_match && rets_match {
            Ok(())
        } else {
            Err(dandelion_err!(DandelionError::Parsing(format!(
                "registered signature for '{id}' does not match its declaration"
            ))))
        }
    }

    fn id_exists(&self, id: &str) -> bool {
        self.functions.contains_key(id)
    }

    fn get_min_set_bytes(&self, id: &FunctionId) -> DandelionResult<Vec<usize>> {
        match self
            .functions
            .get(id.as_str())
            .map(|(params, _)| vec![0; params.len()])
        {
            Some(x) => Ok(x),
            None => err_dandelion!(DandelionError::FunctionRegistry(
                FunctionRegistryError::UnknownFunction(id.to_string())
            )),
        }
    }
}

/// Parses `src` and returns the single composition named `name`, panicking with the rendered
/// diagnostics on a parse error and if there is no such composition.
pub fn parse_composition(src: &str, name: &str, registry: &TestRegistry) -> CompositionTemplate {
    let compositions = CompositionTemplate::parse(src, registry)
        .unwrap_or_else(|e| panic!("failed to parse composition source:\n{e}"));
    compositions
        .into_iter()
        .find(|(id, _, _)| id.as_str() == name)
        .unwrap_or_else(|| panic!("composition '{name}' not found in parsed source"))
        .1
}

/// Builds a [`DataItem`] with the given key and a made-up (zero-sized) position; good enough for
/// tests that only care about item identity/ordering, not actual data contents.
pub fn item(ident: &str, key: u32) -> Arc<DataItem> {
    Arc::new(DataItem::new_local(
        ident.to_string(),
        key,
        test_context(),
        Position { offset: 0, size: 0 },
    ))
}

/// Builds a [`DataSet`] out of the given items.
pub fn data_set(items: Vec<Arc<DataItem>>) -> DataSet {
    DataSet::from_items(Arc::new(items))
}

/// Runs a composition to completion given its composition-level inputs, driving every produced
/// [`memory::data::Invocation`] through `respond` (which stands in for actually running
/// the invoked function) until nothing is left outstanding, then returns the composition outputs.
///
/// `respond` receives each invocation and returns the output sets it produced.
pub fn run_to_completion(
    mut composition: Composition,
    inputs: Vec<DataSet>,
    mut respond: impl FnMut(&memory::data::Invocation) -> Vec<DataSet>,
) -> Vec<DataSet> {
    let mut pending = Vec::new();
    composition.start_execution(inputs, &mut pending);
    while let Some(invocation) = pending.pop() {
        let output = respond(&invocation);
        composition.push_invocation_output(output, invocation.composition_idx, &mut pending);
    }
    composition.collect()
}

//! Shared helpers for the black-box `composition` tests.
//!
//! Everything here only goes through `composition`'s public API (`CompositionTemplate::parse`,
//! `Composition`, `Registry`), the same way a real embedder of the crate would.
//!
//! This file is compiled separately into each `tests/*/main.rs` binary, so any one binary may
//! only use a subset of it. Items that are not used by all binaries carry
//! an `#[allow(dead_code)]`.

use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use composition::{Composition, CompositionTemplate, Executor, Registry};
use dandelion_commons::{
    dandelion_err, err_dandelion, DandelionError, DandelionResult, FunctionId,
    FunctionRegistryError,
};
use memory::{
    data::{DataItem, DataSet, Position},
    Context, LocalData,
};

/// A small real read only context for tests.
#[allow(dead_code)] // not used by every test binary
pub fn test_context() -> Arc<Context> {
    use memory::context::read_only::ReadOnlyContext;
    Arc::new(ReadOnlyContext::from_boxed(vec![0u8; 4096].into_boxed_slice()).expect("context"))
}

/// A simple `Registry` used for tests.
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

/// Parses `src` into a single composition names `name`. Panicks on a parse error.
#[allow(dead_code)] // not used by every test binary
pub fn parse_composition(src: &str, name: &str, registry: &TestRegistry) -> CompositionTemplate {
    let compositions = CompositionTemplate::parse(src, registry)
        .unwrap_or_else(|e| panic!("failed to parse composition source:\n{e}"));
    compositions
        .into_iter()
        .find(|(id, _, _)| id.as_str() == name)
        .unwrap_or_else(|| panic!("composition '{name}' not found in parsed source"))
        .1
}

/// Builds a dummy zero-sized `DataItem` with the given key.
#[allow(dead_code)] // not used by every test binary
pub fn item(ident: &str, key: u32) -> DataItem {
    DataItem::new_local(
        ident.to_string(),
        key,
        LocalData {
            ctx: test_context(),
            pos: Position { offset: 0, size: 0 },
        },
    )
}

/// An invocation as enqueued by the composition, collected by the tests to inspect it.
#[derive(Debug)]
#[allow(dead_code)] // not used by every test binary
pub struct Invocation {
    pub function_id: FunctionId,
    pub composition_idx: usize,
    pub input: Vec<DataSet>,
}

/// An `ExecutionQueue` that simply collects all enqueued invocations.
#[derive(Debug, Default)]
#[allow(dead_code)] // not used by every test binary
pub struct InvocationQueue(pub Vec<Invocation>);

impl Executor for InvocationQueue {
    fn enqueue_invocations<T: IntoIterator<Item = Vec<DataSet>>>(
        &mut self,
        function_id: FunctionId,
        composition_idx: usize,
        inv_input_iter: T,
    ) {
        self.0
            .extend(inv_input_iter.into_iter().map(|input| Invocation {
                function_id: function_id.clone(),
                composition_idx,
                input,
            }));
    }
}

impl Deref for InvocationQueue {
    type Target = Vec<Invocation>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for InvocationQueue {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// Runs a composition to completion given its inputs using `exec_stub` to produce output sets from
/// input sets.
#[allow(dead_code)] // not used by every test binary
pub fn run_to_completion(
    mut composition: Composition,
    inputs: Vec<DataSet>,
    mut exec_stub: impl FnMut(&Invocation) -> Vec<Arc<Vec<DataItem>>>,
) -> Vec<DataSet> {
    let mut pending = InvocationQueue::default();
    composition.start_execution(inputs, &mut pending);
    while let Some(invocation) = pending.pop() {
        let output = exec_stub(&invocation);
        composition.push_invocation_output(output, invocation.composition_idx, &mut pending);
    }
    composition.collect()
}

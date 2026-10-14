mod function;
mod join_iterator;
mod parser;
mod set;
mod sharding;

use std::sync::Arc;

pub use crate::sharding::{AnyShardingMode, AnyShardingParams, DEFAULT_AUTOSHARDING_OFFLOAD_CONST};
use crate::{
    function::Function,
    parser::{ErrorDiagnostic, Parser},
    set::CompositionSet,
    sharding::Sharding,
};
use dandelion_commons::{dandelion_err, DandelionError, DandelionResult, FunctionId};
use log::warn;
use memory::{
    data::{DataSet, Metadata},
    DataItem,
};

/// Functions from the function registry side required build compositions.
pub trait Registry {
    /// Confirms the declared function is registered with matching params and returns.
    fn check_declaration(&self, id: &str, params: &[&str], rets: &[&str]) -> DandelionResult<()>;
    /// Simple lookup whether an identifier is already registered.
    fn id_exists(&self, id: &str) -> bool;
    /// Get min_set_bytes for a function.
    fn get_min_set_bytes(&self, id: &FunctionId) -> DandelionResult<Vec<usize>>;
}

/// Functions from the queue side required to execute compositions.
pub trait Executor {
    /// Enqueues invocations of a composition function from an iterator over the invocation's input sets.
    fn enqueue_invocations<T: IntoIterator<Item = Vec<DataSet>>>(
        &mut self,
        function_id: FunctionId,
        composition_idx: usize,
        inv_input_iter: T,
    );
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct InputSetTemplate {
    set_idx: usize,
    sharding: Sharding,
    optional: bool,
}

#[derive(Debug)]
pub(crate) struct FunctionTemplate {
    id: FunctionId,
    params: Vec<Option<InputSetTemplate>>,
    join_order: Arc<Vec<usize>>,
    returns: Vec<Option<usize>>,
}

#[derive(Debug)]
pub struct CompositionTemplate {
    functions: Vec<FunctionTemplate>,
    params: Vec<usize>,
    returns: Vec<usize>,
    num_sets: usize,
}

impl CompositionTemplate {
    pub fn parse<R: Registry>(
        raw: &str,
        registry: &R,
    ) -> DandelionResult<Vec<(FunctionId, Self, Metadata)>> {
        let parser = Parser::new(registry);
        parser.parse(raw).map_err(|diagnostics| {
            let render = |render_fn: fn(&ErrorDiagnostic, &str) -> String| {
                diagnostics
                    .iter()
                    .map(|diagnostic| render_fn(diagnostic, raw))
                    .collect::<String>()
            };
            warn!(
                "Composition parsing failed: \n{}",
                render(ErrorDiagnostic::render_log)
            );
            dandelion_err!(DandelionError::Parsing(render(
                ErrorDiagnostic::render_response
            )))
        })
    }

    pub fn num_functions(&self) -> usize {
        self.functions.len()
    }
}

pub struct Composition {
    functions: Vec<Arc<Function>>,
    // This is a global setting, does every composition need it's own?
    any_sharding_mode: AnyShardingMode,
    input_sets: Vec<Arc<CompositionSet>>,
    output_sets: Vec<Arc<CompositionSet>>,
}

impl Composition {
    pub fn from_template<R: Registry>(
        template: &CompositionTemplate,
        any_sharding_mode: AnyShardingMode,
        registry: &R,
    ) -> Self {
        let sets: Vec<_> = (0..template.num_sets)
            .map(|_| Arc::new(CompositionSet::new()))
            .collect();

        let input_sets = template
            .params
            .iter()
            .map(|set_idx| sets[*set_idx].clone())
            .collect();
        let output_sets: Vec<_> = template
            .returns
            .iter()
            .map(|set_idx| {
                let out_set = sets[*set_idx].clone();
                // composition output sets always need to be retained
                out_set.mark_retained();
                out_set
            })
            .collect();

        let mut functions = Vec::with_capacity(template.functions.len());
        for (i, f) in template.functions.iter().enumerate() {
            let mut function = Function::new(i, f.id.clone(), f.join_order.clone());
            let mut inputs = Vec::with_capacity(f.params.len());
            for in_opt in f.params.iter() {
                if let Some(in_templ) = in_opt {
                    inputs.push(Some((in_templ.sharding, in_templ.optional)));
                } else {
                    inputs.push(None);
                }
            }
            let outputs = f
                .returns
                .iter()
                .map(|set_idx_opt| set_idx_opt.map(|i| sets[i].clone()))
                .collect();
            let min_set_bytes = registry.get_min_set_bytes(&f.id).unwrap(); // TODO
            function.update_io(&inputs, outputs, min_set_bytes);

            let function_arc = Arc::new(function);

            for (in_idx, in_opt) in f.params.iter().enumerate() {
                if let Some(in_templ) = in_opt {
                    if in_templ.sharding.is_blocking() {
                        sets[in_templ.set_idx].add_blocking_consumer(
                            function_arc.clone(),
                            in_idx,
                            in_templ.sharding.requires_sorting(),
                        );
                    } else {
                        sets[in_templ.set_idx]
                            .add_non_blocking_consumer(function_arc.clone(), in_idx);
                    }
                }
            }

            functions.push(function_arc);
        }

        Composition {
            functions,
            any_sharding_mode,
            input_sets,
            output_sets,
        }
    }

    /// Number of functions in the composition.
    pub fn num_functions(&self) -> usize {
        self.functions.len()
    }

    /// Starts the composition with the given inputs and adds all initial invocations to `out`.
    pub fn start_execution(&mut self, composition_inputs: Vec<DataSet>, exec: &mut impl Executor) {
        debug_assert_eq!(composition_inputs.len(), self.input_sets.len());

        for (i, in_set) in composition_inputs.into_iter().enumerate() {
            self.input_sets[i].set_composition_input(in_set, &self.any_sharding_mode, exec);
        }
        self.input_sets.clear();
    }

    /// Pushes the output of a finished invocation and adds all resulting invocations to `out`.
    pub fn push_invocation_output(
        &self,
        output: Vec<Arc<Vec<DataItem>>>,
        composition_idx: usize,
        exec: &mut impl Executor,
    ) {
        self.functions[composition_idx].add_invocation_output(output, &self.any_sharding_mode, exec)
    }

    pub fn collect(self) -> Vec<DataSet> {
        self.output_sets
            .iter()
            .map(|out_set| out_set.get_set())
            .collect()
    }
}

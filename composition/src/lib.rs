mod function;
mod join_iterator;
mod parser;
mod set;
mod sharding;

use std::sync::Arc;

use crate::{
    function::Function,
    parser::{render_diagnostics, Parser},
    set::CompositionSet,
    sharding::Sharding,
};

/// Re-exported so callers of [`Composition::from_template`] can name the sharding mode without
/// needing access to the (otherwise crate-private) `sharding` module.
pub use crate::sharding::AnyShardingMode;
use dandelion_commons::{
    dandelion_err,
    data::{DataSet, Invocation},
    DandelionError, DandelionResult, FunctionId,
};
use log::warn;

/// Functions from the function registry side required build compositions.
pub trait Registry {
    /// Confirms the declared function is registered with matching params and returns.
    fn check_declaration(
        &self,
        id: &str,
        params: &Vec<&str>,
        rets: &Vec<&str>,
    ) -> DandelionResult<()>;
    /// Simple lookup whether an identifier is already registered.
    fn id_exists(&self, id: &str) -> bool;
    /// Get min_set_bytes for a function.
    fn get_min_set_bytes(&self, id: &FunctionId) -> Vec<usize>;
}

#[derive(Clone)]
pub struct InputSetTemplate {
    set_idx: usize,
    sharding: Sharding,
    optional: bool,
}

pub struct FunctionTemplate {
    id: FunctionId,
    params: Vec<Option<InputSetTemplate>>,
    join_order: Vec<usize>,
    returns: Vec<Option<usize>>,
}

pub struct CompositionTemplate {
    functions: Vec<FunctionTemplate>,
    params: Vec<usize>,
    returns: Vec<usize>,
    num_sets: usize,
}

impl CompositionTemplate {
    pub fn parse<R: Registry>(raw: &str, registry: &R) -> DandelionResult<Vec<(FunctionId, Self)>> {
        let parser = Parser::new(registry);
        parser.parse(raw).map_err(|diagnostics| {
            let err_str = render_diagnostics(raw, &diagnostics);
            warn!("Composition parsing failed: {}", err_str);
            dandelion_err!(DandelionError::Parsing(err_str))
        })
    }
}

pub struct Composition {
    functions: Vec<Arc<Function>>,
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
            let function = Arc::new(Function::new(i, f.id.clone(), f.join_order.clone()));

            let mut inputs = Vec::with_capacity(f.params.len());
            for (in_idx, in_opt) in f.params.iter().enumerate() {
                if let Some(in_templ) = in_opt {
                    let comp_set = sets[in_templ.set_idx].clone();
                    comp_set.add_consumer(
                        function.clone(),
                        in_idx,
                        in_templ.sharding.is_blocking(),
                        in_templ.sharding.requires_sorting(),
                        f.params.len(),
                    );
                    inputs.push(Some((
                        comp_set,
                        in_templ.sharding.clone(),
                        in_templ.optional,
                    )));
                } else {
                    inputs.push(None);
                }
            }
            let outputs = f
                .returns
                .iter()
                .map(|set_idx_opt| set_idx_opt.map(|i| sets[i].clone()))
                .collect();
            function.update_io(inputs, outputs, registry.get_min_set_bytes(&f.id));

            functions.push(function);
        }

        Composition {
            functions,
            any_sharding_mode,
            input_sets,
            output_sets,
        }
    }

    pub fn start_execution(&mut self, composition_inputs: Vec<DataSet>) -> Vec<Invocation> {
        debug_assert_eq!(composition_inputs.len(), self.input_sets.len());

        for (i, in_set) in composition_inputs.into_iter().enumerate() {
            self.input_sets[i].set_composition_input(in_set);
        }
        self.input_sets.clear();

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

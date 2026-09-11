use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    sync::Arc,
};

use dandelion_commons::FunctionId;

use crate::{
    parser::{
        ErrorDiagnostic, FunctionDecl, Registry, Span, SpannedCompositionDecl,
        SpannedFunctionApplication, SpannedFunctionDecl, Statement,
    },
    sharding::{JoinStrategy, Sharding},
    CompositionTemplate, FunctionTemplate, InputSetTemplate,
};

/// Builds the compositions from the parser outputs validating them and building the correct join
/// orders required by the JoinIterators (that compute the shardings) in the process.
pub(super) struct CompositionBuilder<'src, R: Registry> {
    registry: &'src R,
    declared_functions: HashMap<&'src str, &'src FunctionDecl<'src>>,
    composition_ids: HashSet<&'src str>,
    compositions: Vec<(FunctionId, CompositionTemplate)>,
}

impl<'src, R: Registry> CompositionBuilder<'src, R> {
    pub fn new(registry: &'src R) -> Self {
        CompositionBuilder {
            registry,
            declared_functions: HashMap::new(),
            composition_ids: HashSet::new(),
            compositions: Vec::new(),
        }
    }

    pub fn add_declaration(
        &mut self,
        decl: &'src SpannedFunctionDecl<'src>,
    ) -> Result<(), ErrorDiagnostic> {
        self.registry
            .check_declaration(decl.v.name, &decl.v.params, &decl.v.returns)
            .map_err(|err| ErrorDiagnostic::new(decl.span.clone(), err.to_string()))?;
        self.declared_functions.insert(decl.v.name, &decl.v);
        Ok(())
    }

    fn process_function_application(
        &mut self,
        fappl: &SpannedFunctionApplication,
        data_set_ids: &mut HashMap<&str, usize>,
    ) -> Result<FunctionTemplate, ErrorDiagnostic> {
        let fdecl = &self.declared_functions.get(&fappl.v.name).ok_or_else(|| {
            ErrorDiagnostic::new(
                fappl.span.clone(),
                format!("Unknown function '{}'.", fappl.v.name),
            )
        })?;
        if fdecl.params.len() < fappl.v.args.len() || fdecl.returns.len() < fappl.v.rets.len() {
            return Err(ErrorDiagnostic::new(
                fappl.span.clone(),
                format!(
                    "Function declaration and application mismatch for function {}.",
                    fappl.v.name
                ),
            ));
        }
        let num_params = fdecl.params.len();

        // find the indices of the sets in the function application by looking though the definition
        let mut input_templates = Vec::new();
        input_templates.resize(num_params, None);
        // spans of the arguments that filled each parameter slot, kept around to point join
        // related errors at the argument rather than the whole application
        let mut arg_spans: Vec<Option<Span>> = Vec::new();
        arg_spans.resize(num_params, None);
        for arg in fappl.v.args.iter() {
            if let Some(arg_set_idx) = fdecl
                .params
                .iter()
                .position(|param_name| arg.v.name == *param_name)
            {
                let data_set_id = data_set_ids.get(&arg.v.ident).ok_or_else(|| {
                    ErrorDiagnostic::new(
                        arg.span.clone(),
                        format!(
                            "Could not find data set {} used as input for argument {} in function {}.",
                            arg.v.ident, arg.v.name, fappl.v.name
                        ),
                    )
                })?;
                arg_spans[arg_set_idx] = Some(arg.span.clone());
                input_templates[arg_set_idx] = Some(InputSetTemplate {
                    set_idx: *data_set_id,
                    sharding: arg.v.sharding.clone(),
                    optional: arg.v.optional,
                });
            } else {
                return Err(ErrorDiagnostic::new(
                    arg.span.clone(),
                    format!(
                        "Argument {} does not match any of the declared arguments for function {}.",
                        arg.v.name, fappl.v.name
                    ),
                ));
            }
        }

        // find the join order
        let mut processed_sets = Vec::new();
        processed_sets.resize(num_params, false);
        let mut join_order = Vec::new();
        let mut any_join_order = Vec::new();
        if let Some(strategy) = fappl.v.join_strategy.as_ref().and_then(|s| {
            if s.join_strategy_order.is_empty() || s.join_strategies.is_empty() {
                None
            } else {
                Some(s)
            }
        }) {
            debug_assert!(strategy.join_strategy_order.len() - 1 == strategy.join_strategies.len());
            let mut curr_join_chain_any = None;
            let mut curr_strategy = JoinStrategy::Cross;
            for (i, arg_name) in strategy.join_strategy_order.iter().enumerate() {
                // find argument set index
                let arg_set_idx = fdecl
                    .params
                    .iter()
                    .position(|param_name| *param_name == *arg_name)
                    .ok_or_else(|| {
                        ErrorDiagnostic::new(
                            fappl.span.clone(),
                            format!(
                                "Join order for {} contains unknown argument name {}.",
                                fappl.v.name, arg_name
                            ),
                        )
                    })?;
                // best span available for this argument slot, falling back to the whole
                // application when the join clause references an argument that was not passed
                let arg_span = || {
                    arg_spans[arg_set_idx]
                        .clone()
                        .unwrap_or_else(|| fappl.span.clone())
                };

                // check that this set has not already been processed
                if processed_sets[arg_set_idx] {
                    return Err(ErrorDiagnostic::new(
                        arg_span(),
                        format!("Joining argument '{}' twice.", arg_name),
                    ));
                }
                processed_sets[arg_set_idx] = true;

                // get strategy
                if i > 0 {
                    curr_strategy = strategy.join_strategies[i - 1];
                    if curr_strategy == JoinStrategy::Cross {
                        curr_join_chain_any = None;
                    }
                }
                if let Some(ref mut input) = input_templates[arg_set_idx] {
                    match input.sharding {
                        Sharding::Keyed(ref mut strategy) => {
                            if let Some(is_any) = curr_join_chain_any {
                                if is_any {
                                    return Err(ErrorDiagnostic::new(arg_span(), format!(
                                        "Mixing keyed and anyKeyed shardings: Encountered keyed while processing any chain for index {}.", arg_set_idx
                                    )));
                                }
                            }
                            curr_join_chain_any = Some(false);
                            join_order.push(arg_set_idx);
                            if i > 0 {
                                *strategy = curr_strategy;
                            }
                        }
                        Sharding::AnyKeyed(ref mut strategy) => {
                            if let Some(is_any) = curr_join_chain_any {
                                if !is_any {
                                    return Err(ErrorDiagnostic::new(arg_span(), format!(
                                        "Mixing keyed and anyKeyed shardings: Encountered anyKeyed while processing non-any chain for index {}.", arg_set_idx
                                    )));
                                }
                            }
                            curr_join_chain_any = Some(true);
                            any_join_order.push(arg_set_idx);
                            if i > 0 {
                                *strategy = curr_strategy;
                            }
                        }
                        _ => return Err(ErrorDiagnostic::new(arg_span(), format!(
                            "Joining set with non-keyed sharding (set_index: {}, sharding: {:?}.", arg_set_idx, input.sharding
                        ))),
                    }
                }
            }
        }
        // add remaining sets with keyed shardings that have no specific join order
        for (set_idx, set_id_opt) in input_templates.iter().enumerate() {
            if processed_sets[set_idx] {
                continue;
            }
            if let Some(set_id) = set_id_opt {
                if matches!(set_id.sharding, Sharding::Keyed(_)) {
                    processed_sets[set_idx] = true;
                    join_order.push(set_idx);
                }
            }
        }
        // add joined any sets
        join_order.extend(any_join_order);
        // fill up the remaining join order/strategy
        for (set_idx, is_processed) in processed_sets.iter().enumerate() {
            if !is_processed {
                join_order.push(set_idx);
            }
        }

        // find the index in the original definition for each return set in the application
        let mut outputs = Vec::new();
        outputs.resize(fdecl.returns.len(), None);
        for ret in fappl.v.rets.iter() {
            if let Some(ret_set_idx) = fdecl
                .returns
                .iter()
                .position(|return_name| ret.v.name == *return_name)
            {
                let data_set_id = data_set_ids.get(&ret.v.ident).ok_or_else(|| {
                    ErrorDiagnostic::new(
                        ret.span.clone(),
                        format!(
                            "Could not find data set {} used as output for return {} in function {}.",
                            ret.v.ident, ret.v.name, fappl.v.name
                        ),
                    )
                })?;
                outputs[ret_set_idx] = Some(*data_set_id);
            } else {
                return Err(ErrorDiagnostic::new(
                    ret.span.clone(),
                    format!(
                        "Return {} does not match any of the declared returns for function {}.",
                        ret.v.name, fappl.v.name
                    ),
                ));
            }
        }
        Ok(FunctionTemplate {
            id: Arc::new(fdecl.name.to_string()),
            params: input_templates,
            join_order,
            returns: outputs,
        })
    }

    pub fn add_composition(
        &mut self,
        comp: &'src SpannedCompositionDecl<'src>,
    ) -> Result<(), ErrorDiagnostic> {
        // check if composition name is already taken
        if self.registry.id_exists(comp.v.name) || self.composition_ids.contains(&comp.v.name) {
            return Err(ErrorDiagnostic::new(
                comp.span.clone(),
                format!("Composition identifier '{}' is already taken.", comp.v.name),
            ));
        }
        self.composition_ids.insert(comp.v.name);

        // add composition input sets
        let mut data_set_counter = 0usize;
        let mut data_set_ids = HashMap::new();
        for input_set_name in comp.v.params.iter() {
            match data_set_ids.entry(*input_set_name) {
                Entry::Vacant(v) => v.insert(data_set_counter),
                Entry::Occupied(_) => {
                    return Err(ErrorDiagnostic::new(
                        comp.span.clone(),
                        format!("Duplicate set name '{}'.", input_set_name),
                    ))
                }
            };
            data_set_counter += 1;
        }
        let inputs_end_idx = data_set_counter;

        // add composition output sets
        let mut output_map = HashMap::new();
        let output_sets_start = data_set_counter;
        for (output_index, output_set_name) in comp.v.returns.iter().enumerate() {
            match data_set_ids.entry(output_set_name) {
                Entry::Vacant(v) => {
                    v.insert(data_set_counter);
                    output_map.insert(data_set_counter, output_index);
                    data_set_counter += 1;
                }
                // output set is input set
                Entry::Occupied(occupied) => {
                    output_map.insert(*occupied.get(), output_index);
                }
            };
        }
        let output_end_idx = data_set_counter;

        // add all return sets from functions
        for statement in comp.v.statements.iter() {
            match statement {
                Statement::FunctionApplication(spanned_fa) => {
                    for ret in spanned_fa.v.rets.iter() {
                        match data_set_ids.entry(&ret.v.ident) {
                            Entry::Vacant(v) => {
                                v.insert(data_set_counter);
                                data_set_counter += 1;
                            }
                            Entry::Occupied(o) => {
                                if output_sets_start <= *o.get() && *o.get() < output_end_idx {
                                    continue;
                                } else {
                                    return Err(ErrorDiagnostic::new(
                                        ret.span.clone(),
                                        format!("Duplicate set name '{}'.", ret.v.ident),
                                    ));
                                }
                            }
                        }
                    }
                }
                Statement::Loop(_) => {
                    todo!("loop semantics need to be fleshed out and compositions extended to acomodate them");
                }
            }
        }

        // have enumerated all set that are available so can start putting the composition together
        let functions = comp
            .v
            .statements
            .iter()
            .map(|statement| match statement {
                Statement::FunctionApplication(fappl) => {
                    self.process_function_application(fappl, &mut data_set_ids)
                }
                Statement::Loop(_) => {
                    todo!("Need to implement loop support in compositions")
                }
            })
            .collect::<Result<_, _>>()?;

        self.compositions.push((
            Arc::new(comp.v.name.to_string()),
            CompositionTemplate {
                functions,
                params: (0..inputs_end_idx).collect(),
                returns: (inputs_end_idx..output_end_idx).collect(),
                num_sets: data_set_counter,
            },
        ));

        Ok(())
    }

    pub fn collect(self) -> Vec<(FunctionId, CompositionTemplate)> {
        self.compositions
    }
}

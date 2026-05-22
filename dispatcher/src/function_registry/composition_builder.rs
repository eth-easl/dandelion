use std::{
    collections::{btree_map::Entry, BTreeMap, BTreeSet},
    rc::Rc,
    sync::Arc,
};

use dandelion_commons::{
    dandelion_err, err_dandelion, try_with_capacity, CompositionError, DandelionError,
    DandelionResult, FunctionId,
};
use dparser::{FunctionApplication, FunctionDecl, Spanned};
use itertools::Itertools;
use machine_interface::{
    composition::{self, FunctionDependencies, InputSetDescriptor, JoinStrategy, ShardingMode},
    function_driver::Metadata,
};

use crate::function_registry::{FunctionRegistry, FunctionType};

/// Builds the compositions from the parser outputs validating them and building the correct join
/// orders required by the JoinIterators (that compute the shardings) in the process.
pub(super) struct CompositionBuilder<'reg> {
    registry: &'reg FunctionRegistry,
    composition_ids: BTreeSet<String>,
    known_functions: BTreeMap<String, Rc<Spanned<FunctionDecl>>>,
    compositions: Vec<(FunctionId, composition::Composition, Metadata)>,
}

impl<'reg> CompositionBuilder<'reg> {
    /// Creates a new builder using the given function registry.
    pub(super) fn new(registry: &'reg FunctionRegistry) -> Self {
        Self {
            registry,
            composition_ids: BTreeSet::new(),
            known_functions: BTreeMap::new(),
            compositions: Vec::new(),
        }
    }

    /// Adds a declaration checking that it matches the registered function.
    pub(super) fn add_declaration(
        &mut self,
        decl: Rc<Spanned<FunctionDecl>>,
    ) -> DandelionResult<()> {
        // get meta data of registered function
        let lock_guard = self
            .registry
            .function_map
            .read()
            .expect("Function registry lock poisoned!");
        let metadata = match lock_guard.get(&decl.v.name) {
            Some(func_type) => match func_type {
                FunctionType::SystemFunction(func_info) => &func_info.metadata,
                FunctionType::Function(func_info) => &func_info.metadata,
                FunctionType::Composition(comp_info) => &comp_info.metadata,
            },
            None => {
                return err_dandelion!(DandelionError::Composition(
                    CompositionError::InvalidFunctionDeclaration(format!(
                        "Unknown function {}",
                        decl.v.name
                    )),
                ))
            }
        };

        // validate function arguments
        if decl.v.params.len() != metadata.input_sets.len()
            || decl
                .v
                .params
                .iter()
                .zip_eq(metadata.input_sets.iter())
                .any(|(decl_name, (metadata_name, _))| *decl_name != *metadata_name)
        {
            return err_dandelion!(DandelionError::Composition(
                CompositionError::InvalidFunctionDeclaration(format!(
                    "Function arguments do not match registration for function {}.",
                    decl.v.name
                )),
            ));
        }

        // validated function returns
        if decl.v.returns.len() != metadata.output_sets.len()
            || decl
                .v
                .returns
                .iter()
                .zip_eq(metadata.output_sets.iter())
                .any(|(decl_name, metadata_name)| *decl_name != *metadata_name)
        {
            return err_dandelion!(DandelionError::Composition(
                CompositionError::InvalidFunctionDeclaration(format!(
                    "Function returns do not match registration for function {}.",
                    decl.v.name
                )),
            ));
        }

        self.known_functions.insert(decl.v.name.clone(), decl);
        Ok(())
    }

    /// Validates and creates the join order of a single function application
    fn parse_and_check_function_application(
        &mut self,
        fappl: &FunctionApplication,
        data_set_ids: &mut BTreeMap<String, usize>,
    ) -> DandelionResult<FunctionDependencies> {
        let fdecl = &self
            .known_functions
            .get(&fappl.name)
            .ok_or_else(|| {
                dandelion_err!(DandelionError::Composition(
                    CompositionError::UnknownFunction(fappl.name.clone())
                ))
            })?
            .v;
        if fdecl.params.len() < fappl.args.len() || fdecl.returns.len() < fappl.rets.len() {
            return err_dandelion!(DandelionError::Composition(
                CompositionError::InvalidFunctionApplication(format!(
                    "Functino declaration and applicationm mismatch for function {}.",
                    fappl.name
                ))
            ));
        }
        let num_params = fdecl.params.len();

        // find the indices of the sets in the function application by looking though the definition
        let mut input_ids = try_with_capacity!(Vec, num_params)?;
        input_ids.resize(num_params, None);
        for arg in fappl.args.iter() {
            if let Some(arg_set_idx) = fdecl
                .params
                .iter()
                .position(|param_name| arg.v.name == *param_name)
            {
                let data_set_id = data_set_ids.get(&arg.v.ident).ok_or_else(|| {
                    dandelion_err!(DandelionError::Composition(
                        CompositionError::UndefinedDataSet(format!(
                            "Could not find data set {} used as input for argument {} in function {}.",
                            arg.v.ident, arg.v.name, fappl.name
                        ))
                    ))
                })?;
                input_ids[arg_set_idx] = Some(InputSetDescriptor {
                    composition_id: *data_set_id,
                    sharding: ShardingMode::from_parser_sharding(&arg.v.sharding),
                    optional: arg.v.optional,
                });
            } else {
                return err_dandelion!(DandelionError::Composition(
                    CompositionError::InvalidFunctionApplication(format!(
                        "Argument {} does not match any of the declared arguments for function {}.",
                        arg.v.name, fappl.name
                    ))
                ));
            }
        }

        // find the join order
        let mut processed_set = try_with_capacity!(Vec, num_params)?;
        processed_set.resize(num_params, false);
        let mut join_order = try_with_capacity!(Vec, num_params)?;
        let mut join_strategies = try_with_capacity!(Vec, num_params)?;
        let mut any_join_order = try_with_capacity!(Vec, num_params)?;
        let mut any_join_strategies = try_with_capacity!(Vec, num_params)?;

        if let Some(strategy) = fappl.join_strategy.as_ref().and_then(|s| {
            if s.join_strategy_order.is_empty() || s.join_strategies.is_empty() {
                None
            } else {
                Some(s)
            }
        }) {
            debug_assert!(strategy.join_strategies.len() - 1 == strategy.join_strategy_order.len());
            let mut curr_join_chain_any = None;
            for (i, arg_name) in strategy.join_strategy_order.iter().enumerate() {
                // find argument set index
                let arg_set_idx = fdecl
                    .params
                    .iter()
                    .position(|param_name| *param_name == *arg_name)
                    .ok_or_else(|| {
                        dandelion_err!(DandelionError::Composition(
                            CompositionError::InvalidFunctionApplication(format!(
                                "Join order for {} contains unknown argument name {}.",
                                fappl.name, arg_name
                            ))
                        ))
                    })?;

                // check that this set has not already been processed
                if processed_set[arg_set_idx] {
                    return err_dandelion!(DandelionError::Composition(
                        CompositionError::InvalidSecondJoin(format!(
                            "Joining argument '{}' twice.",
                            arg_name
                        ))
                    ));
                }
                processed_set[arg_set_idx] = true;

                // get strategy and add to the corresponding join order
                if i > 0 {
                    let curr_strategy =
                        JoinStrategy::from_parser_strategy(&strategy.join_strategies[i - 1]);
                    join_strategies.push(curr_strategy);
                    if curr_strategy == JoinStrategy::Cross {
                        curr_join_chain_any = None;
                    }
                };
                if let Some(set_id) = input_ids[arg_set_idx] {
                    match set_id.sharding {
                        ShardingMode::Key => {
                            if let Some(is_any) = curr_join_chain_any {
                                if is_any {
                                    return err_dandelion!(DandelionError::Composition(CompositionError::InvalidJoinSharding(
                                        format!("Mixing keyed and anyKeyed shardings: Encountered keyed while processing any chain for index {}.", arg_set_idx)
                                    )));
                                }
                            }
                            curr_join_chain_any = Some(false);
                            join_order.push(arg_set_idx);
                        }
                        ShardingMode::AnyKey => {
                            if let Some(is_any) = curr_join_chain_any {
                                if !is_any {
                                    return err_dandelion!(DandelionError::Composition(CompositionError::InvalidJoinSharding(
                                        format!("Mixing keyed and anyKeyed shardings: Encountered anyKeyed while processing non-any chain for index {}.", arg_set_idx)
                                    )));
                                }
                            }
                            curr_join_chain_any = Some(true);
                            any_join_order.push(arg_set_idx);
                        }
                        _ => return err_dandelion!(DandelionError::Composition(CompositionError::InvalidJoinSharding(
                            format!("Joining set with non-keyed sharding (set_index: {}, sharding: {:?}.", arg_set_idx, set_id.sharding)
                        ))),
                    }
                }
            }
        }
        // add remaining sets with keyed shardings that have no specific join order
        for (set_idx, set_id_opt) in input_ids.iter().enumerate() {
            if processed_set[set_idx] {
                continue;
            }
            if let Some(set_id) = set_id_opt {
                if set_id.sharding == ShardingMode::Key {
                    processed_set[set_idx] = true;
                    join_strategies.push(JoinStrategy::Cross);
                    join_order.push(set_idx);
                }
            }
        }
        // add joined any sets
        join_strategies.append(&mut any_join_strategies);
        join_order.append(&mut any_join_order);
        // fill up the remaining join order/strategy
        for (set_idx, is_processed) in processed_set.iter().enumerate() {
            if !is_processed {
                join_order.push(set_idx);
            }
        }
        if join_order.len() > 0 {
            join_strategies.resize(join_order.len() - 1, JoinStrategy::Cross);
        }

        // find the index in the original definition for each return set in the application
        let mut output_ids = try_with_capacity!(Vec, fdecl.returns.len())?;
        output_ids.resize(fdecl.returns.len(), None);
        for ret in fappl.rets.iter() {
            if let Some(ret_set_idx) = fdecl
                .returns
                .iter()
                .position(|return_name| ret.v.name == *return_name)
            {
                let data_set_id = data_set_ids.get(&ret.v.ident).ok_or(dandelion_err!(
                    DandelionError::Composition(CompositionError::UndefinedDataSet(format!(
                        "Could not find data set {} used as output for return {} in function {}.",
                        ret.v.ident, ret.v.name, fappl.name
                    )))
                ))?;
                output_ids[ret_set_idx] = Some(*data_set_id);
            } else {
                return err_dandelion!(DandelionError::Composition(
                    CompositionError::InvalidFunctionApplication(format!(
                        "Return {} does not match any of the declared returns for function {}.",
                        ret.v.name, fappl.name
                    ))
                ));
            }
        }
        Ok(FunctionDependencies {
            function: Arc::new(fdecl.name.clone()),
            input_set_ids: input_ids,
            join_info: (join_order, join_strategies),
            output_set_ids: output_ids,
        })
    }

    /// Adds a composition of function applications validating them and computing their join
    /// order in the process.
    pub(super) fn add_composition(&mut self, comp: &dparser::Composition) -> DandelionResult<()> {
        // check if composition name is already taken
        if self.registry.exists_name(&comp.name) || self.composition_ids.contains(&comp.name) {
            return err_dandelion!(DandelionError::Composition(
                CompositionError::DuplicateIdentifier(comp.name.clone()),
            ));
        }
        self.composition_ids.insert(comp.name.clone());

        // add composition input sets
        let mut data_set_counter = 0usize;
        let mut data_set_ids = BTreeMap::new();
        for input_set_name in comp.params.iter() {
            match data_set_ids.entry(input_set_name.clone()) {
                Entry::Vacant(v) => v.insert(data_set_counter),
                Entry::Occupied(_) => {
                    return err_dandelion!(DandelionError::Composition(
                        CompositionError::DuplicateSetName(input_set_name.clone()),
                    ))
                }
            };
            data_set_counter += 1;
        }

        // add composition output sets
        let mut output_map = BTreeMap::new();
        let output_sets_start = data_set_counter;
        for (output_index, output_set_name) in comp.returns.iter().enumerate() {
            match data_set_ids.entry(output_set_name.clone()) {
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
        let output_sets_end = data_set_counter;

        // add all return sets from functions
        let return_set_names = comp
            .statements
            .iter()
            .flat_map(|statement| match statement {
                dparser::Statement::FunctionApplication(function_application) => {
                    function_application.v.rets.iter().map(|ret| ret.v.ident.clone())
                }
                dparser::Statement::Loop(_) =>
                    todo!("loop semantics need to be fleshed out and compositions extended to acoomodate them"),
            });
        for return_set_name in return_set_names {
            match data_set_ids.entry(return_set_name.clone()) {
                Entry::Vacant(v) => {
                    v.insert(data_set_counter);
                    data_set_counter += 1;
                }
                Entry::Occupied(o) => {
                    if output_sets_start <= *o.get() && *o.get() < output_sets_end {
                        continue;
                    } else {
                        return err_dandelion!(DandelionError::Composition(
                            CompositionError::DuplicateSetName(return_set_name.clone()),
                        ));
                    }
                }
            }
        }

        // have enumerated all set that are available so can start putting the composition together
        let dependencies = comp
            .statements
            .iter()
            .map(|statement| match statement {
                dparser::Statement::FunctionApplication(fappl) => {
                    self.parse_and_check_function_application(&fappl.v, &mut data_set_ids)
                }
                dparser::Statement::Loop(_) => {
                    todo!("Need to implement loop support in compositions")
                }
            })
            .collect::<DandelionResult<Vec<_>>>()?;
        let metadata = Metadata {
            input_sets: comp
                .params
                .iter()
                .map(|name| (name.clone(), None))
                .collect(),
            output_sets: comp.returns.iter().map(|name| name.clone()).collect(),
            min_set_bytes: vec![],
        };
        self.compositions.push((
            Arc::new(comp.name.clone()),
            composition::Composition {
                dependencies,
                output_map,
            },
            metadata,
        ));

        Ok(())
    }

    /// Finish the building process and get back the compositions. Consumes the builder instance.
    pub(super) fn finish(self) -> Vec<(FunctionId, composition::Composition, Metadata)> {
        self.compositions
    }
}

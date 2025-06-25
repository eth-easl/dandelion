use crate::{
    composition::{Composition, FunctionDependencies, InputSetDescriptor, ShardingMode},
    function_registry::{FunctionDict, Metadata},
};
use dandelion_commons::DandelionError;
use dparser::Module;
use itertools::Itertools;
use std::{collections::BTreeMap, ops::Range, sync::Arc, vec};

fn get_module(comp_string: &str) -> Module {
    return dparser::parse(comp_string).unwrap_or_else(|err| {
        dparser::print_errors(comp_string, err);
        panic!("parsing failed");
    });
}

fn check_metadata(actual_meta: &Metadata, expected_meta: &Metadata) -> bool {
    // check if input dataset
    if actual_meta.input_sets.len() != expected_meta.input_sets.len()
        || actual_meta.output_sets.len() != expected_meta.output_sets.len()
    {
        return false;
    }
    for (actual_in_set, expected_in_set) in actual_meta
        .input_sets
        .iter()
        .zip(expected_meta.input_sets.iter())
    {
        if actual_in_set.0 != expected_in_set.0 {
            return false;
        }
    }
    for (actual_out_set, expected_out_set) in actual_meta
        .output_sets
        .iter()
        .zip(expected_meta.output_sets.iter())
    {
        if actual_out_set != expected_out_set {
            return false;
        }
    }
    return true;
}

fn check_composition(
    actual_composition: &Composition,
    expected_composition: &Composition,
    input_set_range: &Range<usize>,
    output_set_range: &Range<usize>,
) -> bool {
    // check if output maps are same
    assert_eq!(
        expected_composition.output_map,
        actual_composition.output_map
    );
    // check if dependencies are same
    for expected_function in expected_composition.dependencies.iter() {
        let matches = actual_composition
            .dependencies
            .iter()
            .filter_map(|actual_function| {
                if actual_function.function != expected_function.function
                    || actual_function.input_set_ids.len() != expected_function.input_set_ids.len()
                    || actual_function.output_set_ids.len()
                        != expected_function.output_set_ids.len()
                {
                    return None;
                }
                // compare input set ids
                for set_index in 0..actual_function.input_set_ids.len() {
                    let actual_set_opt = actual_function.input_set_ids[set_index];
                    let expected_set_opt = expected_function.input_set_ids[set_index];
                    match (actual_set_opt, expected_set_opt) {
                        (None, None) => (),
                        (None, Some(_)) | (Some(_), None) => return None,
                        (
                            Some(InputSetDescriptor {
                                composition_id: a_index,
                                sharding: a_sharding,
                                optional: a_optional,
                            }),
                            Some(InputSetDescriptor {
                                composition_id: e_index,
                                sharding: e_sharding,
                                optional: e_optional,
                            }),
                        ) => {
                            if a_sharding != e_sharding {
                                return None;
                            }
                            if input_set_range.contains(&e_index) && e_index != a_index {
                                return None;
                            }
                            if a_optional != e_optional {
                                return None;
                            }
                        }
                    };
                }
                for set_index in 0..actual_function.output_set_ids.len() {
                    let actual_set_op = actual_function.output_set_ids[set_index];
                    let expected_set_op = expected_function.output_set_ids[set_index];
                    match (actual_set_op, expected_set_op) {
                        (None, None) => (),
                        (None, Some(_)) | (Some(_), None) => return None,
                        (Some(a_index), Some(e_index)) => {
                            if output_set_range.contains(&e_index) && e_index != a_index {
                                return None;
                            }
                        }
                    };
                }
                return Some(0);
            })
            .count();
        if matches > 1 {
            panic!(
                "Found more than one match for expected function {:?} in {:?}",
                expected_function, actual_composition
            );
        } else if matches < 1 {
            panic!(
                "Found no match for expected function {:?} in composition {:?}",
                expected_function, actual_composition
            );
        }
    }
    return true;
}

fn check_compositions_and_metadata(
    actual: Vec<(u64, Composition, Metadata)>,
    expected: Vec<(Composition, Metadata)>,
    input_set_range: Range<usize>,
    output_set_range: Range<usize>,
) {
    for (expected_comp, expected_meta) in expected {
        // find all for which metadata maches
        let meta_matches = actual
            .iter()
            .filter_map(|(_, actual_comp, actual_meta)| {
                if !check_metadata(actual_meta, &expected_meta) {
                    return None;
                } else {
                    return Some(actual_comp);
                }
            })
            .collect_vec();
        if meta_matches.len() < 1 {
            panic!(
                "Found no metadata match for {:?} with metadata: {:?} in {:?}",
                expected_comp, expected_meta, actual
            );
        }
        let matches = meta_matches
            .iter()
            .filter_map(|actual_composition| {
                if check_composition(
                    actual_composition,
                    &expected_comp,
                    &input_set_range,
                    &output_set_range,
                ) {
                    return Some(0);
                } else {
                    return None;
                }
            })
            .count();
        if matches > 1 {
            panic!(
                "Found more than one match for {:?} with metadata {:?} in {:?}",
                expected_comp, expected_meta, actual
            );
        } else if matches < 1 {
            panic!(
                "Found no match for {:?} with metadata: {:?} in {:?}",
                expected_comp, expected_meta, actual
            );
        }
    }
}

#[test_log::test]
fn test_from_module_non_registered_function() {
    let unregistered_function = r#"
        function not_registered () => ();
    "#;
    let mut function_dict = FunctionDict::new();
    let module = get_module(unregistered_function);
    match Composition::from_module(&module, &mut function_dict) {
        Err(DandelionError::CompositionContainsInvalidFunction(_)) => (),
        Err(err) => panic!(
            "Found wrong error on composition with invalid function: {:?}",
            err
        ),
        Ok(_) => panic!("No error on unregistered function"),
    }
}

#[test_log::test]
fn test_from_module_single_registered_function() {
    let unregistered_function = r#"
        function registered () => ();
    "#;
    let mut function_dict = FunctionDict::new();
    function_dict.insert_or_lookup(String::from("registered"));
    let module = get_module(unregistered_function);
    match Composition::from_module(&module, &mut function_dict) {
        Ok(_) => (),
        Err(err) => panic!("Found unexpected error on from_module {:?}", err),
    }
}

#[test_log::test]
fn test_from_module_minmal_composition() {
    let composition_string = r#"
        function Function () => ();
        composition Composition () => () {
            Function () => ();
        }
    "#;
    let mut function_dict = FunctionDict::new();
    let function_id = function_dict.insert_or_lookup(String::from("Function"));
    let module = get_module(composition_string);
    let compositions = match Composition::from_module(&module, &mut function_dict) {
        Ok(c) => c,
        Err(err) => panic!("Found unexpected error on from_module {:?}", err),
    };
    let expected = vec![(
        Composition {
            dependencies: vec![FunctionDependencies {
                function: function_id,
                join_info: (vec![], vec![]),
                input_set_ids: vec![],
                output_set_ids: vec![],
            }],
            output_map: BTreeMap::new(),
        },
        Metadata {
            input_sets: Arc::new(Vec::new()),
            output_sets: Arc::new(Vec::new()),
        },
    )];
    check_compositions_and_metadata(compositions, expected, 0..0, 0..0);
}

#[test_log::test]
fn test_from_module_minmal_composition_with_inputs() {
    let composition_string = r#"
        function Function (Fin) => (Fout);
        composition Composition (Cin) => (Cout) {
            Function (Fin = all Cin) => (Cout = Fout);
        }
    "#;
    let mut function_dict = FunctionDict::new();
    let function_id = function_dict.insert_or_lookup(String::from("Function"));
    let module = get_module(composition_string);
    let compositions = match Composition::from_module(&module, &mut function_dict) {
        Ok(c) => c,
        Err(err) => panic!("Found unexpected error on from_module: {:?}", err),
    };
    let expected = vec![(
        Composition {
            dependencies: vec![FunctionDependencies {
                function: function_id,
                input_set_ids: vec![Some(InputSetDescriptor {
                    composition_id: 0,
                    sharding: ShardingMode::All,
                    optional: false,
                })],
                join_info: (vec![], vec![]),
                output_set_ids: vec![Some(1)],
            }],
            output_map: BTreeMap::from([(1, 0)]),
        },
        Metadata {
            input_sets: Arc::new(vec![(String::from("Cin"), None)]),
            output_sets: Arc::new(vec![String::from("Cout")]),
        },
    )];
    check_compositions_and_metadata(compositions, expected, 0..1, 1..2);
}

#[test_log::test]
fn test_from_module_minmal_composition_function_with_unused_input() {
    let composition_string = r#"
        function Function (Fin, Unused) => (Fout);
        composition Composition (Cin) => (Cout) {
            Function (Fin = all Cin) => (Cout = Fout);
        }
    "#;
    let mut function_dict = FunctionDict::new();
    let function_id = function_dict.insert_or_lookup(String::from("Function"));
    let module = get_module(composition_string);
    let compositions = match Composition::from_module(&module, &mut function_dict) {
        Ok(c) => c,
        Err(err) => panic!("Found unexpected error on from_module: {:?}", err),
    };
    let expected = vec![(
        Composition {
            dependencies: vec![FunctionDependencies {
                function: function_id,
                input_set_ids: vec![
                    Some(InputSetDescriptor {
                        composition_id: 0,
                        sharding: ShardingMode::All,
                        optional: false,
                    }),
                    None,
                ],
                join_info: (vec![], vec![]),
                output_set_ids: vec![Some(1)],
            }],
            output_map: BTreeMap::from([(1, 0)]),
        },
        Metadata {
            input_sets: Arc::new(vec![(String::from("Cin"), None)]),
            output_sets: Arc::new(vec![String::from("Cout")]),
        },
    )];
    check_compositions_and_metadata(compositions, expected, 0..1, 1..2);
}

#[test_log::test]
fn test_from_module_minmal_composition_function_with_unused_output() {
    let composition_string = r#"
        function Function (Fin) => (Fout, Unused);
        composition Composition (Cin) => (Cout) {
            Function (Fin = all Cin) => (Cout = Fout);
        }
    "#;
    let mut function_dict = FunctionDict::new();
    let function_id = function_dict.insert_or_lookup(String::from("Function"));
    let module = get_module(composition_string);
    let compositions = match Composition::from_module(&module, &mut function_dict) {
        Ok(c) => c,
        Err(err) => panic!("Found unexpected error on from_module: {:?}", err),
    };
    let expected = vec![(
        Composition {
            dependencies: vec![FunctionDependencies {
                function: function_id,
                join_info: (vec![], vec![]),
                input_set_ids: vec![Some(InputSetDescriptor {
                    composition_id: 0,
                    sharding: ShardingMode::All,
                    optional: false,
                })],
                output_set_ids: vec![Some(1), None],
            }],
            output_map: BTreeMap::from([(1, 0)]),
        },
        Metadata {
            input_sets: Arc::new(vec![(String::from("Cin"), None)]),
            output_sets: Arc::new(vec![String::from("Cout")]),
        },
    )];
    check_compositions_and_metadata(compositions, expected, 0..1, 1..2);
}

#[test_log::test]
#[should_panic]
fn test_from_module_minmal_composition_with_missing_input() {
    let composition_string = r#"
        function Function (Fin) => (Fout);
        composition Composition (Cin) => (Cout) {
            Function (Fin = all NonExistent) => (Cout = Fout);
        }
    "#;
    let mut function_dict = FunctionDict::new();
    let function_id = function_dict.insert_or_lookup(String::from("Function"));
    let module = get_module(composition_string);
    let compositions = match Composition::from_module(&module, &mut function_dict) {
        Ok(c) => c,
        Err(err) => panic!("Found unexpected error on from_module: {:?}", err),
    };
    let expected = vec![(
        Composition {
            dependencies: vec![FunctionDependencies {
                function: function_id,
                join_info: (vec![], vec![]),
                input_set_ids: vec![Some(InputSetDescriptor {
                    composition_id: 0,
                    sharding: ShardingMode::All,
                    optional: false,
                })],
                output_set_ids: vec![Some(1)],
            }],
            output_map: BTreeMap::from([(1, 0)]),
        },
        Metadata {
            input_sets: Arc::new(vec![(String::from("Cin"), None)]),
            output_sets: Arc::new(vec![String::from("Cout")]),
        },
    )];
    check_compositions_and_metadata(compositions, expected, 0..1, 1..2);
}

#[test_log::test]
#[should_panic]
fn test_from_module_minmal_composition_missing_output() {
    let composition_string = r#"
        function Function (Fin) => ();
        composition Composition (Cin) => (Cout) { 
            Function (Fin = all Cin) => ();
        }
    "#;
    let mut function_dict = FunctionDict::new();
    let function_id = function_dict.insert_or_lookup(String::from("Function"));
    let module = get_module(composition_string);
    let compositions = match Composition::from_module(&module, &mut function_dict) {
        Ok(c) => c,
        Err(err) => panic!("Found unexpected error on from_module: {:?}", err),
    };
    let expected = vec![(
        Composition {
            dependencies: vec![FunctionDependencies {
                function: function_id,
                join_info: (vec![], vec![]),
                input_set_ids: vec![Some(InputSetDescriptor {
                    composition_id: 0,
                    sharding: ShardingMode::All,
                    optional: false,
                })],
                output_set_ids: vec![Some(1)],
            }],
            output_map: BTreeMap::from([(1, 0)]),
        },
        Metadata {
            input_sets: Arc::new(vec![(String::from("Cin"), None)]),
            output_sets: Arc::new(vec![String::from("Cout")]),
        },
    )];
    check_compositions_and_metadata(compositions, expected, 0..1, 1..2);
}

use crate::{
    composition::{
        get_sharding, AnyShardingMode, AnyShardingParams, CompositionSet, JoinStrategy,
        ShardingMode, SystemInfo,
    },
    memory_domain::{read_only::ReadOnlyContext, Context},
    DataItem, Position,
};
use itertools::Itertools;
use std::{
    sync::{atomic::AtomicUsize, Arc},
    vec,
};

fn create_dummy_set(keys: Vec<u32>) -> CompositionSet {
    let dummy_context: Arc<Context> = Arc::new(ReadOnlyContext::new_static::<u8>(&mut []));
    let items = keys
        .into_iter()
        .enumerate()
        .map(|(i, k)| {
            (
                DataItem {
                    ident: i.to_string(),
                    key: k,
                    data: Position { offset: 0, size: 0 },
                },
                dummy_context.clone(),
            )
        })
        .sorted_by_key(|tuple| tuple.0.key)
        .collect();
    CompositionSet {
        item_list: items,
        set_name: String::new(),
    }
}

fn set_item_sizes(sets: &mut Vec<Option<(ShardingMode, CompositionSet)>>, sizes: &[&[usize]]) {
    debug_assert_eq!(sets.len(), sizes.len());
    for (set_idx, set) in sets.iter_mut().enumerate() {
        if let Some((_, inner)) = set {
            let set_sizes = sizes[set_idx];
            debug_assert_eq!(inner.item_list.len(), set_sizes.len());
            for (itm_idx, (itm, _)) in inner.item_list.iter_mut().enumerate() {
                itm.data.size = set_sizes[itm_idx];
            }
        }
    }
}

/// An array of options for expected sets in the input set vec produced by a sharing.
type SetGroup = Vec<Option<ExpectedSet>>;

/// An array of tuples with the expected keys and item indexes for the items in a set.
type ExpectedSet = Vec<(u32, usize)>;

/// The expected is a list of all vectors of generated sets.
fn check_sharding(actual: Vec<Vec<Option<CompositionSet>>>, expected: Vec<SetGroup>) {
    assert_eq!(
        expected.len(),
        actual.len(),
        "Not the number of set groups that were expected"
    );
    for (set_group_index, (actual_sets, expected_sets)) in
        actual.into_iter().zip(expected.into_iter()).enumerate()
    {
        assert_eq!(
            expected_sets.len(),
            actual_sets.len(),
            "Sets not matching for index {}, ",
            set_group_index
        );
        for (set_index, (expected_set_opt, actual_set_opt)) in expected_sets
            .into_iter()
            .zip(actual_sets.into_iter())
            .enumerate()
        {
            if expected_set_opt.is_none() {
                assert!(
                    actual_set_opt.is_none(),
                    "Expexted none, but found a set for index {}",
                    set_index
                );
                continue;
            }
            let mut expected_set = expected_set_opt.unwrap();
            let mut actual_set = actual_set_opt.unwrap();
            assert_eq!(expected_set.len(), actual_set.item_list.len());
            // sort both lists by item index, since that one should be unique, since we only have a single context
            expected_set.sort_by_key(|item| item.1.clone());
            actual_set.item_list.sort_by_key(|item| item.0.key);
            // have two sorted lists, check that each item index is the expected one and that it has the correct key
            for ((expected_key, expected_ident), (actual_item, _)) in
                expected_set.into_iter().zip(actual_set.item_list)
            {
                assert_eq!(
                    expected_ident.to_string(),
                    actual_item.ident,
                    "for keys {}, {}",
                    expected_key,
                    actual_item.ident
                );
                assert_eq!(expected_key, actual_item.key);
            }
        }
    }
}

#[cfg(test)]
fn print_sharding(actual: &Vec<Vec<Option<CompositionSet>>>) {
    println!("Got sharding:");
    for inv_sets in actual.iter() {
        println!("[");
        for (set_idx, set) in inv_sets.iter().enumerate() {
            if set.is_none() {
                println!("  set {}: [None]", set_idx);
            } else {
                print!("  set {}: [ ", set_idx);
                for (item, _) in set.as_ref().unwrap().item_list.iter() {
                    print!("{:?} ", item);
                }
                println!("]");
            }
        }
        println!("]");
    }
}

#[test]
fn join_it_inner_test() {
    let sets = vec![
        Some((ShardingMode::Key, create_dummy_set(vec![3, 0, 1]))),
        Some((ShardingMode::Key, create_dummy_set(vec![1, 0, 1, 2]))),
    ];

    let join_order = vec![0, 1];
    let join_strategies = vec![JoinStrategy::Inner];

    let sharding = get_sharding(
        sets,
        join_order,
        join_strategies,
        &AnyShardingMode::MaxSharding,
        vec![],
    );
    let expected = vec![
        vec![Some(vec![(0, 1)]), Some(vec![(0, 1)])],
        vec![Some(vec![(1, 2)]), Some(vec![(1, 0), (1, 2)])],
    ];

    print_sharding(&sharding);
    check_sharding(sharding, expected);
}

#[test]
fn join_it_left_test() {
    let sets = vec![
        Some((ShardingMode::Key, create_dummy_set(vec![0, 1, 2]))),
        Some((ShardingMode::Key, create_dummy_set(vec![3, 1, 0, 1]))),
    ];

    let join_order = vec![0, 1];
    let join_strategies = vec![JoinStrategy::Left];

    let sharding = get_sharding(
        sets,
        join_order,
        join_strategies,
        &AnyShardingMode::MaxSharding,
        vec![],
    );
    let expected = vec![
        vec![Some(vec![(0, 0)]), Some(vec![(0, 2)])],
        vec![Some(vec![(1, 1)]), Some(vec![(1, 1), (1, 3)])],
        vec![Some(vec![(2, 2)]), None],
    ];

    print_sharding(&sharding);
    check_sharding(sharding, expected);
}

#[test]
fn join_it_right_test() {
    let sets = vec![
        Some((ShardingMode::Key, create_dummy_set(vec![3, 1, 0, 1]))),
        Some((ShardingMode::Key, create_dummy_set(vec![0, 1, 2]))),
    ];

    let join_order = vec![0, 1];
    let join_strategies = vec![JoinStrategy::Right];

    let sharding = get_sharding(
        sets,
        join_order,
        join_strategies,
        &AnyShardingMode::MaxSharding,
        vec![],
    );
    let expected = vec![
        vec![Some(vec![(0, 2)]), Some(vec![(0, 0)])],
        vec![Some(vec![(1, 1), (1, 3)]), Some(vec![(1, 1)])],
        vec![None, Some(vec![(2, 2)])],
    ];

    print_sharding(&sharding);
    check_sharding(sharding, expected);
}

#[test]
fn join_it_outer_test() {
    let sets = vec![
        Some((ShardingMode::Key, create_dummy_set(vec![0, 1, 2]))),
        Some((ShardingMode::Key, create_dummy_set(vec![3, 1, 0, 1]))),
    ];

    let join_order = vec![0, 1];
    let join_strategies = vec![JoinStrategy::Outer];

    let sharding = get_sharding(
        sets,
        join_order,
        join_strategies,
        &AnyShardingMode::MaxSharding,
        vec![],
    );
    let expected = vec![
        vec![Some(vec![(0, 0)]), Some(vec![(0, 2)])],
        vec![Some(vec![(1, 1)]), Some(vec![(1, 1), (1, 3)])],
        vec![Some(vec![(2, 2)]), None],
        vec![None, Some(vec![(3, 0)])],
    ];

    print_sharding(&sharding);
    check_sharding(sharding, expected);
}

#[test]
fn join_it_cross_test() {
    let sets = vec![
        Some((ShardingMode::Key, create_dummy_set(vec![0, 1, 2]))),
        Some((ShardingMode::Key, create_dummy_set(vec![3, 1, 0, 1]))),
    ];

    let join_order = vec![0, 1];
    let join_strategies = vec![JoinStrategy::Cross];

    let sharding = get_sharding(
        sets,
        join_order,
        join_strategies,
        &AnyShardingMode::MaxSharding,
        vec![],
    );
    let expected = vec![
        vec![Some(vec![(0, 0)]), Some(vec![(0, 2)])],
        vec![Some(vec![(0, 0)]), Some(vec![(1, 1), (1, 3)])],
        vec![Some(vec![(0, 0)]), Some(vec![(3, 0)])],
        vec![Some(vec![(1, 1)]), Some(vec![(0, 2)])],
        vec![Some(vec![(1, 1)]), Some(vec![(1, 1), (1, 3)])],
        vec![Some(vec![(1, 1)]), Some(vec![(3, 0)])],
        vec![Some(vec![(2, 2)]), Some(vec![(0, 2)])],
        vec![Some(vec![(2, 2)]), Some(vec![(1, 1), (1, 3)])],
        vec![Some(vec![(2, 2)]), Some(vec![(3, 0)])],
    ];

    print_sharding(&sharding);
    check_sharding(sharding, expected);
}

#[test]
fn join_it_order_test() {
    let sets = vec![
        Some((ShardingMode::Key, create_dummy_set(vec![3, 1, 0, 1]))),
        Some((ShardingMode::Key, create_dummy_set(vec![0, 1, 2]))),
    ];

    let join_order = vec![1, 0];
    let join_strategies = vec![JoinStrategy::Left];

    let sharding = get_sharding(
        sets,
        join_order,
        join_strategies,
        &AnyShardingMode::MaxSharding,
        vec![],
    );
    let expected = vec![
        vec![Some(vec![(0, 2)]), Some(vec![(0, 0)])],
        vec![Some(vec![(1, 1), (1, 3)]), Some(vec![(1, 1)])],
        vec![None, Some(vec![(2, 2)])],
    ];

    print_sharding(&sharding);
    check_sharding(sharding, expected);
}

#[test]
fn join_it_chain_test() {
    let sets = vec![
        Some((
            ShardingMode::Key,
            create_dummy_set(vec![1, 1234, 123, 124, 134]),
        )),
        Some((
            ShardingMode::Key,
            create_dummy_set(vec![2, 234, 1234, 123, 124]),
        )),
        Some((
            ShardingMode::Key,
            create_dummy_set(vec![3, 134, 234, 1234, 123]),
        )),
        Some((
            ShardingMode::Key,
            create_dummy_set(vec![4, 124, 134, 234, 1234]),
        )),
    ];

    let join_order = vec![0, 1, 2, 3];
    let join_strategies = vec![
        JoinStrategy::Outer,
        JoinStrategy::Outer,
        JoinStrategy::Outer,
    ];

    let sharding = get_sharding(
        sets,
        join_order,
        join_strategies,
        &AnyShardingMode::MaxSharding,
        vec![],
    );
    let expected = vec![
        vec![Some(vec![(1, 0)]), None, None, None],
        vec![None, Some(vec![(2, 0)]), None, None],
        vec![None, None, Some(vec![(3, 0)]), None],
        vec![None, None, None, Some(vec![(4, 0)])],
        vec![
            Some(vec![(123, 2)]),
            Some(vec![(123, 3)]),
            Some(vec![(123, 4)]),
            None,
        ],
        vec![
            Some(vec![(124, 3)]),
            Some(vec![(124, 4)]),
            None,
            Some(vec![(124, 1)]),
        ],
        vec![
            Some(vec![(134, 4)]),
            None,
            Some(vec![(134, 1)]),
            Some(vec![(134, 2)]),
        ],
        vec![
            None,
            Some(vec![(234, 1)]),
            Some(vec![(234, 2)]),
            Some(vec![(234, 3)]),
        ],
        vec![
            Some(vec![(1234, 1)]),
            Some(vec![(1234, 2)]),
            Some(vec![(1234, 3)]),
            Some(vec![(1234, 4)]),
        ],
    ];

    print_sharding(&sharding);
    check_sharding(sharding, expected);
}

#[test]
fn join_it_any_simple_test() {
    let sets = vec![Some((
        ShardingMode::AnyEach,
        create_dummy_set(vec![0, 1, 2, 3]),
    ))];

    let join_order = vec![0];
    let join_strategies = vec![];

    let sharding = get_sharding(
        sets,
        join_order,
        join_strategies,
        &AnyShardingMode::FixedSharding(2),
        vec![],
    );
    let expected = vec![
        vec![Some(vec![(0, 0), (1, 1)])],
        vec![Some(vec![(2, 2), (3, 3)])],
    ];

    print_sharding(&sharding);
    check_sharding(sharding, expected);
}

#[test]
fn join_it_any_joined_keys_test() {
    let sets = vec![
        Some((ShardingMode::AnyKey, create_dummy_set(vec![0, 1, 2, 3, 5]))),
        Some((ShardingMode::AnyKey, create_dummy_set(vec![0, 2, 3, 4, 5]))),
    ];

    let join_order = vec![0, 1];
    let join_strategies = vec![JoinStrategy::Inner];

    let sharding = get_sharding(
        sets,
        join_order,
        join_strategies,
        &AnyShardingMode::FixedSharding(2),
        vec![],
    );
    let expected = vec![
        vec![Some(vec![(0, 0), (2, 2)]), Some(vec![(0, 0), (2, 1)])],
        vec![Some(vec![(3, 3), (5, 4)]), Some(vec![(3, 2), (5, 4)])],
    ];

    print_sharding(&sharding);
    check_sharding(sharding, expected);
}

#[test]
fn join_it_any_chain_test() {
    let sets = vec![
        Some((ShardingMode::AnyEach, create_dummy_set(vec![0, 0, 0]))),
        Some((ShardingMode::AnyKey, create_dummy_set(vec![0, 1, 2, 3, 5]))),
        Some((ShardingMode::AnyKey, create_dummy_set(vec![0, 2, 3, 4, 5]))),
    ];

    let join_order = vec![1, 2, 0];
    let join_strategies = vec![JoinStrategy::Inner, JoinStrategy::Cross];

    let sharding = get_sharding(
        sets,
        join_order,
        join_strategies,
        &AnyShardingMode::FixedSharding(2),
        vec![],
    );
    let expected = vec![
        vec![
            Some(vec![(0, 0), (0, 1), (0, 2)]),
            Some(vec![(0, 0), (2, 2)]),
            Some(vec![(0, 0), (2, 1)]),
        ],
        vec![
            Some(vec![(0, 0), (0, 1), (0, 2)]),
            Some(vec![(3, 3), (5, 4)]),
            Some(vec![(3, 2), (5, 4)]),
        ],
    ];

    print_sharding(&sharding);
    check_sharding(sharding, expected);
}

#[test]
fn join_it_any_chain_test_1() {
    let sets = vec![
        Some((ShardingMode::AnyEach, create_dummy_set(vec![0, 1]))),
        Some((
            ShardingMode::AnyEach,
            create_dummy_set(vec![0, 1, 2, 3, 4, 5]),
        )),
        Some((ShardingMode::AnyEach, create_dummy_set(vec![0, 1, 2, 3]))),
    ];

    let join_order = vec![0, 1, 2];
    let join_strategies = vec![JoinStrategy::Cross, JoinStrategy::Cross];

    let sharding_6 = get_sharding(
        sets.clone(),
        join_order.clone(),
        join_strategies.clone(),
        &AnyShardingMode::FixedSharding(6),
        vec![],
    );
    assert_eq!(sharding_6.len(), 6); // <- should get 6 shardings
    assert_eq!(sharding_6[0].len(), 3); // <- should still get 3 sets
    assert_eq!(sharding_6[0][0].as_ref().unwrap().len(), 2);
    assert_eq!(sharding_6[0][1].as_ref().unwrap().len(), 1); // <- parallelized set
    assert_eq!(sharding_6[0][2].as_ref().unwrap().len(), 4);

    let sharding_3 = get_sharding(
        sets.clone(),
        join_order.clone(),
        join_strategies.clone(),
        &AnyShardingMode::FixedSharding(3),
        vec![],
    );
    assert_eq!(sharding_3.len(), 3); // <- should get 3 shardings
    assert_eq!(sharding_3[0].len(), 3); // <- should still get 3 sets
    assert_eq!(sharding_3[0][0].as_ref().unwrap().len(), 2);
    assert_eq!(sharding_3[0][1].as_ref().unwrap().len(), 2); // <- parallelized set
    assert_eq!(sharding_3[0][2].as_ref().unwrap().len(), 4);

    let sharding_4 = get_sharding(
        sets.clone(),
        join_order.clone(),
        join_strategies.clone(),
        &AnyShardingMode::FixedSharding(4),
        vec![],
    );
    assert_eq!(sharding_4.len(), 4); // <- should get 4 shardings
    assert_eq!(sharding_4[0].len(), 3); // <- should still get 3 sets
    assert_eq!(sharding_4[0][0].as_ref().unwrap().len(), 2);
    assert_eq!(sharding_4[0][1].as_ref().unwrap().len(), 6);
    assert_eq!(sharding_4[0][2].as_ref().unwrap().len(), 1); // <- parallelized set
}

#[test]
fn join_it_any_chain_test_2() {
    let sets = vec![
        Some((ShardingMode::AnyEach, create_dummy_set(vec![0, 1, 2]))),
        Some((ShardingMode::AnyEach, create_dummy_set(vec![0, 1]))),
    ];

    let join_order = vec![0, 1];
    let join_strategies = vec![JoinStrategy::Cross];

    let sharding_2 = get_sharding(
        sets.clone(),
        join_order.clone(),
        join_strategies.clone(),
        &AnyShardingMode::FixedSharding(2),
        vec![],
    );
    assert_eq!(sharding_2.len(), 2); // <- should get 2 shardings
    assert_eq!(sharding_2[0].len(), 2); // <- should still get 2 sets
    assert_eq!(sharding_2[0][0].as_ref().unwrap().len(), 3);
    assert_eq!(sharding_2[0][1].as_ref().unwrap().len(), 1); // <- parallelized set

    let sharding_3 = get_sharding(
        sets.clone(),
        join_order.clone(),
        join_strategies.clone(),
        &AnyShardingMode::FixedSharding(3),
        vec![],
    );
    assert_eq!(sharding_3.len(), 3); // <- should get 3 shardings
    assert_eq!(sharding_3[0].len(), 2); // <- should still get 2 sets
    assert_eq!(sharding_3[0][0].as_ref().unwrap().len(), 1); // <- parallelized set
    assert_eq!(sharding_3[0][1].as_ref().unwrap().len(), 2);

    let sharding_6 = get_sharding(
        sets.clone(),
        join_order.clone(),
        join_strategies.clone(),
        &AnyShardingMode::FixedSharding(6),
        vec![],
    );
    assert_eq!(sharding_6.len(), 6); // <- should get 6 shardings
    assert_eq!(sharding_6[0].len(), 2); // <- should still get 2 sets
    assert_eq!(sharding_6[0][0].as_ref().unwrap().len(), 1); // <- parallelized set
    assert_eq!(sharding_6[0][1].as_ref().unwrap().len(), 1); // <- parallelized set
}

#[test]
fn join_it_any_chain_test_3() {
    let sets = vec![
        Some((ShardingMode::Each, create_dummy_set(vec![0, 0]))),
        Some((ShardingMode::AnyEach, create_dummy_set(vec![0, 0, 0]))),
        Some((ShardingMode::AnyKey, create_dummy_set(vec![0, 1, 1]))),
    ];

    let join_order = vec![0, 1, 2];
    let join_strategies = vec![JoinStrategy::Cross, JoinStrategy::Cross];

    let sharding_2 = get_sharding(
        sets.clone(),
        join_order.clone(),
        join_strategies.clone(),
        &AnyShardingMode::FixedSharding(2),
        vec![],
    );
    assert_eq!(sharding_2.len(), 2); // <- should get 2 shardings
    assert_eq!(sharding_2[0].len(), 3); // <- should still get 3 sets
    assert_eq!(sharding_2[0][0].as_ref().unwrap().len(), 1); // <- always parallelized (not any)
    assert_eq!(sharding_2[0][1].as_ref().unwrap().len(), 3);
    assert_eq!(sharding_2[0][2].as_ref().unwrap().len(), 3);

    let sharding_4 = get_sharding(
        sets.clone(),
        join_order.clone(),
        join_strategies.clone(),
        &AnyShardingMode::FixedSharding(4),
        vec![],
    );
    assert_eq!(sharding_4.len(), 4); // <- should get 4 shardings
    assert_eq!(sharding_4[0].len(), 3); // <- should still get 3 sets
    assert_eq!(sharding_4[0][0].as_ref().unwrap().len(), 1); // <- always parallelized (not any)
    assert_eq!(sharding_4[0][1].as_ref().unwrap().len(), 3);
    assert_eq!(sharding_4[0][2].as_ref().unwrap().len(), 1); // <- parallelized any set

    let sharding_6 = get_sharding(
        sets.clone(),
        join_order.clone(),
        join_strategies.clone(),
        &AnyShardingMode::FixedSharding(6),
        vec![],
    );
    assert_eq!(sharding_6.len(), 6); // <- should get 6 shardings
    assert_eq!(sharding_6[0].len(), 3); // <- should still get 3 sets
    assert_eq!(sharding_6[0][0].as_ref().unwrap().len(), 1); // <- always parallelized (not any)
    assert_eq!(sharding_6[0][1].as_ref().unwrap().len(), 1); // <- parallelized any set
    assert_eq!(sharding_6[0][2].as_ref().unwrap().len(), 3);
}

#[test]
fn join_it_any_chain_test_4() {
    let sets = vec![
        Some((ShardingMode::Each, create_dummy_set(vec![0, 0]))),
        Some((ShardingMode::AnyKey, create_dummy_set(vec![0, 2, 0, 1, 3]))),
        Some((ShardingMode::AnyKey, create_dummy_set(vec![0, 1, 1, 4]))),
    ];

    let join_order = vec![1, 2, 0];

    let join_strategies_inner = vec![JoinStrategy::Inner, JoinStrategy::Cross];
    let sharding_inner = get_sharding(
        sets.clone(),
        join_order.clone(),
        join_strategies_inner.clone(),
        &AnyShardingMode::FixedSharding(10),
        vec![],
    );
    assert_eq!(sharding_inner.len(), 4); // <- should get 4 shardings
    assert_eq!(sharding_inner[0].len(), 3); // <- should still get 3 sets
    assert_eq!(sharding_inner[0][0].as_ref().unwrap().len(), 1); // <- always parallelized (not any)
    assert_eq!(sharding_inner[0][1].as_ref().unwrap().len(), 2); // <- parallelized any set
    assert_eq!(sharding_inner[0][2].as_ref().unwrap().len(), 1); // <- parallelized any set

    let join_strategies_left = vec![JoinStrategy::Left, JoinStrategy::Cross];
    let sharding_left = get_sharding(
        sets.clone(),
        join_order.clone(),
        join_strategies_left.clone(),
        &AnyShardingMode::FixedSharding(10),
        vec![],
    );
    assert_eq!(sharding_left.len(), 8); // <- should get 2*4=8 shardings
    assert_eq!(sharding_left[0].len(), 3); // <- should still get 3 sets
    assert_eq!(sharding_left[0][0].as_ref().unwrap().len(), 1); // <- always parallelized (not any)
    assert_eq!(sharding_left[0][1].as_ref().unwrap().len(), 2); // <- parallelized any set
    assert_eq!(sharding_left[0][2].as_ref().unwrap().len(), 1); // <- parallelized any set

    let join_strategies_right = vec![JoinStrategy::Right, JoinStrategy::Cross];
    let sharding_right = get_sharding(
        sets.clone(),
        join_order.clone(),
        join_strategies_right.clone(),
        &AnyShardingMode::FixedSharding(10),
        vec![],
    );
    assert_eq!(sharding_right.len(), 6); // <- should get 2*3=6 shardings
    assert_eq!(sharding_right[0].len(), 3); // <- should still get 3 sets
    assert_eq!(sharding_right[0][0].as_ref().unwrap().len(), 1); // <- always parallelized (not any)
    assert_eq!(sharding_right[0][1].as_ref().unwrap().len(), 2); // <- parallelized any set
    assert_eq!(sharding_right[0][2].as_ref().unwrap().len(), 1); // <- parallelized any set

    let join_strategies_outer = vec![JoinStrategy::Outer, JoinStrategy::Cross];
    let sharding_outer = get_sharding(
        sets.clone(),
        join_order.clone(),
        join_strategies_outer.clone(),
        &AnyShardingMode::FixedSharding(10),
        vec![],
    );
    assert_eq!(sharding_outer.len(), 10); // <- should get 2*5=10 shardings
    assert_eq!(sharding_outer[0].len(), 3); // <- should still get 3 sets
    assert_eq!(sharding_outer[0][0].as_ref().unwrap().len(), 1); // <- always parallelized (not any)
    assert_eq!(sharding_outer[0][1].as_ref().unwrap().len(), 2); // <- parallelized any set
    assert_eq!(sharding_outer[0][2].as_ref().unwrap().len(), 1); // <- parallelized any set
}

#[test]
fn join_it_any_chain_test_5() {
    let sets = vec![Some((
        ShardingMode::AnyKey,
        create_dummy_set(vec![0, 2, 0, 1, 3]),
    ))];

    let join_order = vec![0];
    let join_strategies = vec![];

    let sharding_2 = get_sharding(
        sets.clone(),
        join_order.clone(),
        join_strategies.clone(),
        &AnyShardingMode::FixedSharding(2),
        vec![],
    );
    assert_eq!(sharding_2.len(), 2); // <- should get 2 shardings
    assert_eq!(sharding_2[0].len(), 1); // <- should still get 1 set
    assert_eq!(sharding_2[0][0].as_ref().unwrap().len(), 3); // <- parallelized any set

    let sharding_4 = get_sharding(
        sets.clone(),
        join_order.clone(),
        join_strategies.clone(),
        &AnyShardingMode::FixedSharding(4),
        vec![],
    );
    assert_eq!(sharding_4.len(), 4); // <- should get 4 shardings
    assert_eq!(sharding_4[0].len(), 1); // <- should still get 1 set
    assert_eq!(sharding_4[0][0].as_ref().unwrap().len(), 2); // <- parallelized any set
}

#[test]
fn join_it_any_chain_test_6() {
    let sets = vec![
        Some((ShardingMode::AnyKey, create_dummy_set(vec![0, 1, 4]))),
        Some((ShardingMode::AnyKey, create_dummy_set(vec![0, 2, 0, 3]))),
        Some((ShardingMode::AnyKey, create_dummy_set(vec![0, 1, 1]))),
        Some((ShardingMode::AnyEach, create_dummy_set(vec![5, 5]))),
    ];

    let join_order = vec![1, 2, 0, 3];
    let join_strategies = vec![
        JoinStrategy::Inner,
        JoinStrategy::Right,
        JoinStrategy::Cross,
    ];

    let sharding_2 = get_sharding(
        sets.clone(),
        join_order.clone(),
        join_strategies.clone(),
        &AnyShardingMode::FixedSharding(2),
        vec![],
    );
    assert_eq!(sharding_2.len(), 2); // <- should get 2 shardings
    assert_eq!(sharding_2[0].len(), 4); // <- should still get 4 sets
    assert_eq!(sharding_2[0][0].as_ref().unwrap().len(), 3);
    assert_eq!(sharding_2[0][1].as_ref().unwrap().len(), 2);
    assert_eq!(sharding_2[0][2].as_ref().unwrap().len(), 1);
    assert_eq!(sharding_2[0][3].as_ref().unwrap().len(), 1); // <- parallelized any set
    assert_eq!(sharding_2[0][0].as_ref().unwrap().item_list[0].0.key, 0);
    assert_eq!(sharding_2[0][0].as_ref().unwrap().item_list[1].0.key, 1);
    assert_eq!(sharding_2[0][0].as_ref().unwrap().item_list[2].0.key, 4);
    assert_eq!(sharding_2[0][1].as_ref().unwrap().item_list[0].0.key, 0);
    assert_eq!(sharding_2[0][1].as_ref().unwrap().item_list[1].0.key, 0);
    assert_eq!(sharding_2[0][2].as_ref().unwrap().item_list[0].0.key, 0);
    assert_eq!(sharding_2[0][3].as_ref().unwrap().item_list[0].0.key, 5);

    let sharding_3 = get_sharding(
        sets.clone(),
        join_order.clone(),
        join_strategies.clone(),
        &AnyShardingMode::FixedSharding(3),
        vec![],
    );
    assert_eq!(sharding_3.len(), 3); // <- should get 3 shardings
    assert_eq!(sharding_3[0].len(), 4); // <- should still get 4 sets
    assert_eq!(sharding_3[0][0].as_ref().unwrap().len(), 1); // <- parallelized any set
    assert_eq!(sharding_3[0][1].as_ref().unwrap().len(), 2); // <- parallelized any set
    assert_eq!(sharding_3[0][2].as_ref().unwrap().len(), 1); // <- parallelized any set
    assert_eq!(sharding_3[0][3].as_ref().unwrap().len(), 2);
    assert_eq!(sharding_3[0][0].as_ref().unwrap().item_list[0].0.key, 0);
    assert_eq!(sharding_3[0][1].as_ref().unwrap().item_list[0].0.key, 0);
    assert_eq!(sharding_3[0][1].as_ref().unwrap().item_list[1].0.key, 0);
    assert_eq!(sharding_3[0][2].as_ref().unwrap().item_list[0].0.key, 0);
    assert_eq!(sharding_3[0][3].as_ref().unwrap().item_list[0].0.key, 5);
    assert_eq!(sharding_3[0][3].as_ref().unwrap().item_list[1].0.key, 5);
}

#[test]
fn join_it_any_chain_test_7() {
    let sets = vec![
        Some((ShardingMode::Each, create_dummy_set(vec![0]))),
        Some((ShardingMode::AnyEach, create_dummy_set(vec![0]))),
        None,
    ];

    let join_order = vec![0, 1, 2];
    let join_strategies = vec![JoinStrategy::Cross, JoinStrategy::Cross];

    let sharding = get_sharding(
        sets.clone(),
        join_order.clone(),
        join_strategies.clone(),
        &AnyShardingMode::FixedSharding(usize::MAX),
        vec![],
    );
    assert_eq!(sharding.len(), 1); // <- should get 1 sharding
    assert_eq!(sharding[0].len(), 3); // <- should still get 3 sets
    assert_eq!(sharding[0][0].as_ref().unwrap().len(), 1);
    assert_eq!(sharding[0][1].as_ref().unwrap().len(), 1);
    assert!(sharding[0][2].is_none());
    assert_eq!(sharding[0][0].as_ref().unwrap().item_list[0].0.key, 0);
    assert_eq!(sharding[0][1].as_ref().unwrap().item_list[0].0.key, 0);
}

#[test]
fn join_it_any_auto_sharding_test() {
    let mut sets = vec![Some((
        ShardingMode::AnyEach,
        create_dummy_set(vec![0, 1, 2, 3]),
    ))];

    let join_order = vec![0];
    let join_strategies = vec![];

    // large offload_const, no min_set_size -> should shard over local nodes
    set_item_sizes(&mut sets, &[&[2, 2, 2, 2]]);
    let sharding = get_sharding(
        sets.clone(),
        join_order.clone(),
        join_strategies.clone(),
        &AnyShardingMode::AutoSharding(AnyShardingParams {
            sys_info: Arc::new(SystemInfo {
                num_local_cores: AtomicUsize::new(2),
                num_remote_cores: AtomicUsize::new(2),
            }),
            offload_const: 10, // -> should definitely not offload
        }),
        vec![],
    );
    let expected = vec![
        vec![Some(vec![(0, 0), (1, 1)])],
        vec![Some(vec![(2, 2), (3, 3)])],
    ];
    print_sharding(&sharding);
    check_sharding(sharding, expected);
}

#[test]
fn join_it_any_auto_sharding_test_1() {
    let mut sets = vec![Some((
        ShardingMode::AnyEach,
        create_dummy_set(vec![0, 1, 2, 3]),
    ))];

    let join_order = vec![0];
    let join_strategies = vec![];

    // small offload_const, no min_set_size -> should shard over all nodes
    set_item_sizes(&mut sets, &[&[2, 2, 2, 2]]);
    let sharding = get_sharding(
        sets.clone(),
        join_order.clone(),
        join_strategies.clone(),
        &AnyShardingMode::AutoSharding(AnyShardingParams {
            sys_info: Arc::new(SystemInfo {
                num_local_cores: AtomicUsize::new(2),
                num_remote_cores: AtomicUsize::new(2),
            }),
            offload_const: 1, // -> no offload overhead
        }),
        vec![],
    );
    let expected = vec![
        vec![Some(vec![(0, 0)])],
        vec![Some(vec![(1, 1)])],
        vec![Some(vec![(2, 2)])],
        vec![Some(vec![(3, 3)])],
    ];
    print_sharding(&sharding);
    check_sharding(sharding, expected);
}

#[test]
fn join_it_any_auto_sharding_test_2() {
    let mut sets = vec![Some((
        ShardingMode::AnyEach,
        create_dummy_set(vec![0, 1, 2, 3]),
    ))];

    let join_order = vec![0];
    let join_strategies = vec![];

    // small offload_const, relevant min_set_size -> should shard over local nodes
    set_item_sizes(&mut sets, &[&[2, 2, 2, 2]]);
    let sharding = get_sharding(
        sets.clone(),
        join_order.clone(),
        join_strategies.clone(),
        &AnyShardingMode::AutoSharding(AnyShardingParams {
            sys_info: Arc::new(SystemInfo {
                num_local_cores: AtomicUsize::new(2),
                num_remote_cores: AtomicUsize::new(2),
            }),
            offload_const: 1, // -> no offload overhead
        }),
        vec![4],
    );
    let expected = vec![
        vec![Some(vec![(0, 0), (1, 1)])],
        vec![Some(vec![(2, 2), (3, 3)])],
    ];
    print_sharding(&sharding);
    check_sharding(sharding, expected);
}

#[test]
fn join_it_any_auto_sharding_test_3() {
    let mut sets = vec![Some((
        ShardingMode::AnyEach,
        create_dummy_set(vec![0, 1, 2, 3]),
    ))];

    let join_order = vec![0];
    let join_strategies = vec![];

    // small offload_const, relevant min_set_size -> should shard over local nodes
    set_item_sizes(&mut sets, &[&[4, 1, 1, 1]]);
    let sharding = get_sharding(
        sets.clone(),
        join_order.clone(),
        join_strategies.clone(),
        &AnyShardingMode::AutoSharding(AnyShardingParams {
            sys_info: Arc::new(SystemInfo {
                num_local_cores: AtomicUsize::new(2),
                num_remote_cores: AtomicUsize::new(2),
            }),
            offload_const: 1, // -> no offload overhead
        }),
        vec![3],
    );
    let expected = vec![
        vec![Some(vec![(1, 1), (2, 2), (3, 3)])],
        vec![Some(vec![(0, 0)])],
    ];
    print_sharding(&sharding);
    check_sharding(sharding, expected);
}

#[test]
fn join_it_any_auto_sharding_test_4() {
    let mut sets = vec![
        Some((ShardingMode::AnyEach, create_dummy_set(vec![0, 1, 2, 3]))),
        Some((ShardingMode::AnyKey, create_dummy_set(vec![0, 1, 2, 3]))),
        Some((ShardingMode::AnyKey, create_dummy_set(vec![1, 2, 4, 5]))),
    ];

    let join_order = vec![1, 2, 0];
    let join_strategies = vec![JoinStrategy::Inner, JoinStrategy::Cross];

    // small offload_const, relevant min_set_size -> should shard over local nodes
    set_item_sizes(&mut sets, &[&[4, 1, 3, 5], &[100, 4, 4, 1], &[1, 1, 1, 1]]);
    let sharding = get_sharding(
        sets.clone(),
        join_order.clone(),
        join_strategies.clone(),
        &AnyShardingMode::AutoSharding(AnyShardingParams {
            sys_info: Arc::new(SystemInfo {
                num_local_cores: AtomicUsize::new(6),
                num_remote_cores: AtomicUsize::new(2),
            }),
            offload_const: 1, // -> no offload overhead
        }),
        vec![4, 4, 4],
    );
    let expected = vec![
        vec![
            Some(vec![(1, 1), (2, 2)]),
            Some(vec![(1, 1)]),
            Some(vec![(1, 0)]),
        ],
        vec![Some(vec![(0, 0)]), Some(vec![(1, 1)]), Some(vec![(1, 0)])],
        vec![Some(vec![(3, 3)]), Some(vec![(1, 1)]), Some(vec![(1, 0)])],
        vec![
            Some(vec![(1, 1), (2, 2)]),
            Some(vec![(2, 2)]),
            Some(vec![(2, 1)]),
        ],
        vec![Some(vec![(0, 0)]), Some(vec![(2, 2)]), Some(vec![(2, 1)])],
        vec![Some(vec![(3, 3)]), Some(vec![(2, 2)]), Some(vec![(2, 1)])],
    ];
    print_sharding(&sharding);
    check_sharding(sharding, expected);
}

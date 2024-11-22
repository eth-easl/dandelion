use std::ops::Sub;
use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Bound,
};

/// Structure to keep track of occupation of a range of indices
#[derive(Debug)]
pub struct RangePool<Element> {
    /// Ranges of inidices that are currntly not occupied
    /// Keys are the starting indices, Values are 1 past the end, meaning the starting indices of the range that would come after
    ranges: BTreeMap<Element, Element>,
    /// Set with unoccupied ranges of inidices sorted by size and second by starting index
    /// Used for fast lookup of minimal sized range
    sizes: BTreeSet<(Element, Element)>,
}

impl<Element> RangePool<Element>
where
    Element: Copy + Ord + Sub<Output = Element>,
{
    pub fn new(initial_range: std::ops::Range<Element>) -> Self {
        let initial_size = initial_range.end - initial_range.start;
        let mut ranges = BTreeMap::new();
        ranges.insert(initial_range.start, initial_range.end);
        let mut sizes = BTreeSet::new();
        sizes.insert((initial_size, initial_range.start));
        return Self { ranges, sizes };
    }

    pub fn get(&mut self, requested_size: Element, min_value: Element) -> Option<Element> {
        // find smallest size range that fits the requested size
        let (original_size, original_start) = *self
            .sizes
            .range((
                Bound::Included(&(requested_size, min_value)),
                Bound::Unbounded,
            ))
            .next()?;
        // need to remove the element and reinsert with the adapted size
        self.sizes.remove(&(original_size, original_start));
        return if original_size > requested_size {
            let remaining_size = original_size - requested_size;
            self.sizes.insert((remaining_size, original_start));
            let original_end = self
                .ranges
                .get_mut(&original_start)
                .expect("Should have range in ranges that was found in sizes");
            *original_end = *original_end - requested_size;
            // start of the current
            Some(*original_end)
        } else {
            // the range has the exact size we need so can just remove it
            self.ranges.remove(&original_start);
            Some(original_start)
        };
    }

    pub fn insert(&mut self, start: Element, mut end: Element) {
        // check if we can concatinate with range after
        if let Some(next_end) = self.ranges.remove(&end) {
            // if we found need to remove from sizes
            let was_removed = self.sizes.remove(&(next_end - end, end));
            debug_assert!(
                was_removed,
                "Should have found next element in sizes, as it was in ranges"
            );
            end = next_end;
        }
        // check if there is a node before the insert we can append to
        if let Some((previous_start, previous_end)) = self
            .ranges
            .range_mut((Bound::Unbounded, Bound::Excluded(&start)))
            .next_back()
            .and_then(|(previous_start, previous_end)| {
                if *previous_end == start {
                    Some((previous_start, previous_end))
                } else {
                    None
                }
            })
        {
            // can merge with previous entry
            // need to remove it from sizes and reinsert new size
            let was_removed = self
                .sizes
                .remove(&(*previous_end - *previous_start, *previous_start));
            debug_assert!(
                was_removed,
                "Should have found previous element in sizes as it was in ranges"
            );
            self.sizes.insert((end - *previous_start, *previous_start));
            // update entry to include inserted range
            *previous_end = end;
        } else {
            // can't merge with previous entry
            // insert into ranges
            self.ranges.insert(start, end);
            // insert into sizes
            self.sizes.insert((end - start, start));
        }
    }
}

#[test]
fn test_range_pool() {
    let mut new_pool = RangePool::new(0u8..10u8);
    // get more than available
    assert_eq!(new_pool.get(11, u8::MIN), None);
    // check pool gets emptied properly
    assert_eq!(new_pool.get(10, u8::MIN), Some(0));
    assert_eq!(new_pool.get(1, u8::MIN), None);

    // check simple reinsertion
    new_pool.insert(1, 3);
    assert_eq!(new_pool.get(2, u8::MIN), Some(1));

    // check concatinating reinsertion
    new_pool.insert(4, 6);
    new_pool.insert(6, 8);
    assert_eq!(new_pool.get(4, u8::MIN), Some(4));
}

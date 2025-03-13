use crate::composition::{CompositionSet, GlobalSetId, OutputMap};
use dandelion_commons::{DandelionError, DandelionResult, SetRegistryError};
use futures::{
    lock::Mutex,
    task::{Context, Poll, Waker},
    Future, FutureExt,
};
use std::{
    collections::{btree_map::Entry, BTreeMap},
    sync::Arc,
};

struct SetEntry {
    /// Number of places the set will be awaited
    consumers: usize,
    /// Number of places still expected to contribute to the set before it is complete
    producers: usize,
    /// List of registered wakers
    wait_list: Vec<Option<Waker>>,
    /// Current error or composition set (to be added to if still exected)
    set: DandelionResult<CompositionSet>,
}

impl SetEntry {
    fn add_set(&mut self, set: DandelionResult<CompositionSet>) -> DandelionResult<()> {
        self.producers -= 1;
        // not using match statement, as the bindings of variables make it overly complicated
        if self.set.is_err() {
            // if set has not been set yet, always replace
            if let Err(DandelionError::SetRegistry(SetRegistryError::UnretrievableSet)) = self.set {
                self.set = set;
            }
            // otherwise keep current set error and return;
            // current set is not an error
        } else {
            // if the incomming is an error replace the current ok with the error
            if set.is_err() {
                self.set = set;
            // both are okay, so unwrap and fuse them
            } else {
                self.set.as_mut().unwrap().combine(&mut set.unwrap())?;
            }
        }
        Ok(())
    }
}

/// Future for awaiting the completion of a set
pub struct SetFuture {
    set_map: Arc<Mutex<BTreeMap<GlobalSetId, SetEntry>>>,
    waker_index: Option<usize>,
    key: GlobalSetId,
}

impl Future for SetFuture {
    type Output = DandelionResult<Vec<Option<(OutputMap, CompositionSet)>>>;
    fn poll(mut self: core::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut guard = match self.set_map.lock().poll_unpin(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(lock) => lock,
        };

        let SetEntry {
            consumers: waiting,
            producers: expected,
            wait_list,
            set,
        } = guard
            .get_mut(&self.key)
            .expect("SetFuture should always find an entry");

        // If more contributions are expected, update waker and return pending
        if *expected > 0 {
            // Update waker
            if let Some(waker_index) = self.waker_index {
                wait_list[waker_index] = Some(cx.waker().clone());
            } else {
                wait_list.push(Some(cx.waker().clone()));
                let new_index = wait_list.len() - 1;
                drop(guard);
                self.waker_index = Some(new_index);
            }
            Poll::Pending
        // expected == 0, meaning we can take what is in the set and remove ourselfs from waiting
        } else {
            // TODO remove waker from list when taking set entry
            let set_clone = set
                .clone()
                .and_then(|set| Ok(vec![Some((OutputMap::Global(self.key), set))]));
            *waiting -= 1;
            if let Some(waker_index) = self.waker_index {
                wait_list[waker_index] = None;
            }
            if *waiting == 0 {
                guard.remove(&self.key);
            }
            Poll::Ready(set_clone)
        }
    }
}

// TODO implement drop to decrease waiting by 1

pub struct SetRegistry {
    set_map: Arc<Mutex<BTreeMap<GlobalSetId, SetEntry>>>,
}

/// The set registry keeps track of all sets that are expected to be produced and their completeness.
/// The sets are tracket on a composition level, meaning, a composition will enter a set into the registry,
/// when it comes back already recombined from the sharded execution.
/// First iteration: the number of expected producers and consumers is registered before any of the producers or consumers are enqueued.
/// Any local composition that directly contributes is counted as one producer and one comsumer.
///
/// For later:
/// For this a set can be in one of the following states:
/// - not produced and awatied by 0
/// - not produced and awaited by 1 or more
/// - partially produced and awaited by 0 or more
/// - fully produced and waited by 0 or more
/// To simplify concurrecny we demand the following invariants:
/// - all producers need to be registered before any of them start to work
/// - all consumers need to be registered before any of the producers start work
///     or after the producers started only by a already registered consumer (i.e. a consumer can add more of them at any time)
/// This allows us to determine when it is safe to remove a set from the registry,
/// without the risk of later access by either a consumer or a producer.
/// As all producers need to be registered before any of them start to work,
/// we can safely know that the set is complete, when one of the producers reduces the count to 0.
/// Additionally we know that we can remove a complete set when the producer and consumer count are reduced to 0,
/// as if there are no more consumers, they cannot add any, and a complete set implies all producers finished.
/// TODO: still unsolved, consumer count going to 0, before registering the first producer
/// TODO: handle dropping the composition and thus the producer potentially not finishing
/// Idea: give out item to consumers that when dropped reduces count, so we can remove when count reaches 0
impl SetRegistry {
    pub fn new() -> Self {
        SetRegistry {
            set_map: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub async fn register_set(
        &self,
        index: GlobalSetId,
        producers: usize,
        consumers: usize,
    ) -> DandelionResult<()> {
        let mut guard = self.set_map.lock().await;
        match guard.entry(index) {
            Entry::Occupied(_) => {
                return Err(DandelionError::SetRegistry(
                    dandelion_commons::SetRegistryError::InsertExisting,
                ))
            }
            Entry::Vacant(vacant) => {
                vacant.insert(SetEntry {
                    consumers,
                    producers,
                    wait_list: Vec::with_capacity(consumers),
                    set: Err(DandelionError::SetRegistry(
                        SetRegistryError::UnretrievableSet,
                    )),
                });
            }
        }
        return Ok(());
    }

    /// Get future for the set with a certain index
    pub fn get_set_future(&self, index: &GlobalSetId) -> SetFuture {
        SetFuture {
            set_map: self.set_map.clone(),
            waker_index: None,
            key: *index,
        }
    }

    pub async fn insert_set(
        &self,
        index: GlobalSetId,
        set: DandelionResult<CompositionSet>,
    ) -> DandelionResult<()> {
        let mut guard = self.set_map.lock().await;
        match guard.entry(index) {
            Entry::Occupied(mut occupied) => {
                let existing = occupied.get_mut();
                existing.add_set(set)?;
                if existing.producers == 0 {
                    for waker_option in existing.wait_list.iter() {
                        if let Some(waker) = waker_option {
                            waker.wake_by_ref();
                        }
                    }
                }
                Ok(())
            }
            Entry::Vacant(_) => Err(DandelionError::SetRegistry(SetRegistryError::NonExisting)),
        }
    }

    // Previous attempt, keeping for now in case we need it
    // pub async fn await_set(&self, index: SetId) -> SetFuture {
    //     let mut guard = self.set_map.lock().await;
    //     match guard.entry(index) {
    //         Entry::Occupied(mut occupied) => occupied.get_mut().consumers += 1,
    //         Entry::Vacant(vacant) => {
    //             vacant.insert(SetEntry {
    //                 consumers: 1,
    //                 producers: 0,
    //                 wait_list: Vec::new(),
    //                 set: Err(DandelionError::SetRegistryError(
    //                     SetRegistryError::DefaultSet,
    //                 )),
    //             });
    //         }
    //     }
    //     SetFuture {
    //         waker_index: None,
    //         key: index,
    //         set_map: self.set_map.clone(),
    //     }
    // }

    // pub async fn will_insert_set(&self, index: SetId, new_expected: usize) -> () {
    //     let mut guard = self.set_map.lock().await;
    //     match guard.entry(index) {
    //         Entry::Occupied(mut occupied) => {
    //             occupied.get_mut().producers += new_expected;
    //         }
    //         Entry::Vacant(vacant) => {
    //             vacant.insert(SetEntry {
    //                 consumers: 0,
    //                 producers: new_expected,
    //                 wait_list: Vec::new(),
    //                 set: Err(DandelionError::SetRegistryError(
    //                     SetRegistryError::DefaultSet,
    //                 )),
    //             });
    //         }
    //     };
    // }

    // pub async fn insert_set(&self, index: SetId, new_set: DandelionResult<CompositionSet>) {
    //     let mut guard = self.set_map.lock().await;
    //     match guard.entry(index) {
    //         Entry::Vacant(vacant) => {
    //             vacant.insert(SetEntry {
    //                 consumers: 0,
    //                 producers: 0,
    //                 wait_list: Vec::new(),
    //                 set: new_set,
    //             });
    //         }
    //         Entry::Occupied(mut occupied) => {
    //             let SetEntry {
    //                 consumers: _,
    //                 producers: expected,
    //                 wait_list,
    //                 set,
    //             } = occupied.get_mut();

    //             // combine new set with existing one or default one
    //             match (set, new_set) {
    //                 (
    //                     old @ Err(DandelionError::SetRegistryError(SetRegistryError::DefaultSet)),
    //                     Ok(n_set),
    //                 ) => *old = Ok(n_set),
    //                 (Ok(o_set), Ok(mut n_set)) => {
    //                     // TODO: implement error handling on the combination
    //                     o_set.combine(&mut n_set).unwrap();
    //                     // this borrow faults
    //                     // if let Err(combine_err) = combine_error {
    //                     // *old = Err(combine_err);
    //                     // }
    //                 }
    //                 (old @ Ok(_), Err(err)) => *old = Err(err),
    //                 (Err(_), Ok(_)) | (Err(_), Err(_)) => (),
    //             };
    //             // if let Some(insert_set) = insert_opt {
    //             //     *set = insert_set;
    //             // }

    //             // count down the expeceted set number and wake up waiting one if all are present
    //             *expected -= 1;
    //             if *expected == 0 {
    //                 for waker in wait_list {
    //                     waker.wake_by_ref();
    //                 }
    //             }
    //         }
    //     };
    // }
}

// #[cfg(test)]
// mod tests {
//     use std::task::Poll;

//     use futures::{
//         task::{noop_waker, Context},
//         Future,
//     };

//     use crate::composition::CompositionSet;
//     #[test]
//     fn access_after_insert() {
//         let registry = super::SetRegistry::new();
//         let noop_waker = noop_waker();
//         let mut test_context = Context::from_waker(&noop_waker);

//         // register that the set will be produced
//         let promise_future =
//             core::pin::pin!(registry.will_insert_set(1, 1)).poll(&mut test_context);
//         assert_eq!(Poll::Ready(()), promise_future);

//         let mut initial_future =
//             match core::pin::pin!(registry.await_set(1)).poll(&mut test_context) {
//                 Poll::Ready(set_future) => core::pin::pin!(set_future),
//                 Poll::Pending => panic!("initial registry wait insert failed"),
//             };
//         let mut duplicate_future =
//             match core::pin::pin!(registry.await_set(1)).poll(&mut test_context) {
//                 Poll::Ready(set_future) => core::pin::pin!(set_future),
//                 Poll::Pending => panic!("second registry wait insert failed"),
//             };
//         let initial_pending = initial_future.as_mut().poll(&mut test_context);
//         let duplicate_pending = duplicate_future.as_mut().poll(&mut test_context);
//         assert!(initial_pending.is_pending());
//         assert!(duplicate_pending.is_pending());
//         let insert_future = registry.insert_set(
//             1,
//             Ok(CompositionSet {
//                 context_list: Vec::new(),
//                 set_index: 0,
//             }),
//         );
//         let _ = core::pin::pin!(insert_future).poll(&mut test_context);
//         let initial_ready = initial_future.as_mut().poll(&mut test_context);
//         assert!(initial_ready.is_ready());
//         let duplicate_ready = duplicate_future.as_mut().poll(&mut test_context);
//         assert!(duplicate_ready.is_ready());
//     }
// }

use crate::{
    function_driver::{
        functions::FunctionConfig, ComputeResource, EngineWorkQueue, WorkDone, WorkToDo,
    },
    machine_config::EngineType,
    memory_domain::{self, Context},
    promise::Debt,
};
use core::marker::Send;
use dandelion_commons::{
    err_dandelion, records::RecordPoint, DandelionError, DandelionResult, FunctionRegistryError,
};
use std::{
    future::Future,
    sync::atomic::{AtomicBool, Ordering},
    task::{Poll, RawWaker, RawWakerVTable, Waker},
    thread::spawn,
};

extern crate alloc;

pub trait EngineLoop {
    fn init(core_id: u8) -> DandelionResult<Box<Self>>;
    fn run(
        &mut self,
        config: FunctionConfig,
        context: Context,
        output_sets: &Vec<String>,
    ) -> DandelionResult<Context>;
    fn get_engine_type(&self) -> EngineType;
}

// Either use a local atomic bool as a waker or a channel if we want blocking

// TODO: make sencond blocking waker
// functions for the waker
fn waker_clone(data: *const ()) -> RawWaker {
    RawWaker::new(data, &WAKER_TABLE)
}

fn waker_wake(data: *const ()) {
    let atomic = unsafe { &*(data as *const AtomicBool) };
    atomic.store(true, Ordering::Release);
}

unsafe fn waker_wake_by_ref(data: *const ()) {
    waker_wake(data);
}

unsafe fn waker_drop(_: *const ()) {}

const WAKER_TABLE: RawWakerVTable =
    RawWakerVTable::new(waker_clone, waker_wake, waker_wake_by_ref, waker_drop);

fn manual_pull(queue: &mut impl EngineWorkQueue) -> (WorkToDo, Debt) {
    let mut queue_future = core::pin::pin!(queue.get_engine_args());
    //
    let new_atomic = AtomicBool::new(false);
    let raw_waker = RawWaker::new(new_atomic.as_ptr() as *const (), &WAKER_TABLE);
    let waker = unsafe { Waker::from_raw(raw_waker) };
    let mut context = std::task::Context::from_waker(&waker);
    loop {
        if let Poll::Ready(work) = queue_future.as_mut().poll(&mut context) {
            return work;
        }
        // means it is still pending, wait for waker to be woken
        while new_atomic
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            core::hint::spin_loop();
        }
    }
}

fn run_thread<E: EngineLoop>(core_id: u8, mut queue: impl EngineWorkQueue) {
    // set core affinity
    if !core_affinity::set_for_current(core_affinity::CoreId { id: core_id.into() }) {
        log::error!("core received core id that could not be set");
        return;
    }
    let mut engine_state = E::init(core_id).expect("Failed to initialize thread state");
    'engine: loop {
        // TODO catch unwind so we can always return an error or shut down gracefully
        let (args, debt) = manual_pull(&mut queue);
        match args {
            WorkToDo::FunctionArguments {
                function_id: _,
                function_alternatives,
                input_sets,
                metadata,
                caching,
                mut recorder,
            } => {
                let engine_type = engine_state.get_engine_type();
                let alternative = match function_alternatives
                    .into_iter()
                    .find(|alt| alt.engine == engine_type)
                {
                    Some(alt) => alt,
                    None => {
                        drop(recorder);
                        debt.fulfill(err_dandelion!(DandelionError::FunctionRegistry(
                            FunctionRegistryError::UnknownFunctionAlternative,
                        )));
                        continue;
                    }
                };
                let function = match alternative.load_function(caching, &mut recorder) {
                    Ok(func) => func,
                    Err(err) => {
                        drop(recorder);
                        debt.fulfill(Err(err));
                        continue;
                    }
                };

                recorder.record(RecordPoint::LoadStart);
                let mut function_context =
                    match function.load(&alternative.domain, alternative.context_size) {
                        Ok(con) => con,
                        Err(err) => {
                            drop(recorder);
                            debt.fulfill(Err(err));
                            continue;
                        }
                    };

                recorder.record(RecordPoint::TransferStart);

                function_context.content.reserve(metadata.input_sets.len());

                for (set_index, (input_set_name, static_set)) in
                    metadata.input_sets.iter().enumerate()
                {
                    // need to add each input set to the content
                    // the input_sets vec can have less entries than the functions defined sets (not all sets need to be used in composition)
                    let transfer_option = static_set
                        .as_ref()
                        .or_else(|| input_sets.get(set_index).and_then(|set| set.as_ref()));
                    let capacity = transfer_option.map_or(0, |set| set.len());
                    function_context.content.push(Some(crate::DataSet {
                        ident: input_set_name.clone(),
                        buffers: Vec::with_capacity(capacity),
                    }));
                    if let Some(transfer_set) = transfer_option {
                        for (source_set_index, source_item_index, source_context) in transfer_set {
                            let transfer_result = memory_domain::transfer_data_item(
                                &mut function_context,
                                source_context,
                                set_index,
                                128,
                                input_set_name.as_str(),
                                source_set_index,
                                source_item_index,
                            );

                            if let Err(transfer_error) = transfer_result {
                                drop(recorder);
                                debt.fulfill(Err(transfer_error));
                                continue 'engine;
                            }
                        }
                    }
                }

                recorder.record(RecordPoint::EngineStart);

                let result = engine_state.run(
                    function.config.clone(),
                    function_context,
                    &metadata.output_sets,
                );

                if let Ok(ref context) = result {
                    log::debug!("content: {:?}", context.content);
                }

                recorder.record(RecordPoint::EngineEnd);
                drop(recorder);

                let results = result.and_then(|context| Ok(WorkDone::Context(context)));
                debt.fulfill(results);
            }
            WorkToDo::Shutdown(_) => {
                debt.fulfill(Ok(WorkDone::Resources(vec![ComputeResource::CPU(core_id)])));
                return;
            }
        }
    }
}

pub fn start_thread<E: EngineLoop>(
    cpu_slot: u8,
    queue: impl EngineWorkQueue + Send + 'static,
) -> () {
    spawn(move || run_thread::<E>(cpu_slot, queue));
}

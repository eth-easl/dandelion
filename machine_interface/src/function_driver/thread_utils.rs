use crate::{
    function_driver::{
        functions::FunctionConfig, ComputeResource, EngineWorkQueue, WorkDone, WorkToDo,
    },
    machine_config::EngineType,
    memory_domain::{self, Context},
    preemption,
};
use core::marker::Send;
use dandelion_commons::{
    records::RecordPoint, DandelionError, DandelionResult, FunctionRegistryError, Priority,
};
use std::sync::{atomic::AtomicBool, Arc};
use std::thread::spawn;

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
    /// Returns the preemption flag if this engine supports preemption, returns None otherwise
    fn get_preempt_flag(&self) -> Option<Arc<AtomicBool>> {
        None
    }
    /// Resume a previously preempted function. Default returns an error (not supported).
    fn resume(&mut self) -> DandelionResult<Context> {
        Err(DandelionError::NotImplemented)
    }
}

/// Run a single function on the given engine and fulfill the debt with the result.
/// Used both for normal execution and for running high-priority work on a temporary engine.
fn run_single_function<E: EngineLoop>(
    engine: &mut Box<E>,
    args: WorkToDo,
    debt: crate::promise::Debt,
    core_id: u8,
) {
    match args {
        WorkToDo::FunctionArguments {
            function_alternatives,
            input_sets,
            metadata,
            caching,
            mut recorder,
        } => {
            let engine_type = engine.get_engine_type();
            let alternative = match function_alternatives
                .into_iter()
                .find(|alt| alt.engine == engine_type)
            {
                Some(alt) => alt,
                None => {
                    drop(recorder);
                    debt.fulfill(Err(DandelionError::FunctionRegistry(
                        FunctionRegistryError::UnknownFunctionAlternative,
                    )));
                    return;
                }
            };
            let function = match alternative.load_function(caching, &mut recorder) {
                Ok(func) => func,
                Err(err) => {
                    drop(recorder);
                    debt.fulfill(Err(err));
                    return;
                }
            };

            recorder.record(RecordPoint::LoadStart);
            let mut function_context =
                match function.load(&alternative.domain, alternative.context_size) {
                    Ok(con) => con,
                    Err(err) => {
                        drop(recorder);
                        debt.fulfill(Err(err));
                        return;
                    }
                };

            recorder.record(RecordPoint::TransferStart);
            function_context.content.reserve(metadata.input_sets.len());

            for (set_index, (input_set_name, static_set)) in
                metadata.input_sets.iter().enumerate()
            {
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
                        if let Err(transfer_error) = memory_domain::transfer_data_item(
                            &mut function_context,
                            source_context,
                            set_index,
                            128,
                            input_set_name.as_str(),
                            source_set_index,
                            source_item_index,
                        ) {
                            drop(recorder);
                            debt.fulfill(Err(transfer_error));
                            return;
                        }
                    }
                }
            }

            recorder.record(RecordPoint::EngineStart);
            let result = engine.run(
                function.config.clone(),
                function_context,
                &metadata.output_sets,
            );

            if let Ok(ref context) = result {
                log::debug!("content: {:?}", context.content);
            }

            recorder.record(RecordPoint::EngineEnd);
            drop(recorder);

            let results = result.map(|context| WorkDone::Context(context));
            debt.fulfill(results);
        }
        WorkToDo::Shutdown(_) => {
            debt.fulfill(Ok(WorkDone::Resources(vec![ComputeResource::CPU(core_id)])));
        }
    }
}

fn run_thread<E: EngineLoop>(core_id: u8, queue: Box<dyn EngineWorkQueue>) {
    // set core affinity
    if !core_affinity::set_for_current(core_affinity::CoreId { id: core_id.into() }) {
        log::error!("core received core id that could not be set");
        return;
    }
    let mut engine_state = E::init(core_id).expect("Failed to initialize thread state");

    // Install signal handler for preemption SIGUSR1.
    preemption::install_signal_handler();
    let preempt_flag = engine_state.get_preempt_flag();

    // Get the thread's pthread_t for signal delivery
    let thread_id = unsafe { libc::pthread_self() };

    // Get the preemption registry from the queue
    let registry = queue.preemption_registry().clone();

    'engine: loop {
        // TODO catch unwind so we can always return an error or shut down gracefully
        let (args, debt, priority) = queue.get_engine_args();
        match args {
            WorkToDo::FunctionArguments {
                function_alternatives,
                input_sets,
                metadata,
                caching,
                mut recorder,
            } => {
                // If running best-effort work and engine supports preemption,
                // register with the preemption registry so it can be interrupted
                let registered = if priority == Priority::BestEffort {
                    if let Some(ref flag) = preempt_flag {
                        registry.register(thread_id, flag.clone());
                        true
                    } else {
                        false
                    }
                } else {
                    false
                };

                let engine_type = engine_state.get_engine_type();
                let alternative = match function_alternatives
                    .into_iter()
                    .find(|alt| alt.engine == engine_type)
                {
                    Some(alt) => alt,
                    None => {
                        if registered {
                            registry.deregister(thread_id);
                        }
                        drop(recorder);
                        debt.fulfill(Err(DandelionError::FunctionRegistry(
                            FunctionRegistryError::UnknownFunctionAlternative,
                        )));
                        continue;
                    }
                };
                let function = match alternative.load_function(caching, &mut recorder) {
                    Ok(func) => func,
                    Err(err) => {
                        if registered {
                            registry.deregister(thread_id);
                        }
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
                            if registered {
                                registry.deregister(thread_id);
                            }
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
                                if registered {
                                    registry.deregister(thread_id);
                                }
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

                // Deregister from preemption registry before fulfilling the debt.
                // Also clear the preempt flag to handle the race condition where a signal
                // arrives right as the task finishes (between vcpu.run() returning and
                // deregistering). The empty signal handler won't cause harm, but we need
                // the flag cleared so it doesn't affect the next task.
                if registered {
                    registry.deregister(thread_id);
                    if let Some(ref flag) = preempt_flag {
                        flag.store(false, std::sync::atomic::Ordering::Release);
                    }
                }

                match result {
                    Err(DandelionError::Preempted) => {
                        // Function was preempted — don't fulfill the debt yet.
                        // The engine_state (KvmLoop) holds the suspended VM state internally.
                        log::debug!("Best-effort function preempted, handling high-priority work");
                        drop(recorder);

                        // Create a fresh engine for the high-priority function.
                        // This gives us a new VM + vCPU while the original's state is preserved.
                        let mut hp_engine = E::init(core_id)
                            .expect("Failed to initialize temporary engine for high-priority work");

                        // Grab the high-priority work from the queue (blocking).
                        // The work is already queued — push() happens before the preemption signal.
                        let (hp_args, hp_debt, _hp_priority) = queue.get_engine_args();

                        // Run the high-priority function on the temporary engine
                        run_single_function(&mut hp_engine, hp_args, hp_debt, core_id);

                        // Drop the temporary engine — its VM + vCPU are freed
                        drop(hp_engine);

                        // Now resume the preempted best-effort function on the original engine.
                        // The original KvmLoop still has VM memory attached and vCPU state preserved.
                        log::debug!("Resuming preempted best-effort function");

                        // Re-register with preemption registry since we're running best-effort again
                        let re_registered = if let Some(ref flag) = preempt_flag {
                            registry.register(thread_id, flag.clone());
                            true
                        } else {
                            false
                        };

                        let resume_result = engine_state.resume();

                        // Deregister after resume completes
                        if re_registered {
                            registry.deregister(thread_id);
                        }

                        match resume_result {
                            Ok(ref context) => {
                                log::debug!("Resumed content: {:?}", context.content);
                            }
                            Err(ref e) => {
                                log::debug!("Resume failed: {:?}", e);
                            }
                        }
                        let resume_results = resume_result.map(|ctx| WorkDone::Context(ctx));
                        debt.fulfill(resume_results);
                    }
                    Ok(ref context) => {
                        log::debug!("content: {:?}", context.content);
                        recorder.record(RecordPoint::EngineEnd);
                        drop(recorder);
                        debt.fulfill(result.map(|ctx| WorkDone::Context(ctx)));
                    }
                    Err(ref _e) => {
                        // Some other error (not preemption)
                        recorder.record(RecordPoint::EngineEnd);
                        drop(recorder);
                        debt.fulfill(result.map(|ctx| WorkDone::Context(ctx)));
                    }
                }
            }
            WorkToDo::Shutdown(_) => {
                debt.fulfill(Ok(WorkDone::Resources(vec![ComputeResource::CPU(core_id)])));
                return;
            }
        }
    }
}

pub fn start_thread<E: EngineLoop>(cpu_slot: u8, queue: Box<dyn EngineWorkQueue + Send>) -> () {
    spawn(move || run_thread::<E>(cpu_slot, queue));
}

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
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
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

/// Register the current engine thread as running best-effort work so a high-priority
/// arrival can interrupt it.
fn register_preemptible_thread(
    registry: &Arc<preemption::PreemptionRegistry>,
    thread_id: libc::pthread_t,
    preempt_flag: &Option<Arc<AtomicBool>>,
) -> bool {
    if let Some(flag) = preempt_flag {
        registry.register(thread_id, flag.clone());
        true
    } else {
        false
    }
}

/// Deregister the current engine thread and clear any stale preemption flag.
///
/// Clearing the flag here avoids carrying an old preemption request into the next task or
/// resume attempt, while still preserving any request that arrives after the next registration.
fn deregister_preemptible_thread(
    registry: &Arc<preemption::PreemptionRegistry>,
    thread_id: libc::pthread_t,
    preempt_flag: &Option<Arc<AtomicBool>>,
    registered: bool,
) {
    if registered {
        registry.deregister(thread_id);
        if let Some(flag) = preempt_flag {
            flag.store(false, Ordering::Release);
        }
    }
}

/// Handle a preempted best-effort function by repeatedly:
/// 1. running the next high-priority task on a temporary engine,
/// 2. re-registering the original engine thread for preemption,
/// 3. resuming the best-effort function.
///
/// If the resume itself is preempted again, the loop repeats until the best-effort function
/// finally completes or returns a non-preemption error.
fn finish_preempted_best_effort<E: EngineLoop>(
    engine_state: &mut Box<E>,
    queue: &dyn EngineWorkQueue,
    debt: crate::promise::Debt,
    core_id: u8,
    registry: &Arc<preemption::PreemptionRegistry>,
    thread_id: libc::pthread_t,
    preempt_flag: &Option<Arc<AtomicBool>>,
) {
    loop {
        log::debug!("Best-effort function preempted, handling high-priority work");

        // Create a fresh engine for the high-priority function.
        // This gives us a new VM + vCPU while the original's state is preserved.
        let mut hp_engine =
            E::init(core_id).expect("Failed to initialize temporary engine for high-priority work");

        // Grab the high-priority work from the queue (blocking).
        // The work is already queued — push() happens before the preemption signal.
        let (hp_args, hp_debt, hp_priority) = queue.get_engine_args();
        debug_assert_eq!(
            hp_priority,
            Priority::High,
            "Expected high-priority work while servicing a preempted best-effort task"
        );

        // Run the high-priority function on the temporary engine.
        run_single_function(&mut hp_engine, hp_args, hp_debt, core_id);

        // Drop the temporary engine — its VM + vCPU are freed.
        drop(hp_engine);

        // Now resume the preempted best-effort function on the original engine.
        // The original engine still has its suspended state preserved.
        log::debug!("Resuming preempted best-effort function");

        let resume_registered = register_preemptible_thread(registry, thread_id, preempt_flag);
        let resume_result = engine_state.resume();
        deregister_preemptible_thread(
            registry,
            thread_id,
            preempt_flag,
            resume_registered,
        );

        match resume_result {
            Err(DandelionError::Preempted) => {
                log::debug!("Best-effort function preempted again during resume");
                continue;
            }
            Ok(ref context) => {
                log::debug!("Resumed content: {:?}", context.content);
            }
            Err(ref e) => {
                log::debug!("Resume failed: {:?}", e);
            }
        }

        let resume_results = resume_result.map(|ctx| WorkDone::Context(ctx));
        debt.fulfill(resume_results);
        return;
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
                // register with the preemption registry so it can be interrupted.
                let registered = if priority == Priority::BestEffort {
                    register_preemptible_thread(&registry, thread_id, &preempt_flag)
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
                        deregister_preemptible_thread(
                            &registry,
                            thread_id,
                            &preempt_flag,
                            registered,
                        );
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
                        deregister_preemptible_thread(
                            &registry,
                            thread_id,
                            &preempt_flag,
                            registered,
                        );
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
                            deregister_preemptible_thread(
                                &registry,
                                thread_id,
                                &preempt_flag,
                                registered,
                            );
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
                                deregister_preemptible_thread(
                                    &registry,
                                    thread_id,
                                    &preempt_flag,
                                    registered,
                                );
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
                deregister_preemptible_thread(&registry, thread_id, &preempt_flag, registered);

                match result {
                    Err(DandelionError::Preempted) => {
                        // Function was preempted — don't fulfill the debt yet.
                        // The engine_state holds the suspended VM state internally.
                        drop(recorder);
                        finish_preempted_best_effort(
                            &mut engine_state,
                            queue.as_ref(),
                            debt,
                            core_id,
                            &registry,
                            thread_id,
                            &preempt_flag,
                        );
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        machine_config::EngineType,
        memory_domain::{malloc::MallocMemoryDomain, MemoryDomain, MemoryResource},
        promise::PromiseBuffer,
    };
    use std::{
        cell::RefCell,
        collections::VecDeque,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Mutex,
        },
    };

    fn test_engine_type() -> EngineType {
        #[cfg(feature = "kvm")]
        {
            return EngineType::Kvm;
        }
        #[cfg(all(not(feature = "kvm"), feature = "mmu"))]
        {
            return EngineType::Process;
        }
        #[cfg(all(not(feature = "kvm"), not(feature = "mmu"), feature = "cheri"))]
        {
            return EngineType::Cheri;
        }
        #[cfg(all(
            not(feature = "kvm"),
            not(feature = "mmu"),
            not(feature = "cheri"),
            feature = "reqwest_io"
        ))]
        {
            return EngineType::Reqwest;
        }
        #[cfg(not(any(
            feature = "kvm",
            feature = "mmu",
            feature = "cheri",
            feature = "reqwest_io"
        )))]
        panic!("thread_utils tests require at least one engine feature");
    }

    struct ScriptedState {
        resume_results: Mutex<VecDeque<DandelionResult<Context>>>,
        resume_calls: AtomicUsize,
    }

    thread_local! {
        static TEST_STATE: RefCell<Option<Arc<ScriptedState>>> = RefCell::new(None);
    }

    struct TestStateGuard;

    impl Drop for TestStateGuard {
        fn drop(&mut self) {
            TEST_STATE.with(|slot| {
                slot.replace(None);
            });
        }
    }

    fn install_test_state(
        resume_results: Vec<DandelionResult<Context>>,
    ) -> (Arc<ScriptedState>, TestStateGuard) {
        let state = Arc::new(ScriptedState {
            resume_results: Mutex::new(VecDeque::from(resume_results)),
            resume_calls: AtomicUsize::new(0),
        });

        TEST_STATE.with(|slot| {
            assert!(
                slot.replace(Some(state.clone())).is_none(),
                "test state already installed"
            );
        });

        (state, TestStateGuard)
    }

    struct ScriptedEngine {
        state: Arc<ScriptedState>,
        preempt_flag: Arc<AtomicBool>,
    }

    impl EngineLoop for ScriptedEngine {
        fn init(_core_id: u8) -> DandelionResult<Box<Self>> {
            let state = TEST_STATE.with(|slot| {
                slot.borrow()
                    .as_ref()
                    .cloned()
                    .expect("scripted engine test state should be installed")
            });
            Ok(Box::new(Self {
                state,
                preempt_flag: Arc::new(AtomicBool::new(false)),
            }))
        }

        fn run(
            &mut self,
            _config: FunctionConfig,
            _context: Context,
            _output_sets: &Vec<String>,
        ) -> DandelionResult<Context> {
            panic!("ScriptedEngine::run should not be called in this test")
        }

        fn get_engine_type(&self) -> EngineType {
            test_engine_type()
        }

        fn get_preempt_flag(&self) -> Option<Arc<AtomicBool>> {
            Some(self.preempt_flag.clone())
        }

        fn resume(&mut self) -> DandelionResult<Context> {
            self.state.resume_calls.fetch_add(1, Ordering::SeqCst);
            self.state
                .resume_results
                .lock()
                .expect("resume_results lock poisoned")
                .pop_front()
                .expect("No scripted resume result available")
        }
    }

    struct ScriptedQueue {
        items: Mutex<VecDeque<(WorkToDo, crate::promise::Debt, Priority)>>,
        promise_buffer: PromiseBuffer,
        preemption_registry: Arc<preemption::PreemptionRegistry>,
    }

    impl ScriptedQueue {
        fn new() -> Self {
            Self {
                items: Mutex::new(VecDeque::new()),
                promise_buffer: PromiseBuffer::init(8),
                preemption_registry: Arc::new(preemption::PreemptionRegistry::new()),
            }
        }

        fn enqueue(
            &self,
            work: WorkToDo,
            priority: Priority,
        ) -> crate::promise::Promise {
            let (promise, debt) = self.promise_buffer.get_promise().unwrap();
            self.items
                .lock()
                .expect("ScriptedQueue items lock poisoned")
                .push_back((work, debt, priority));
            promise
        }
    }

    impl EngineWorkQueue for ScriptedQueue {
        fn get_engine_args(&self) -> (WorkToDo, crate::promise::Debt, Priority) {
            self.items
                .lock()
                .expect("ScriptedQueue items lock poisoned")
                .pop_front()
                .expect("ScriptedQueue expected queued work")
        }

        fn try_get_engine_args(&self) -> Option<(WorkToDo, crate::promise::Debt, Priority)> {
            self.items
                .lock()
                .expect("ScriptedQueue items lock poisoned")
                .pop_front()
        }

        fn preemption_registry(&self) -> &Arc<preemption::PreemptionRegistry> {
            &self.preemption_registry
        }
    }

    fn make_context() -> Context {
        let domain = MallocMemoryDomain::init(MemoryResource::None)
            .expect("Should initialize malloc memory domain");
        domain
            .acquire_context(8)
            .expect("Should allocate scripted test context")
    }

    #[test_log::test]
    fn handles_repeated_preemption_while_resuming_best_effort_work() {
        let core_id = 7;
        let queue = Box::new(ScriptedQueue::new());
        let hp_promise_1 = queue.enqueue(WorkToDo::Shutdown(test_engine_type()), Priority::High);
        let hp_promise_2 = queue.enqueue(WorkToDo::Shutdown(test_engine_type()), Priority::High);

        let promise_buffer = PromiseBuffer::init(1);
        let (be_promise, be_debt) = promise_buffer.get_promise().unwrap();

        let (state, _guard) = install_test_state(vec![
            Err(DandelionError::Preempted),
            Ok(make_context()),
        ]);

        let mut engine_state =
            ScriptedEngine::init(core_id).expect("Should initialize scripted engine");
        let preempt_flag = engine_state
            .get_preempt_flag()
            .expect("Scripted engine should expose a preempt flag");
        let thread_id = unsafe { libc::pthread_self() };
        let registry = queue.preemption_registry().clone();

        let preempt_flag_option = Some(preempt_flag.clone());
        finish_preempted_best_effort(
            &mut engine_state,
            queue.as_ref(),
            be_debt,
            core_id,
            &registry,
            thread_id,
            &preempt_flag_option,
        );

        assert_eq!(2, state.resume_calls.load(Ordering::SeqCst));
        assert!(
            !registry.preempt_one(),
            "engine thread should be deregistered after resume handling completes"
        );
        assert!(
            !preempt_flag.load(Ordering::Acquire),
            "stale preemption flags should be cleared after resume handling"
        );

        match futures::executor::block_on(hp_promise_1).expect("first HP promise should resolve") {
            WorkDone::Resources(resources) => {
                assert_eq!(1, resources.len());
                assert!(matches!(resources[0], ComputeResource::CPU(id) if id == core_id));
            }
            _ => panic!("first HP work should return resources for shutdown placeholder"),
        }

        match futures::executor::block_on(hp_promise_2).expect("second HP promise should resolve") {
            WorkDone::Resources(resources) => {
                assert_eq!(1, resources.len());
                assert!(matches!(resources[0], ComputeResource::CPU(id) if id == core_id));
            }
            _ => panic!("second HP work should return resources for shutdown placeholder"),
        }

        match futures::executor::block_on(be_promise).expect("BE promise should resolve") {
            WorkDone::Context(_) => {}
            _ => panic!("best-effort work should finish with a context after repeated resume"),
        }
    }
}

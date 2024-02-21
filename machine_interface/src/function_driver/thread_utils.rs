use crate::{
    function_driver::{EngineArguments, FunctionConfig, WorkQueue},
    memory_domain::Context,
    promise::Promise,
};
use core::marker::Send;
use dandelion_commons::{records::RecordPoint, DandelionError, DandelionResult};
use std::{
    marker::PhantomData,
    thread::{spawn, JoinHandle},
};

extern crate alloc;

pub trait EngineLoop {
    fn init(core_id: u8) -> DandelionResult<Box<Self>>;
    fn run(
        &mut self,
        config: FunctionConfig,
        context: Context,
        output_sets: Vec<String>,
    ) -> DandelionResult<Context>;
}

/// Need the command receiver to be blocking on the worker thread,
/// that is easier if with this type, as it does not need async to block
type CommandSender = std::sync::mpsc::Sender<()>;
type CommandReceiver = std::sync::mpsc::Receiver<()>;

pub struct ThreadController<E: EngineLoop> {
    _loop_marker: PhantomData<E>,
    /// command sender that works accross threads
    command_sender: CommandSender,
    thread_handle: Option<JoinHandle<()>>,
}

fn thread_abort() -> () {}

fn run_thread<E: EngineLoop>(
    core_id: u8,
    command_receiver: CommandReceiver,
    queue: Box<dyn WorkQueue>,
) {
    // set core affinity
    if !core_affinity::set_for_current(core_affinity::CoreId { id: core_id.into() }) {
        log::error!("core received core id that could not be set");
        return;
    }
    let mut engine_state = E::init(core_id).expect("Failed to initialize thread state");
    loop {
        if command_receiver.try_recv().is_ok() {
            return;
        }
        let (promise, debt) = Promise::new(thread_abort);
        let EngineArguments {
            config,
            context,
            output_sets,
            mut recorder,
        } = queue.get_engine_args(promise);
        if let Err(err) = recorder.record(RecordPoint::EngineStart) {
            debt.fulfill(Box::new(Err(err)));
            continue;
        }
        let result = engine_state.run(config, context, output_sets);
        if result.is_ok() {
            if let Err(err) = recorder.record(RecordPoint::EngineEnd) {
                debt.fulfill(Box::new(Err(err)));
                continue;
            }
        }
        let results = Box::new(result.and_then(|context| Ok((context, recorder))));
        debt.fulfill(results);
    }
}

impl<E: EngineLoop> ThreadController<E> {
    pub fn new(cpu_slot: u8, queue: Box<dyn WorkQueue + Send>) -> Self {
        let (command_sender, command_receiver) = std::sync::mpsc::channel();
        let thread_handle = spawn(move || run_thread::<E>(cpu_slot, command_receiver, queue));
        return ThreadController {
            command_sender,
            thread_handle: Some(thread_handle),
            _loop_marker: PhantomData,
        };
    }

    // TODO remove if better way to abort is ready
    fn send_command(&mut self) -> DandelionResult<()> {
        match self.command_sender.send(()) {
            Err(_) => Err(DandelionError::EngineError),
            Ok(_) => Ok(()),
        }
    }
}

impl<E: EngineLoop> Drop for ThreadController<E> {
    fn drop(&mut self) {
        // TODO: actually interrup the thread and handle take down
        // if let Some(handle) = self.thread_handle.take() {
        //     // drop channel
        //     let _res = self.command_sender.send(());
        //     handle
        //         .join()
        //         .expect("Join thread handle in Thread controller should not panic");
        // }
    }
}

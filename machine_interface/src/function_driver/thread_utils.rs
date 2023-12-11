use core::marker::Send;
use dandelion_commons::{
    records::{RecordPoint, Recorder},
    DandelionError, DandelionResult,
};
use log::info;
use std::thread::{spawn, JoinHandle};

pub trait ThreadState {
    fn init(core_id: u8) -> DandelionResult<Box<Self>>;
}

pub struct DefaultState {}
impl ThreadState for DefaultState {
    fn init(_core_id: u8) -> DandelionResult<Box<Self>> {
        return Ok(Box::new(DefaultState {}));
    }
}

pub trait ThreadPayload {
    type State: ThreadState;
    fn run(self, state: &mut Self::State) -> DandelionResult<()>;
}

pub enum ThreadCommand<P: ThreadPayload + Send> {
    Abort,
    Run(Recorder, P),
}

type CommandSender<P> = std::sync::mpsc::Sender<ThreadCommand<P>>;
type CommandReceiver<P> = std::sync::mpsc::Receiver<ThreadCommand<P>>;
type ResultSender = futures::channel::mpsc::Sender<DandelionResult<()>>;
type ResultReceiver = futures::channel::mpsc::Receiver<DandelionResult<()>>;

pub struct ThreadController<P: ThreadPayload + Send> {
    /// command sender that works accross threads
    /// TODO: could this also be futures channel?
    command_sender: CommandSender<P>,
    /// receiver for worker thread result
    result_receiver: ResultReceiver,
    thread_handle: Option<JoinHandle<()>>,
}

/// Try to send a result on the channel
/// returns true on success, false on irecoverable error
fn try_send(result: DandelionResult<()>, sender: &mut ResultSender) -> bool {
    loop {
        match sender.try_send(result.clone()) {
            Err(err) if err.is_full() => (),
            Err(_) => return false,
            Ok(()) => return true,
        }
    }
}

fn run_thread<P: ThreadPayload + Send>(
    core_id: u8,
    command_receiver: CommandReceiver<P>,
    mut result_sender: ResultSender,
) {
    // set core affinity
    if !core_affinity::set_for_current(core_affinity::CoreId { id: core_id.into() }) {
        try_send(Err(DandelionError::EngineResourceError), &mut result_sender);
        return;
    }
    if let Ok(mut thread_state) = P::State::init(core_id) {
        info!("engine running on core {}", core_id);
        // TODO would be nice to check, but would require to block on creation path
        // try_send(Ok(()), &mut result_sender);
        for command in command_receiver.iter() {
            let (mut recorder, payload) = match command {
                ThreadCommand::Abort => return,
                ThreadCommand::Run(rec, pay) => (rec, pay),
            };
            let result = payload.run(&mut thread_state);
            // ignore recorder errors for now, as we detect on record of engine return
            let _ = recorder.record(RecordPoint::EngineEnd);
            if !try_send(result, &mut result_sender) {
                return;
            }
        }
    } else {
        try_send(Err(DandelionError::EngineError), &mut result_sender);
    }
    return;
}

impl<P: ThreadPayload + Send + 'static> ThreadController<P> {
    pub fn new(cpu_slot: u8) -> Self {
        let (command_sender, command_receiver) = std::sync::mpsc::channel();
        let (result_sender, result_receiver) = futures::channel::mpsc::channel(0);
        let thread_handle = spawn(move || run_thread(cpu_slot, command_receiver, result_sender));
        return ThreadController {
            command_sender,
            result_receiver,
            thread_handle: Some(thread_handle),
        };
    }

    pub fn send_command(&mut self, command: ThreadCommand<P>) -> DandelionResult<()> {
        match self.command_sender.send(command) {
            Err(_) => Err(DandelionError::EngineError),
            Ok(_) => Ok(()),
        }
    }

    pub fn poll_next(
        &mut self,
        ctx: &mut futures::task::Context,
    ) -> futures::task::Poll<Option<DandelionResult<()>>> {
        use futures::Stream;
        return core::pin::Pin::new(&mut self.result_receiver)
            .as_mut()
            .poll_next(ctx);
    }
}

impl<P: ThreadPayload + Send> Drop for ThreadController<P> {
    fn drop(&mut self) {
        if let Some(handle) = self.thread_handle.take() {
            // drop channel
            let _res = self.command_sender.send(ThreadCommand::Abort);
            handle
                .join()
                .expect("Join thread handle in Thread controller should not panic");
        }
    }
}

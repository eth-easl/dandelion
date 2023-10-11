use dandelion_commons::{records::Recorder, DandelionError, DandelionResult};
use futures::{channel::oneshot, lock::Mutex};
use machine_interface::{
    function_driver::{Engine, FunctionConfig},
    memory_domain::Context,
};
use std::collections::VecDeque;

pub struct EngineQueue {
    pub internals: Mutex<(
        Vec<Box<dyn Engine>>,
        VecDeque<oneshot::Sender<Box<dyn Engine>>>,
    )>,
}

impl EngineQueue {
    pub async fn perform_single_run(
        &self,
        config: &FunctionConfig,
        context: Context,
        output_sets: &Vec<String>,
        recorder: Recorder,
    ) -> (DandelionResult<()>, Context) {
        let engine_rec = match self.get_engine().await {
            Ok(rec) => rec,
            Err(err) => return (Err(err), context),
        };
        let mut engine = match engine_rec.await {
            Ok(eng) => eng,
            Err(_) => return (Err(DandelionError::DispatcherChannelError), context),
        };
        let (engine_result, out_context) = engine.run(config, context, output_sets, recorder).await;
        let yield_result = self.yield_engine(engine).await;
        let end_result = match (engine_result, yield_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(err), _) | (Ok(()), Err(err)) => Err(err),
        };
        return (end_result, out_context);
    }

    async fn get_engine(&self) -> DandelionResult<oneshot::Receiver<Box<dyn Engine>>> {
        let mut mux_guard = self.internals.lock().await;
        // create new lock
        let (sender, receiver) = oneshot::channel();
        if let Some(engine) = mux_guard.0.pop() {
            let sender_result: Result<(), Box<dyn Engine>> = sender.send(engine);
            if let Err(engine) = sender_result {
                mux_guard.0.push(engine);
                return Err(DandelionError::DispatcherChannelError);
            }
        } else {
            mux_guard.1.push_back(sender);
        }
        return Ok(receiver);
    }
    async fn yield_engine(&self, engine: Box<dyn Engine>) -> DandelionResult<()> {
        let mut mux_guard = self.internals.lock().await;
        if let Some(sender) = mux_guard.1.pop_front() {
            match sender.send(engine) {
                Ok(()) => (),
                Err(_) => return Err(DandelionError::DispatcherChannelError),
            }
        } else {
            mux_guard.0.push(engine);
        }
        Ok(())
    }
}

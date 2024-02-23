use crate::{
    function_driver::{
        ComputeResource, EngineArguments, FunctionArguments, FunctionConfig, WorkQueue,
    },
    memory_domain::Context,
};
use core::marker::Send;
use dandelion_commons::{records::RecordPoint, DandelionResult};
use std::thread::spawn;

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

fn run_thread<E: EngineLoop>(core_id: u8, queue: Box<dyn WorkQueue>) {
    // set core affinity
    if !core_affinity::set_for_current(core_affinity::CoreId { id: core_id.into() }) {
        log::error!("core received core id that could not be set");
        return;
    }
    let mut engine_state = E::init(core_id).expect("Failed to initialize thread state");
    loop {
        // TODO catch unwind so we can always return an error or shut down gracefully
        let (args, debt) = queue.get_engine_args();
        match args {
            EngineArguments::FunctionArguments(func_args) => {
                let FunctionArguments {
                    config,
                    context,
                    output_sets,
                    mut recorder,
                } = func_args;
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
            EngineArguments::Shutdown(resource_returner) => {
                resource_returner(vec![ComputeResource::CPU(core_id)]);
                return;
            }
        }
    }
}

pub fn start_thread<E: EngineLoop>(cpu_slot: u8, queue: Box<dyn WorkQueue + Send>) -> () {
    spawn(move || run_thread::<E>(cpu_slot, queue));
}

use crate::{
    function_driver::{
        ComputeResource, EngineArguments, FunctionArguments, FunctionConfig, TransferArguments,
        WorkQueue,
    },
    memory_domain::{self, Context},
};
use core::marker::Send;
use dandelion_commons::{records::RecordPoint, DandelionResult};
use std::thread::spawn;

extern crate alloc;

pub trait EngineLoop {
    fn init(resource: ComputeResource) -> DandelionResult<Box<Self>>;
    fn run(
        &mut self,
        config: FunctionConfig,
        context: Context,
        output_sets: std::sync::Arc<Vec<String>>,
    ) -> DandelionResult<Context>;
}

fn run_thread<E: EngineLoop>(core_id: u8, queue: Box<dyn WorkQueue>) {
    // set core affinity
    if !core_affinity::set_for_current(core_affinity::CoreId { id: core_id.into() }) {
        log::error!("core received core id that could not be set");
        return;
    }
    let mut engine_state =
        E::init(ComputeResource::CPU(core_id)).expect("Failed to initialize thread state");
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
            EngineArguments::TransferArguments(transfer_args) => {
                let TransferArguments {
                    source,
                    mut destination,
                    destination_set_index,
                    destination_allignment,
                    destination_item_index,
                    destination_set_name,
                    source_set_index,
                    source_item_index,
                    recorder,
                } = transfer_args;
                let transfer_result = memory_domain::transfer_data_item(
                    &mut destination,
                    &source,
                    destination_set_index,
                    destination_allignment,
                    destination_item_index,
                    destination_set_name.as_str(),
                    source_set_index,
                    source_item_index,
                )
                .and(Ok((destination, recorder)));
                debt.fulfill(Box::new(transfer_result));
                continue;
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

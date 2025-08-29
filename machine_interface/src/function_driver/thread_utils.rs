use crate::{
    function_driver::{ComputeResource, FunctionConfig, WorkDone, WorkQueue, WorkToDo},
    memory_domain::{self, Context},
};
use core::marker::Send;
use dandelion_commons::{
    records::{RecordPoint, Recorder},
    DandelionResult,
};
use std::thread::spawn;

#[cfg(feature = "auto_batching")]
use crate::function_driver::BatchInfo;

extern crate alloc;

pub trait EngineLoop {
    fn init(resource: ComputeResource) -> DandelionResult<Box<Self>>;
    fn run(
        &mut self,
        config: FunctionConfig,
        context: Context,
        output_sets: std::sync::Arc<Vec<String>>,
        recorder: Recorder,
    ) -> DandelionResult<Context>;
}

pub fn run_thread<E: EngineLoop>(
    initialisation_resource: ComputeResource,
    queue: Box<dyn WorkQueue>,
) {
    // get CPU from resource
    let core_id = match initialisation_resource {
        ComputeResource::CPU(id) => id,
        ComputeResource::GPU(id, _, _) => id,
    };
    // set core affinity
    if !core_affinity::set_for_current(core_affinity::CoreId { id: core_id.into() }) {
        log::error!("core received core id that could not be set");
        return;
    }
    let mut engine_state =
        E::init(initialisation_resource).expect("Failed to initialize thread state");
    loop {
        // TODO catch unwind so we can always return an error or shut down gracefully
        let (args, debt) = queue.get_engine_args();
        match args {
            WorkToDo::FunctionArguments {
                config,
                context,
                output_sets,
                mut recorder,
            } => {
                recorder.record(RecordPoint::EngineStart);

                let subrecorder = recorder.get_sub_recorder();
                let result = engine_state.run(config, context, output_sets, subrecorder);

                recorder.record(RecordPoint::EngineEnd);
                drop(recorder);

                let results = result.and_then(|context| Ok(WorkDone::Context(context)));
                debt.fulfill(results);
            }
            WorkToDo::TransferArguments {
                source,
                mut destination,
                destination_set_index,
                destination_allignment,
                destination_item_index,
                destination_set_name,
                source_set_index,
                source_item_index,
                mut recorder,
            } => {
                recorder.record(RecordPoint::TransferStart);

                let transfer_result = memory_domain::transfer_data_item(
                    &mut destination,
                    source,
                    destination_set_index,
                    destination_allignment,
                    destination_item_index,
                    destination_set_name.as_str(),
                    source_set_index,
                    source_item_index,
                );

                recorder.record(RecordPoint::TransferEnd);
                drop(recorder);

                let transfer_return = transfer_result.and(Ok(WorkDone::Context(destination)));
                debt.fulfill(transfer_return);
                continue;
            }
            WorkToDo::ParsingArguments {
                driver,
                path,
                static_domain,
                mut recorder,
            } => {
                recorder.record(RecordPoint::ParsingStart);
                let function_result = driver.parse_function(path, &static_domain);
                recorder.record(RecordPoint::ParsingEnd);
                drop(recorder);
                match function_result {
                    Ok(function) => debt.fulfill(Ok(WorkDone::Function(function))),
                    Err(err) => debt.fulfill(Err(err)),
                }
                continue;
            }
            WorkToDo::LoadingArguments {
                function,
                domain,
                ctx_size,
                mut recorder,
            } => {
                recorder.record(RecordPoint::LoadStart);
                let load_result = function.load(&domain, ctx_size);
                recorder.record(RecordPoint::LoadEnd);
                drop(recorder);
                match load_result {
                    Ok(context) => debt.fulfill(Ok(WorkDone::Context(context))),
                    Err(err) => debt.fulfill(Err(err)),
                }
                continue;
            }
            #[cfg(feature = "auto_batching")]
            WorkToDo::BatchAtom {
                function_id,
                inputs,
                recorder,
                inputs_vec,
                children_debts,
            } => {
                debt.fulfill(Ok(WorkDone::SharedContext(BatchInfo {
                    batch_pos: 0,
                    inputs_vec,
                    context_arc: None,
                    children_debts,
                })));
                continue;
            }
            WorkToDo::Shutdown() => {
                debt.fulfill(Ok(WorkDone::Resources(vec![ComputeResource::CPU(core_id)])));
                return;
            }
        }
    }
}

pub fn start_thread<E: EngineLoop>(cpu_slot: u8, queue: Box<dyn WorkQueue + Send>) -> () {
    spawn(move || run_thread::<E>(ComputeResource::CPU(cpu_slot), queue));
}

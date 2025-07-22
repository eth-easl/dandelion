use crate::{
    function_driver::{
        ComputeResource, Driver, Function, FunctionConfig, SystemFunction, WorkDone, WorkQueue,
        WorkToDo,
    },
    memory_domain::transfer_data_item,
    promise::Debt,
};
use core_affinity::set_for_current;
use dandelion_commons::{records::RecordPoint, DandelionError, DandelionResult};
use futures::StreamExt;
use std::sync::Arc;
use tokio::{runtime::Builder, sync::RwLock};

async fn engine_loop(queue: Box<dyn WorkQueue + Send>) -> Debt {
    log::debug!("Reqwest engine Init");
    #[cfg(feature = "reqwest_io")]
    let http_client = reqwest::Client::new();

    // TODO FIX! This should not be necessary!
    let mut queue_ref = Box::leak(queue);
    let mut tuple;
    let worker_lock = Arc::new(RwLock::new(()));
    loop {
        (tuple, queue_ref) = queue_ref.into_future().await;
        let (args, debt) = if let Some((tuple_args, tuple_debt)) = tuple {
            (tuple_args, tuple_debt)
        } else {
            panic!("Workqueue poll next returned none")
        };
        match args {
            WorkToDo::FunctionArguments {
                config,
                context,
                output_sets,
                mut recorder,
            } => {
                recorder.record(RecordPoint::EngineStart);

                // let result = engine_state.run(config, context, output_sets);
                log::debug!("Reqwest engine running function");
                let function = match config {
                    FunctionConfig::SysConfig(sys_func) => sys_func,
                    _ => {
                        drop(recorder);
                        debt.fulfill(Err(DandelionError::ConfigMissmatch));
                        continue;
                    }
                };
                log::trace!("Handling system function for type: {}", function);
                match function {
                    #[cfg(feature = "reqwest_io")]
                    SystemFunction::HTTP => {
                        tokio::spawn(super::reqwest::run_http_request(
                            context,
                            http_client.clone(),
                            output_sets,
                            debt,
                            recorder,
                        ));
                    }
                    SystemFunction::MEMCACHED => {
                        tokio::spawn(super::memcached::run_memcached_request(
                            context,
                            output_sets,
                            debt,
                            recorder,
                        ));
                    }
                    SystemFunction::FileLoad => {
                        super::file_system::file_load(context, debt, recorder);
                    }
                    SystemFunction::FileMedatada => {
                        super::file_system::metadata_load(context, debt, recorder);
                    }
                    #[allow(unreachable_patterns)]
                    _ => {
                        drop(recorder);
                        debt.fulfill(Err(DandelionError::MalformedConfig));
                    }
                };
                continue;
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

                let transfer_result = transfer_data_item(
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

                let transfer_return = transfer_result.and(Ok(WorkDone::Context(destination)));
                drop(recorder);
                debt.fulfill(transfer_return);
                continue;
            }
            WorkToDo::ParsingArguments {
                driver,
                binary_data,
            } => {
                let function_result = driver.parse_function(&binary_data);
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
            WorkToDo::Shutdown() => {
                let _ = worker_lock.write_owned().await;
                return debt;
            }
        }
    }
}

fn outer_engine(core_id: u8, queue: Box<dyn WorkQueue + Send>) {
    // set core affinity
    if !core_affinity::set_for_current(core_affinity::CoreId { id: core_id.into() }) {
        log::error!("core received core id that could not be set");
        return;
    }
    let runtime = Builder::new_multi_thread()
        .on_thread_start(move || {
            if !set_for_current(core_affinity::CoreId { id: core_id.into() }) {
                return;
            }
        })
        .worker_threads(1)
        .enable_all()
        .build()
        .or(Err(DandelionError::EngineError))
        .unwrap();
    let debt = runtime.block_on(engine_loop(queue));
    drop(runtime);
    debt.fulfill(Ok(WorkDone::Resources(vec![ComputeResource::CPU(core_id)])));
}

pub struct SystemDriver {}

impl Driver for SystemDriver {
    fn start_engine(
        &self,
        resource: ComputeResource,
        queue: Box<dyn WorkQueue + Send>,
    ) -> DandelionResult<()> {
        log::debug!("Starting hyper engine");
        let core_id = match resource {
            ComputeResource::CPU(core) => core,
            _ => return Err(DandelionError::EngineResourceError),
        };
        // check that core is available
        let available_cores = match core_affinity::get_core_ids() {
            None => return Err(DandelionError::EngineResourceError),
            Some(cores) => cores,
        };
        if !available_cores
            .iter()
            .find(|x| x.id == usize::from(core_id))
            .is_some()
        {
            return Err(DandelionError::EngineResourceError);
        }
        std::thread::spawn(move || outer_engine(core_id, queue));
        return Ok(());
    }

    fn parse_function(&self, _binary_data: &Vec<u8>) -> DandelionResult<Function> {
        return Err(DandelionError::CalledSystemFuncParser);
    }
}

use crate::{
    function_driver::{
        ComputeResource, Driver, Function, FunctionConfig, ReqwestWork, SystemFunction, WorkDone, WorkQueue, WorkToDo
    },
    promise::Debt,
};
use core_affinity::set_for_current;
use dandelion_commons::{
    records::RecordPoint,
    DandelionError, DandelionResult,
};
use futures::StreamExt;
use tokio::runtime::Builder;
use core::str::FromStr;
use std::sync::Arc;
use reqwest::Client;
use super::{distributed::IntermediateDataPool, http::http_run};
use super::distributed::{dandelion_send, dandelion_recv};
use crate::memory_domain;

async fn engine_loop(queue: Box<dyn WorkQueue + Send>, data_pool: Arc<IntermediateDataPool>) -> Debt {
    log::debug!("Reqwest engine Init");
    let client = Client::new();
    // TODO FIX! This should not be necessary!
    let mut queue_ref = Box::leak(queue);
    let mut tuple;
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
                recorder,
            } => {
                // let result = engine_state.run(config, context, output_sets);
                log::debug!("Reqwest engine running function");
                let function = match config {
                    FunctionConfig::SysConfig(sys_func) => sys_func,
                    _ => {
                        debt.fulfill(Box::new(Err(DandelionError::ConfigMissmatch)));
                        continue;
                    }
                };
                match function {
                    SystemFunction::HTTP => {
                        tokio::spawn(http_run(
                            context,
                            client.clone(),
                            output_sets,
                            debt,
                            recorder,
                        ));
                    }
                    SystemFunction::SEND => {
                        tokio::spawn(dandelion_send(
                            context,
                            client.clone(),
                            debt,
                            recorder,
                        ));
                    }
                    SystemFunction::RECV => {
                        tokio::spawn(dandelion_recv(
                            context,
                            output_sets,
                            data_pool.clone(),
                            debt,
                            recorder,
                        ));
                    }
                    #[allow(unreachable_patterns)]
                    _ => {
                        debt.fulfill(Box::new(Err(DandelionError::MalformedConfig)));
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
                match recorder.record(RecordPoint::TransferStart) {
                    Ok(()) => (),
                    Err(err) => {
                        debt.fulfill(Box::new(Err(err)));
                        continue;
                    }
                }
                let transfer_result = memory_domain::transfer_data_item(
                    &mut destination,
                    &source,
                    destination_set_index,
                    destination_allignment,
                    destination_item_index,
                    destination_set_name.as_str(),
                    source_set_index,
                    source_item_index,
                );
                match recorder.record(RecordPoint::TransferEnd) {
                    Ok(()) => (),
                    Err(err) => {
                        debt.fulfill(Box::new(Err(err)));
                        continue;
                    }
                }
                let transfer_return = transfer_result.and(Ok(WorkDone::Context(destination)));
                debt.fulfill(Box::new(transfer_return));
                continue;
            }
            WorkToDo::ParsingArguments {
                driver,
                path,
                static_domain,
                mut recorder,
            } => {
                recorder.record(RecordPoint::ParsingStart).unwrap();
                let function_result = driver.parse_function(path, static_domain);
                recorder.record(RecordPoint::ParsingEnd).unwrap();
                match function_result {
                    Ok(function) => debt.fulfill(Box::new(Ok(WorkDone::Function(function)))),
                    Err(err) => debt.fulfill(Box::new(Err(err))),
                }
                continue;
            }
            WorkToDo::Reqwest { 
                work,
                mut recorder,
            } => {
                match work {
                    ReqwestWorkToDo::PostData {
                        id,
                        content,
                        binary,
                    } => {
                        recorder.record(RecordPoint::PostDataStart).unwrap();
                        let post_data_result = data_pool.post(id, content, binary).await;
                        recorder.record(RecordPoint::PostDataEnd).unwrap();
                        match post_data_result {
                            Ok(_) => debt.fulfill(Box::new(Ok(WorkDone::PostData))),
                            Err(err) => debt.fulfill(Box::new(Err(err))),
                        }
                        continue;
                    }
                }
            }
            WorkToDo::Shutdown() => {
                return debt;
            }
        }
    }
}

fn outer_engine(core_id: u8, queue: Box<dyn WorkQueue + Send>, data_pool: Arc<IntermediateDataPool>) {
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
    let debt = runtime.block_on(engine_loop(queue, data_pool.clone()));
    drop(runtime);
    debt.fulfill(Box::new(Ok(WorkDone::Resources(vec![
        ComputeResource::CPU(core_id),
    ]))));
}

pub struct ReqwestDriver {
    data_pool: Arc<IntermediateDataPool>
}

impl ReqwestDriver {
    pub fn new() -> Self {
        Self {
            data_pool: Arc::new(IntermediateDataPool::new())
        }
    }
}

impl Driver for ReqwestDriver {
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
        let data_pool = self.data_pool.clone();
        std::thread::spawn(move || outer_engine(core_id, queue, data_pool.clone()));
        return Ok(());
    }

    fn parse_function(
        &self,
        function_path: String,
        static_domain: &'static dyn crate::memory_domain::MemoryDomain,
    ) -> DandelionResult<Function> {
        if function_path.len() == 0 {
            return Err(DandelionError::CalledSystemFuncParser);
        }
        let system_function = SystemFunction::from_str(&function_path).unwrap();
        return Ok(Function {
            requirements: crate::DataRequirementList {
                input_requirements: vec![],
                static_requirements: vec![],
            },
            context: static_domain.acquire_context(0)?,
            config: FunctionConfig::SysConfig(system_function),
        });
    }
}

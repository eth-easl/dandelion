use crate::{
    function_driver::{
        thread_utils::{DefaultState, ThreadCommand, ThreadController, ThreadPayload},
        ComputeResource, Driver, Engine, Function, FunctionConfig, WasmConfig,
    },
    interface::{read_output_structs, setup_input_structs},
    memory_domain::{Context, ContextType, MemoryDomain},
    DataRequirementList, DataSet,
};
use core::{
    future::{ready, Future},
    pin::Pin,
};
use dandelion_commons::{
    records::{RecordPoint, Recorder},
    DandelionError, DandelionResult,
};
use futures::task::Poll;
use libloading::{Library, Symbol};
use log::error;
use std::sync::{Arc, Mutex};

type WasmEntryPoint = fn(&mut [u8], usize) -> Option<i32>;

struct WasmCommand {
    lib: Arc<Library>,
    context: Arc<Mutex<Option<Context>>>,
    sysdata_offset: usize,
    recorder: Option<Recorder>,
}
unsafe impl Send for WasmCommand {}

impl ThreadPayload for WasmCommand {
    type State = DefaultState;
    fn run(self, _state: &mut Self::State) -> DandelionResult<()> {
        match unsafe { self.lib.get::<WasmEntryPoint>(b"run") } {
            Ok(entry_point) => {
                // TODO handle errors

                let mut guard = self.context.lock().unwrap();

                // take out the context
                let mut ctx = guard.take().unwrap();
                let wasm_context = match &mut ctx.context {
                    ContextType::Wasm(wasm_context) => wasm_context,
                    _ => panic!("invalid context type"),
                };

                // call entry point
                let ret = entry_point(&mut wasm_context.mem, sysdata_offset);
                
                // put context back
                *guard = Some(ctx);

                return match ret {
                    Some(_) => Ok(()),
                    None => Err(DandelionError::EngineError),
                };
            }
            Err(_) => Err(DandelionError::EngineError),
        }
    }
}

struct WasmFuture<'a> {
    engine: &'a mut WasmEngine,
    context: Arc<Mutex<Option<Context>>>,
    system_data_offset: usize,
}

// exits the function
impl Future for WasmFuture<'_> {
    type Output = (DandelionResult<()>, Context);
    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut futures::task::Context,
    ) -> futures::task::Poll<Self::Output> {
        match self.engine.thread_controller.poll_next(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Some(Ok(()))) => {
                // TODO handle errors (we don't have a context if locking fails)
                let mut guard = self.context.lock().unwrap();
                if !guard.is_some() {
                    return Poll::Pending;
                };
                let mut context = guard.take().unwrap();
                let res = read_output_structs::<u32, u32>(&mut context, self.system_data_offset);
                Poll::Ready((res, context))
            }
            _ => {
                let context = self.context.lock().unwrap().take().unwrap();
                Poll::Ready((Err(DandelionError::EngineError), context))
            }
        }
    }
}

pub struct WasmEngine {
    thread_controller: ThreadController<WasmCommand>,
}

impl Engine for WasmEngine {
    fn run(
        &mut self,
        config: &FunctionConfig,
        mut context: Context,
        output_set_names: &Vec<String>,
        mut recorder: Recorder,
    ) -> Pin<Box<dyn Future<Output = (DandelionResult<()>, Context)> + '_ + Send>> {
        if let Err(err) = recorder.record(RecordPoint::EngineStart) {
            return Box::pin(core::future::ready((Err(err), context)));
        }

        // error shorthand
        use DandelionError::*;
        macro_rules! err {
            ($err:expr) => {
                return Box::pin(ready((Err($err), context)))
            };
        }

        // extract config and context
        let wasm_config = match config {
            FunctionConfig::WasmConfig(wasm_config) => wasm_config,
            _ => err!(ConfigMissmatch),
        };

        // setup input structs
        if let Err(err) = setup_input_structs::<u32, u32>(
            &mut context,
            wasm_config.system_data_struct_offset,
            output_set_names,
        ) {
            err!(err)
        };

        // share context with thread
        let context_ = Arc::new(Mutex::new(Some(context)));

        // send run command to thread
        let cmd = ThreadCommand::Run(
            recorder,
            WasmCommand {
                context: context_.clone(),
                lib: wasm_config.lib.clone(),
                recorder: Some(recorder),
                sysdata_offset: wasm_config.system_data_struct_offset,
            },
        );

        // TODO give back context if send fails (moved it into the Arc)
        match self.thread_controller.send_command(cmd) {
            Ok(()) => (),
            Err(err) => {
                let err_context = context_.lock().unwrap().take().unwrap();
                return Box::pin(futures::future::ready((Err(err), err_context)));
            }
        };
        Box::<WasmFuture>::pin(WasmFuture {
            engine: self,
            context: context_,
            system_data_offset: wasm_config.system_data_struct_offset,
        })
    }

    fn abort(&mut self) -> DandelionResult<()> {
        unimplemented!()
    }
}

pub struct WasmDriver {}

impl Driver for WasmDriver {
    fn start_engine(&self, resource: ComputeResource) -> DandelionResult<Box<dyn Engine>> {
        // sanity checks; extract core id
        let cpu_slot = match resource {
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
            .find(|x| x.id == usize::from(cpu_slot))
            .is_some()
        {
            return Err(DandelionError::EngineResourceError);
        }

        // create channels and spawn threads
        return Ok(Box::new(WasmEngine {
            thread_controller: ThreadController::new(cpu_slot),
        }));
    }

    fn parse_function(
        &self,
        function_path: String,
        static_domain: &Box<dyn MemoryDomain>,
    ) -> DandelionResult<Function> {
        let lib = unsafe {
            Library::new(function_path).map_err(|e| {
                error!("error: {}", e);
                DandelionError::MalformedConfig
            })
        }?;

        macro_rules! call {
            ($fname:expr, $type:ty) => {
                match unsafe { lib.get::<Symbol<$type>>($fname.as_bytes()) } {
                    Ok(f) => f(),
                    Err(_) => return Err(DandelionError::MalformedConfig),
                }
            };
        }

        let sd_struct_offset = call!("get_wasm_sdk_sysdata_offset", fn() -> usize);
        let sdk_heap_base = call!("get_sdk_heap_base", fn() -> usize);
        let sdk_heap_size = call!("get_sdk_heap_size", fn() -> usize);
        let wasm_mem_size = call!("get_wasm_mem_size", fn() -> usize);

        let mut context = static_domain.acquire_context(wasm_mem_size)?;

        // there must be one data set, which would normally describe the
        // elf sections to be copied
        context.content = vec![Some(DataSet {
            ident: String::from("static"),
            buffers: vec![],
        })];
        Ok(Function {
            config: FunctionConfig::WasmConfig(WasmConfig {
                lib: Arc::new(lib),
                wasm_mem_size,
                sdk_heap_base,
                sdk_heap_size,
                system_data_struct_offset: sd_struct_offset,
            }),
            requirements: DataRequirementList {
                size: wasm_mem_size,
                static_requirements: vec![],
                input_requirements: vec![],
            },
            context,
        })
    }
}

use crate::{
    DataSet,
    function_driver::{
        thread_utils::{DefaultState, ThreadCommand, ThreadController, ThreadPayload},
        ComputeResource, Driver, Engine, FunctionConfig, Function, WasmtimeConfig
    },
    memory_domain::{Context, ContextType, MemoryDomain, wasmtime::WasmtimeContext},
    DataRequirementList,
    interface::{_32_bit::DandelionSystemData, read_output_structs, setup_input_structs}, 
};
use dandelion_commons::{
    DandelionResult, DandelionError,
    records::{RecordPoint, Recorder},
};
use futures::{task::Poll, StreamExt};
use core::{
    future::{ready, Future},
    pin::Pin,
};
use std::{
    sync::{atomic::{AtomicBool, Ordering}, Mutex, Arc},
    thread::spawn,
};
use log::{error, info};

use wasmtime;


struct WasmtimeCommand {
    context: Arc<Mutex<Option<Context>>>,
    sysdata_offset: usize,
}

unsafe impl Send for WasmtimeCommand {}

impl ThreadPayload for WasmtimeCommand {
    type State = DefaultState;
    fn run(self, _state: &mut Self::State) -> DandelionResult<()> {
        // get context
        let mut guard = self.context.lock().unwrap();
        let mut ctx = guard.take().unwrap();

        let wasm_context: &mut Box<WasmtimeContext> = match &mut ctx.context {
            ContextType::Wasmtime(wasm_context) => wasm_context,
            _ => panic!("invalid context type"),
        };
        let memory = wasm_context.memory.take().unwrap();
        let store = &mut wasm_context.store.as_mut().unwrap();
        let module = wasm_context.module.take().unwrap();

        // buffer the system data struct because it will be overridden at instantiation
        let mut sysdata_buffer = [0u8; core::mem::size_of::<DandelionSystemData>()];
        let _ = memory.read(&store, self.sysdata_offset, &mut sysdata_buffer);

        // instantiate module
        let instance = wasmtime::Instance::new(store, &module, &[memory.into()]).unwrap();

        // write the system data struct back
        let _ = memory.write(wasm_context.store.as_mut().unwrap(), self.sysdata_offset, &sysdata_buffer);

        // call entry point
        let entry = instance.get_typed_func::<(), ()>(wasm_context.store.as_mut().unwrap(), "_start").unwrap();
        let ret = entry.call(wasm_context.store.as_mut().unwrap(), ());

        // put memory back into context
        wasm_context.memory = Some(memory);

        // put context back
        *guard = Some(ctx);

        match ret {
            Ok(_) => Ok(()),
            Err(_) => Err(DandelionError::EngineError),
        }
    }
}

struct WasmtimeFuture<'a> {
    engine: &'a mut WasmtimeEngine,
    context: Arc<Mutex<Option<Context>>>,
    system_data_struct_offset: usize,
}

impl Future for WasmtimeFuture<'_> {
    type Output = (DandelionResult<()>, Context);
    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut futures::task::Context,
    ) -> futures::task::Poll<Self::Output> {
        match self.engine.thread_controller.poll_next(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Some(Ok(()))) => {
                let mut guard = self.context.lock().unwrap();
                if !guard.is_some() { return Poll::Pending };
                let mut context = guard.take().unwrap();
                context.content.clear();
                let res = read_output_structs::<u32, u32>(&mut context, self.system_data_struct_offset);
                Poll::Ready((res, context))
            },
            _ => {
                // error
                let context = self.context.lock().unwrap().take().unwrap();
                Poll::Ready((Err(DandelionError::EngineError), context))
            }
        }
    }
}

pub struct WasmtimeEngine {
    thread_controller: ThreadController<WasmtimeCommand>,
}

impl Engine for WasmtimeEngine {
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
            FunctionConfig::WasmtimeConfig(c) => c,
            _ => err!(ConfigMissmatch),
        };

        // compile module
        {
            let wasm_context = match &mut context.context {
                ContextType::Wasmtime(wasm_context) => wasm_context,
                _ => err!(ConfigMissmatch),
            };
            wasm_context.module = Some(
                match unsafe { 
                    wasmtime::Module::deserialize(
                        &wasm_config.wasmtime_engine, 
                        &wasm_config.precompiled_module,
                    ) 
                } {
                    Ok(module) => module,
                    Err(_) => err!(EngineError),
                }
            );
        }

        // setup input structs
        if let Err(err) = setup_input_structs::<u32, u32>(
            &mut context, 
            wasm_config.system_data_struct_offset, 
            output_set_names
        ) { err!(err) };

        // share context with thread
        let context_ = Arc::new(Mutex::new(Some(context)));

        // send run command to thread
        let cmd = ThreadCommand::Run(
            recorder,
            WasmtimeCommand {
                context: context_.clone(),
                sysdata_offset: wasm_config.system_data_struct_offset,
            },
        );
        
        // TODO give back context if send fails (moved it into the Arc)
        let r = self.thread_controller.send_command(cmd);
        match r {
            Ok(()) => (),
            Err(_) => (),
        };
        Box::<WasmtimeFuture>::pin(WasmtimeFuture { 
            engine: self, 
            context: context_, 
            system_data_struct_offset: wasm_config.system_data_struct_offset,
        })
    }
    fn abort(&mut self) -> DandelionResult<()> {
        unimplemented!()
    }
}

const SDK_HEAP_PAGES : usize = 2048;    // 128MB

pub struct WasmtimeDriver {}

impl Driver for WasmtimeDriver {
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
        return Ok(Box::new(WasmtimeEngine {
            thread_controller: ThreadController::new(cpu_slot),
        }));
    }

    fn parse_function(
        &self,
        function_config: String,
        static_domain: &Box<dyn MemoryDomain>,
    ) -> DandelionResult<Function> {

        info!("parsing wasmtime function {}", &function_config);
        info!("pre-compilation enabled: {}", cfg!(feature = "wasmtime-precompiled"));

        // shorthand to map any error below to a config missmatch
        macro_rules! map_cfg_err {
            ($e:expr, $dbg:expr) => {
                $e.map_err(|_| {
                    error!($dbg);
                    DandelionError::ConfigMissmatch
                })?
            }
        }

        let wasmtime_engine = wasmtime::Engine::default();
        let mut store: wasmtime::Store::<()> = wasmtime::Store::new(&wasmtime_engine, ());

        // read file and precompile module
        let wasm_module_content = map_cfg_err!{ 
            std::fs::read(&function_config),
            "could not read function file"
        };
        let precompiled_module = if cfg!(feature = "wasmtime-precompiled") {
            wasm_module_content
        } else if cfg!(feature = "wasmtime-jit") {
            map_cfg_err!{ 
                store.engine().precompile_module(&wasm_module_content),
                "could not precompile module"
            }
        } else {
            unreachable!()
        };

        // instantiate module to extract layout data
        let module = unsafe {
            map_cfg_err!{ 
                wasmtime::Module::deserialize(store.engine(), &precompiled_module),
                "could not deserialize module"
            }
        };
        let memory_ty = module
            .imports().next()
            .ok_or_else(|| {
                error!("module {} has no imports", function_config);
                DandelionError::ConfigMissmatch
            })?
            .ty().memory()
            .ok_or_else(|| {
                error!("module {} has no memory import", function_config);
                DandelionError::ConfigMissmatch
            })?
            .clone();
        let memory =    map_cfg_err!{ 
            wasmtime::Memory::new(&mut store, memory_ty.clone()), 
            "could not create memory" 
        };
        let instance =  map_cfg_err!{ 
            wasmtime::Instance::new(&mut store, &module, &[memory.into()]),
            "could not instantiate module"
        };
        
        let system_data_struct_offset = {
            let v = instance
                .get_global(&mut store, "__dandelion_system_data")
                .ok_or_else(|| {
                    error!("module {} has no system data struct", function_config);
                    DandelionError::ConfigMissmatch
                })?
                .get(&mut store);
            match v {
                wasmtime::Val::I32(x) => x as usize,
                _ => return Err(DandelionError::ConfigMissmatch)
            }
        };
        let wasm_mem_min_pages = memory_ty.minimum() as usize;
        let sdk_heap_size = SDK_HEAP_PAGES * 65536;
        let sdk_heap_base = wasm_mem_min_pages * 65536;
        let total_mem_size = (wasm_mem_min_pages + SDK_HEAP_PAGES) * 65536;

        let mut context = static_domain.acquire_context(total_mem_size)?;

        context.content = vec![Some(
            DataSet {
                ident: String::from("static"),
                buffers: vec![],
            }
        )];
        Ok(Function {
            config: FunctionConfig::WasmtimeConfig(WasmtimeConfig {
                precompiled_module,
                total_mem_size,
                sdk_heap_base,
                system_data_struct_offset,
                wasmtime_engine,
            }),
            requirements: DataRequirementList {
                size: total_mem_size,
                static_requirements: vec![],
                input_requirements: vec![],
            },
            context,
        })
    }
}
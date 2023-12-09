use crate::{
    DataSet,
    function_driver::{Driver, Engine, FunctionConfig, Function, WasmtimeConfig},
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
    recorder: Option<Recorder>,
}

unsafe impl Send for WasmtimeCommand {}

fn run_thread(
    core_id: u8,
    command_receiver: std::sync::mpsc::Receiver<WasmtimeCommand>,
    mut result_sender: futures::channel::mpsc::Sender<DandelionResult<()>>,
) -> () {

    // set core
    if !core_affinity::set_for_current(core_affinity::CoreId { id: core_id.into() }) {
        return;
    };
    info!("WASMTIME engine running on core {}", core_id);

    '_commandloop: for cmd in command_receiver.iter() {
        // TODO improve safety
        let WasmtimeCommand { context, sysdata_offset, recorder } = cmd;

        // get context
        let mut guard = context.lock().unwrap();
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
        let _ = memory.read(&store, sysdata_offset, &mut sysdata_buffer);

        // instantiate module
        let instance = wasmtime::Instance::new(store, &module, &[memory.into()]).unwrap();

        // write the system data struct back
        let _ = memory.write(wasm_context.store.as_mut().unwrap(), sysdata_offset, &sysdata_buffer);

        // call entry point
        let entry = instance.get_typed_func::<(), ()>(wasm_context.store.as_mut().unwrap(), "_start").unwrap();
        let ret = entry.call(wasm_context.store.as_mut().unwrap(), ());

        // put memory back into context
        wasm_context.memory = Some(memory);

        // put context back
        *guard = Some(ctx);

        // record
        if let Some(mut recorder) = recorder {
            let _ = recorder.record(RecordPoint::EngineEnd);
        }

        let msg = match ret {
            Ok(_) => Ok(()),
            Err(_) => Err(DandelionError::EngineError),
        };

        // try sending until succeeds
        let mut not_sent = true;
        while not_sent {
            not_sent = match result_sender.try_send(msg.clone()) {
                Ok(()) => false,
                Err(err) if err.is_full() => true,
                Err(_) => return ,
            }
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
        match self.engine.result_receiver.poll_next_unpin(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Some(Ok(()))) => {
                let mut guard = self.context.lock().unwrap();
                if !guard.is_some() { return Poll::Pending };
                let mut context = guard.take().unwrap();
                context.content.clear();
                let res = read_output_structs::<u32, u32>(&mut context, self.system_data_struct_offset);
                self.engine.is_running.store(false, Ordering::Relaxed);
                Poll::Ready((res, context))
            },
            _ => {
                // error
                self.engine.is_running.store(false, Ordering::Relaxed);
                let context = self.context.lock().unwrap().take().unwrap();
                Poll::Ready((Err(DandelionError::EngineError), context))
            }
        }
    }
}

pub struct WasmtimeEngine {
    is_running: AtomicBool,
    command_sender: std::sync::mpsc::Sender<WasmtimeCommand>,
    result_receiver: futures::channel::mpsc::Receiver<DandelionResult<()>>,
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

        // check if engine is running
        if self.is_running.load(Ordering::Relaxed) {
            err!(EngineAlreadyRunning)
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
        let cmd = WasmtimeCommand {
            context: context_.clone(),
            sysdata_offset: wasm_config.system_data_struct_offset,
            recorder: Some(recorder),
        };
        
        // TODO give back context if send fails (moved it into the Arc)
        let r = self.command_sender.send(cmd);
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
    fn start_engine(&self, config: Vec<u8>) -> DandelionResult<Box<dyn Engine>> {
        if config.len() != 1 {
            return Err(DandelionError::ConfigMissmatch);
        }
        let cpu_slot = config[0];
        // check that core is available
        let available_cores = match core_affinity::get_core_ids() {
            None => return Err(DandelionError::EngineError),
            Some(cores) => cores,
        };
        if !available_cores
            .iter()
            .find(|x| x.id == usize::from(cpu_slot))
            .is_some()
        { return Err(DandelionError::MalformedConfig); }

        // create channels and spawn threads
        let (command_sender, command_receiver) = std::sync::mpsc::channel();
        let (result_sender, result_receiver) = futures::channel::mpsc::channel(0);
        let _thread_handle = spawn(move || run_thread(cpu_slot, command_receiver, result_sender));
        let is_running = AtomicBool::new(false);
        return Ok(Box::new(WasmtimeEngine {
            command_sender,
            result_receiver,
            is_running,
        }));
    }

    fn parse_function(
        &self,
        function_config: String,
        static_domain: &Box<dyn MemoryDomain>,
    ) -> DandelionResult<Function> {

        // shorthand to map any error below to a config missmatch
        macro_rules! map_cfg_err {
            ($e:expr) => {
                $e.map_err(|_| DandelionError::ConfigMissmatch)?
            }
        }

        let wasmtime_engine = wasmtime::Engine::default();
        let mut store: wasmtime::Store::<()> = wasmtime::Store::new(&wasmtime_engine, ());

        // read file and precompile module
        let wasm_module_content = map_cfg_err!{ std::fs::read(&function_config) };
        let precompiled_module = map_cfg_err!{ store.engine().precompile_module(&wasm_module_content) };

        // instantiate module to extract layout data
        let module = unsafe {
            map_cfg_err!{ 
                wasmtime::Module::deserialize(store.engine(), &precompiled_module) 
            }
        };
        let memory_ty = module
            .imports().next()
            .ok_or(DandelionError::ConfigMissmatch)?
            .ty().memory()
            .ok_or(DandelionError::ConfigMissmatch)?
            .clone();
        let memory =    map_cfg_err!{ wasmtime::Memory::new(&mut store, memory_ty.clone()) };
        let instance =  map_cfg_err!{ wasmtime::Instance::new(&mut store, &module, &[memory.into()]) };
        
        let system_data_struct_offset = {
            let v = instance
                .get_global(&mut store, "__dandelion_system_data")
                .ok_or(DandelionError::ConfigMissmatch)?
                .get(&mut store);
            match v {
                wasmtime::Val::I32(x) => x as usize,
                _ => return Err(DandelionError::ConfigMissmatch),
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
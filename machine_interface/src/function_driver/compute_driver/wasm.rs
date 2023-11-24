use crate::{
    DataSet,
    function_driver::{Driver, Engine, FunctionConfig, Function, WasmConfig},
    memory_domain::{Context, ContextType, MemoryDomain},
    DataRequirementList,
    interface::{read_output_structs, setup_input_structs}, 
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
use libloading::{Library, Symbol};

type WasmEntryPoint = fn(&mut [u8]) -> Option<i32>;

struct WasmCommand {
    lib: Arc<Library>,
    context: Arc<Mutex<Option<Context>>>,
    recorder: Option<Recorder>,
}

unsafe impl Send for WasmCommand {}

// enters the function
fn run_thread(
    core_id: u8,
    command_receiver: std::sync::mpsc::Receiver<WasmCommand>,
    mut result_sender: futures::channel::mpsc::Sender<DandelionResult<()>>,
) -> () {

    // set core
    if !core_affinity::set_for_current(core_affinity::CoreId { id: core_id.into() }) {
        return;
    };
    println!("WASM engine running on core {}", core_id);

    'commandloop: for cmd in command_receiver.iter() {
        let WasmCommand { lib, context, recorder } = cmd;
        match unsafe { lib.get::<WasmEntryPoint>(b"run") } {
            Ok(entry_point) => {
                // TODO handle errors

                let mut guard = context.lock().unwrap();

                // take out the context
                let mut ctx = guard.take().unwrap();
                let wasm_context = match &mut ctx.context {
                    ContextType::Wasm(wasm_context) => wasm_context,
                    _ => panic!("invalid context type"),
                };

                // call entry point
                let ret = entry_point(&mut wasm_context.mem);
                
                // put context back
                *guard = Some(ctx);

                // record
                if let Some(mut recorder) = recorder {
                    let _ = recorder.record(RecordPoint::EngineEnd);
                }

                let msg = match ret {
                    Some(_) => Ok(()),
                    None => Err(DandelionError::EngineError)
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
            },
            Err(_) => break 'commandloop,
        }
    };
}

struct WasmFuture<'a> {
    engine: &'a mut WasmEngine,
    context: Arc<Mutex<Option<Context>>>,
    system_data_offset: usize,
    // base_addr: usize,
}

// exits the function
impl Future for WasmFuture<'_> {
    type Output = (DandelionResult<()>, Context);
    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut futures::task::Context,
    ) -> futures::task::Poll<Self::Output> {
        match self.engine.result_receiver.poll_next_unpin(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Some(Ok(()))) => {
                // TODO handle errors (we don't have a context if locking fails)
                let mut guard = self.context.lock().unwrap();
                if !guard.is_some() { return Poll::Pending };
                let mut context = guard.take().unwrap();
                context.content.clear();
                let res = read_output_structs::<u32, u32>(&mut context, self.system_data_offset);
                self.engine.is_running.store(false, Ordering::Release);
                Poll::Ready((res, context))
            },
            _ => {
                self.engine.is_running.store(false, Ordering::Release);
                let context = self.context.lock().unwrap().take().unwrap();
                Poll::Ready((Err(DandelionError::EngineError), context))
            }
        }
    }
}

pub struct WasmEngine {
    is_running: AtomicBool,
    command_sender: std::sync::mpsc::Sender<WasmCommand>,
    result_receiver: futures::channel::mpsc::Receiver<DandelionResult<()>>,
    // thread_handle: Option<JoinHandle<()>>,
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

        // check that engine isn't already running
        if self.is_running.swap(true, Ordering::AcqRel) { 
            err!(EngineAlreadyRunning) 
        };

        // extract config and context
        let wasm_config = match config {
            FunctionConfig::WasmConfig(wasm_config) => wasm_config,
            _ => err!(ConfigMissmatch),
        };

        // setup input structs
        if let Err(err) = setup_input_structs::<u32, u32>(
            &mut context, 
            wasm_config.system_data_struct_offset, 
            output_set_names
        ) { err!(err) };

        // share context with thread
        let context_ = Arc::new(Mutex::new(Some(context)));

        // send run command to thread
        let cmd = WasmCommand { 
            context: context_.clone(),
            lib: wasm_config.lib.clone(),
            recorder: Some(recorder),
        };

        // TODO give back context if send fails (moved it into the Arc)
        match self.command_sender.send(cmd) {
            Ok(()) => (),
            Err(_) => (),
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
    fn start_engine(&self, config: Vec<u8>) -> DandelionResult<Box<dyn Engine>> {
        
        // sanity checks; extract core id
        if config.len() != 1 {
            return Err(DandelionError::ConfigMissmatch);
        }
        let cpu_slot: u8 = config[0];
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
        return Ok(Box::new(WasmEngine {
            command_sender,
            result_receiver,
            // thread_handle: Some(thread_handle),
            is_running,
        }));
    }

    fn parse_function(
        &self,
        function_path: String,
        static_domain: &Box<dyn MemoryDomain>,
    ) -> DandelionResult<Function> {

        let lib = unsafe { Library::new(function_path).map_err(|e| {
            println!("error: {}", e);
            DandelionError::MalformedConfig
        }) }?;

        macro_rules! call {
            ($fname:expr, $type:ty) => {
                match unsafe { lib.get::<Symbol<$type>>($fname.as_bytes()) } {
                    Ok(f) => f(),
                    Err(_) => return Err(DandelionError::MalformedConfig),
                }
            };
        }

        let sd_struct_offset =  call!("get_wasm_sdk_sysdata_offset",  fn() -> usize);
        let sdk_heap_base =     call!("get_sdk_heap_base",            fn() -> usize);
        let sdk_heap_size =     call!("get_sdk_heap_size",            fn() -> usize);
        let wasm_mem_size =     call!("get_wasm_mem_size",            fn() -> usize);

        let mut context = static_domain.acquire_context(wasm_mem_size)?;
        
        // there must be one data set, which would normally describe the 
        // elf sections to be copied
        context.content = vec![Some(
            DataSet {
                ident: String::from("static"),
                buffers: vec![],
            }
        )];
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
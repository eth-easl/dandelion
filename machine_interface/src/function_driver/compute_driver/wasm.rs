use crate::{
    function_driver::{Driver, Engine, FunctionConfig, Function, WasmConfig},
    memory_domain::{Context, ContextType, MemoryDomain, wasm::{WasmContext, WasmLayoutDescription}},
    // util::elf_parser,
    DataRequirementList,
    interface::{_32_bit::DandelionSystemData, read_output_structs, setup_input_structs}, DataSet, DataItem, Position,
};
use dandelion_commons::{records::Recorder, DandelionResult, DandelionError};
use futures::{task::Poll, StreamExt};
use core::{
    future::{ready, Future},
    pin::Pin,
};
use std::{
    cell::RefCell,
    sync::atomic::{AtomicBool, Ordering},
    thread::{spawn, JoinHandle},
    rc::Rc,
};
use libloading::{Library, Symbol};

type WasmEntryPoint = fn(&mut[u8], &mut DandelionSystemData) -> Option<i32>;

/// The `lib` refers to the loaded dynamic library.
/// When the function is invoked, it will mutate `data` and `sdk_sysdata`, which
/// are stored in the `WasmContext`. Nobody is allowed to access `data` and 
/// `sdk_sysdata` while the function is running.
struct WasmCommand {
    data: Rc<RefCell<Vec<u8>>>,
    sdk_sysdata: Rc<RefCell<DandelionSystemData>>,
    lib: Rc<Library>, //Symbol<'a, WasmEntryPoint>,
}

unsafe impl Send for WasmCommand {}

// enters the function
fn run_thread(
    command_receiver: std::sync::mpsc::Receiver<WasmCommand>,
    mut result_sender: futures::channel::mpsc::Sender<DandelionResult<()>>,
) -> () {

    let msg = match command_receiver.recv() {
        Ok( WasmCommand { data, sdk_sysdata, lib } ) => {
            match unsafe { lib.get::<WasmEntryPoint>(b"run") } {
                Ok(entry_point) => {
                    let ret = entry_point(data.borrow_mut().as_mut_slice(), &mut sdk_sysdata.borrow_mut());
                    match ret {
                        Some(_) => Ok(()),
                        None => Err(DandelionError::EngineError)
                    }
                },
                Err(_) => Err(DandelionError::EngineError),
            }
        },
        Err(_) => Err(DandelionError::EngineError),
    };

    while !result_sender.try_send(msg.clone()).is_ok() {}
}

struct WasmFuture<'a> {
    engine: &'a mut WasmEngine,
    context: Option<Context>,
    system_data_offset: usize,
    base_addr: usize,
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
                let mut context = self.context.take().unwrap();
                context.content.clear();
                let res = read_output_structs::<u32, u32>(&mut context, self.system_data_offset);
                self.engine.is_running.store(false, Ordering::Release);
                Poll::Ready((res, context))
            },
            _ => {
                self.engine.is_running.store(false, Ordering::Release);
                Poll::Ready((Err(DandelionError::EngineError), self.context.take().unwrap()))
            }
        }
    }
}

pub struct WasmEngine {
    is_running: AtomicBool,
    command_sender: std::sync::mpsc::Sender<WasmCommand>,
    result_receiver: futures::channel::mpsc::Receiver<DandelionResult<()>>,
    thread_handle: Option<JoinHandle<()>>,
}

impl Engine for WasmEngine {
    fn run(
        &mut self,
        config: &FunctionConfig,
        mut context: Context,
        output_set_names: &Vec<String>,
        mut recorder: Recorder,
    ) -> Pin<Box<dyn Future<Output = (DandelionResult<()>, Context)> + '_ + Send>> {
        // TODO: what is Recorder?
        
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

        // prepare context for inputs
        match &mut context.context {
            ContextType::Wasm(ref mut wasm_context) => {
                wasm_context.prepare_for_inputs(
                    wasm_config.system_data_region_base,
                    wasm_config.system_data_region_end,
                    wasm_config.sdk_heap_base,
                    wasm_config.system_data_struct_offset
                );
            },
            _ => err!(ConfigMissmatch),
        };

        // setup input structs
        if let Err(err) = setup_input_structs::<u32, u32>(
            &mut context, 
            wasm_config.system_data_struct_offset, 
            output_set_names
        ) { err!(err) };

        let layout: &WasmLayoutDescription = match &context.context {
            ContextType::Wasm(wasm_context) => wasm_context.layout.as_ref().unwrap(),
            _ => err!(ConfigMissmatch),
        };
        let data = layout.sysdata_region.clone();
        let sdk_sysdata = layout.sdk_sysdata.clone();

        // send run command to thread
        let cmd = WasmCommand { 
            data,
            sdk_sysdata,
            lib: wasm_config.lib.clone(), // entry_point 
        };
        match self.command_sender.send(cmd) {
            Ok(()) => (),
            Err(_) => err!(EngineError),
        };
        let context_base_addr = wasm_config.system_data_region_base;
        Box::<WasmFuture>::pin(WasmFuture {
            engine: self,
            context: Some(context),
            system_data_offset: wasm_config.system_data_struct_offset,
            base_addr: context_base_addr
        })
    }

    fn abort(&mut self) -> DandelionResult<()> {
        unimplemented!()
    }
}

pub struct WasmDriver {}

impl Driver for WasmDriver {
    fn start_engine(&self, config: Vec<u8>) -> DandelionResult<Box<dyn Engine>> {
        let (command_sender, command_receiver) = std::sync::mpsc::channel();
        let (result_sender, result_receiver) = futures::channel::mpsc::channel(0);
        let thread_handle = spawn(move || run_thread(command_receiver, result_sender));
        let is_running = AtomicBool::new(false);
        return Ok(Box::new(WasmEngine {
            command_sender,
            result_receiver,
            thread_handle: Some(thread_handle),
            is_running,
        }));
    }

    fn parse_function(
        &self,
        function: Vec<u8>,
        static_domain: &Box<dyn MemoryDomain>,
    ) -> DandelionResult<Function> {

        /*
        // using our elf parser
        // TODO: fix relocations
        unsafe {

            let elf = elf_parser::ParsedElf::new(&function)?;

            // allocate executable memory
            let addr = mmap(
                0 as *mut _,
                function.len(),
                libc::PROT_READ | libc::PROT_WRITE | libc::PROT_EXEC,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            );
            assert_ne!(addr, libc::MAP_FAILED);

            // copy code to executable memory
            std::ptr::copy_nonoverlapping(function.as_ptr(), addr as *mut u8, function.len());

            macro_rules! get_symbol_as {
                ($name:literal, $ftype:ty) => {{
                    let (offset, _) = elf.get_symbol_by_name(&function, $name).unwrap();
                    let obj: $ftype = core::mem::transmute(addr as usize + offset);
                    obj
                }};
            }

            // call sanity_check function
            let sanity_check_function = get_symbol_as!("sanity_check", fn() -> i32);
            let res = sanity_check_function();
            assert_eq!(res, 42);

            // get system data region in wasm memory space
            let get_sysdata_wasm_offset_function = get_symbol_as!("get_wasm_sysdata_region_offset", fn() -> usize);
            let wasm_sysdata_region_offset = get_sysdata_wasm_offset_function();
            println!("base of the sysdata reserved region in wasm memory: {}", wasm_sysdata_region_offset);
            let get_sysdata_wasm_size_function = get_symbol_as!("get_wasm_sysdata_region_size", fn() -> usize);
            let wasm_sysdata_region_size = get_sysdata_wasm_size_function();
            println!("size of the sysdata reserved region in wasm memory: {}", wasm_sysdata_region_size);
        };
        */

        // usin "sd" for "system_data"

        let obj_file_path: String = String::from_utf8(function).unwrap();
        let lib = unsafe { Library::new(obj_file_path).unwrap() };

        let f_get_sd_region_offset: Symbol<fn() -> usize> = unsafe { 
            lib.get(b"get_wasm_sysdata_region_offset").unwrap() 
        };
        let sd_region_offset = f_get_sd_region_offset();

        let f_get_sd_region_size: Symbol<fn() -> usize> = unsafe { 
            lib.get(b"get_wasm_sysdata_region_size").unwrap() 
        };
        let sd_region_size = f_get_sd_region_size();
        
        let f_get_sdk_sd_offset: Symbol<fn() -> usize> = unsafe { 
            lib.get(b"get_wasm_sdk_sysdata_offset").unwrap() 
        };
        let sd_struct_offset = f_get_sdk_sd_offset();

        // the sdk needs a heap base pointer for `dandelion_alloc()`
        // we define the sdk's heap to be the end of the wasm heap
        // which is not used by the wasm module by default
        // the heap base and end pointers are passed via system data

        let f_sdk_heap_base: Symbol<fn() -> usize> = unsafe { 
            lib.get(b"get_sdk_heap_base").unwrap() 
        };
        let sdk_heap_base = f_sdk_heap_base();

        let f_get_sdk_heap_size: Symbol<fn() -> usize> = unsafe { 
            lib.get(b"get_sdk_heap_size").unwrap() 
        };
        let sdk_heap_size = f_get_sdk_heap_size();
        let sdk_heap_end = sdk_heap_base + sdk_heap_size;

        let mut context = static_domain.acquire_context(sdk_heap_end, 0)?; //sd_region_offset)?;
        // there must be one data set, which would normally describe the sections to be copied
        let static_bufs = vec![
            // fill the space from 0 until the start of the system data region
            DataItem {
                ident: String::from(""),
                data: Position{
                    offset: 0,
                    size: sd_region_offset,
                },
                key: 0,
            },
            // fill the space from the end of the system data region until the start of the sdk heap
            DataItem {
                ident: String::from(""),
                data: Position {
                    offset: sd_region_offset + sd_region_size,
                    size: sdk_heap_base - (sd_region_offset + sd_region_size),
                },
                key: 0,
            },
        ];
        context.content = vec![Some(
            DataSet {
                ident: String::from("static"),
                buffers: static_bufs,
            }
        )];
        println!("heap end: {:?}", sdk_heap_end);
        Ok(Function {
            config: FunctionConfig::WasmConfig(WasmConfig {
                lib: Rc::new(lib),
                system_data_region_base: sd_region_offset,
                system_data_region_end: sd_region_offset + sd_region_size,
                sdk_heap_base,
                system_data_struct_offset: sd_struct_offset,
            }),
            requirements: DataRequirementList {
                size: sdk_heap_end,
                static_requirements: vec![
                    // what is the difference between this and the static_bufs above?
                    Position{
                        offset: 0,
                        size: sd_region_offset,
                    },
                    Position {
                        offset: sd_region_offset + sd_region_size,
                        size: sdk_heap_base - (sd_region_offset + sd_region_size),
                    },
                ],
                input_requirements: vec![],
            },
            context,
        })
        
        // Err(DandelionError::NotImplemented)
    }
}
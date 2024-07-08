use crate::{
    function_driver::{
        thread_utils::{start_thread, EngineLoop},
        ComputeResource, Driver, Function, FunctionConfig, WasmConfig, WorkQueue,
    },
    interface::{read_output_structs, setup_input_structs},
    memory_domain::{Context, ContextType, MemoryDomain},
    DataRequirementList, DataSet,
};
use dandelion_commons::{DandelionError, DandelionResult};
use libloading::{Library, Symbol};
use log::error;
use std::sync::Arc;

type WasmEntryPoint = fn(&mut [u8], usize) -> Option<i32>;

struct WasmLoop {}

impl EngineLoop for WasmLoop {
    fn init(_core_id: ComputeResource) -> DandelionResult<Box<Self>> {
        Ok(Box::new(WasmLoop {}))
    }
    fn run(
        &mut self,
        config: FunctionConfig,
        mut context: Context,
        output_set_names: Arc<Vec<String>>,
    ) -> DandelionResult<Context> {
        let wasm_config = match config {
            FunctionConfig::WasmConfig(wasm_config) => wasm_config,
            _ => return Err(DandelionError::ConfigMissmatch),
        };
        // setup input structs
        setup_input_structs::<u32, u32>(
            &mut context,
            wasm_config.system_data_struct_offset,
            &output_set_names,
        )?;
        match unsafe { wasm_config.lib.get::<WasmEntryPoint>(b"run") } {
            Ok(entry_point) => {
                // TODO handle errors

                // take out the context
                {
                    let wasm_context = match context.context {
                        ContextType::Wasm(ref mut wasm_context) => wasm_context,
                        _ => panic!("invalid context type"),
                    };

                    // call entry point
                    let _ =
                        entry_point(&mut wasm_context.mem, wasm_config.system_data_struct_offset)
                            .ok_or(DandelionError::EngineError)?;
                }
                // put context back

                read_output_structs::<u32, u32>(
                    &mut context,
                    wasm_config.system_data_struct_offset,
                )?;
                return Ok(context);
            }
            Err(_) => Err(DandelionError::EngineError),
        }
    }
}

pub struct WasmDriver {}

impl Driver for WasmDriver {
    fn start_engine(
        &self,
        resource: ComputeResource,
        queue: Box<dyn WorkQueue + Send + Sync>,
    ) -> DandelionResult<()> {
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
        start_thread::<WasmLoop>(cpu_slot, queue);
        return Ok(());
    }

    fn parse_function(
        &self,
        function_path: String,
        static_domain: &'static dyn MemoryDomain,
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
                static_requirements: vec![],
                input_requirements: vec![],
            },
            context,
        })
    }
}

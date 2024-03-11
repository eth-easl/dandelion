// TODO remove unneeded imports; just took everything from wasm.rs
use crate::{
    function_driver::{
        thread_utils::{DefaultState, ThreadCommand, ThreadController, ThreadPayload},
        ComputeResource, Driver, Engine, Function, FunctionConfig, GpuConfig, WasmConfig,
    },
    interface::{read_output_structs, setup_input_structs},
    memory_domain::{gpu::GpuContext, Context, ContextType, MemoryDomain},
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
use libc::{c_void, size_t};
use libloading::{Library, Symbol};
use log::error;
use std::{
    ffi::CString,
    ptr::null,
    sync::{Arc, Mutex},
};

mod hip;

// Temporary to get used to FFI and build.rs
#[link(name = "hip_interface_lib")]
extern "C" {
    fn gpu_toy_launch(gpu_id: u8);
}

// TODO remove pub once Engine.run implemented; this is just for basic testing
pub struct GpuCommand {
    pub gpu_id: u8,
}
unsafe impl Send for GpuCommand {}

impl ThreadPayload for GpuCommand {
    type State = DefaultState;

    fn run(self, state: &mut Self::State) -> DandelionResult<()> {
        // unsafe {
        //     gpu_toy_launch(self.gpu_id);
        // }

        // TODO handle errors
        // set gpu
        if hip::set_device(self.gpu_id as u32) != 0 {
            eprintln!("set_device");
        }

        // load module
        let mut module = hip::ModuleT::new();
        let fname =
            CString::new("/home/smithj/dandelion/machine_interface/hip_interface/module.hsaco")
                .unwrap();
        if hip::module_load(&mut module, fname) != 0 {
            eprintln!(
                "{}",
                hip::get_error_string(hip::get_last_error())
                    .into_string()
                    .unwrap()
            );
        }

        // load kernels
        let mut kernel_set: hip::FunctionT = null();
        let kname = CString::new("set_mem").unwrap();
        if hip::module_get_function(&mut kernel_set, &module, kname) != 0 {
            eprintln!("get_function");
        }
        let mut kernel_check: hip::FunctionT = null();
        let kname = CString::new("check_mem").unwrap();
        if hip::module_get_function(&mut kernel_check, &module, kname) != 0 {
            eprintln!("get_function 2");
        }

        // allocate device memory, prepare args
        let mut array: *const c_void = null();
        let arr_elem: u32 = 256;
        let elem_size: u32 = std::mem::size_of::<f64>() as u32;
        let arr_size: u32 = arr_elem * elem_size;
        if hip::malloc(&mut array, arr_size as usize) != 0 {
            eprintln!("malloc");
        }

        let args: [*const c_void; 2] = [
            &array as *const _ as *const c_void,
            &arr_elem as *const _ as *const c_void,
        ];

        // launch them
        let block_width: u32 = 1024;
        if hip::module_launch_kernel(
            kernel_set,
            (arr_elem + block_width - 1) / block_width,
            1,
            1,
            block_width,
            1,
            1,
            0,
            null(),
            args.as_ptr(),
            null(),
        ) != 0
        {
            eprintln!("launch_kernel");
        }

        if hip::module_launch_kernel(
            kernel_check,
            1,
            1,
            1,
            1,
            1,
            1,
            0,
            null(),
            args.as_ptr(),
            null(),
        ) != 0
        {
            eprintln!("launch_kernel");
        }

        if hip::device_synchronize() != 0 {
            eprintln!("device_synch");
        }
        Ok(())
    }
}

// TODO GpuFuture

pub struct GpuEngine {
    gpu_id: u8,
    thread_controller: ThreadController<GpuCommand>,
}

impl Engine for GpuEngine {
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

        todo!()
    }

    fn abort(&mut self) -> DandelionResult<()> {
        todo!()
    }
}

pub struct GpuDriver {}

impl Driver for GpuDriver {
    fn start_engine(
        &self,
        resource: crate::function_driver::ComputeResource,
    ) -> dandelion_commons::DandelionResult<Box<dyn crate::function_driver::Engine>> {
        // extract resources TODO update once we get Vec of ComputeResources
        let (cpu_slot, gpu_id) = match resource {
            ComputeResource::GPU(cpu, gpu) => (cpu, gpu),
            _ => return Err(DandelionError::EngineResourceError),
        };
        // check that core is available
        let available_cores = match core_affinity::get_core_ids() {
            None => return Err(DandelionError::EngineError),
            Some(cores) => cores,
        };
        if !available_cores
            .iter()
            .any(|x| x.id == usize::from(cpu_slot))
        {
            return Err(DandelionError::EngineResourceError);
        }
        // TODO check gpu is available

        Ok(Box::new(GpuEngine {
            gpu_id,
            thread_controller: ThreadController::new(cpu_slot),
        }))
    }

    fn parse_function(
        &self,
        function_path: String,
        static_domain: &Box<dyn crate::memory_domain::MemoryDomain>,
    ) -> dandelion_commons::DandelionResult<crate::function_driver::Function> {
        // For now ignore function path and domain and hardwire everything to get MVP
        // Actually might just never call this in the tests for now
        Ok(Function {
            config: FunctionConfig::GpuConfig(GpuConfig {}),
            requirements: DataRequirementList {
                static_requirements: vec![],
                input_requirements: vec![],
            },
            context: static_domain.acquire_context(0)?,
        })
    }
}

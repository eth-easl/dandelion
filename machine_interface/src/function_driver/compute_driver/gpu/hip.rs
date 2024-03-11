//! Rust bindings for a subset of the HIP runtime API as of ROCm 5.7.1

use libc::{c_void, size_t};
use std::{ffi::CString, ptr::null};

// TODO improve Types, eg. drop() for ModuleT to not leak memory
pub type ErrorT = u32;
/// typedef struct ihipModule_t* hipModule_t
type _ModuleT = *const c_void;
pub struct ModuleT(_ModuleT);
/// typedef struct iHipModuleSymbol_t* hipFunction_t
pub type FunctionT = *const c_void;
/// typedef struct iHipStream_t* hipStream_t
pub type StreamT = *const c_void;

impl ModuleT {
    // this is kinda not so nice; you should never hold an invalid Module.
    // Instead, loadModule should return it in a Result
    pub fn new() -> Self {
        Self(null())
    }
}
impl Drop for ModuleT {
    fn drop(&mut self) {
        unsafe {
            if hipModuleUnload(self.0) != 0 {
                panic!("Unloading module failed");
            }
        }
    }
}

#[link(name = "amdhip64")]
extern "C" {
    fn hipSetDevice(gpu_id: u32) -> ErrorT;
    fn hipDeviceSynchronize() -> ErrorT;
    fn hipModuleLoad(module: *mut _ModuleT, fname: *const i8) -> ErrorT;
    fn hipModuleUnload(module: _ModuleT) -> ErrorT;
    fn hipModuleGetFunction(function: *mut FunctionT, module: _ModuleT, kname: *const i8)
        -> ErrorT;
    fn hipModuleLaunchKernel(
        function: FunctionT,
        gridDimX: u32,
        gridDimY: u32,
        gridDimZ: u32,
        blockDimX: u32,
        blockDimY: u32,
        blockDimZ: u32,
        sharedMemBytes: u32,
        stream: StreamT,
        kernel_params: *const *const c_void,
        extra: *const *const c_void,
    ) -> ErrorT;
    fn hipGetErrorString(hipError: ErrorT) -> *const i8;
    fn hipGetLastError() -> ErrorT;
    // TODO add device_ptr type with automatic drop that frees
    fn hipMalloc(ptr: *mut *const c_void, size: size_t) -> ErrorT;
    fn hipFree(ptr: *const c_void);
}

// TODO: convert to Result<T, E> once talked with Tom
pub fn set_device(gpu_id: u32) -> ErrorT {
    unsafe { hipSetDevice(gpu_id) }
}

pub fn device_synchronize() -> ErrorT {
    unsafe { hipDeviceSynchronize() }
}

pub fn get_error_string(hip_error: ErrorT) -> CString {
    unsafe { CString::from_raw(hipGetErrorString(hip_error) as *mut i8) }
}

pub fn get_last_error() -> ErrorT {
    unsafe { hipGetLastError() }
}

pub fn module_load(module: &mut ModuleT, fname: CString) -> ErrorT {
    let mut ret: _ModuleT = null();
    let error;
    unsafe {
        error = hipModuleLoad(&mut ret as *mut _ModuleT, fname.as_ptr());
    }
    module.0 = ret;
    error
}

pub fn module_get_function(function: &mut FunctionT, module: &ModuleT, kname: CString) -> ErrorT {
    unsafe { hipModuleGetFunction(function as *mut FunctionT, module.0, kname.as_ptr()) }
}

#[allow(clippy::too_many_arguments)]
pub fn module_launch_kernel(
    function: FunctionT,
    grid_dim_x: u32,
    grid_dim_y: u32,
    grid_dim_z: u32,
    block_dim_x: u32,
    block_dim_y: u32,
    block_dim_z: u32,
    shared_mem_bytes: u32,
    stream: StreamT,
    kernel_params: *const *const c_void,
    extra: *const *const c_void,
) -> ErrorT {
    unsafe {
        hipModuleLaunchKernel(
            function,
            grid_dim_x,
            grid_dim_y,
            grid_dim_z,
            block_dim_x,
            block_dim_y,
            block_dim_z,
            shared_mem_bytes,
            stream,
            kernel_params,
            extra,
        )
    }
}

pub fn malloc(ptr: &mut *const c_void, size: size_t) -> ErrorT {
    unsafe { hipMalloc(ptr as *mut *const c_void, size) }
}

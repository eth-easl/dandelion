//! Rust bindings for a subset of the HIP runtime API as of ROCm 6.1.2

use dandelion_commons::{DandelionError, DandelionResult};
use libc::{c_void, size_t};
use std::{
    ffi::{CStr, CString},
    ptr::null,
};

type ErrorT = u32;

// typedef struct ihipModule_t* hipModule_t
type _ModuleT = *const c_void;
pub struct ModuleT(_ModuleT);

unsafe impl Send for ModuleT {}
unsafe impl Sync for ModuleT {}

// typedef struct iHipModuleSymbol_t* hipFunction_t
type _FunctionT = *const c_void;
pub struct FunctionT(_FunctionT);

unsafe impl Send for FunctionT {}
unsafe impl Sync for FunctionT {}

// typedef struct iHipStream_t* hipStream_t
pub type StreamT = *const c_void;
pub const DEFAULT_STREAM: StreamT = null();

// has to be pub to allow address-getting when preparing args
pub struct DeviceAllocation {
    pub ptr: *const c_void,
    pub size: usize,
    device: u8,
}

unsafe impl Send for DeviceAllocation {}
unsafe impl Sync for DeviceAllocation {}

// Should be associated with a DeviceAllocation; maybe use lifetimes for this in the future
#[repr(C)] // We take a raw pointers, so make sure the layout is as expected
pub struct DevicePointer {
    pub ptr: *const c_void,
}

#[link(name = "amdhip64")]
extern "C" {
    fn hipGetDeviceCount(count: *const i32) -> ErrorT;
    fn hipSetDevice(gpu_id: i32) -> ErrorT;
    fn hipGetDevice(deviceId: *const i32) -> ErrorT;
    fn hipDeviceSynchronize() -> ErrorT;
    fn hipDeviceSetLimit(limit: u32, value: size_t) -> ErrorT;
    fn hipModuleLoad(module: *mut _ModuleT, fname: *const i8) -> ErrorT;
    fn hipModuleLoadData(module: *mut _ModuleT, image: *const c_void) -> ErrorT;
    fn hipModuleUnload(module: _ModuleT) -> ErrorT;
    fn hipModuleGetFunction(
        function: *mut _FunctionT,
        module: _ModuleT,
        kname: *const i8,
    ) -> ErrorT;
    fn hipModuleLaunchKernel(
        function: _FunctionT,
        gridDimX: u32,
        gridDimY: u32,
        gridDimZ: u32,
        blockDimX: u32,
        blockDimY: u32,
        blockDimZ: u32,
        sharedMemBytes: usize,
        stream: StreamT,
        kernel_params: *const *const c_void,
        extra: *const *const c_void,
    ) -> ErrorT;
    fn hipGetErrorString(hipError: ErrorT) -> *const i8;
    fn hipMalloc(ptr: *mut *const c_void, size: size_t) -> ErrorT;
    fn hipFree(ptr: *const c_void) -> ErrorT;
    fn hipMemcpyHtoD(dst: *const c_void, src: *const c_void, sizeBytes: size_t) -> ErrorT;
    fn hipMemcpyDtoH(dst: *const c_void, src: *const c_void, sizeBytes: size_t) -> ErrorT;
    fn hipMemset(dst: *const c_void, value: i32, sizeBytes: size_t) -> ErrorT;
}

fn get_error_string(hip_error: ErrorT) -> String {
    unsafe {
        CStr::from_ptr(hipGetErrorString(hip_error) as *mut i8)
            .to_str()
            .expect("Invalid ROCm error string (shouldn't happen)")
            .to_string()
    }
}

macro_rules! checked_call {
    ($fcall: expr) => {
        unsafe {
            let error = $fcall;
            if error != 0 {
                return Err(DandelionError::HipError(get_error_string(error)));
            }
        }
    };
}

pub fn set_device(gpu_id: u8) -> DandelionResult<()> {
    checked_call!(hipSetDevice(gpu_id as i32));
    Ok(())
}

pub fn get_device_count() -> DandelionResult<usize> {
    let mut ret: i32 = -1;
    checked_call!(hipGetDeviceCount(&mut ret as *const i32));

    ret.try_into()
        .map_err(|_| DandelionError::EngineResourceError)
}

pub fn get_device() -> DandelionResult<u8> {
    let mut ret: i32 = 0;
    checked_call!(hipGetDevice(&mut ret as *const i32));

    ret.try_into()
        .map_err(|_| DandelionError::EngineResourceError)
}

pub fn device_synchronize() -> DandelionResult<()> {
    checked_call!(hipDeviceSynchronize());
    Ok(())
}

pub fn limit_heap_size(size: usize) -> DandelionResult<()> {
    // hipLimitMallocHeapSize = 2
    checked_call!(hipDeviceSetLimit(2, size));
    Ok(())
}

pub fn module_load(path: &str) -> DandelionResult<ModuleT> {
    let mut ret: _ModuleT = null();
    let fname =
        CString::new(path).or(Err(DandelionError::HipError("Invalid Module Path".into())))?;
    checked_call!(hipModuleLoad(&mut ret as *mut _ModuleT, fname.as_ptr()));
    Ok(ModuleT(ret))
}

/// # Safety
/// Requires *image* to point to a valid hsaco code object
pub unsafe fn module_load_data(image: *const c_void) -> DandelionResult<ModuleT> {
    let mut ret: _ModuleT = null();

    checked_call!(hipModuleLoadData(&mut ret as *mut _ModuleT, image));
    Ok(ModuleT(ret))
}

pub fn module_get_function(module: &ModuleT, name: &str) -> DandelionResult<FunctionT> {
    let mut ret: _FunctionT = null();
    let kname = CString::new(name).or(Err(DandelionError::HipError("Invalid Name".into())))?;
    checked_call!(hipModuleGetFunction(
        &mut ret as *mut _FunctionT,
        module.0,
        kname.as_ptr()
    ));
    Ok(FunctionT(ret))
}

/// # Safety
/// Requires *kernel_params* to point to an array of valid pointers to kernel arguments
#[allow(clippy::too_many_arguments)]
pub unsafe fn module_launch_kernel(
    function: &FunctionT,
    grid_dim_x: u32,
    grid_dim_y: u32,
    grid_dim_z: u32,
    block_dim_x: u32,
    block_dim_y: u32,
    block_dim_z: u32,
    shared_mem_bytes: u64,
    stream: StreamT,
    kernel_params: *const *const c_void,
    extra: *const *const c_void,
) -> DandelionResult<()> {
    checked_call!(hipModuleLaunchKernel(
        function.0,
        grid_dim_x,
        grid_dim_y,
        grid_dim_z,
        block_dim_x,
        block_dim_y,
        block_dim_z,
        shared_mem_bytes as usize,
        stream,
        kernel_params,
        extra,
    ));
    Ok(())
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

pub fn malloc(ptr: &mut *const c_void, size: size_t) -> ErrorT {
    unsafe { hipMalloc(ptr as *mut *const c_void, size) }
}

impl DeviceAllocation {
    pub fn try_new(size: usize) -> DandelionResult<Self> {
        let mut ret: *const c_void = null();
        checked_call!(hipMalloc(&mut ret as *mut *const c_void, size));
        // zero out memory
        checked_call!(hipMemset(ret, 0, size));

        let device = get_device()?;
        Ok(Self {
            ptr: ret,
            size,
            device,
        })
    }

    pub fn zero_out(&mut self) -> DandelionResult<()> {
        checked_call!(hipMemset(self.ptr, 0, self.size));
        Ok(())
    }

    pub fn zero_size(&mut self, size: usize) -> DandelionResult<()> {
        checked_call!(hipMemset(self.ptr, 0, size));
        Ok(())
    }
}

impl Drop for DeviceAllocation {
    fn drop(&mut self) {
        // Not entirely sure if this is required but device allocations are freed off the hot path anyway
        let curr_dev = get_device().expect("Need to be able to get current device before freeing");
        set_device(self.device).expect("Need to be able to set device before freeing");
        unsafe {
            if hipFree(self.ptr) != 0 {
                panic!("Freeing a device pointer failed (this shouldn't happen)");
            }
        }
        set_device(curr_dev).expect("Need to be able to restore device after freeing");
    }
}

/// # Safety
/// Requires *src* to point to valid memory
pub unsafe fn memcpy_h_to_d(
    dst: &DevicePointer,
    dev_offset: isize,
    src: *const c_void,
    size_bytes: usize,
) -> DandelionResult<()> {
    checked_call!(hipMemcpyHtoD(
        dst.ptr.byte_offset(dev_offset),
        src,
        size_bytes
    ));
    Ok(())
}

/// # Safety
/// Requires *dst* to point to valid memory
pub unsafe fn memcpy_d_to_h(
    dst: *const c_void,
    src: &DevicePointer,
    size_bytes: usize,
) -> DandelionResult<()> {
    checked_call!(hipMemcpyDtoH(dst, src.ptr, size_bytes));
    Ok(())
}

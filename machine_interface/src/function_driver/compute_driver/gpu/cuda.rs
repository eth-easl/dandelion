//! Rust bindings for a subset of the CUDA Device API

use dandelion_commons::{DandelionError, DandelionResult};
use libc::{c_void, size_t};
use std::{
    ffi::{CStr, CString},
    ptr::null,
};

type ErrorT = u32;
type CUresult = u32;
type CUdevice = *const i32;
type CUcontext = *const c_void;

type CUmodule = *const c_void;
pub struct Module(CUmodule);

unsafe impl Send for Module {}
unsafe impl Sync for Module {}

type CUfunction = *const c_void;
pub struct Function(CUfunction);

unsafe impl Send for Function {}
unsafe impl Sync for Function {}

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

#[link(name = "cudart")]
unsafe extern "C" {
    fn cudaGetErrorString(error: ErrorT) -> *const i8;
    fn cudaGetDevice(device_: *const i32) -> ErrorT;
    fn cudaGetDeviceCount(count: *const i32) -> ErrorT;
    fn cudaSetDevice(device: *const i32) -> ErrorT;
    fn cudaDeviceSetLimit(limit: u32, value: size_t) -> ErrorT;

    fn cudaMalloc(ptr: *mut *const c_void, size: size_t) -> ErrorT;
    fn cudaMemset(dst: *const c_void, value: i32, count: size_t) -> ErrorT;
    fn cudaFree(ptr: *const c_void) -> ErrorT;
    fn cudaMemcpy(dst: *const c_void, src: *const c_void, count: size_t, kind: u8) -> ErrorT;
}

#[link(name = "cuda")]
unsafe extern "C" {
    fn cuGetErrorString(error: CUresult, pStr: *mut *const i8) -> CUresult;

    fn cuInit(flags: u32) -> CUresult;
    fn cuDeviceGet(device: *mut CUdevice, ordinal: i32) -> CUresult;
    fn cuCtxCreate(pctx: *mut CUcontext, flags: u32, dev: CUdevice) -> CUresult;
    fn cuDevicePrimaryCtxRetain(pctx: *mut CUcontext, dev: CUdevice) -> CUresult;
    fn cuCtxSetCurrent(ctx: CUcontext) -> CUresult;
    fn cuCtxDestroy(pctx: CUcontext) -> CUresult;
    fn cuCtxSynchronize() -> CUresult;

    fn cuModuleLoad(module: *mut CUmodule, fname: *const i8) -> CUresult;
    fn cuModuleLoadData(module: *mut CUmodule, image: *const c_void) -> CUresult;
    fn cuModuleUnload(module: CUmodule) -> CUresult;
    fn cuModuleGetFunction(
        function: *mut CUfunction,
        module: CUmodule,
        kname: *const i8,
    ) -> CUresult;
    fn cuLaunchKernel(
        function: CUfunction,
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
    ) -> CUresult;

    // fn cuMemAlloc(ptr: *mut *const c_void, size: size_t) -> CUresult;
    // fn cuMemsetD8(dst: *const c_void, value: i8, sizeBytes: size_t) -> CUresult;
    // fn cuMemFree(ptr: *const c_void) -> CUresult;
    // fn cuMemcpyHtoD(dst: *const c_void, src: *const c_void, sizeBytes: size_t) -> CUresult;
    // fn cuMemcpyDtoH(dst: *const c_void, src: *const c_void, sizeBytes: size_t) -> CUresult;
}

fn get_cu_error_string(error_code: CUresult) -> String {
    let mut error_str_ptr: *const i8 = null();
    unsafe {
        let _ = cuGetErrorString(error_code, &mut error_str_ptr);
        CStr::from_ptr(error_str_ptr)
            .to_str()
            .expect("Invalid CUDA error string (shouldn't happen)")
            .to_string()
    }
}

fn get_cuda_error_string(error_code: ErrorT) -> String {
    unsafe {
        CStr::from_ptr(cudaGetErrorString(error_code) as *mut i8)
            .to_str()
            .expect("Invalid CUDA error string (shouldn't happen)")
            .to_string()
    }
}

macro_rules! cu_checked_call {
    ($fcall: expr) => {
        unsafe {
            let error = $fcall;
            if error != 0 {
                return Err(DandelionError::CudaError(get_cu_error_string(error)));
            }
        }
    };
}

macro_rules! cuda_checked_call {
    ($fcall: expr) => {
        unsafe {
            let error = $fcall;
            if error != 0 {
                return Err(DandelionError::CudaError(get_cuda_error_string(error)));
            }
        }
    };
}

pub fn initialize() -> DandelionResult<()> {
    cu_checked_call!(cuInit(0));
    Ok(())
}

pub fn get_device() -> DandelionResult<u8> {
    let mut ret: i32 = 0;
    cuda_checked_call!(cudaGetDevice(&mut ret as *const i32));

    ret.try_into()
        .map_err(|_| DandelionError::EngineResourceError)
}

pub fn get_device_count() -> DandelionResult<usize> {
    let mut ret: i32 = -1;
    cuda_checked_call!(cudaGetDeviceCount(&mut ret as *const i32));

    ret.try_into()
        .map_err(|_| DandelionError::EngineResourceError)
}

pub fn set_device(gpu_id: u8) -> DandelionResult<()> {
    cuda_checked_call!(cudaSetDevice(gpu_id as *const i32));

    let mut device: CUdevice = 0 as CUdevice;
    cu_checked_call!(cuDeviceGet(&mut device, gpu_id as i32));

    let mut primary_ctx: CUcontext = null();
    cu_checked_call!(cuDevicePrimaryCtxRetain(&mut primary_ctx, device));
    cu_checked_call!(cuCtxSetCurrent(primary_ctx));

    Ok(())
}

pub fn create_context(device: CUdevice) -> DandelionResult<CUcontext> {
    let mut context: CUcontext = null();
    // TODO: look at the context flags
    // 0 means automatic scheduling
    // https://docs.nvidia.com/cuda/cuda-driver-api/group__CUDA__CTX.html#group__CUDA__CTX_1g65dc0012348bc84810e2103a40d8e2cf
    cu_checked_call!(cuCtxCreate(&mut context, 0, device));

    context
        .try_into()
        .map_err(|_| DandelionError::EngineResourceError)
}

pub fn finalize(context: CUcontext) -> DandelionResult<()> {
    cu_checked_call!(cuCtxDestroy(context));
    Ok(())
}

pub fn device_synchronize() -> DandelionResult<()> {
    cu_checked_call!(cuCtxSynchronize());
    Ok(())
}

pub fn limit_heap_size(size: usize) -> DandelionResult<()> {
    // cudaLimitMallocHeapSize = 2
    cuda_checked_call!(cudaDeviceSetLimit(2, size));
    Ok(())
}

pub fn module_load(path: &str) -> DandelionResult<Module> {
    let mut ret: CUmodule = null();
    let fname =
        CString::new(path).or(Err(DandelionError::CudaError("Invalid Module Path".into())))?;
    cu_checked_call!(cuModuleLoad(&mut ret, fname.as_ptr()));
    Ok(Module(ret))
}

/// # Safety
/// Requires *image* to point to a valid hsaco code object
pub fn module_load_data(image: *const c_void) -> DandelionResult<Module> {
    let mut ret: CUmodule = null();
    cu_checked_call!(cuModuleLoadData(&mut ret as *mut CUmodule, image));
    Ok(Module(ret))
}

pub fn module_get_function(module: &Module, name: &str) -> DandelionResult<Function> {
    let mut ret: CUfunction = null();
    let kname = CString::new(name).or(Err(DandelionError::CudaError("Invalid Name".into())))?;
    cu_checked_call!(cuModuleGetFunction(
        &mut ret as *mut CUfunction,
        module.0,
        kname.as_ptr()
    ));
    Ok(Function(ret))
}

/// # Safety
/// Requires *kernel_params* to point to an array of valid pointers to kernel arguments
#[allow(clippy::too_many_arguments)]
pub fn module_launch_kernel(
    function: &Function,
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
) -> DandelionResult<()> {
    cu_checked_call!(cuLaunchKernel(
        function.0,
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
    ));
    Ok(())
}

impl Drop for Module {
    fn drop(&mut self) {
        unsafe {
            let error_code = cuModuleUnload(self.0);
            if error_code != 0 {
                panic!(
                    "Unloading module failed: {} - {}",
                    error_code,
                    get_cu_error_string(error_code)
                );
            }
        }
    }
}

pub fn gpu_malloc(ptr: *mut *const c_void, size: size_t) -> DandelionResult<()> {
    cuda_checked_call!(cudaMalloc(ptr, size));
    Ok(())
}

pub fn gpu_zero_mem(ptr: *const c_void, size: size_t) -> DandelionResult<()> {
    cuda_checked_call!(cudaMemset(ptr, 0, size));
    Ok(())
}

pub fn gpu_free(ptr: *const c_void) -> DandelionResult<()> {
    cuda_checked_call!(cudaFree(ptr));
    Ok(())
}

impl DeviceAllocation {
    pub fn try_new(size: usize) -> DandelionResult<Self> {
        let mut ret: *const c_void = null();
        let _ = gpu_malloc(&mut ret, size);
        // zero out memory
        let _ = gpu_zero_mem(ret, size);

        let device = get_device()?;
        Ok(Self {
            ptr: ret as *const c_void,
            size,
            device,
        })
    }

    pub fn zero_out(&mut self) -> DandelionResult<()> {
        gpu_zero_mem(self.ptr, self.size)
    }

    pub fn zero_size(&mut self, size: usize) -> DandelionResult<()> {
        gpu_zero_mem(self.ptr, size)
    }
}

impl Drop for DeviceAllocation {
    fn drop(&mut self) {
        // Not entirely sure if this is required but device allocations are freed off the hot path anyway
        let curr_dev = get_device().expect("Need to be able to get current device before freeing");
        set_device(self.device).expect("Need to be able to set device before freeing");
        let _ = gpu_free(self.ptr);
        set_device(curr_dev).expect("Need to be able to restore device after freeing");
    }
}

/// # Safety
/// Requires *src* to point to valid memory
pub fn memcpy_h_to_d(
    dst: &DevicePointer,
    dev_offset: isize,
    src: *const c_void,
    size_bytes: usize,
) -> DandelionResult<()> {
    cuda_checked_call!(cudaMemcpy(
        dst.ptr.byte_offset(dev_offset),
        src,
        size_bytes,
        1
    ));
    Ok(())
}

/// # Safety
/// Requires *dst* to point to valid memory
pub fn memcpy_d_to_h(
    dst: *const c_void,
    src: &DevicePointer,
    size_bytes: usize,
) -> DandelionResult<()> {
    cuda_checked_call!(cudaMemcpy(dst, src.ptr, size_bytes, 2));
    Ok(())
}

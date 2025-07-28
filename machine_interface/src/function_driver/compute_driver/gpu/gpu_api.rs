#[cfg(all(feature = "cuda", feature = "hip"))]
compile_error!("Cannot compile with both the cuda and hip features");

#[cfg_attr(feature = "cuda", path = "cuda.rs")]
#[cfg_attr(feature = "hip", path = "hip.rs")]
pub mod gpu_api;

pub use gpu_api::{
    get_device_count, limit_heap_size, memcpy_d_to_h, memcpy_h_to_d, module_get_function,
    module_launch_kernel, module_load_data, set_device, synchronize, DeviceAllocation,
    DevicePointer, Function, Module, DEFAULT_STREAM,
};

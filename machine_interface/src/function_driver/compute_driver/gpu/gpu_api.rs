#[cfg(all(feature = "hip", feature = "cuda"))]
compile_error!("Cannot compile with both the hip and cuda features");

#[cfg_attr(feature = "hip", path = "hip.rs")]
#[cfg_attr(feature = "cuda", path = "cuda.rs")]
pub mod gpu_api;

pub use gpu_api::{
    limit_heap_size,
    get_device_count,
    set_device,
    module_load_data,
    module_launch_kernel,
    module_get_function,
    memcpy_h_to_d,
    memcpy_d_to_h,
    synchronize,
    Function,
    Module,
    DeviceAllocation,
    DevicePointer,
    DEFAULT_STREAM
};

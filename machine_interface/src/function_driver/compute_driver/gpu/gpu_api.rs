#[cfg(feature = "hip")]
pub mod gpu_api {
    include!("hip.rs");
}

#[cfg(feature = "cuda")]
pub mod gpu_api {
    include!("cuda.rs");
}

pub use gpu_api::{
    initialize,
    limit_heap_size,
    create_context,
    get_device_count,
    set_device,
    module_load_data,
    module_launch_kernel,
    module_get_function,
    memcpy_h_to_d,
    memcpy_d_to_h,
    Function,
    Module,
    DeviceAllocation,
    DevicePointer,
    DEFAULT_STREAM
};

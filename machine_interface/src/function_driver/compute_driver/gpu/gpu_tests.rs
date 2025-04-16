#[cfg(feature = "hip")]
mod hip_tests;

#[cfg(feature = "cuda")]
mod cuda_tests;

mod load_utils;
mod tests_utils;

use crate::function_driver::{
    compute_driver::gpu::{GpuProcessDriver, GpuThreadDriver},
    Driver,
};
use dandelion_commons::records::{Archive, ArchiveInit, RecordPoint};
use std::sync::{Arc, Mutex};

// To force tests to run sequentially as we might otherwise run out of GPU memory
lazy_static::lazy_static! {
    static ref GPU_LOCK: Mutex<()> = Mutex::new(());
}

fn get_driver() -> Box<dyn Driver> {
    #[cfg(all(feature = "gpu_process", feature = "gpu_thread"))]
    panic!("gpu_process and gpu_thread enabled simultaneously");

    #[cfg(not(any(feature = "gpu_process", feature = "gpu_thread")))]
    panic!("Neither gpu_process nor gpu_thread enabled");

    #[cfg(feature = "gpu_process")]
    return Box::new(GpuProcessDriver {});

    #[cfg(feature = "gpu_thread")]
    return Box::new(GpuThreadDriver {});
}

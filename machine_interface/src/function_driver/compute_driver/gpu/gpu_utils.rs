// TODO remove unneeded imports; just took everything from wasm.rs
use crate::memory_domain::Context;
use dandelion_commons::{DandelionError, DandelionResult};
use libc::c_void;
use std::collections::HashMap;

use self::super::{config_parsing::GridSizing, hip};

use super::hip::DevicePointer;

pub fn get_data_length(ident: &str, context: &Context) -> DandelionResult<usize> {
    let dataset = context
        .content
        .iter()
        .find(|&elem| match elem {
            Some(set) => set.ident == ident,
            _ => false,
        })
        .ok_or(DandelionError::ConfigMissmatch)?
        .as_ref()
        .unwrap(); // okay, as we matched successfully

    let length = dataset
        .buffers
        .iter()
        .fold(0usize, |acc, item| acc + item.data.size);

    Ok(length)
}

pub fn copy_data_to_device(
    ident: &str,
    context: &Context,
    base: *mut u8,
    dev_ptr: &DevicePointer,
) -> DandelionResult<()> {
    let dataset = context
        .content
        .iter()
        .find(|&elem| match elem {
            Some(set) => set.ident == ident,
            _ => false,
        })
        .ok_or(DandelionError::ConfigMissmatch)?
        .as_ref()
        .unwrap(); // okay, as we matched successfully

    let mut total = 0isize;
    for item in &dataset.buffers {
        let length = item.data.size;
        let offset = item.data.offset;
        let src = unsafe { base.byte_offset((offset) as isize) } as *const c_void;
        hip::memcpy_h_to_d(dev_ptr, total, src, length)?;
        total += length as isize;
    }
    Ok(())
}

// subject to change; not super happy with this
pub fn get_grid_size(
    gs: &GridSizing,
    buffers: &HashMap<String, (usize, usize)>,
) -> DandelionResult<u32> {
    match gs {
        GridSizing::Absolute(size) => Ok(*size),
        GridSizing::CoverBuffer {
            bufname,
            dimensionality,
            block_dim,
        } => {
            if *dimensionality > 3 || *dimensionality == 0 {
                return Err(DandelionError::ConfigMissmatch);
            }
            let bufsize = buffers
                .get(bufname)
                .ok_or(DandelionError::ConfigMissmatch)?
                .1;
            let side_length = (bufsize as f64).powf(1.0 / (*dimensionality as f64)).ceil() as u32;
            Ok((side_length + block_dim - 1) / block_dim)
        }
    }
}

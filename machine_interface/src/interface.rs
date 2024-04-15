use std::collections::HashMap;

#[cfg(feature = "gpu")]
use crate::function_driver::compute_driver::gpu::{buffer_pool::BufferPool, hip};

use crate::{
    memory_domain::{Context, ContextState, ContextTrait},
    DataItem, DataSet, Position,
};
use dandelion_commons::{DandelionError, DandelionResult};
use libc::{c_int, c_void, size_t, uintptr_t};
extern crate alloc;
use std::fmt::Debug;

pub trait SizedIntTrait
where
    Self: Sized + Copy + Default + Debug,
{
    fn from_native(ptr: usize) -> DandelionResult<Self>;
    fn to_native(self) -> DandelionResult<usize>;
}

/// macro to convert usize to SizeT
macro_rules! size_t {
    ($val:expr) => {
        SizeT::from_native($val)?
    };
}
/// macro to convert SizeT to usize
macro_rules! usize {
    ($val:expr) => {
        SizeT::to_native($val)?
    };
}
/// macro to convert usize to PtrT
macro_rules! ptr_t {
    ($val:expr) => {
        PtrT::from_native($val)?
    };
}
/// macro to convert PtrT to usize
macro_rules! usize_ptr {
    ($val:expr) => {
        PtrT::to_native($val)?
    };
}

#[allow(dead_code)]
pub mod _32_bit {
    use super::*;

    impl SizedIntTrait for u32 {
        fn from_native(ptr: usize) -> DandelionResult<u32> {
            u32::try_from(ptr).map_err(|_| DandelionError::UsizeTypeConversionError)
        }
        fn to_native(self) -> DandelionResult<usize> {
            usize::try_from(self).map_err(|_| DandelionError::UsizeTypeConversionError)
        }
    }

    pub type DandelionSystemData = super::DandelionSystemData<u32, u32>;
}

#[allow(dead_code)]
pub mod _64_bit {
    use super::*;

    impl SizedIntTrait for u64 {
        fn from_native(ptr: usize) -> DandelionResult<u64> {
            u64::try_from(ptr).map_err(|_| DandelionError::UsizeTypeConversionError)
        }
        fn to_native(self) -> DandelionResult<usize> {
            usize::try_from(self).map_err(|_| DandelionError::UsizeTypeConversionError)
        }
    }

    pub type DandelionSystemData = super::DandelionSystemData<u64, u64>;
}

#[allow(dead_code)]
pub mod _native {
    use super::*;

    pub type DandelionSystemDataNative = DandelionSystemData<uintptr_t, size_t>;

    impl SizedIntTrait for uintptr_t {
        fn from_native(ptr: usize) -> DandelionResult<uintptr_t> {
            Ok(ptr as uintptr_t)
        }
        fn to_native(self) -> DandelionResult<usize> {
            Ok(self as usize)
        }
    }
}

#[derive(Debug, Clone, Default)]
#[repr(C)]
pub struct DandelionSystemData<PtrT: SizedIntTrait, SizeT: SizedIntTrait> {
    exit_code: c_int,
    heap_begin: PtrT,       // uintptr_t,
    heap_end: PtrT,         // uintptr_t,
    input_sets_len: SizeT,  // size_t,
    input_sets: PtrT,       // *const IoSetInfo,
    output_sets_len: SizeT, // size_t,
    output_sets: PtrT,      // *const IoSetInfo,
    input_bufs: PtrT,       // *const IoBufferDescriptor,
    output_bufs: PtrT,      // *const IoBufferDescriptor,
}

#[derive(Clone, Debug)]
#[repr(C)]
struct IoSetInfo<PtrT: SizedIntTrait, SizeT: SizedIntTrait> {
    ident: PtrT,      // uintptr_t,
    ident_len: SizeT, // size_t,
    offset: SizeT,    // size_t,
}

#[derive(Clone)]
#[repr(C)]
struct IoBufferDescriptor<PtrT: SizedIntTrait, SizeT: SizedIntTrait> {
    ident: PtrT,      // uintptr_t,
    ident_len: SizeT, // size_t,
    data: PtrT,       // uintptr_t,
    data_len: SizeT,  // size_t,
    key: SizeT,       // size_t,
}

#[cfg(feature = "gpu")]
/// Only really used for GPU, but defined here so we can access private fields of DandelionSystemData
pub fn write_gpu_outputs<PtrT: SizedIntTrait, SizeT: SizedIntTrait>(
    context: &mut Context,
    system_data_offset: usize,
    base: *mut u8,
    output_set_names: &[String],
    device_buffers: &HashMap<String, (usize, usize)>,
    buffer_pool: &BufferPool,
) -> DandelionResult<()> {
    // read the system buffer
    let mut system_struct = DandelionSystemData::<PtrT, SizeT>::default();
    context.read(
        system_data_offset,
        core::slice::from_mut(&mut system_struct),
    )?;

    let output_set_number = usize!(system_struct.output_sets_len);
    let mut output_set_info = vec![];
    if output_set_info.try_reserve(output_set_number + 1).is_err() {
        return Err(DandelionError::OutOfMemory);
    }
    let empty_output_set = IoSetInfo::<PtrT, SizeT> {
        ident: ptr_t!(0),
        ident_len: size_t!(0),
        offset: size_t!(0),
    };
    output_set_info.resize_with(output_set_number + 1, || empty_output_set.clone());
    context.read(usize_ptr!(system_struct.output_sets), &mut output_set_info)?;

    let mut output_buffers: Vec<IoBufferDescriptor<PtrT, SizeT>> = Vec::new();
    if output_buffers
        .try_reserve_exact(output_set_names.len())
        .is_err()
    {
        return Err(DandelionError::OutOfMemory);
    }
    for (i, output_name) in output_set_names.iter().enumerate() {
        // alignment shouldn't really make a huge difference
        let (dev_ptr_idx, size) = device_buffers
            .get(output_name)
            .ok_or(DandelionError::ConfigMissmatch)?;
        let buf_offset = context.get_free_space(*size, 8)?;

        let dst = unsafe { base.byte_offset(buf_offset as isize) } as *const c_void;
        let dev_ptr = buffer_pool.get(*dev_ptr_idx)?;
        hip::memcpy_d_to_h(dst, &dev_ptr, *size)?;

        output_buffers.push(IoBufferDescriptor {
            ident: ptr_t!(0),
            ident_len: size_t!(0),
            data: ptr_t!(buf_offset),
            data_len: size_t!(*size),
            key: size_t!(0),
        });
        output_set_info[i].offset = size_t!(i);
    }
    output_set_info[output_set_number].offset = size_t!(output_set_number);

    context.write(usize_ptr!(system_struct.output_sets), &output_set_info)?;

    let output_buffers_offset: PtrT =
        ptr_t!(context.get_free_space_and_write_slice(&output_buffers[..])? as usize);

    system_struct.output_bufs = output_buffers_offset;

    context.write(system_data_offset, core::slice::from_ref(&system_struct))?;
    Ok(())
}

pub fn setup_input_structs<PtrT: SizedIntTrait, SizeT: SizedIntTrait>(
    context: &mut Context,
    system_data_offset: usize,
    output_set_names: &Vec<String>,
) -> DandelionResult<()> {
    // prepare information to set up input sets, output sets and input buffers
    let input_buffer_number = context
        .content
        .iter()
        .map(|set_opt| {
            set_opt
                .as_ref()
                .and_then(|set| Some(set.buffers.len()))
                .unwrap_or(0)
        })
        .sum();
    let input_set_number = context.content.len();
    let output_set_number = output_set_names.len();

    let mut input_buffers: Vec<IoBufferDescriptor<PtrT, SizeT>> = Vec::new();
    if input_buffers
        .try_reserve_exact(input_buffer_number)
        .is_err()
    {
        return Err(DandelionError::OutOfMemory);
    }

    let mut input_sets: Vec<IoSetInfo<PtrT, SizeT>> = Vec::new();
    if input_sets
        .try_reserve_exact(context.content.len() + 1)
        .is_err()
    {
        return Err(DandelionError::OutOfMemory);
    }
    // start writing input set info structs
    for c in 0..context.content.len() {
        // get name and length
        let (name, buffer_len) = context.content[c]
            .as_ref()
            .and_then(|set| Some((set.ident.clone(), set.buffers.len())))
            .unwrap_or((String::from(""), 0));
        let name_length = name.len();
        // find space and write string
        let mut string_offset = 0;
        if name_length != 0 {
            string_offset = context.get_free_space_and_write_slice(name.as_bytes())? as usize;
        }
        input_sets.push(IoSetInfo::<PtrT, SizeT> {
            ident: ptr_t!(string_offset),
            ident_len: size_t!(name_length),
            offset: size_t!(input_buffers.len()),
        });
        // find buffers
        for b in 0..buffer_len {
            let (name, offset, size, key) = context.content[c]
                .as_ref()
                .and_then(|set| {
                    let buffer = &set.buffers[b];
                    return Some((
                        buffer.ident.clone(),
                        buffer.data.offset,
                        buffer.data.size,
                        buffer.key,
                    ));
                })
                .unwrap_or((String::from(""), 0, 0, 0));
            let name_length = name.len();
            let mut string_offset = 0;
            if name_length != 0 {
                string_offset = context.get_free_space_and_write_slice(name.as_bytes())? as usize;
            }
            input_buffers.push(IoBufferDescriptor::<PtrT, SizeT> {
                ident: ptr_t!(string_offset),
                ident_len: size_t!(name_length),
                data: ptr_t!(offset),
                data_len: size_t!(size),
                key: size_t!(key as usize),
            });
        }
    }
    // write input sentinel set
    input_sets.push(IoSetInfo::<PtrT, SizeT> {
        ident: ptr_t!(0),
        ident_len: size_t!(0),
        offset: size_t!(input_buffers.len()),
    });

    let mut output_sets: Vec<IoSetInfo<PtrT, SizeT>> = Vec::new();
    if output_sets
        .try_reserve_exact(output_set_names.len() + 1)
        .is_err()
    {
        return Err(DandelionError::OutOfMemory);
    }

    // start writing output set info structs
    for out_set_name in output_set_names.iter() {
        // insert the name into the context
        let mut string_offset = 0;
        let string_len = out_set_name.len();
        if string_len != 0 {
            string_offset =
                context.get_free_space_and_write_slice(out_set_name.as_bytes())? as usize;
        }
        output_sets.push(IoSetInfo::<PtrT, SizeT> {
            ident: ptr_t!(string_offset),
            ident_len: size_t!(string_len),
            offset: size_t!(0),
        });
    }
    output_sets.push(IoSetInfo::<PtrT, SizeT> {
        ident: ptr_t!(0),
        ident_len: size_t!(0),
        offset: size_t!(0),
    });

    let input_sets_offset: PtrT =
        ptr_t!(context.get_free_space_and_write_slice(&input_sets[..])? as usize);
    let output_sets_offset: PtrT =
        ptr_t!(context.get_free_space_and_write_slice(&output_sets[..])? as usize);
    let input_buffers_offset: PtrT =
        ptr_t!(context.get_free_space_and_write_slice(&input_buffers[..])? as usize);

    let heap_begin: PtrT = ptr_t!(context.get_last_item_end());
    let heap_end: PtrT = ptr_t!(context.size - 128);

    // fill in data for input sets
    // input set number and pointer (offset)
    // needs to happen after to get correct lower bound on stack
    let system_buffer = DandelionSystemData::<PtrT, SizeT> {
        exit_code: 0,
        heap_begin,
        heap_end,
        input_sets_len: size_t!(input_set_number),
        input_sets: input_sets_offset,
        output_sets_len: size_t!(output_set_number),
        output_sets: output_sets_offset,
        input_bufs: input_buffers_offset,
        output_bufs: PtrT::default(),
    };

    context.write(system_data_offset, core::slice::from_ref(&system_buffer))?;
    Ok(())
}

pub fn read_output_structs<PtrT: SizedIntTrait, SizeT: SizedIntTrait>(
    context: &mut Context,
    base_address: usize,
) -> DandelionResult<()> {
    context.clear_metadata();
    // read the system buffer
    let mut system_struct = DandelionSystemData::<PtrT, SizeT>::default();
    context.read(base_address, core::slice::from_mut(&mut system_struct))?;

    // get exit value
    let exit_value = system_struct.exit_code;
    context.state = ContextState::Run(exit_value);

    // get output set number +1 for sentinel set
    let output_set_number = usize!(system_struct.output_sets_len);
    if output_set_number == 0 {
        return Ok(());
    }
    let output_buffers_offset: usize = usize_ptr!(system_struct.output_bufs);
    // load output set info, + 1 to include sentinel set
    let mut output_set_info = vec![];
    if output_set_info.try_reserve(output_set_number + 1).is_err() {
        return Err(DandelionError::OutOfMemory);
    }
    let empty_output_set = IoSetInfo::<PtrT, SizeT> {
        ident: ptr_t!(0),
        ident_len: size_t!(0),
        offset: size_t!(0),
    };
    output_set_info.resize_with(output_set_number + 1, || empty_output_set.clone());
    context.read(usize_ptr!(system_struct.output_sets), &mut output_set_info)?;

    let mut output_sets = vec![];
    if output_sets.try_reserve(output_set_number).is_err() {
        return Err(DandelionError::OutOfMemory);
    }

    let output_buffer_number: usize = usize!(output_set_info[output_set_number].offset);

    let mut output_buffers = vec![];
    if output_buffers.try_reserve(output_buffer_number).is_err() {
        return Err(DandelionError::OutOfMemory);
    }
    let empty_output_buffer = IoBufferDescriptor::<PtrT, SizeT> {
        ident: ptr_t!(0),
        ident_len: size_t!(0),
        data: ptr_t!(0),
        data_len: size_t!(0),
        key: size_t!(0),
    };
    output_buffers.resize_with(output_buffer_number, || empty_output_buffer.clone());
    context.read(output_buffers_offset, &mut output_buffers)?;
    assert_eq!(
        output_buffers
            .as_ptr()
            .align_offset(std::mem::align_of::<IoBufferDescriptor<PtrT, SizeT>>()),
        0
    );
    assert_eq!(output_buffers.len(), output_buffer_number);

    for output_set in 0..output_set_number {
        let ident_offset = usize_ptr!(output_set_info[output_set].ident);
        let ident_length = usize!(output_set_info[output_set].ident_len);
        let mut set_ident = vec![0u8; ident_length];
        context.read(ident_offset, &mut set_ident)?;
        let set_ident_string = String::from_utf8(set_ident).unwrap_or("".to_string());
        let first_buffer = usize!(output_set_info[output_set].offset);
        let one_past_last_buffer = usize!(output_set_info[output_set + 1].offset);
        let buffer_number = one_past_last_buffer - first_buffer;
        let mut buffers = Vec::new();
        if buffers.try_reserve(buffer_number).is_err() {
            return Err(DandelionError::OutOfMemory);
        }
        for buffer_index in first_buffer..one_past_last_buffer {
            let buffer_ident_offset = usize_ptr!(output_buffers[buffer_index].ident);
            let buffer_ident_length = usize!(output_buffers[buffer_index].ident_len);
            let mut buffer_ident = vec![0u8; buffer_ident_length];
            context.read(buffer_ident_offset, &mut buffer_ident)?;
            let data_offset = usize_ptr!(output_buffers[buffer_index].data);
            let data_length = usize!(output_buffers[buffer_index].data_len);
            let key = usize!(output_buffers[buffer_index].key);
            let ident_string = String::from_utf8(buffer_ident).unwrap_or("".to_string());
            buffers.push(DataItem {
                ident: ident_string,
                data: Position {
                    offset: data_offset,
                    size: data_length,
                },
                key: key as u32,
            });
            context.occupy_space(data_offset, data_length)?;
        }
        // always need to push the set, to keep the numbering
        output_sets.push(Some(DataSet {
            ident: set_ident_string,
            buffers: buffers,
        }));
    }
    context.content = output_sets;
    Ok(())
}

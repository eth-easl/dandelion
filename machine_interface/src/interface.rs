use crate::{
    memory_domain::{Context, ContextTrait},
    DataItem, DataSet, Position,
};
use dandelion_commons::{DandelionError, DandelionResult};
use libc::{c_int, size_t, uintptr_t};
extern crate alloc;

#[repr(C)]
struct DandelionSystemData {
    exit_code: c_int,
    heap_begin: uintptr_t,
    heap_end: uintptr_t,
    input_sets_len: size_t,
    input_sets: *const IoSetInfo,
    output_sets_len: size_t,
    output_sets: *const IoSetInfo,
    input_bufs: *const IoBuffer,
    output_bufs: *const IoBuffer,
}

#[repr(C)]
struct IoSetInfo {
    ident: uintptr_t,
    ident_len: size_t,
    offset: size_t,
}

#[repr(C)]
struct IoBuffer {
    ident: uintptr_t,
    ident_len: size_t,
    data: uintptr_t,
    data_len: size_t,
}

const SYSTEM_STRUCT_SIZE: usize = core::mem::size_of::<DandelionSystemData>();
const IO_SET_INFO_SIZE: usize = core::mem::size_of::<IoSetInfo>();
const IO_BUFFER_SIZE: usize = core::mem::size_of::<IoBuffer>();

pub fn setup_input_structs(
    context: &mut Context,
    system_data_offset: usize,
    output_set_names: &Vec<String>,
) -> DandelionResult<()> {
    // prepare information to set up input sets, output sets and input buffers
    let input_buffer_number = context.content.iter().fold(0, |acc, set| {
        acc + set
            .as_ref()
            .and_then(|s| Some(s.buffers.len()))
            .unwrap_or(0)
    });
    let input_set_number = context.content.len();
    let output_set_number = output_set_names.len();

    let io_info_size = input_buffer_number * IO_BUFFER_SIZE
        + (input_set_number + output_set_number + 2) * IO_SET_INFO_SIZE;
    let io_info_offset = context.get_free_space(io_info_size, 8)?;

    // input and output io buffer pointers (output just 0 as the application sets them)
    let mut io_data_vec = Vec::new();
    if io_data_vec.try_reserve_exact(io_info_size).is_err() {
        return Err(DandelionError::OutOfMemory);
    }
    io_data_vec.resize(io_info_size, 0);
    let in_set_buffer = unsafe {
        core::slice::from_raw_parts_mut(
            io_data_vec.as_mut_ptr() as *mut IoSetInfo,
            input_set_number + 1,
        )
    };
    let out_set_buffer = unsafe {
        core::slice::from_raw_parts_mut(
            (io_data_vec.as_mut_ptr() as *mut IoSetInfo).add(input_set_number + 1),
            output_set_number + 1,
        )
    };
    let input_buffers = unsafe {
        core::slice::from_raw_parts_mut(
            io_data_vec
                .as_mut_ptr()
                .add((input_set_number + output_set_number + 2) * IO_SET_INFO_SIZE)
                as *mut IoBuffer,
            input_buffer_number,
        )
    };
    // start writing input set info structs
    let mut buffer_count = 0;
    for set_index in 0..input_set_number {
        // get name and length
        let (name, buffer_num) = match &context.content[set_index] {
            None => (vec![], 0),
            Some(set) => (set.ident.as_bytes().to_vec(), set.buffers.len()),
        };
        let name_length = name.len();
        // find space and write string
        let mut string_offset = 0;
        if name_length != 0 {
            string_offset = context.get_free_space(name.len(), 8)?;
            context.write(string_offset, name)?;
        }
        in_set_buffer[set_index].ident = string_offset;
        in_set_buffer[set_index].ident_len = name_length;
        in_set_buffer[set_index].offset = buffer_count;
        // find buffers
        for buffer_index in buffer_count..buffer_count + buffer_num {
            if let Some(set) = &context.content[set_index] {
                let buffer = &set.buffers[buffer_index - buffer_count];
                let name = buffer.ident.as_bytes().to_vec();
                let name_length = name.len();
                let offset = buffer.data.offset;
                let size = buffer.data.size;
                let mut string_offset = 0;
                if name_length != 0 {
                    string_offset = context.get_free_space(name_length, 8)?;
                    context.write(string_offset, name)?;
                }
                input_buffers[buffer_index].ident = string_offset;
                input_buffers[buffer_index].ident_len = name_length;
                input_buffers[buffer_index].data = offset;
                input_buffers[buffer_index].data_len = size;
            }
        }
        buffer_count += buffer_num;
    }
    // write input sentinel set
    in_set_buffer[input_set_number].offset = buffer_count;

    // start writing output set info structs
    for (index, out_set_name) in output_set_names.iter().enumerate() {
        // insert the name into the context
        let mut string_offset = 0;
        let string_len = out_set_name.len();
        if string_len != 0 {
            string_offset = context.get_free_space(out_set_name.len(), 8)?;
            context.write(string_offset, out_set_name.as_bytes().to_vec())?;
        }
        out_set_buffer[index].ident = string_offset;
        out_set_buffer[index].ident_len = string_len;
    }
    context.write(io_info_offset, io_data_vec)?;

    // fill in data for input sets
    // input set number and pointer (offset)
    // needs to happen after to get correct lower bound on stack
    let system_buffer = Box::new(DandelionSystemData {
        exit_code: 0,
        heap_begin: context.get_last_item_end(),
        heap_end: (context.size - 128),
        input_sets_len: input_set_number,
        input_sets: io_info_offset as *const IoSetInfo,
        output_sets_len: output_set_number,
        output_sets: (io_info_offset + (input_set_number + 1) * IO_SET_INFO_SIZE)
            as *const IoSetInfo,
        input_bufs: (io_info_offset + (input_set_number + output_set_number + 2) * IO_SET_INFO_SIZE)
            as *const IoBuffer,
        output_bufs: core::ptr::null(),
    });
    let system_vec = unsafe {
        alloc::vec::Vec::<u8>::from_raw_parts(
            Box::into_raw(system_buffer) as *mut u8,
            SYSTEM_STRUCT_SIZE,
            SYSTEM_STRUCT_SIZE,
        )
    };
    context.write(system_data_offset, system_vec)?;

    return Ok(());
}

pub fn read_output_structs(context: &mut Context, base_address: usize) -> DandelionResult<()> {
    // read the system buffer
    let system_struct_vec = context.read(base_address, SYSTEM_STRUCT_SIZE)?;
    // transform
    let system_struct = unsafe { &*(system_struct_vec.as_ptr() as *const DandelionSystemData) };
    // get exit value
    let _exit_value = system_struct.exit_code;
    // get output set number +1 for sentinel set
    let output_set_number = system_struct.output_sets_len;
    if output_set_number == 0 {
        context.content = vec![];
        return Ok(());
    }
    let output_set_info_offset: usize = system_struct.output_sets as usize;
    let output_buffers_offset: usize = system_struct.output_bufs as usize;
    // load output set info, + 1 to include sentinel set
    let output_set_vec = context.read(
        output_set_info_offset,
        (output_set_number + 1) * IO_SET_INFO_SIZE,
    )?;
    let output_set_info = unsafe {
        core::slice::from_raw_parts(
            output_set_vec.as_ptr() as *const IoSetInfo,
            output_set_number + 1,
        )
    };
    let mut output_sets = vec![];
    if output_sets.try_reserve(output_set_number).is_err() {
        return Err(DandelionError::OutOfMemory);
    }
    let output_buffer_number = output_set_info[output_set_number].offset;

    let output_buffer_vec =
        context.read(output_buffers_offset, output_buffer_number * IO_BUFFER_SIZE)?;
    let output_buffers = unsafe {
        core::slice::from_raw_parts(
            output_buffer_vec.as_ptr() as *const IoBuffer,
            output_buffer_number,
        )
    };

    for output_set in 0..output_set_number {
        let ident_offset = output_set_info[output_set].ident;
        let ident_length = output_set_info[output_set].ident_len;
        let set_ident = context.read(ident_offset, ident_length)?;
        let set_ident_string = String::from_utf8(set_ident).unwrap_or("".to_string());
        let first_buffer = output_set_info[output_set].offset;
        let one_past_last_buffer = output_set_info[output_set + 1].offset;
        let buffer_number = one_past_last_buffer - first_buffer;
        let mut buffers = Vec::new();
        if buffers.try_reserve(buffer_number).is_err() {
            return Err(DandelionError::OutOfMemory);
        }
        for buffer_index in first_buffer..one_past_last_buffer {
            let buffer_ident_offset = output_buffers[buffer_index].ident as usize;
            let buffer_ident_length = output_buffers[buffer_index].ident_len;
            let buffer_ident = context.read(buffer_ident_offset, buffer_ident_length)?;
            let data_offset = output_buffers[buffer_index].data as usize;
            let data_length = output_buffers[buffer_index].data_len;
            let ident_string = String::from_utf8(buffer_ident).unwrap_or("".to_string());
            buffers.push(DataItem {
                ident: ident_string,
                data: Position {
                    offset: data_offset,
                    size: data_length,
                },
            })
        }
        // only add output set if there are actual buffers for it.
        output_sets.push(Some(DataSet {
            ident: set_ident_string,
            buffers: buffers,
        }));
    }
    context.content = output_sets;
    Ok(())
}

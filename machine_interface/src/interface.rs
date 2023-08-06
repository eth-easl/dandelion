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
    input_bufs: *const IoBufferDescriptor,
    output_bufs: *const IoBufferDescriptor,
}

#[repr(C)]
struct IoSetInfo {
    ident: uintptr_t,
    ident_len: size_t,
    offset: size_t,
}

#[repr(C)]
struct IoBufferDescriptor {
    ident: uintptr_t,
    ident_len: size_t,
    data: uintptr_t,
    data_len: size_t,
}

const SYSTEM_STRUCT_SIZE: usize = core::mem::size_of::<DandelionSystemData>();
const IO_SET_INFO_SIZE: usize = core::mem::size_of::<IoSetInfo>();
const IO_BUFFER_SIZE: usize = core::mem::size_of::<IoBufferDescriptor>();

pub fn setup_input_structs(
    context: &mut Context,
    system_data_offset: usize,
    output_set_names: Vec<String>,
) -> DandelionResult<()> {
    // prepare information to set up input sets, output sets and input buffers
    let input_buffer_number = context.content.iter().map(|set| set.buffers.len()).sum();
    let input_set_number = context.content.len();
    let output_set_number = output_set_names.len();

    let mut input_buffers: Vec<IoBufferDescriptor> = Vec::new();
    if input_buffers
        .try_reserve_exact(input_buffer_number)
        .is_err()
    {
        return Err(DandelionError::OutOfMemory);
    }

    let mut input_sets: Vec<IoSetInfo> = Vec::new();
    if input_sets
        .try_reserve_exact(context.content.len() + 1)
        .is_err()
    {
        return Err(DandelionError::OutOfMemory);
    }
    // start writing input set info structs
    for c in 0..context.content.len() {
        // get name and length
        let name = context.content[c].ident.as_bytes().to_vec();
        let name_length = name.len();
        // find space and write string
        let mut string_offset = 0;
        if name_length != 0 {
            string_offset = context.get_free_space(name.len(), 8)?;
            context.write(string_offset, name)?;
        }
        input_sets.push(IoSetInfo {
            ident: string_offset,
            ident_len: name_length,
            offset: input_buffers.len(),
        });
        // find buffers
        for b in 0..context.content[c].buffers.len() {
            let buffer = &context.content[c].buffers[b];
            let name = buffer.ident.as_bytes().to_vec();
            let name_length = name.len();
            let offset = buffer.data.offset;
            let size = buffer.data.size;
            let mut string_offset = 0;
            if name_length != 0 {
                string_offset = context.get_free_space(name_length, 8)?;
                context.write(string_offset, name)?;
            }
            input_buffers.push(IoBufferDescriptor {
                ident: string_offset,
                ident_len: name_length,
                data: offset,
                data_len: size,
            });
        }
    }
    // write input sentinel set
    input_sets.push(IoSetInfo {
        ident: 0,
        ident_len: 0,
        offset: input_buffers.len(),
    });

    let mut output_sets: Vec<IoSetInfo> = Vec::new();
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
            string_offset = context.get_free_space(out_set_name.len(), 8)?;
            context.write(string_offset, out_set_name.as_bytes().to_vec())?;
        }
        output_sets.push(IoSetInfo {
            ident: string_offset,
            ident_len: string_len,
            offset: 0,
        });
    }

    let input_sets_offset = context.get_free_space_and_write_slice(&input_sets[..])?;
    let output_sets_offset = context.get_free_space_and_write_slice(&output_sets[..])?;
    let input_buffers_offset = context.get_free_space_and_write_slice(&input_buffers[..])?;

    // fill in data for input sets
    // input set number and pointer (offset)
    // needs to happen after to get correct lower bound on stack
    let system_buffer = DandelionSystemData {
        exit_code: 0,
        heap_begin: context.get_last_item_end(),
        heap_end: (context.size - 128),
        input_sets_len: input_set_number,
        input_sets: input_sets_offset,
        output_sets_len: output_set_number,
        output_sets: output_sets_offset,
        input_bufs: input_buffers_offset,
        output_bufs: core::ptr::null(),
    };
    let system_buffer_data =
        unsafe { safe_transmute::to_bytes::transmute_to_bytes_unchecked(&system_buffer) };
    assert_eq!(
        system_data_offset % std::mem::align_of::<DandelionSystemData>(),
        0
    );
    let mut system_buffer_vec: Vec<u8> = Vec::new();
    if system_buffer_vec
        .try_reserve_exact(system_buffer_data.len())
        .is_err()
    {
        return Err(DandelionError::OutOfMemory);
    }

    context.write(system_data_offset, system_buffer_vec)?;
    Ok(())
}

pub fn read_output_structs(context: &mut Context, base_address: usize) -> DandelionResult<()> {
    // read the system buffer
    let system_struct_vec = context.read(base_address, SYSTEM_STRUCT_SIZE)?;
    assert_eq!(
        system_struct_vec
            .as_ptr()
            .align_offset(std::mem::align_of::<DandelionSystemData>()),
        0
    );
    let system_struct: &DandelionSystemData = unsafe {
        safe_transmute::base::from_bytes_pedantic(&system_struct_vec[..])
            .expect("wrong size for system struct")
    };
    // get exit value
    let _exit_value = system_struct.exit_code;
    // get output set number +1 for sentinel set
    let output_set_number = system_struct.output_sets_len;
    if output_set_number == 0 {
        context.content = vec![];
        return Ok(());
    }
    let output_buffers_offset: usize = system_struct.output_bufs as usize;
    // load output set info, + 1 to include sentinel set
    let output_set_vec = context.read(
        system_struct.output_sets as usize,
        (output_set_number + 1) * IO_SET_INFO_SIZE,
    )?;
    assert_eq!(
        output_set_vec
            .as_ptr()
            .align_offset(std::mem::align_of::<IoSetInfo>()),
        0
    );
    let output_set_info = unsafe {
        safe_transmute::base::transmute_many::<IoSetInfo, safe_transmute::guard::AllOrNothingGuard>(
            &output_set_vec[..],
        )
        .expect("invalid output_set_info")
    };
    assert_eq!(output_set_info.len(), output_set_number + 1);

    let mut output_sets = vec![];
    if output_sets.try_reserve(output_set_number).is_err() {
        return Err(DandelionError::OutOfMemory);
    }
    // TODO could this be another field on DandelionSystemData instead?
    let output_buffer_number = output_set_info[output_set_number].offset;

    let output_buffer_vec =
        context.read(output_buffers_offset, output_buffer_number * IO_BUFFER_SIZE)?;
    assert_eq!(
        output_buffer_vec
            .as_ptr()
            .align_offset(std::mem::align_of::<IoBufferDescriptor>()),
        0
    );
    let output_buffers = unsafe {
        safe_transmute::base::transmute_many::<
            IoBufferDescriptor,
            safe_transmute::guard::AllOrNothingGuard,
        >(&output_buffer_vec[..])
        .expect("invalid output_buffers")
    };
    assert_eq!(output_buffers.len(), output_buffer_number);

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
        output_sets.push(DataSet {
            ident: set_ident_string,
            buffers: buffers,
        });
    }
    context.content = output_sets;
    Ok(())
}

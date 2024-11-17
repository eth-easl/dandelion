use std::{
    fs::File,
    io::{self, Read},
    mem,
};

use crate::{memory_domain::Context, DataItem, DataSet, Position};

pub fn read_tensor_from_file(file_path: &str, file_folder: &str) -> io::Result<Vec<f32>> {
    let full_path = format!("{file_folder}/{file_path}.bin");

    let mut file = File::open(&full_path)?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;

    let floats: &[f32] = unsafe {
        std::slice::from_raw_parts(
            buffer.as_ptr() as *const f32,
            buffer.len() / mem::size_of::<f32>(),
        )
    };

    Ok(floats.to_vec())
}

pub fn add_number(name: &str, value: i64, mut function_context: &mut Context) {
    let offset = function_context
        .get_free_space_and_write_slice(&vec![value as i64])
        .expect("Should have space");
    function_context.content.push(Some(DataSet {
        ident: name.to_string(),
        buffers: vec![DataItem {
            ident: name.to_string(),
            data: Position {
                offset: offset as usize,
                size: 8,
            },
            key: 0,
        }],
    }));
}

pub fn add_empty_buffer(name: &str, size: usize, mut function_context: &mut Context) {
    let offset = function_context
        .get_free_space_and_write_slice(&vec![0f32; size / 4])
        .expect("Should have space");
    function_context.content.push(Some(DataSet {
        ident: name.to_string(),
        buffers: vec![DataItem {
            ident: name.to_string(),
            data: Position {
                offset: offset as usize,
                size: size,
            },
            key: 0,
        }],
    }));
}

pub fn add_buffer(name: &str, size: usize, path: &str, mut function_context: &mut Context) {
    let offset = function_context
        .get_free_space_and_write_slice(&read_tensor_from_file(&name, &path).unwrap())
        .expect("Should have space");
    function_context.content.push(Some(DataSet {
        ident: name.to_string(),
        buffers: vec![DataItem {
            ident: name.to_string(),
            data: Position {
                offset: offset as usize,
                size: size,
            },
            key: 0,
        }],
    }));
}

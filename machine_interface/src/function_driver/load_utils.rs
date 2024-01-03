use crate::{
    memory_domain::{read_only::ReadOnlyContext, transefer_memory, Context, MemoryDomain},
    DataItem, DataRequirementList, DataSet, Position,
};
use dandelion_commons::{DandelionError, DandelionResult};

pub fn load_u8_from_file(full_path: String) -> DandelionResult<Vec<u8>> {
    let mut file = match std::fs::File::open(full_path) {
        Ok(f) => f,
        Err(_) => return Err(DandelionError::FileError),
    };

    let mut buffer = Vec::<u8>::new();
    use std::io::Read;
    let _file_size = match file.read_to_end(&mut buffer) {
        Ok(s) => s,
        Err(_) => return Err(DandelionError::FileError),
    };
    return Ok(buffer);
}

pub fn load_static(
    domain: &Box<dyn MemoryDomain>,
    static_context: &Context,
    requirement_list: &DataRequirementList,
) -> DandelionResult<Context> {
    let mut function_context = domain.acquire_context(requirement_list.size)?;

    if static_context.content.len() != 1 {
        return Err(DandelionError::ConfigMissmatch);
    }
    // copy sections to the new context
    let static_set = static_context.content[0]
        .as_ref()
        .ok_or(DandelionError::ConfigMissmatch)?;
    if static_set.buffers.len() != requirement_list.static_requirements.len() {
        return Err(DandelionError::ConfigMissmatch);
    }
    let layout = &static_set.buffers;
    let static_pairs = layout
        .iter()
        .zip(requirement_list.static_requirements.iter());
    let mut max_end = 0;
    for (item, requirement) in static_pairs {
        let position = item.data;
        if requirement.size < position.size {
            return Err(DandelionError::ConfigMissmatch);
        }
        transefer_memory(
            &mut function_context,
            static_context,
            requirement.offset,
            position.offset,
            position.size,
        )?;
        max_end = core::cmp::max(max_end, requirement.offset + requirement.size);
    }
    // round up to next page
    max_end = ((max_end + 4095) / 4096) * 4096;
    function_context.occupy_space(0, max_end)?;
    return Ok(function_context);
}

/// prepares the stdio set, specifically argv and environ,
/// as well as the python root and script folders
pub fn load_python_user_env(
    script_path: &std::path::Path,
    script_name: String,
) -> DandelionResult<(Context, Context)> {
    // load script dir and make into context
    let mut item_list = Vec::new();
    let mut data = Vec::new();
    load_python_files(script_path, String::from(""), &mut item_list, &mut data);
    let mut script_context = ReadOnlyContext::new(data.into_boxed_slice())?;
    script_context.content.push(Some(DataSet {
        ident: String::from(""),
        buffers: item_list,
    }));
    // make stdio set
    let mut argv = format!("python3\0/scripts/{}\0", script_name)
        .as_bytes()
        .to_vec();
    let argv_len = argv.len();
    let mut env = format!("PYTHONHOME=/pylib\0PYTHONPATH=/pylib/lib\0LC_ALL=POSIX\0")
        .as_bytes()
        .to_vec();
    let env_len = env.len();
    argv.append(&mut env);
    let mut stdio_context = ReadOnlyContext::new(argv.into_boxed_slice())?;
    stdio_context.content.push(Some(DataSet {
        ident: String::from("stdio"),
        buffers: vec![
            DataItem {
                ident: String::from("argv"),
                data: Position {
                    offset: 0,
                    size: argv_len,
                },
                key: 0,
            },
            DataItem {
                ident: String::from("environ"),
                data: Position {
                    offset: argv_len,
                    size: env_len,
                },
                key: 0,
            },
        ],
    }));

    return Ok((script_context, stdio_context));
}

pub fn load_python_root(path: &std::path::Path) -> DandelionResult<Context> {
    let mut item_list = Vec::new();
    let mut data = Vec::new();
    load_python_files(path, String::from("lib"), &mut item_list, &mut data);
    println!("total root size: {}", data.len());
    let mut context = ReadOnlyContext::new(data.into_boxed_slice())?;
    context.content.push(Some(DataSet {
        ident: String::from("pylib"),
        buffers: item_list,
    }));
    return Ok(context);
}

/// takes a path to a python folder containing a module
/// loads each file into the context and creates the corresponding data items
fn load_python_files(
    path: &std::path::Path,
    function_path: String,
    item_list: &mut Vec<DataItem>,
    data: &mut Vec<u8>,
) {
    let dir_iterator = std::fs::read_dir(path).expect("Could not open python directory");
    for entry_result in dir_iterator {
        let entry = entry_result.expect("Should be able to get entry");
        if entry.file_type().expect("").is_dir() {
            let mut new_function_path = function_path.clone();
            new_function_path.push('/');
            new_function_path.push_str(
                entry
                    .file_name()
                    .as_os_str()
                    .to_str()
                    .expect("folder name should be str"),
            );
            load_python_files(entry.path().as_path(), new_function_path, item_list, data);
            continue;
        }
        let name = format!(
            "{}/{}",
            function_path,
            entry
                .file_name()
                .into_string()
                .expect("File name should be sting")
        );
        if name.ends_with(".py") {
            // println!("Adding file: {}", name);
            let mut file_array = match std::fs::read(entry.path()) {
                Ok(array) => array,
                Err(_) => continue,
            };
            let file_offset = data.len();
            // println!("name: {}", name);
            item_list.push(DataItem {
                ident: name,
                data: crate::Position {
                    offset: file_offset,
                    size: file_array.len(),
                },
                key: 0,
            });
            data.append(&mut file_array);
        }
    }
}

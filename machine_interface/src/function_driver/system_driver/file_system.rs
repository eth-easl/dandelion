use crate::{
    memory_domain::{
        system_domain::{DataPosition, SystemContext},
        Context, ContextTrait,
    },
    promise::Debt,
    DataItem, DataSet, Position,
};
use core::slice;
use dandelion_commons::{records::Recorder, DandelionError, DandelionResult, SysFunctionError};
use libc::{c_void, mmap, munmap};
use log::{debug, trace};
use std::{collections::BTreeMap, fs::File, os::fd::AsRawFd, ptr};

#[derive(Debug)]
pub(crate) struct FileMapping {
    data_ptr: *const u8,
    offset: usize,
    size: usize,
}

unsafe impl Send for FileMapping {}
unsafe impl Sync for FileMapping {}

impl FileMapping {
    pub(crate) fn get_slice(&self) -> &[u8] {
        unsafe {
            let start_pointer = self.data_ptr.add(self.offset);
            slice::from_raw_parts(start_pointer, self.size)
        }
    }
}

impl Drop for FileMapping {
    fn drop(&mut self) {
        unsafe {
            munmap(self.data_ptr as *mut c_void, self.size);
        }
    }
}

/// Function that reads all the paths from a context and returns:
/// - item name
/// - item key
/// - tripple of file path, scan start offset and size of chunk that is part of the read.
fn get_paths(mut context: Context) -> DandelionResult<Vec<(String, u32, (String, u64, usize))>> {
    let path_set = match context.content.iter_mut().find(|set_option| {
        if let Some(set) = set_option {
            set.ident == "paths"
        } else {
            false
        }
    }) {
        Some(set) => set.take().unwrap(),
        _ => {
            return Err(DandelionError::SysFunctionError(
                SysFunctionError::InvalidArg(String::from("No paths set")),
            ))
        }
    };
    trace!("Starting to parse paths");
    path_set
        .buffers
        .into_iter()
        .map(|path_item| {
            let mut path_buffer = Vec::with_capacity(path_item.data.size);
            path_buffer.resize(path_item.data.size, 0u8);
            context.read(path_item.data.offset, &mut path_buffer)?;
            trace!("paring path from path buffer: {:?}", path_buffer);
            let path_str = std::str::from_utf8(&path_buffer)
                .and_then(|path_string| Ok(path_string))
                .or(Err(DandelionError::SysFunctionError(
                    SysFunctionError::InvalidArg(String::from(
                        "Path given to file function not a utf8 string",
                    )),
                )))?;
            let trpple = if let Some((path, range_string)) = path_str.split_once('\n') {
                let (start_str, end_str) = range_string.split_once('-').ok_or_else(|| {
                    DandelionError::SysFunctionError(SysFunctionError::InvalidArg(format!(
                        "File path with range has invalid range: {}",
                        path_str
                    )))
                })?;
                let start = start_str.parse().or_else(|_| {
                    Err(DandelionError::SysFunctionError(
                        SysFunctionError::InvalidArg(format!(
                            "Could not parse range start in {}",
                            path_str
                        )),
                    ))
                })?;
                let size = end_str.parse().or_else(|_| {
                    Err(DandelionError::SysFunctionError(
                        SysFunctionError::InvalidArg(format!(
                            "Could not parse range start in {}",
                            path_str
                        )),
                    ))
                })?;
                (path.to_string(), start, size)
            } else {
                (path_str.to_string(), 0, usize::MAX)
            };
            Ok((path_item.ident, path_item.key, trpple))
        })
        .collect::<DandelionResult<Vec<_>>>()
}

pub(super) fn file_load(context: Context, dept: Debt, mut recorder: Recorder) {
    let names_and_paths = match get_paths(context) {
        Ok(p) => p,
        Err(err) => {
            dept.fulfill(Err(err));
            return;
        }
    };

    let mut new_map = BTreeMap::new();
    let mut offset = 0usize;
    let mut content = Vec::with_capacity(names_and_paths.len());
    for (item_name, key, path) in names_and_paths.into_iter() {
        let (size, file_path) = match File::open(&path.0) {
            Ok(f) => {
                let file_length = match f.metadata() {
                    Ok(meta) => usize::try_from(meta.len()).unwrap(),
                    Err(f_error) => {
                        dept.fulfill(Err(DandelionError::SysFunctionError(
                            SysFunctionError::FileError(f_error.kind()),
                        )));
                        return;
                    }
                };
                // need to set to smaller range than file size if the range was set.
                let map_length = usize::min(file_length, path.2);
                // offset needs to be rounded to page
                let page_offset = (path.1 / 4096) * 4096;
                let offset = usize::try_from(path.1 - page_offset).unwrap();
                let map_ptr = unsafe {
                    mmap(
                        ptr::null_mut(),
                        map_length,
                        libc::PROT_READ,
                        libc::MAP_PRIVATE,
                        f.as_raw_fd(),
                        page_offset as i64,
                    )
                };
                if map_ptr == libc::MAP_FAILED {
                    debug!(
                        "faild to mmap file with input: len {}, fd: {}, offset: {}",
                        map_length,
                        f.as_raw_fd(),
                        page_offset as i64
                    );
                    dept.fulfill(Err(DandelionError::SysFunctionError(
                        SysFunctionError::FileError(std::io::Error::last_os_error().kind()),
                    )));
                    return;
                }
                let file_map = FileMapping {
                    data_ptr: map_ptr as *const u8,
                    offset,
                    size: map_length,
                };
                (
                    map_length,
                    DataPosition::File(std::sync::Arc::new(file_map)),
                )
            }
            Err(error) => {
                dept.fulfill(Err(DandelionError::SysFunctionError(
                    SysFunctionError::FileError(error.kind()),
                )));
                return;
            }
        };
        content.push(DataItem {
            ident: item_name,
            data: Position {
                offset: offset,
                size,
            },
            key,
        });
        new_map.insert(offset, (file_path, size));
        offset += size;
    }

    let mut new_context = Context::new(
        crate::memory_domain::ContextType::System(Box::new(SystemContext {
            local_offset_to_data_position: new_map,
            size: offset,
        })),
        offset,
    );
    new_context.content = vec![Some(DataSet {
        ident: String::from("files"),
        buffers: content,
    })];

    recorder.record(dandelion_commons::records::RecordPoint::EngineEnd);
    drop(recorder);
    dept.fulfill(Ok(crate::function_driver::WorkDone::Context(new_context)));
}

pub(super) fn metadata_load(context: Context, debt: Debt, mut recorder: Recorder) {
    let names_and_paths = match get_paths(context) {
        Ok(p) => p,
        Err(err) => {
            debt.fulfill(Err(err));
            return;
        }
    };

    let mut new_map = BTreeMap::new();
    let mut offset = 0usize;
    let mut content = Vec::with_capacity(names_and_paths.len());
    for (item_name, key, path) in names_and_paths.into_iter() {
        let file_size = match File::open(path.0) {
            Ok(f) => match f.metadata() {
                Ok(meta) => meta.len(),
                Err(f_error) => {
                    debt.fulfill(Err(DandelionError::SysFunctionError(
                        SysFunctionError::FileError(f_error.kind()),
                    )));
                    return;
                }
            },
            Err(error) => {
                debt.fulfill(Err(DandelionError::SysFunctionError(
                    SysFunctionError::FileError(error.kind()),
                )));
                return;
            }
        };
        let item_size = size_of::<u64>();
        content.push(DataItem {
            ident: item_name,
            data: Position {
                offset: offset,
                size: item_size,
            },
            key,
        });
        new_map.insert(
            offset,
            (
                DataPosition::Vec(file_size.to_le_bytes().to_vec()),
                item_size,
            ),
        );
        offset += item_size;
    }

    let context_size = content.len() * 8;
    let mut new_context = Context::new(
        crate::memory_domain::ContextType::System(Box::new(SystemContext {
            local_offset_to_data_position: new_map,
            size: context_size,
        })),
        context_size,
    );
    new_context.content = vec![Some(DataSet {
        ident: String::from("files"),
        buffers: content,
    })];

    recorder.record(dandelion_commons::records::RecordPoint::EngineEnd);
    drop(recorder);
    debt.fulfill(Ok(crate::function_driver::WorkDone::Context(new_context)));
}

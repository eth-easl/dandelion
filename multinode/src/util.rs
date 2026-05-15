use std::sync::Arc;

// For types which have the same name for prot and machine_interface,
// use the full ones to make sure there is no mix ups
use dandelion_commons::{err_dandelion, DandelionError, DandelionResult, MultinodeError};
use machine_interface::{
    composition::{CompositionSet, ItemData},
    function_driver::{functions::SystemFunction, system_driver::IoData},
    machine_config,
    memory_domain::{bytes_context::BytesContext, Context, ContextType},
    Position,
};
use prost::bytes::{Bytes, BytesMut};

use crate::proto::{self, metadata_item};

/// Translates dandelion engine types to protocol engine types.
pub(crate) fn engine_type_dtop(t: machine_config::EngineType) -> proto::EngineType {
    match t {
        #[cfg(feature = "reqwest_io")]
        machine_config::EngineType::System => proto::EngineType::EngineReqwest,
        #[cfg(feature = "cheri")]
        machine_config::EngineType::Cheri => proto::EngineType::EngineCheri,
        #[cfg(feature = "mmu")]
        machine_config::EngineType::Process => proto::EngineType::EngineProcess,
        #[cfg(feature = "kvm")]
        machine_config::EngineType::Kvm => proto::EngineType::EngineKvm,
    }
}

/// Translates protocol engine types to dandelion engine types.
pub(crate) fn engine_type_ptod(t: i32) -> DandelionResult<machine_config::EngineType> {
    match proto::EngineType::try_from(t).unwrap() {
        #[cfg(feature = "reqwest_io")]
        proto::EngineType::EngineReqwest => Ok(machine_config::EngineType::System),
        #[cfg(feature = "cheri")]
        proto::EngineType::EngineCheri => Ok(machine_config::EngineType::Cheri),
        #[cfg(feature = "mmu")]
        proto::EngineType::EngineProcess => Ok(machine_config::EngineType::Process),
        #[cfg(feature = "kvm")]
        proto::EngineType::EngineKvm => Ok(machine_config::EngineType::Kvm),
        _ => err_dandelion!(DandelionError::Multinode(MultinodeError::ConfigError(
            "Unknown engine type!".to_string(),
        ))),
    }
}

pub(crate) fn system_function_ptod(proto_function: i32) -> DandelionResult<SystemFunction> {
    match proto::SystemFunction::try_from(proto_function).unwrap() {
        proto::SystemFunction::Http => Ok(SystemFunction::HTTP),
    }
}

pub(crate) fn system_function_dtop(
    dandelion_function: &SystemFunction,
) -> DandelionResult<proto::SystemFunction> {
    match dandelion_function {
        SystemFunction::HTTP => Ok(proto::SystemFunction::Http),
        _ => unimplemented!(),
    }
}

/// Takes a `CompositionSet` reference and translates it into a protocol data set.
fn composition_set_to_proto(set: &CompositionSet, offset: &mut u64) -> proto::MetadataSet {
    let mut items = Vec::with_capacity(set.len());
    for (item, data) in set {
        match data {
            ItemData::LocalData(_) => {
                // don't need to send items with no size
                if item.data.size > 0 {
                    *offset += item.data.size as u64;
                    items.push(proto::MetadataItem {
                        ident: item.ident.clone(),
                        key: item.key,
                        data: Some(metadata_item::Data::LocalEndOffset(*offset)),
                    });
                }
            }
            ItemData::IoData(io_data) => {
                let IoData {
                    original_item,
                    original_data: _,
                    function,
                    set_index,
                } = io_data;
                if original_item.data.size > 0 {
                    *offset += original_item.data.size as u64;
                    items.push(proto::MetadataItem {
                        ident: item.ident.clone(),
                        key: item.key,
                        data: Some(metadata_item::Data::IoData(proto::IoData {
                            set_index: *set_index as u64,
                            end_offset: *offset,
                            function: system_function_dtop(function).unwrap() as i32,
                        })),
                    });
                }
            }
            ItemData::RemoteData() => todo!(),
        }
    }
    // assigning name equal to index, as they are ignored on the receiver node anyway
    // so the effort to get the correct name would be wasted.
    proto::MetadataSet {
        ident: set.get_name().clone(),
        items,
    }
}

/// Takes a (reference to a) vector of optional `CompositionSet` instances and translates each of
/// them into the corresponding protocol data set.
pub(crate) fn composition_sets_to_proto(
    sets: Vec<Option<CompositionSet>>,
) -> (
    Vec<proto::MetadataSet>,
    Option<(Vec<Option<CompositionSet>>, u64)>,
) {
    let mut metadata_sets = Vec::with_capacity(sets.len());
    let mut offset: u64 = 0;

    for set in sets.iter() {
        if let Some(s) = set {
            metadata_sets.push(composition_set_to_proto(s, &mut offset));
        } else {
            metadata_sets.push(proto::MetadataSet {
                ident: format!("empty_set"),
                items: vec![],
            });
        }
    }
    if offset > 0 {
        (metadata_sets, Some((sets, offset)))
    } else {
        (metadata_sets, None)
    }
}

/// Takes a (reference to a) vector of protocol data sets, translates them into a `BytesContext`
/// and returns a vector of optional `CompositionSet` that hold the reference to the context.
pub(crate) fn proto_data_sets_to_composition_sets(
    proto_sets: Vec<proto::MetadataSet>,
    data_buf: Option<Bytes>,
) -> Vec<Option<CompositionSet>> {
    // TODO: expect data buffer, not option, option does not make sense here if we expect any actual data items to be in there.
    // TODO: also does not need to be split, simply converting from MetaDataItems to items should be enough
    // create context sets with correct offsets to the buffer
    let mut sets = Vec::with_capacity(proto_sets.len());

    let mut mut_buffer = data_buf.map(|buf| BytesMut::from(buf));
    let mut last_end_offset = 0;

    for protobuf_set in proto_sets.into_iter() {
        let mut item_list = Vec::with_capacity(protobuf_set.items.len());
        for protobuf_item in protobuf_set.items.into_iter() {
            match protobuf_item.data.unwrap() {
                metadata_item::Data::LocalEndOffset(offset) => {
                    assert!(last_end_offset < offset as usize);
                    let end_offset = offset as usize;
                    let buffer_size = end_offset - last_end_offset;
                    let new_frame = mut_buffer.as_mut().unwrap().split_to(buffer_size).freeze();
                    let new_context = Arc::new(Context::new(
                        ContextType::Bytes(Box::new(BytesContext::new(vec![new_frame]))),
                        buffer_size,
                    ));
                    item_list.push((
                        machine_interface::DataItem {
                            ident: protobuf_item.ident,
                            key: protobuf_item.key,
                            data: Position {
                                offset: 0,
                                size: buffer_size,
                            },
                        },
                        ItemData::LocalData(new_context),
                    ));
                    last_end_offset = end_offset;
                }
                metadata_item::Data::IoData(io_data) => {
                    let proto::IoData {
                        end_offset: offset,
                        function,
                        set_index,
                    } = io_data;
                    let end_offset = offset as usize;
                    assert!(last_end_offset < end_offset as usize);
                    let buffer_size = end_offset - last_end_offset;
                    let new_frame = mut_buffer.as_mut().unwrap().split_to(buffer_size).freeze();
                    let new_context = Arc::new(Context::new(
                        ContextType::Bytes(Box::new(BytesContext::new(vec![new_frame]))),
                        buffer_size,
                    ));
                    let buffer_size = end_offset - last_end_offset;
                    item_list.push((
                        machine_interface::DataItem {
                            ident: String::new(),
                            data: Position { offset: 0, size: 0 },
                            key: protobuf_item.key,
                        },
                        ItemData::IoData(IoData {
                            original_item: machine_interface::DataItem {
                                ident: String::new(),
                                data: Position {
                                    offset: 0,
                                    size: buffer_size,
                                },
                                key: protobuf_item.key,
                            },
                            original_data: new_context,
                            function: system_function_ptod(function).unwrap(),
                            set_index: set_index as usize,
                        }),
                    ));
                    last_end_offset = end_offset;
                }
                _ => todo!(),
            };
        }
        sets.push(CompositionSet::from_item_list(
            protobuf_set.ident,
            item_list,
        ));
    }

    sets
}

const METADATA_SIZE_BITS: u32 = 32;
const METADATA_FLAG_BITS: u32 = 32;

// Metadata Flags
pub const NO_FLAGS: u32 = 0x0;
pub const ADDITIONAL_DATA_BUFFER: u32 = 0x1;

pub fn pack_metadata_size_and_flags(size: u32, flags: u32) -> u64 {
    let size_part = size as u64;
    let flags_part = (flags as u64) << METADATA_SIZE_BITS;
    flags_part | size_part
}

pub fn unpack_metadata_size_and_flags(packed: u64) -> (u32, u32) {
    let size_mask = (1 << METADATA_SIZE_BITS) - 1;
    let flags_mask = (1 << METADATA_FLAG_BITS) - 1;

    let size = (packed & size_mask) as u32;
    let flags = ((packed >> METADATA_SIZE_BITS) & flags_mask) as u32;

    (size, flags)
}

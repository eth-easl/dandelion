use std::sync::Arc;

// For types which have the same name for prot and machine_interface,
// use the full ones to make sure there is no mix ups
use dandelion_commons::{err_dandelion, DandelionError, DandelionResult, MultinodeError};
use machine_interface::{
    composition::{CompositionSet, ItemData, RemoteData},
    function_driver::{functions::SystemFunction, system_driver::IoData},
    machine_config,
    memory_domain::{bytes_context::BytesContext, Context, ContextType},
    DataItem, Position,
};
use prost::bytes::{Bytes, BytesMut};
use tokio::sync::OnceCell;

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

fn remote_data_dtop(remote_data: RemoteData) -> proto::RemoteData {
    proto::RemoteData {
        node_id: remote_data.node_id,
        data_id: remote_data.data_id,
    }
}

fn remote_data_ptod(remote_data: proto::RemoteData) -> RemoteData {
    RemoteData {
        node_id: remote_data.node_id,
        data_id: remote_data.data_id,
    }
}

// TODO: find a better interface for exporting data
/// Takes a (reference to a) vector of optional `CompositionSet` instances and translates each of
/// them into the corresponding protocol data set.
pub(crate) fn composition_sets_to_proto(
    sets: Vec<Option<CompositionSet>>,
    mut export_local_data: impl FnMut(&DataItem, Arc<Context>) -> RemoteData,
) -> (
    Vec<proto::MetadataSet>,
    Option<(Vec<Option<CompositionSet>>, u64)>,
) {
    let mut metadata_sets = Vec::with_capacity(sets.len());
    let mut offset: u64 = 0;

    for set in sets.iter() {
        if let Some(s) = set {
            let mut metadata_items = Vec::with_capacity(s.len());
            for (item, data) in s {
                match data {
                    ItemData::LocalData(context) => {
                        let remote_data = export_local_data(item, context.clone());
                        metadata_items.push(proto::MetadataItem {
                            ident: item.ident.clone(),
                            key: item.key,
                            data: Some(metadata_item::Data::RemoteData(remote_data_dtop(
                                remote_data,
                            ))),
                        });
                    }
                    ItemData::IoData(io_data) => {
                        let IoData {
                            original_position,
                            original_data: _,
                            resolved: _,
                            function,
                            set_index,
                        } = io_data;
                        // TODO: fuse with serialization, so we can use the resolved without race conditions
                        if original_position.size > 0 {
                            offset += original_position.size as u64;
                            metadata_items.push(proto::MetadataItem {
                                ident: item.ident.clone(),
                                key: item.key,
                                data: Some(metadata_item::Data::IoData(proto::IoData {
                                    set_index: *set_index as u64,
                                    end_offset: offset,
                                    function: system_function_dtop(function).unwrap() as i32,
                                })),
                            });
                        }
                    }
                    ItemData::RemoteData(remote_data) => {
                        metadata_items.push(proto::MetadataItem {
                            ident: item.ident.clone(),
                            key: item.key,
                            data: Some(metadata_item::Data::RemoteData(remote_data_dtop(
                                *remote_data,
                            ))),
                        });
                    }
                }
            }
            metadata_sets.push(proto::MetadataSet {
                ident: s.get_name().clone(),
                items: metadata_items,
            });
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
                            ident: protobuf_item.ident,
                            data: Position { offset: 0, size: 0 },
                            key: protobuf_item.key,
                        },
                        ItemData::IoData(IoData {
                            original_position: Position {
                                offset: 0,
                                size: buffer_size,
                            },
                            resolved: OnceCell::new(),
                            original_data: new_context,
                            function: system_function_ptod(function).unwrap(),
                            set_index: set_index as usize,
                        }),
                    ));
                    last_end_offset = end_offset;
                }
                metadata_item::Data::RemoteData(remote_data) => {
                    item_list.push((
                        machine_interface::DataItem {
                            ident: protobuf_item.ident,
                            key: protobuf_item.key,
                            data: Position { offset: 0, size: 0 },
                        },
                        ItemData::RemoteData(remote_data_ptod(remote_data)),
                    ));
                }
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

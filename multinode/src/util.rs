use std::sync::Arc;

// For types which have the same name for prot and machine_interface,
// use the full ones to make sure there is no mix ups
use dandelion_commons::{err_dandelion, DandelionError, DandelionResult, MultinodeError};
use machine_interface::composition::{CompositionSet, CompositionSetData};
use machine_interface::memory_domain::bytes_context::BytesContext;
use machine_interface::memory_domain::{Context, ContextType};
use machine_interface::{machine_config, Position};
use prost::bytes::{Bytes, BytesMut};

use crate::proto;

/// Translates dandelion engine types to protocol engine types.
pub fn engine_type_dtop(t: machine_config::EngineType) -> proto::EngineType {
    match t {
        #[cfg(feature = "reqwest_io")]
        machine_config::EngineType::Reqwest => proto::EngineType::EngineReqwest,
        #[cfg(feature = "cheri")]
        machine_config::EngineType::Cheri => proto::EngineType::EngineCheri,
        #[cfg(feature = "mmu")]
        machine_config::EngineType::Process => proto::EngineType::EngineProcess,
        #[cfg(feature = "kvm")]
        machine_config::EngineType::Kvm => proto::EngineType::EngineKvm,
    }
}

/// Translates protocol engine types to dandelion engine types.
pub fn engine_type_ptod(t: i32) -> DandelionResult<machine_config::EngineType> {
    match proto::EngineType::from_i32(t).unwrap() {
        #[cfg(feature = "reqwest_io")]
        proto::EngineType::EngineReqwest => Ok(machine_config::EngineType::Reqwest),
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

/// Takes a `CompositionSet` reference and translates it into a protocol data set.
pub fn composition_set_to_proto(
    set: &CompositionSet,
    offset: &mut u64,
    data_set: &mut Vec<CompositionSetData>,
) -> proto::MetadataSet {
    let set_idx = set.get_set_idx();
    let mut items = Vec::with_capacity(set.len());
    for itm_idx in 0..set.len() {
        let (ident, key, data) = set.get_item_metadata(itm_idx);
        *offset += data.size as u64;
        items.push(proto::MetadataItem {
            ident,
            key,
            end_offset: *offset,
        });
        data_set.push(data);
    }
    // assigning name equal to index, as they are ignored on the receiver node anyway
    // so the effort to get the correct name would be wasted.
    proto::MetadataSet {
        ident: format!("set_{}", set_idx),
        items,
    }
}

/// Takes a (reference to a) vector of optional `CompositionSet` instances and translates each of
/// them into the corresponding protocol data set.
pub fn composition_sets_to_proto(
    sets: &Vec<Option<CompositionSet>>,
) -> (Vec<proto::MetadataSet>, (Vec<CompositionSetData>, u64)) {
    let mut metadata_sets = Vec::with_capacity(sets.len());
    let mut data_set = Vec::with_capacity(sets.len());
    let mut offset: u64 = 0;

    for set in sets.iter() {
        if let Some(s) = set {
            metadata_sets.push(composition_set_to_proto(s, &mut offset, &mut data_set));
        } else {
            metadata_sets.push(proto::MetadataSet {
                ident: format!("empty_set"),
                items: vec![],
            });
        }
    }
    (metadata_sets, (data_set, offset))
}

/// Takes a (reference to a) vector of protocol data sets and translates them into a `BytesContext`
/// that contains all of the sets.
pub fn proto_data_sets_to_context(
    protobuf_sets: Vec<proto::MetadataSet>,
    data_buf: Option<Bytes>,
) -> Context {
    // create context sets with correct offsets to the buffer
    let mut sets = Vec::with_capacity(protobuf_sets.len());
    let mut frames = Vec::new();
    let mut last_end_offset = 0usize;

    let mut mut_data_buf;
    if let Some(buf) = data_buf {
        mut_data_buf = Some(BytesMut::from(buf));
    } else {
        mut_data_buf = None;
    }

    for protobuf_set in protobuf_sets.into_iter() {
        let mut items = Vec::with_capacity(protobuf_set.items.len());
        for protobuf_itm in protobuf_set.items.into_iter() {
            // let data_ptr = protobuf_itm.data.as_ptr();
            let new_frame;
            let buf_size = protobuf_itm.end_offset as usize - last_end_offset;

            if buf_size > 0 {
                new_frame = mut_data_buf.as_mut().unwrap().split_to(buf_size).freeze();
            } else {
                new_frame = Bytes::new();
            }

            items.push(machine_interface::DataItem {
                ident: protobuf_itm.ident,
                data: Position {
                    offset: last_end_offset,
                    size: new_frame.len(),
                },
                key: protobuf_itm.key,
            });
            frames.push(new_frame);
            last_end_offset = protobuf_itm.end_offset as usize;
        }
        sets.push(Some(machine_interface::DataSet {
            ident: protobuf_set.ident.clone(),
            buffers: items,
        }));
    }

    // create context over the protobuf
    let mut context = Context::new(
        ContextType::Bytes(Box::new(BytesContext::new(frames))),
        last_end_offset,
    );
    context.content = sets;

    context
}

/// Takes a (reference to a) vector of protocol data sets, translates them into a `BytesContext`
/// and returns a vector of optional `CompositionSet` that hold the reference to the context.
pub fn proto_data_sets_to_composition_sets(
    proto_sets: Vec<proto::MetadataSet>,
    data_buf: Option<Bytes>,
) -> Vec<Option<CompositionSet>> {
    let num_sets = proto_sets.len();
    let context = proto_data_sets_to_context(proto_sets, data_buf);
    let context_arc = Arc::new(context);
    (0..num_sets)
        .map(|set_id| Some(CompositionSet::from((set_id, vec![context_arc.clone()]))))
        .collect::<Vec<_>>()
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

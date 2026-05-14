// For types which have the same name for prot and machine_interface,
// use the full ones to make sure there is no mix ups
use dandelion_commons::{err_dandelion, DandelionError, DandelionResult, MultinodeError};
use machine_interface::{
    composition::CompositionSet,
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

/// Takes a `CompositionSet` reference and translates it into a protocol data set.
fn composition_set_to_proto(set: &CompositionSet, offset: &mut u64) -> proto::MetadataSet {
    let mut items = Vec::with_capacity(set.len());
    for (item, _) in set {
        // don't need to send items with no size
        if item.data.size > 0 {
            *offset += item.data.size as u64;
            items.push(proto::MetadataItem {
                ident: item.ident.clone(),
                key: item.key,
                data: Some(metadata_item::Data::EndOffset(*offset)),
            });
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

/// Takes a (reference to a) vector of protocol data sets and translates them into a `BytesContext`
/// that contains all of the sets.
fn proto_data_sets_to_context(
    protobuf_sets: Vec<proto::MetadataSet>,
    data_buf: Option<Bytes>,
) -> Context {
    // TODO: expect data buffer, not option, option does not make sense here if we expect any actual data items to be in there.
    // TODO: also does not need to be split, simply converting from MetaDataItems to items should be enough
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

            let end_offset = match protobuf_itm.data.unwrap() {
                metadata_item::Data::EndOffset(offset) => offset as usize,
                _ => unimplemented!(),
            };
            let buf_size = end_offset - last_end_offset;

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
            last_end_offset = end_offset;
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
pub(crate) fn proto_data_sets_to_composition_sets(
    proto_sets: Vec<proto::MetadataSet>,
    data_buf: Option<Bytes>,
) -> Vec<Option<CompositionSet>> {
    let context = proto_data_sets_to_context(proto_sets, data_buf);
    CompositionSet::from_context(context)
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

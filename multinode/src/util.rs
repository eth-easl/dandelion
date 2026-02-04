use std::sync::Arc;

use dandelion_commons::{DandelionError, DandelionResult, MultinodeError};
use machine_interface::composition::CompositionSet;
use machine_interface::memory_domain::bytes_context::BytesContext;
use machine_interface::memory_domain::{Context, ContextType};
use machine_interface::{machine_config, DataItem, DataSet, Position};
use prost::bytes::Bytes;

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
pub fn engine_type_ptod(t: proto::EngineType) -> DandelionResult<machine_config::EngineType> {
    match t {
        #[cfg(feature = "reqwest_io")]
        proto::EngineType::EngineReqwest => Ok(machine_config::EngineType::Reqwest),
        #[cfg(feature = "cheri")]
        proto::EngineType::EngineCheri => Ok(machine_config::EngineType::Cheri),
        #[cfg(feature = "mmu")]
        proto::EngineType::EngineProcess => Ok(machine_config::EngineType::Process),
        #[cfg(feature = "kvm")]
        proto::EngineType::EngineKvm => Ok(machine_config::EngineType::Kvm),
        _ => Err(DandelionError::Multinode(MultinodeError::ConfigError(
            "Unknown engine type!".to_string(),
        ))),
    }
}

/// Takes a `CompositionSet` reference and translates it into a protocol data set.
pub fn composition_set_to_proto(set: &CompositionSet) -> proto::DataSet {
    let set_idx = set.get_set_idx();
    let mut items = Vec::with_capacity(set.len());
    for itm_idx in 0..set.len() {
        let (ident, key, data) = set.get_item(itm_idx);
        items.push(proto::DataItem { ident, key, data });
    }
    proto::DataSet {
        ident: format!("set_{}", set_idx),
        items,
    }
}

/// Takes a (reference to a) vector of optional `CompositionSet` instances and translates each of
/// them into the corresponding protocol data set.
pub fn composition_sets_to_proto(sets: &Vec<Option<CompositionSet>>) -> Vec<proto::DataSet> {
    let mut serialized_sets = Vec::with_capacity(sets.len());
    for set in sets.iter() {
        if let Some(s) = set {
            serialized_sets.push(composition_set_to_proto(s));
        } else {
            serialized_sets.push(proto::DataSet {
                ident: format!("empty_set"),
                items: vec![],
            });
        }
    }
    serialized_sets
}

/// Takes a (reference to a) vector of protocol data sets and translates them into a `BytesContext`
/// that contains all of the sets.
pub fn proto_data_sets_to_context(protobuf_sets: &Vec<proto::DataSet>, buf: Bytes) -> Context {
    // create context sets with correct offsets to the buffer
    let buf_base_ptr = buf.as_ptr();
    let buf_size = buf.len();
    let mut sets = Vec::with_capacity(protobuf_sets.len());
    for protobuf_set in protobuf_sets.iter() {
        let mut items = Vec::with_capacity(protobuf_set.items.len());
        for protobuf_itm in protobuf_set.items.iter() {
            let data_ptr = protobuf_itm.data.as_ptr();
            items.push(DataItem {
                ident: protobuf_itm.ident.clone(),
                data: Position {
                    offset: unsafe { data_ptr.offset_from(buf_base_ptr) as usize },
                    size: protobuf_itm.data.len(),
                },
                key: protobuf_itm.key,
            });
        }
        sets.push(Some(DataSet {
            ident: protobuf_set.ident.clone(),
            buffers: items,
        }));
    }

    // create context over the protobuf
    let mut context = Context::new(
        ContextType::Bytes(Box::new(BytesContext::new(vec![buf]))),
        buf_size,
    );
    context.content = sets;

    context
}

/// Takes a (reference to a) vector of protocol data sets, translates them into a `BytesContext`
/// and returns a vector of optional `CompositionSet` that hold the reference to the context.
pub fn proto_data_sets_to_composition_sets(
    proto_sets: &Vec<proto::DataSet>,
    buf: Bytes,
) -> Vec<Option<CompositionSet>> {
    let num_sets = proto_sets.len();
    let context = proto_data_sets_to_context(proto_sets, buf);
    let context_arc = Arc::new(context);
    (0..num_sets)
        .map(|set_id| Some(CompositionSet::from((set_id, vec![context_arc.clone()]))))
        .collect::<Vec<_>>()
}

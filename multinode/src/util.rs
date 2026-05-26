// For types which have the same name for prot and machine_interface,
// use the full ones to make sure there is no mix ups
use crate::proto::{self, item_data};
use dandelion_commons::{
    err_dandelion, records::Recorder, DandelionError, DandelionResult, MultinodeError,
};
use machine_interface::{
    composition::{CompositionSet, ItemData, RemoteData},
    function_driver::{functions::SystemFunction, system_driver::IoData},
    machine_config,
    memory_domain::Context,
    DataItem, Position,
};
use prost::bytes::Bytes;
use std::{sync::Arc, time::Duration};
use tokio::sync::{mpsc, OnceCell};

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

pub(crate) fn recorder_dtop(
    _recorder: Recorder,
    _start_epoch: Duration,
) -> Option<proto::Timestamps> {
    #[cfg(feature = "timestamp")]
    {
        use dandelion_commons::records::RecordPoint;

        Some(proto::Timestamps {
            start_epoch: _start_epoch.as_micros() as u64,
            fetching_start: _recorder
                .get_timestamp(RecordPoint::FetchingStart)
                .as_micros() as u64,
            fetching_end: _recorder
                .get_timestamp(RecordPoint::FetchingEnd)
                .as_micros() as u64,
            parsing_start: _recorder
                .get_timestamp(RecordPoint::ParsingStart)
                .as_micros() as u64,
            parsing_end: _recorder.get_timestamp(RecordPoint::ParsingEnd).as_micros() as u64,
            load_start: _recorder.get_timestamp(RecordPoint::LoadStart).as_micros() as u64,
            transfer_start: _recorder
                .get_timestamp(RecordPoint::TransferStart)
                .as_micros() as u64,
            engine_start: _recorder
                .get_timestamp(RecordPoint::EngineStart)
                .as_micros() as u64,
            engine_end: _recorder.get_timestamp(RecordPoint::EngineEnd).as_micros() as u64,
        })
    }

    #[cfg(not(feature = "timestamp"))]
    None
}

pub(crate) fn recorder_add_timestamps(
    mut _recorder: Recorder,
    _timestamps: Option<proto::Timestamps>,
    _local_reference: u128,
) {
    #[cfg(feature = "timestamp")]
    {
        use dandelion_commons::records::RecordPoint;
        if let Some(remote_time) = _timestamps {
            let proto::Timestamps {
                start_epoch,
                fetching_start,
                fetching_end,
                parsing_start,
                parsing_end,
                load_start,
                transfer_start,
                engine_start,
                engine_end,
            } = remote_time;
            // The local RemoteTake and the local_reference were taken at approximately same time.
            // Both the local reference and the start_epoch are offsets from unix epoch start.
            // The every remote timestamp is an offset from the start_epoch.
            // To find the real timestamps, we need to add the remote timestamps to the RemoteTake value,
            // and then add the time the transfer took (start_epoch - local_reference).
            let wire_time = start_epoch - (_local_reference as u64);
            let start_offset =
                wire_time + _recorder.get_timestamp(RecordPoint::RemoteTake).as_micros() as u64;
            // set all the timestamps we received from remote
            _recorder.set_timestamp(RecordPoint::FetchingStart, fetching_start + start_offset);
            _recorder.set_timestamp(RecordPoint::FetchingEnd, fetching_end + start_offset);
            _recorder.set_timestamp(RecordPoint::ParsingStart, parsing_start + start_offset);
            _recorder.set_timestamp(RecordPoint::ParsingEnd, parsing_end + start_offset);
            _recorder.set_timestamp(RecordPoint::LoadStart, load_start + start_offset);
            _recorder.set_timestamp(RecordPoint::TransferStart, transfer_start + start_offset);
            _recorder.set_timestamp(RecordPoint::EngineStart, engine_start + start_offset);
            _recorder.set_timestamp(RecordPoint::EngineEnd, engine_end + start_offset);
        }
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

fn remote_data_dtop(remote_data: &RemoteData) -> proto::RemoteData {
    proto::RemoteData {
        node_id: remote_data.node_id,
        data_id: remote_data.data_id,
    }
}

fn remote_data_ptod(
    remote_data: proto::RemoteData,
    delete_sender: Option<mpsc::UnboundedSender<RemoteData>>,
) -> RemoteData {
    match delete_sender {
        Some(delete_sender) => {
            RemoteData::delete_on_drop(remote_data.node_id, remote_data.data_id, delete_sender)
        }
        None => RemoteData::new(remote_data.node_id, remote_data.data_id),
    }
}

fn item_data_dtop(
    item: &DataItem,
    data: ItemData,
    export_local_data: &mut impl FnMut(&DataItem, Arc<Context>) -> RemoteData,
) -> proto::ItemData {
    match data {
        ItemData::LocalData(context) => {
            let remote_data = export_local_data(item, context);
            // TODO: send data directly for smaller items
            proto::ItemData {
                data: Some(item_data::Data::RemoteData(remote_data_dtop(&remote_data))),
            }
        }
        ItemData::IoData(io_data) => {
            let IoData {
                original_position,
                original_data,
                resolved: _,
                function,
                set_index,
            } = io_data;
            // TODO for data we want to send along: fuse with serialization, so we can use the `resolved` without race conditions
            let mut register_item = item.clone();
            register_item.data = original_position;
            let data = item_data_dtop(&register_item, *original_data, export_local_data);
            proto::ItemData {
                data: Some(item_data::Data::IoData(Box::new(proto::IoData {
                    set_index: set_index as u64,
                    function: system_function_dtop(&function).unwrap() as i32,
                    data: Some(Box::new(data)),
                }))),
            }
        }
        ItemData::RemoteData(remote_data) => proto::ItemData {
            data: Some(item_data::Data::RemoteData(remote_data_dtop(&remote_data))),
        },
    }
}

fn item_data_and_ref(
    item: &DataItem,
    data: ItemData,
    export_local_data: &mut impl FnMut(&DataItem, Arc<Context>) -> RemoteData,
) -> (proto::ItemData, Option<RemoteData>) {
    match data {
        ItemData::LocalData(context) => {
            let remote_data = export_local_data(item, context);
            // TODO: send data directly for smaller items
            (
                proto::ItemData {
                    data: Some(item_data::Data::RemoteData(remote_data_dtop(&remote_data))),
                },
                Some(remote_data),
            )
        }
        ItemData::IoData(io_data) => {
            let IoData {
                original_position,
                original_data,
                resolved: _,
                function,
                set_index,
            } = io_data;
            // TODO for data we want to send along: fuse with serialization, so we can use the `resolved` without race conditions
            let mut register_item = item.clone();
            register_item.data = original_position;
            let (data, remote_data) =
                item_data_and_ref(&register_item, *original_data, export_local_data);
            (
                proto::ItemData {
                    data: Some(item_data::Data::IoData(Box::new(proto::IoData {
                        set_index: set_index as u64,
                        function: system_function_dtop(&function).unwrap() as i32,
                        data: Some(Box::new(data)),
                    }))),
                },
                remote_data,
            )
        }
        ItemData::RemoteData(remote_data) => (
            proto::ItemData {
                data: Some(item_data::Data::RemoteData(remote_data_dtop(&remote_data))),
            },
            Some(remote_data),
        ),
    }
}

fn item_data_ptod(
    data: proto::ItemData,
    delete_sender: Option<mpsc::UnboundedSender<RemoteData>>,
) -> ItemData {
    match data.data.unwrap() {
        item_data::Data::IoData(io_data) => {
            let proto::IoData {
                function,
                set_index,
                data: input_data,
            } = *io_data;
            // For when the data can be data sent along with the request
            // let end_offset = offset as usize;
            // assert!(last_end_offset < end_offset as usize);
            // let buffer_size = end_offset - last_end_offset;
            // let new_frame = mut_buffer.as_mut().unwrap().split_to(buffer_size).freeze();
            // let new_context = Arc::new(Context::new(
            //     ContextType::Bytes(Box::new(BytesContext::new(vec![new_frame]))),
            //     buffer_size,
            // ));
            // let buffer_size = end_offset - last_end_offset;

            ItemData::IoData(IoData {
                original_position: Position { offset: 0, size: 0 },
                resolved: Arc::new(OnceCell::new()),
                original_data: Box::new(item_data_ptod(*input_data.unwrap(), delete_sender)),
                function: system_function_ptod(function).unwrap(),
                set_index: set_index as usize,
            })

            // last_end_offset = end_offset;
        }
        item_data::Data::RemoteData(remote_data) => {
            ItemData::RemoteData(remote_data_ptod(remote_data, delete_sender))
        }
    }
}

// TODO: find a better interface for exporting data
/// Takes a (reference to a) vector of optional `CompositionSet` instances and translates each of
/// them into the corresponding protocol data set.
pub(crate) fn composition_sets_to_proto(
    sets: Vec<Option<CompositionSet>>,
    mut export_local_data: impl FnMut(&DataItem, Arc<Context>) -> RemoteData,
) -> Vec<proto::MetadataSet>
// TODO change to (Vec<(Position, Arc<Context>)>,u64) when we add sending data directly with the request
    // No need to keep as composition set.
    // Option<(Vec<Option<CompositionSet>>, u64)>,
{
    let mut metadata_sets = Vec::with_capacity(sets.len());
    // let mut offset: u64 = 0;

    for set_option in sets.into_iter() {
        if let Some(set) = set_option {
            let mut metadata_items = Vec::with_capacity(set.len());
            let set_name = set.get_name().clone();
            for (item, data) in set {
                metadata_items.push(proto::MetadataItem {
                    data: Some(item_data_dtop(&item, data, &mut export_local_data)),
                    ident: item.ident,
                    key: item.key,
                });
            }
            metadata_sets.push(proto::MetadataSet {
                ident: set_name,
                items: metadata_items,
            });
        } else {
            metadata_sets.push(proto::MetadataSet {
                ident: format!("empty_set"),
                items: vec![],
            });
        }
    }
    // if offset > 0 {
    //     (metadata_sets, Some((sets, offset)))
    // } else {
    // }
    metadata_sets
}

pub(crate) fn composition_sets_to_proto_and_refs(
    sets: Vec<Option<CompositionSet>>,
    mut export_local_data: impl FnMut(&DataItem, Arc<Context>) -> RemoteData,
) -> (Vec<proto::MetadataSet>, Vec<RemoteData>)
// TODO change to (Vec<(Position, Arc<Context>)>,u64) when we add sending data directly with the request
    // No need to keep as composition set.
    // Option<(Vec<Option<CompositionSet>>, u64)>,
{
    let mut metadata_sets = Vec::with_capacity(sets.len());
    let mut remote_references = Vec::new();
    // let mut offset: u64 = 0;

    for set_option in sets.into_iter() {
        if let Some(set) = set_option {
            let mut metadata_items = Vec::with_capacity(set.len());
            let set_name = set.get_name().clone();
            for (item, data) in set {
                let (proto_item, remote_data_option) =
                    item_data_and_ref(&item, data, &mut export_local_data);
                metadata_items.push(proto::MetadataItem {
                    data: Some(proto_item),
                    ident: item.ident,
                    key: item.key,
                });
                if let Some(remote_data) = remote_data_option {
                    remote_references.push(remote_data);
                }
            }
            metadata_sets.push(proto::MetadataSet {
                ident: set_name,
                items: metadata_items,
            });
        } else {
            metadata_sets.push(proto::MetadataSet {
                ident: format!("empty_set"),
                items: vec![],
            });
        }
    }
    // if offset > 0 {
    //     (metadata_sets, Some((sets, offset)))
    // } else {
    // }
    (metadata_sets, remote_references)
}

/// Takes a (reference to a) vector of protocol data sets, translates them into a `BytesContext`
/// and returns a vector of optional `CompositionSet` that hold the reference to the context.
pub(crate) fn proto_data_sets_to_composition_sets(
    proto_sets: Vec<proto::MetadataSet>,
    _data_buf: Option<Bytes>,
) -> Vec<Option<CompositionSet>> {
    proto_data_sets_to_composition_sets_inner(proto_sets, _data_buf, None)
}

pub(crate) fn proto_data_sets_to_composition_sets_with_delete_on_drop(
    proto_sets: Vec<proto::MetadataSet>,
    _data_buf: Option<Bytes>,
    delete_sender: mpsc::UnboundedSender<RemoteData>,
) -> Vec<Option<CompositionSet>> {
    proto_data_sets_to_composition_sets_inner(proto_sets, _data_buf, Some(delete_sender))
}

fn proto_data_sets_to_composition_sets_inner(
    proto_sets: Vec<proto::MetadataSet>,
    _data_buf: Option<Bytes>,
    delete_sender: Option<mpsc::UnboundedSender<RemoteData>>,
) -> Vec<Option<CompositionSet>> {
    // TODO: expect data buffer, not option, option does not make sense here if we expect any actual data items to be in there.
    // TODO: also does not need to be split, simply converting from MetaDataItems to items should be enough
    // create context sets with correct offsets to the buffer
    let mut sets = Vec::with_capacity(proto_sets.len());

    // let mut mut_buffer = data_buf.map(|buf| BytesMut::from(buf));
    // let mut last_end_offset = 0;

    for protobuf_set in proto_sets.into_iter() {
        let mut item_list = Vec::with_capacity(protobuf_set.items.len());
        for protobuf_item in protobuf_set.items.into_iter() {
            item_list.push((
                machine_interface::DataItem {
                    ident: protobuf_item.ident,
                    data: Position { offset: 0, size: 0 },
                    key: protobuf_item.key,
                },
                item_data_ptod(protobuf_item.data.unwrap(), delete_sender.clone()),
            ));
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

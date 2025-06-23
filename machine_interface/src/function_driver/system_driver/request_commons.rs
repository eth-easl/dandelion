use crate::{
    function_driver::WorkDone,
    memory_domain::{
        system_domain::{system_context_write_from_bytes, SystemContext},
        Context, ContextTrait, ContextType,
    },
    promise::Debt,
    DataItem, DataSet, Position,
};
use bytes::Buf;
use dandelion_commons::{
    records::{RecordPoint, Recorder},
    DandelionError, DandelionResult, SysFunctionError,
};
use std::{collections::BTreeMap, sync::Arc};

pub(super) trait Request
where
    Self: Sized,
{
    fn from_raw(raw_request: Vec<u8>, item_name: String, item_key: u32) -> DandelionResult<Self>;
}
pub(super) struct ResponseInformation {
    /// name of the original request data item
    pub(super) item_name: String,
    // key of the original request data item
    pub(super) item_key: u32,
    /// contains both the status line as well as all headers
    pub(super) preamble: String,
    pub(super) body: bytes::Bytes,
}

pub(super) fn parse_requests<RequestType: Request>(
    context: &Context,
) -> DandelionResult<Vec<RequestType>> {
    let request_set = match context.content.iter().find(|set_option| {
        if let Some(set) = set_option {
            return set.ident == "request";
        } else {
            return false;
        }
    }) {
        Some(Some(set)) => set,
        _ => {
            return Err(DandelionError::SysFunctionError(
                SysFunctionError::InvalidArg(String::from("No request set")),
            ))
        }
    };
    let request_info: DandelionResult<Vec<RequestType>> = request_set
        .buffers
        .iter()
        .map(|set_item| {
            let mut request_buffer = Vec::with_capacity(set_item.data.size);
            request_buffer.resize(set_item.data.size, 0);
            context.read(set_item.data.offset, &mut request_buffer)?;
            RequestType::from_raw(request_buffer, set_item.ident.clone(), set_item.key)
        })
        .collect();
    return request_info;
}

fn response_write(context: &mut Context, response: ResponseInformation) -> DandelionResult<()> {
    let ResponseInformation {
        item_name,
        item_key,
        preamble,
        mut body,
    } = response;

    let preamble_len = preamble.len();
    let body_len = body.len();
    let response_len = preamble_len + body_len;
    // allocate space in the context for the entire response
    let response_start = context.get_free_space(response_len, 128)?;

    match &mut context.context {
        ContextType::System(destination_ctxt) => {
            let preamble_bytes = bytes::Bytes::from(preamble.into_bytes());
            system_context_write_from_bytes(
                destination_ctxt,
                preamble_bytes,
                response_start,
                preamble_len,
            );
            system_context_write_from_bytes(
                destination_ctxt,
                body.clone(),
                response_start + preamble_len,
                body_len,
            );
        }
        _ => {
            context.write(response_start, preamble.as_bytes())?;
            let mut bytes_read = 0;
            while bytes_read < body_len {
                let chunk = body.chunk();
                let reading = chunk.len();
                context.write(response_start + preamble_len + bytes_read, chunk)?;
                body.advance(reading);
                bytes_read += reading;
            }
            assert_eq!(
                0,
                body.remaining(),
                "Body should have non remaining as we have read the amount given as len in the beginning"
            );
        }
    }

    if let Some(response_set) = &mut context.content[0] {
        response_set.buffers.push(DataItem {
            ident: item_name.clone(),
            key: item_key,
            data: Position {
                offset: response_start,
                size: response_len,
            },
        })
    }
    if let Some(body_set) = &mut context.content[1] {
        body_set.buffers.push(DataItem {
            ident: item_name,
            key: item_key,
            data: Position {
                offset: response_start + preamble_len,
                size: response_len - preamble_len,
            },
        })
    }

    return Ok(());
}

pub(super) fn responses_write(
    context_size: usize,
    output_set_names: Arc<Vec<String>>,
    debt: Debt,
    mut recorder: Recorder,
    responses: Vec<ResponseInformation>,
) {
    let mut out_context = Context::new(
        ContextType::System(Box::new(SystemContext {
            local_offset_to_data_position: BTreeMap::new(),
            size: context_size,
        })),
        context_size,
    );

    if !output_set_names.is_empty() {
        out_context.content = vec![None, None];
        if output_set_names.iter().any(|elem| elem == "response") {
            out_context.content[0] = Some(DataSet {
                ident: String::from("response"),
                buffers: vec![],
            })
        }
        if output_set_names.iter().any(|elem| elem == "body") {
            out_context.content[1] = Some(DataSet {
                ident: String::from("body"),
                buffers: vec![],
            })
        }
        let write_results: DandelionResult<Vec<_>> = responses
            .into_iter()
            .map(|response| response_write(&mut out_context, response))
            .collect();
        if let Err(err) = write_results {
            drop(recorder);
            debt.fulfill(Err(err));
            return;
        }
    }

    recorder.record(RecordPoint::EngineEnd);
    drop(recorder);
    debt.fulfill(Ok(WorkDone::Context(out_context)));
    return;
}

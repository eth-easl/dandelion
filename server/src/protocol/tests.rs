use super::*;
use bytes::Buf;
use dandelion_commons::trace::Recorder;
use http_body_util::BodyExt;
use memory::DataSet;
use prost::{
    encoding::{decode_varint, encode_varint, encoded_len_varint},
    Message,
};
use std::{fmt::Debug, io::IoSlice, sync::Arc, time::Instant};

fn encode(metadata: &impl Message, payload: &[u8]) -> Vec<u8> {
    let mut body = envelope::encode_header(metadata);
    body.extend_from_slice(payload);
    body
}

fn versioned(version: u64, rest: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    encode_varint(version, &mut body);
    body.extend_from_slice(rest);
    body
}

fn frame(bytes: &[u8]) -> Vec<Bytes> {
    if bytes.is_empty() {
        vec![]
    } else {
        vec![Bytes::copy_from_slice(bytes)]
    }
}

fn decode_response<M: Message + Default>(mut body: &[u8]) -> (M, &[u8]) {
    assert_eq!(decode_varint(&mut body).unwrap(), PROTOCOL_VERSION);
    let metadata = M::decode_length_delimited(&mut body).unwrap();
    (metadata, body)
}

fn request_error<T: Debug>(result: DandelionResult<T>) -> FrontendError {
    match result.unwrap_err().error {
        DandelionError::RequestError(error) => error,
        error => panic!("Unexpected error: {error:?}"),
    }
}

fn target(name: &str) -> Option<proto::dandelion_request::Target> {
    Some(proto::dandelion_request::Target::Name(name.into()))
}

#[tokio::test]
async fn test_invocation_by_name_round_trip() {
    let payload = b"123\0\xffab";
    let name = "f".repeat(128);
    let input_sets = vec![
        proto::InputSet {
            items: [0, 3, 2]
                .into_iter()
                .map(|payload_length| proto::InputItem {
                    identifier: "item".into(),
                    key: 7,
                    payload_length,
                })
                .collect(),
        },
        proto::InputSet { items: vec![] },
        proto::InputSet {
            items: vec![proto::InputItem {
                identifier: "other".into(),
                key: 9,
                payload_length: 2,
            }],
        },
    ];
    let input_set_number = input_sets.len();
    let metadata = proto::DandelionRequest {
        target: target(&name),
        sets: input_sets,
    };
    let bytes = Bytes::from(encode(&metadata, payload));
    let payload_start = bytes.len() - payload.len();
    let frames = (0..bytes.len()).map(|i| bytes.slice(i..i + 1)).collect();
    let invocation = DandelionRequest::decode(frames).unwrap();
    assert!(matches!(invocation.target, InvocationTarget::Name(ref decoded) if decoded == &name));
    assert_eq!(
        invocation.context.get_chunk_ref(0, 5).unwrap().as_ptr(),
        bytes[payload_start..].as_ptr()
    );
    let sets = invocation
        .context
        .content
        .iter()
        .map(|set| {
            set.as_ref()
                .unwrap()
                .items
                .iter()
                .map(|item| {
                    (
                        item.ident.as_str(),
                        item.key,
                        item.data.offset,
                        item.data.size,
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        sets,
        [
            vec![("item", 7, 0, 0), ("item", 7, 0, 3), ("item", 7, 3, 2)],
            vec![],
            vec![("other", 9, 5, 2)],
        ]
    );
    let mut decoded_payload = [0u8; 7];
    invocation.context.read(0, &mut decoded_payload).unwrap();
    assert_eq!(&decoded_payload, payload);

    let outputs = DataSet::from_context(invocation.context)
        .into_iter()
        .map(|set| set.items)
        .collect();
    let recorder = Recorder::new(Arc::new("f".into()), input_set_number, Instant::now());
    let mut body = DandelionBody::new(outputs, &recorder).unwrap();
    let mut buffer = body.frame().await.unwrap().unwrap().into_data().unwrap();
    let mut slices = [IoSlice::new(&[]); 16];
    let count = buffer.chunks_vectored(&mut slices);
    assert_eq!(slices[1].as_ptr(), bytes[payload_start..].as_ptr());
    let expected = slices[..count]
        .iter()
        .flat_map(|slice| slice.iter().copied())
        .collect::<Vec<_>>();
    let mut actual = Vec::new();
    while buffer.has_remaining() {
        actual.push(buffer.get_u8());
    }
    assert_eq!(actual, expected);
    assert!(body.frame().await.is_none());
    let (response, response_payload) =
        decode_response::<proto::DandelionDeserializeResponse>(&actual);
    // Empty sets are kept, so every set stays at its index.
    assert_eq!(response.sets, metadata.sets);
    assert_eq!(response_payload, payload);
}

#[test]
fn test_invocation_by_composition() {
    let metadata = proto::DandelionRequest {
        target: Some(proto::dandelion_request::Target::Composition(
            "source".into(),
        )),
        sets: vec![],
    };
    let invocation = DandelionRequest::decode(frame(&encode(&metadata, &[]))).unwrap();
    assert!(
        matches!(invocation.target, InvocationTarget::Composition(source) if source == "source")
    );
}

#[test]
fn test_function_registration_with_binary() {
    let set = |identifier: &str, static_items| proto::RegistrationInputSet {
        identifier: identifier.into(),
        static_items,
    };
    let metadata = proto::RegisterFunction {
        name: "f".into(),
        context_size: 4096,
        engine_type: "Process".into(),
        executable: Some(proto::register_function::Executable::BinaryLength(2)),
        input_sets: vec![
            set("dynamic", None),
            set("empty", Some(proto::StaticInputItems::default())),
            set(
                "static",
                Some(proto::StaticInputItems {
                    items: vec![proto::StaticInputItem {
                        identifier: "config".into(),
                        payload_length: 3,
                    }],
                }),
            ),
        ],
        output_sets: vec!["output".into()],
        min_set_bytes: vec![8],
    };
    let registration = RegisterFunction::decode(frame(&encode(&metadata, b"\0\xffcfg"))).unwrap();
    assert_eq!(registration.name, "f");
    assert_eq!(registration.context_size, 4096);
    assert_eq!(registration.engine_type, "Process");
    assert!(matches!(&registration.executable, Executable::Binary(binary) if *binary == [0, 0xff]));
    assert_eq!(
        registration.input_sets,
        [
            ("dynamic".into(), None),
            ("empty".into(), Some(vec![])),
            (
                "static".into(),
                Some(vec![("config".into(), b"cfg".to_vec())])
            ),
        ]
    );
    assert_eq!(registration.output_sets, ["output"]);
    assert_eq!(registration.min_set_bytes, [8]);
}

#[test]
fn test_function_registration_with_local_path() {
    let metadata = proto::RegisterFunction {
        name: "f".into(),
        context_size: 4096,
        engine_type: "Process".into(),
        executable: Some(proto::register_function::Executable::LocalPath(
            "path".into(),
        )),
        ..Default::default()
    };
    let registration = RegisterFunction::decode(frame(&encode(&metadata, &[]))).unwrap();
    assert!(matches!(&registration.executable, Executable::LocalPath(path) if path == "path"));
}

#[test]
fn test_composition_registration() {
    let metadata = proto::RegisterChain {
        composition: "source".into(),
    };
    let registration = RegisterChain::decode(frame(&encode(&metadata, &[]))).unwrap();
    assert_eq!(registration.composition, "source");
}

#[tokio::test]
async fn test_status_response() {
    let body = DandelionBody::status("done").collect().await.unwrap();
    let body = body.to_bytes();
    let (status, rest) = decode_response::<proto::StatusResponse>(&body);
    assert_eq!(status.message, "done");
    assert!(rest.is_empty());
}

#[test]
fn test_malformed_envelope() {
    let decode = |bytes: &[u8]| request_error(DandelionRequest::decode(frame(bytes)));
    let valid = encode(
        &proto::DandelionRequest {
            target: target("f"),
            sets: vec![],
        },
        &[],
    );
    let metadata = &valid[encoded_len_varint(PROTOCOL_VERSION)..];
    for (body, expected) in [
        // Invalid protocol version.
        (vec![], FrontendError::ViolatedSpec),
        // Unsupported protocol version.
        (
            versioned(PROTOCOL_VERSION + 1, metadata),
            FrontendError::ViolatedSpec,
        ),
        // Invalid metadata length.
        (
            versioned(PROTOCOL_VERSION, &[]),
            FrontendError::ViolatedSpec,
        ),
        (
            versioned(PROTOCOL_VERSION, &[0x80]),
            FrontendError::ViolatedSpec,
        ),
        (
            versioned(PROTOCOL_VERSION, &[0xff; 11]),
            FrontendError::ViolatedSpec,
        ),
        // Truncated metadata.
        (versioned(PROTOCOL_VERSION, &[1]), FrontendError::StreamEnd),
        // Invalid Protobuf metadata.
        (
            versioned(PROTOCOL_VERSION, &[1, 0xff]),
            FrontendError::MalformedMessage,
        ),
    ] {
        assert_eq!(decode(&body), expected, "{body:?}");
    }
}

#[test]
fn test_invalid_invocation() {
    let decode = |bytes: &[u8]| request_error(DandelionRequest::decode(frame(bytes)));
    let missing = encode(&proto::DandelionRequest::default(), &[]);
    assert_eq!(decode(&missing), FrontendError::MalformedMessage);

    let metadata = proto::DandelionRequest {
        target: target("f"),
        sets: vec![proto::InputSet {
            items: vec![proto::InputItem {
                payload_length: 3,
                ..Default::default()
            }],
        }],
    };
    assert_eq!(decode(&encode(&metadata, b"ab")), FrontendError::StreamEnd);
    assert_eq!(
        decode(&encode(&metadata, b"abcd")),
        FrontendError::ViolatedSpec
    );
}

#[test]
fn test_invalid_registration() {
    let decode = |metadata: &proto::RegisterFunction, payload: &[u8]| {
        request_error(RegisterFunction::decode(frame(&encode(metadata, payload))))
    };
    let valid = proto::RegisterFunction {
        name: "f".into(),
        engine_type: "Process".into(),
        context_size: 4096,
        executable: Some(proto::register_function::Executable::BinaryLength(2)),
        ..Default::default()
    };
    let unnamed = proto::RegisterFunction {
        name: String::new(),
        ..valid.clone()
    };
    assert_eq!(decode(&unnamed, b"ab"), FrontendError::MalformedMessage);
    let no_engine = proto::RegisterFunction {
        engine_type: String::new(),
        ..valid.clone()
    };
    assert_eq!(decode(&no_engine, b"ab"), FrontendError::MalformedMessage);
    let no_executable = proto::RegisterFunction {
        executable: None,
        ..valid.clone()
    };
    assert_eq!(decode(&no_executable, b""), FrontendError::MalformedMessage);
    assert_eq!(decode(&valid, b"a"), FrontendError::StreamEnd);
    assert_eq!(decode(&valid, b"abc"), FrontendError::ViolatedSpec);

    let chain = proto::RegisterChain {
        composition: "source".into(),
    };
    assert_eq!(
        request_error(RegisterChain::decode(frame(&encode(&chain, b"a")))),
        FrontendError::ViolatedSpec
    );
}

#[test]
fn test_invalid_output() {
    let mut context = Context::new(ContextType::Bytes(Box::new(BytesContext::new(vec![]))), 0);
    context.content = vec![Some(ContextDataSet {
        total_size: 1,
        items: vec![ContextDataItem {
            ident: "item".into(),
            key: 0,
            data: Position { offset: 0, size: 1 },
        }],
    })];
    let outputs = DataSet::from_context(context)
        .into_iter()
        .map(|set| set.items)
        .collect();
    let recorder = Recorder::new(Arc::new("f".into()), 0, Instant::now());
    let error = DandelionBody::new(outputs, &recorder).err().unwrap();
    assert_eq!(error.error, DandelionError::InvalidRead);
}

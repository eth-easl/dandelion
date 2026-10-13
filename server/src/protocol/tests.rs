use super::*;
use bytes::Buf;
use dandelion_commons::records::Recorder;
use http_body_util::BodyExt;
use machine_interface::{composition::CompositionSet, memory_domain::ContextTrait};
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
    vec![Bytes::copy_from_slice(bytes)]
}

fn decode_response<M: Message + Default>(mut body: &[u8]) -> (M, &[u8]) {
    assert_eq!(decode_varint(&mut body).unwrap(), PROTOCOL_VERSION);
    let metadata = M::decode_length_delimited(&mut body).unwrap();
    (metadata, body)
}

fn invalid_request<T: Debug>(result: DandelionResult<T>) -> String {
    match result.unwrap_err().error {
        DandelionError::RequestError(FrontendError::InvalidRequest(message)) => message,
        error => panic!("Unexpected error: {error:?}"),
    }
}

fn target(name: &str) -> Option<proto::dandelion_request::Target> {
    Some(proto::dandelion_request::Target::Name(name.into()))
}

#[tokio::test]
async fn test_happy_path() {
    // Invocation by name, delivered in one-byte frames with empty frames in between.
    let payload = b"123\0\xffab";
    let name = "f".repeat(128);
    let metadata = proto::DandelionRequest {
        target: target(&name),
        sets: vec![
            proto::InputSet {
                identifier: "input".into(),
                items: [0, 3, 2]
                    .into_iter()
                    .map(|payload_length| proto::InputItem {
                        identifier: "item".into(),
                        key: 7,
                        payload_length,
                    })
                    .collect(),
            },
            proto::InputSet {
                identifier: "empty".into(),
                items: vec![],
            },
            proto::InputSet {
                identifier: "second".into(),
                items: vec![proto::InputItem {
                    identifier: "other".into(),
                    key: 9,
                    payload_length: 2,
                }],
            },
        ],
    };
    let bytes = Bytes::from(encode(&metadata, payload));
    let payload_start = bytes.len() - payload.len();
    let mut frames = vec![Bytes::new()];
    for i in 0..bytes.len() {
        frames.push(bytes.slice(i..i + 1));
        frames.push(Bytes::new());
    }
    let invocation = DandelionRequest::decode(frames).unwrap();
    assert!(matches!(invocation.target, InvocationTarget::Name(ref decoded) if decoded == &name));
    assert_eq!(
        invocation.context.get_chunk_ref(0, 5).unwrap().as_ptr(),
        bytes[payload_start..].as_ptr()
    );
    assert!(invocation.context.content[1]
        .as_ref()
        .unwrap()
        .buffers
        .is_empty());
    let second = &invocation.context.content[2].as_ref().unwrap().buffers[0].data;
    assert_eq!((second.offset, second.size), (5, 2));

    // Invocation response, built from the decoded request without copying its payload.
    let outputs = CompositionSet::from_context(invocation.context)
        .into_iter()
        .map(|set| set.map(|set| set.into_local()))
        .collect();
    let recorder = Recorder::new(Arc::new("f".into()), Instant::now());
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
    assert_eq!(
        response.sets,
        [metadata.sets[0].clone(), metadata.sets[2].clone()]
    );
    assert_eq!(response_payload, payload);

    // Invocation by inline composition.
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

    // Function registration with an uploaded binary and static inputs.
    let set = |identifier: &str, static_items| proto::RegistrationInputSet {
        identifier: identifier.into(),
        static_items,
    };
    let metadata = proto::RegisterFunction {
        name: "f".into(),
        context_size: 4096,
        engine_type: "Process".into(),
        local_path: "path".into(),
        binary_length: 2,
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
    assert_eq!(registration.local_path, "path");
    assert_eq!(registration.binary, [0, 0xff]);
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

    // Composition registration.
    let metadata = proto::RegisterChain {
        composition: "source".into(),
    };
    let registration = RegisterChain::decode(frame(&encode(&metadata, &[]))).unwrap();
    assert_eq!(registration.composition, "source");

    // Status response.
    let body = DandelionBody::status("done").collect().await.unwrap();
    let body = body.to_bytes();
    let (status, rest) = decode_response::<proto::StatusResponse>(&body);
    assert_eq!(status.message, "done");
    assert!(rest.is_empty());
}

#[test]
fn test_malformed_envelope() {
    let decode = |bytes: &[u8]| invalid_request(DandelionRequest::decode(frame(bytes)));
    let valid = encode(
        &proto::DandelionRequest {
            target: target("f"),
            sets: vec![],
        },
        &[],
    );
    let metadata = &valid[encoded_len_varint(PROTOCOL_VERSION)..];
    for (body, expected) in [
        (vec![], "Invalid protocol version"),
        (
            versioned(PROTOCOL_VERSION + 1, metadata),
            "Unsupported protocol version",
        ),
        (versioned(PROTOCOL_VERSION, &[]), "Invalid metadata length"),
        (
            versioned(PROTOCOL_VERSION, &[0x80]),
            "Invalid metadata length",
        ),
        (
            versioned(PROTOCOL_VERSION, &[0xff; 11]),
            "Invalid metadata length",
        ),
        (versioned(PROTOCOL_VERSION, &[1]), "Truncated metadata"),
        (
            versioned(PROTOCOL_VERSION, &[1, 0xff]),
            "Invalid Protobuf metadata",
        ),
    ] {
        let message = decode(&body);
        assert!(message.contains(expected), "{body:?}: {message}");
    }
}

#[test]
fn test_invalid_invocation() {
    let decode = |bytes: &[u8]| invalid_request(DandelionRequest::decode(frame(bytes)));
    let missing = encode(&proto::DandelionRequest::default(), &[]);
    assert_eq!(decode(&missing), "Missing invocation target");

    let metadata = proto::DandelionRequest {
        target: target("f"),
        sets: vec![proto::InputSet {
            items: vec![proto::InputItem {
                payload_length: 3,
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    assert_eq!(decode(&encode(&metadata, b"ab")), "Payload is truncated");
    assert_eq!(
        decode(&encode(&metadata, b"abcd")),
        "Unexpected trailing payload bytes"
    );
}

#[test]
fn test_invalid_registration() {
    let decode = |metadata: &proto::RegisterFunction, payload: &[u8]| {
        invalid_request(RegisterFunction::decode(frame(&encode(metadata, payload))))
    };
    let valid = proto::RegisterFunction {
        name: "f".into(),
        engine_type: "Process".into(),
        context_size: 4096,
        binary_length: 2,
        ..Default::default()
    };
    let unnamed = proto::RegisterFunction {
        name: String::new(),
        ..valid.clone()
    };
    assert_eq!(decode(&unnamed, b"ab"), "Missing function name");
    let no_engine = proto::RegisterFunction {
        engine_type: String::new(),
        ..valid.clone()
    };
    assert_eq!(decode(&no_engine, b"ab"), "Missing engine type");
    assert_eq!(decode(&valid, b"a"), "Payload is truncated");
    assert_eq!(decode(&valid, b"abc"), "Unexpected trailing payload bytes");

    let chain = proto::RegisterChain {
        composition: "source".into(),
    };
    assert_eq!(
        invalid_request(RegisterChain::decode(frame(&encode(&chain, b"a")))),
        "Composition registration must not contain payload"
    );
}

#[test]
fn test_invalid_output() {
    let mut context = Context::new(ContextType::Bytes(Box::new(BytesContext::new(vec![]))), 0);
    context.content = vec![Some(DataSet {
        ident: "output".into(),
        buffers: vec![DataItem {
            ident: "item".into(),
            key: 0,
            data: Position { offset: 0, size: 1 },
        }],
    })];
    let outputs = CompositionSet::from_context(context)
        .into_iter()
        .map(|set| set.map(|set| set.into_local()))
        .collect();
    let recorder = Recorder::new(Arc::new("f".into()), Instant::now());
    let error = DandelionBody::new(outputs, &recorder).err().unwrap();
    assert!(matches!(
        error.error,
        DandelionError::RequestError(FrontendError::InternalError(_))
    ));
}

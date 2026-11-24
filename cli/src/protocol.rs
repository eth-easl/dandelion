use anyhow::{anyhow, bail, Result};
use prost::{encoding::encode_varint, Message};

pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/dandelion_frontend.rs"));
}

/// Protocol version this client speaks. Must match `dandelion_server::protocol::PROTOCOL_VERSION`.
pub const PROTOCOL_VERSION: u64 = 1;

/// Encodes a body from its metadata and the item payloads in traversal order.
pub fn encode<'a>(
    metadata: &impl Message,
    payloads: impl IntoIterator<Item = &'a [u8]>,
) -> Vec<u8> {
    let mut body = Vec::new();
    encode_varint(PROTOCOL_VERSION, &mut body);
    metadata
        .encode_length_delimited(&mut body)
        .expect("a Vec grows as needed");
    for payload in payloads {
        body.extend_from_slice(payload);
    }
    body
}

/// Splits a body into its metadata and the payload that follows it.
pub fn decode<M: Message + Default>(mut body: &[u8]) -> Result<(M, &[u8])> {
    let version = prost::encoding::decode_varint(&mut body)
        .map_err(|_| anyhow!("response does not start with a protocol version"))?;
    if version != PROTOCOL_VERSION {
        bail!(
            "server speaks protocol version {version}, this client speaks version {PROTOCOL_VERSION}"
        );
    }
    let metadata = M::decode_length_delimited(&mut body)
        .map_err(|e| anyhow!("response metadata is malformed: {e}"))?;
    Ok((metadata, body))
}

#[cfg(test)]
mod server_compat {
    use std::{sync::Arc, time::Instant};

    use crate::client::{
        decode_invoke_response, decode_status, encode_composition, encode_function, encode_invoke,
        Executable, FunctionRegistration, InputSet, InvokeTarget, Item, Set,
    };
    use bytes::Bytes;
    use dandelion_commons::trace::Recorder;
    use dandelion_server::protocol::{
        self as server, DandelionBody, DandelionRequest, InvocationTarget, RegisterComposition,
        RegisterFunction,
    };
    use http_body_util::BodyExt;
    use memory::{
        context::{bytes::BytesContext, ContextTrait},
        DataItem, DataSet, LocalData, Position,
    };

    fn item(identifier: &str, key: u32, data: &[u8]) -> Item {
        Item {
            identifier: identifier.into(),
            key,
            data: data.to_vec(),
        }
    }

    /// Splits a body into one-byte frames, the worst case for the server's frame handling.
    fn frames(body: Vec<u8>) -> Vec<Bytes> {
        let body = Bytes::from(body);
        (0..body.len()).map(|i| body.slice(i..i + 1)).collect()
    }

    /// Reads the items of a set the server decoded back into the CLI's model.
    fn items_of(set: &DataSet) -> Vec<Item> {
        set.items
            .iter()
            .map(|i| {
                let local = i.local_ref().unwrap();
                let mut data = vec![0; local.pos.size];
                local.ctx.read(local.pos.offset, &mut data).unwrap();
                item(&i.ident, i.key, &data)
            })
            .collect()
    }

    #[test]
    fn protocol_versions_match() {
        assert_eq!(super::PROTOCOL_VERSION, server::PROTOCOL_VERSION);
    }

    #[test]
    fn function_registration_with_binary_and_statics() {
        let reg = FunctionRegistration {
            name: "matmul".into(),
            context_size: 64 << 20,
            engine_type: "Kvm".into(),
            executable: Executable::Binary(b"\x7fELF binary".to_vec()),
            input_sets: vec![
                InputSet {
                    name: "matrices".into(),
                    static_items: None,
                },
                InputSet {
                    name: "config".into(),
                    static_items: Some(vec![item("params", 0, b"P1"), item("more", 4, b"")]),
                },
                InputSet {
                    name: "empty".into(),
                    static_items: Some(vec![]),
                },
            ],
            output_sets: vec!["result".into()],
            min_set_bytes: vec![4096, 0],
        };
        let decoded = RegisterFunction::decode(frames(encode_function(&reg))).unwrap();
        assert_eq!(decoded.name, "matmul");
        assert_eq!(decoded.context_size, 64 << 20);
        assert_eq!(decoded.engine_type, "Kvm");
        match decoded.executable {
            server::Executable::Binary(parts) => assert_eq!(parts.concat(), b"\x7fELF binary"),
            other => panic!("unexpected executable {other:?}"),
        }
        let sets: Vec<(String, Option<Vec<Item>>)> = decoded
            .input_sets
            .iter()
            .map(|(name, set)| (name.clone(), set.as_ref().map(items_of)))
            .collect();
        assert_eq!(
            sets,
            [
                ("matrices".into(), None),
                (
                    "config".into(),
                    Some(vec![item("params", 0, b"P1"), item("more", 4, b"")])
                ),
                ("empty".into(), Some(vec![])),
            ]
        );
        assert_eq!(decoded.output_sets, ["result"]);
        assert_eq!(decoded.min_set_bytes, [4096, 0]);
    }

    #[test]
    fn function_registration_with_server_path() {
        let reg = FunctionRegistration {
            name: "f".into(),
            context_size: 1,
            engine_type: "Process".into(),
            executable: Executable::LocalPath("/opt/fns/f".into()),
            input_sets: vec![],
            output_sets: vec![],
            min_set_bytes: vec![],
        };
        let decoded = RegisterFunction::decode(frames(encode_function(&reg))).unwrap();
        match decoded.executable {
            server::Executable::LocalPath(path) => assert_eq!(path, "/opt/fns/f"),
            other => panic!("unexpected executable {other:?}"),
        }
    }

    #[test]
    fn composition_registration() {
        let text = "composition C (x) => (y) {}";
        let decoded = RegisterComposition::decode(frames(encode_composition(text))).unwrap();
        assert_eq!(decoded.composition, text);
    }

    #[test]
    fn invocation_requests() {
        let sets = vec![
            Set {
                items: vec![item("a", 0, b"AAA"), item("b", 5, b"")],
            },
            Set::default(),
            Set {
                items: vec![item("c", 1, b"x")],
            },
        ];
        let decoded = DandelionRequest::decode(frames(encode_invoke(
            &InvokeTarget::Name("f".into()),
            &sets,
        )))
        .unwrap();
        assert!(matches!(decoded.target, InvocationTarget::Name(ref n) if n == "f"));
        let got: Vec<Vec<Item>> = decoded.sets.iter().map(items_of).collect();
        let want: Vec<Vec<Item>> = sets.into_iter().map(|s| s.items).collect();
        assert_eq!(got, want);

        let decoded = DandelionRequest::decode(frames(encode_invoke(
            &InvokeTarget::Composition("COMP".into()),
            &[],
        )))
        .unwrap();
        assert!(matches!(decoded.target, InvocationTarget::Composition(ref c) if c == "COMP"));
        assert!(decoded.sets.is_empty());
    }

    async fn collect(body: DandelionBody) -> Vec<u8> {
        body.collect().await.unwrap().to_bytes().to_vec()
    }

    #[tokio::test]
    async fn invocation_responses() {
        let payload = b"hello\x00\xff";
        let ctx = Arc::new(BytesContext::create_context(
            vec![Bytes::from_static(payload)],
            payload.len(),
        ));
        let local = |offset, size| LocalData {
            ctx: ctx.clone(),
            pos: Position { offset, size },
        };
        let sets = vec![
            Arc::new(vec![
                DataItem::new_local("msg".into(), 0, local(0, 5)),
                DataItem::new_local("bin".into(), 9, local(5, 2)),
            ]),
            Arc::new(vec![]),
            Arc::new(vec![DataItem::new_local("empty".into(), 1, local(0, 0))]),
        ];
        let recorder = Recorder::new(Arc::new("f".into()), 0, Instant::now());
        let body = collect(DandelionBody::new(sets, &recorder).unwrap()).await;

        let resp = decode_invoke_response(&body).unwrap();
        assert_eq!(resp.sets.len(), 3);
        assert_eq!(
            resp.sets[0].items,
            [item("msg", 0, b"hello"), item("bin", 9, b"\x00\xff")]
        );
        assert!(resp.sets[1].items.is_empty());
        assert_eq!(resp.sets[2].items, [item("empty", 1, b"")]);
    }

    #[tokio::test]
    async fn status_responses() {
        let body = collect(DandelionBody::status("Function registered")).await;
        assert_eq!(decode_status(&body).unwrap(), "Function registered");
    }
}

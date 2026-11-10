//! End-to-end benchmark for the composition scheduling.
//!
//! Starts a server and runs compositions with very wide fan-outs and very long chains of
//! the `test_{name}_basic` function to measure the scheduling overhead.
//!
//! Run it and see the runtimes with: `cargo bench --features <engine> --bench composition_stress`.
//! Pass `fan_out`, `fan_out_fan_in` or `chain` as an argument to only run the matching benchmark.

#[cfg(any(feature = "mmu", feature = "kvm", feature = "cheri"))]
#[path = "../tests/common/mod.rs"]
mod common;

#[cfg(any(feature = "mmu", feature = "kvm", feature = "cheri"))]
mod composition_stress {
    use dandelion_server::protocol::PROTOCOL_VERSION;
    use prost::Message;
    use proto::{dandelion_request::Target, register_function::Executable};
    use reqwest::blocking::Client;
    use std::{
        process::{Command, Stdio},
        time::{Duration, Instant},
    };

    use crate::common::ServerKiller;

    const ENDPOINT: &str = "http://localhost:8080/hot/chain_scaling";
    const FUNCTION_NAME: &str = "basic";
    /// Number of unmeasured requests sent before the measured ones, to get past cold start effects.
    const WARMUP_RUNS: usize = 2;
    /// Number of measured requests per composition.
    const MEASURED_RUNS: usize = 10;
    /// Number of parallel invocations in the fan-out tests.
    const FAN_OUT_WIDTHS: [usize; 6] = [1, 16, 128, 512, 1024, 2048];
    /// Number of sequential invocations in the chain test.
    const CHAIN_LENGTHS: [usize; 6] = [1, 16, 64, 256, 512, 1024];
    /// Context size for the basic function. Kept small so wide fan-outs fit into the memory domain.
    /// 8MiB is about the minimum for KVM, which reserves ~2MiB of page tables below the stack.
    const CONTEXT_SIZE: u64 = 0x80_0000;

    #[allow(dead_code)]
    mod proto {
        include!(concat!(env!("OUT_DIR"), "/dandelion_frontend.rs"));
    }

    fn encode(metadata: &impl Message, payload: &[u8]) -> Vec<u8> {
        let mut body = Vec::new();
        prost::encoding::encode_varint(PROTOCOL_VERSION, &mut body);
        metadata.encode_length_delimited(&mut body).unwrap();
        body.extend_from_slice(payload);
        body
    }

    fn decode<M: Message + Default>(mut body: &[u8]) -> (M, &[u8]) {
        assert_eq!(
            prost::encoding::decode_varint(&mut body).unwrap(),
            PROTOCOL_VERSION
        );
        let metadata = M::decode_length_delimited(&mut body).unwrap();
        (metadata, body)
    }

    /// Starts the server with only error logs, so the log output does not distort timings.
    fn start_server() -> ServerKiller {
        let server = Command::new(assert_cmd::cargo::cargo_bin!())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env("RUST_LOG", "error")
            .spawn()
            .unwrap();
        let mut server_killer = ServerKiller::new("Server", server);
        server_killer.check_for_start();
        server_killer
    }

    fn client() -> Client {
        Client::builder()
            .http2_prior_knowledge()
            .timeout(Some(Duration::from_secs(300)))
            .build()
            .unwrap()
    }

    fn register_basic(client: &Client) {
        let version;
        let engine_type;
        #[cfg(feature = "mmu")]
        {
            version = format!("elf_mmu_{}", std::env::consts::ARCH);
            engine_type = String::from("Process");
        }
        #[cfg(feature = "kvm")]
        {
            version = format!("elf_kvm_{}", std::env::consts::ARCH);
            engine_type = String::from("Kvm");
        }
        #[cfg(feature = "cheri")]
        {
            version = "elf_cheri";
            engine_type = String::from("Cheri");
        }
        let basic_path = format!(
            "{}/../machine_interface/tests/data/test_{}_basic",
            env!("CARGO_MANIFEST_DIR"),
            version,
        );
        let register_request = proto::RegisterFunction {
            name: String::from(FUNCTION_NAME),
            context_size: CONTEXT_SIZE,
            engine_type,
            executable: Some(Executable::LocalPath(basic_path)),
            input_sets: vec![proto::RegistrationInputSet {
                identifier: String::from("A"),
                static_items: None,
            }],
            output_sets: vec![String::from("Out")],
            min_set_bytes: vec![],
        };
        let resp = client
            .post("http://localhost:8080/register/function")
            .body(encode(&register_request, &[]))
            .send()
            .unwrap();
        assert!(resp.status().is_success());
    }

    fn register_composition(client: &Client, composition: String) {
        let resp = client
            .post("http://localhost:8080/register/composition")
            .body(encode(&proto::RegisterChain { composition }, &[]))
            .send()
            .unwrap();
        let status = resp.status();
        assert!(
            status.is_success(),
            "Failed to register composition: {}",
            resp.text().unwrap()
        );
    }

    fn send_request(
        client: &Client,
        composition: &str,
        num_items: usize,
        expected_outputs: usize,
    ) -> Duration {
        let data: Vec<u8> = (0..num_items as u64).flat_map(u64::to_le_bytes).collect();
        let request = proto::DandelionRequest {
            target: Some(Target::Name(String::from(composition))),
            sets: vec![proto::InputSet {
                items: (0..num_items as u32)
                    .map(|key| proto::InputItem {
                        identifier: String::from(""),
                        key,
                        payload_length: size_of::<u64>() as u64,
                    })
                    .collect(),
            }],
        };
        let body = encode(&request, &data);

        let start = Instant::now();
        let resp = client.post(ENDPOINT).body(body).send().unwrap();
        let status = resp.status();
        let resp_body = resp.bytes().unwrap();
        let elapsed = start.elapsed();

        assert!(
            status.is_success(),
            "Request to {} failed: {}",
            composition,
            String::from_utf8_lossy(&resp_body)
        );
        let response = decode::<proto::DandelionDeserializeResponse>(&resp_body).0;
        // every basic invocation emits one item, so this checks that all invocations ran
        assert_eq!(1, response.sets.len());
        assert_eq!(expected_outputs, response.sets[0].items.len());
        elapsed
    }

    fn measure(
        client: &Client,
        label: &str,
        composition: &str,
        num_items: usize,
        invocations: usize,
        expected_outputs: usize,
    ) {
        for _ in 0..WARMUP_RUNS {
            send_request(client, composition, num_items, expected_outputs);
        }
        let mut runtimes: Vec<Duration> = (0..MEASURED_RUNS)
            .map(|_| send_request(client, composition, num_items, expected_outputs))
            .collect();
        runtimes.sort();
        let total: Duration = runtimes.iter().sum();
        let mean = total / MEASURED_RUNS as u32;
        let median = runtimes[MEASURED_RUNS / 2];
        println!(
            "[stress] {:<28} invocations: {:>6} | min: {:>10.3?} | median: {:>10.3?} | mean: {:>10.3?} | max: {:>10.3?} | median per invocation: {:>10.3?}",
            label,
            invocations,
            runtimes[0],
            median,
            mean,
            runtimes[MEASURED_RUNS - 1],
            median / invocations as u32,
        );
    }

    fn declaration() -> String {
        format!("function {}(A) => (Out);", FUNCTION_NAME)
    }

    /// One invocation of the basic function per input item.
    fn fan_out_composition(name: &str) -> String {
        format!(
            r#"
            {decl}
            composition {name}(In) => (Res) {{
                {f}(A = each In) => (Res = Out);
            }}
            "#,
            decl = declaration(),
            f = FUNCTION_NAME,
        )
    }

    /// One invocation per input item, all of which are joined into a single final invocation.
    fn fan_out_fan_in_composition(name: &str) -> String {
        format!(
            r#"
            {decl}
            composition {name}(In) => (Res) {{
                {f}(A = each In) => (Mid = Out);
                {f}(A = all Mid) => (Res = Out);
            }}
            "#,
            decl = declaration(),
            f = FUNCTION_NAME,
        )
    }

    /// A chain of `length` invocations, each depending on the output of the previous one.
    fn chain_composition(name: &str, length: usize) -> String {
        assert!(length > 0);
        let mut body = String::new();
        let mut previous = String::from("In");
        for i in 0..length {
            let output = if i == length - 1 {
                String::from("Res")
            } else {
                format!("S{}", i)
            };
            body.push_str(&format!(
                "    {f}(A = all {previous}) => ({output} = Out);\n",
                f = FUNCTION_NAME,
            ));
            previous = output;
        }
        format!(
            "{decl}\ncomposition {name}(In) => (Res) {{\n{body}}}\n",
            decl = declaration(),
        )
    }

    pub fn bench_fan_out() {
        let server = start_server();
        let client = client();
        register_basic(&client);

        let name = "fan_out";
        register_composition(&client, fan_out_composition(name));
        for width in FAN_OUT_WIDTHS {
            measure(
                &client,
                &format!("fan_out width {}", width),
                name,
                width,
                width,
                width,
            );
        }

        server.assert_running();
    }

    pub fn bench_fan_out_fan_in() {
        let server = start_server();
        let client = client();
        register_basic(&client);

        let name = "fan_out_fan_in";
        register_composition(&client, fan_out_fan_in_composition(name));
        for width in FAN_OUT_WIDTHS {
            measure(
                &client,
                &format!("fan_out_fan_in width {}", width),
                name,
                width,
                width + 1,
                1,
            );
        }

        server.assert_running();
    }

    pub fn bench_chain() {
        let server = start_server();
        let client = client();
        register_basic(&client);

        for length in CHAIN_LENGTHS {
            let name = format!("chain_{}", length);
            register_composition(&client, chain_composition(&name, length));
            measure(
                &client,
                &format!("chain length {}", length),
                &name,
                1,
                length,
                1,
            );
        }

        server.assert_running();
    }
}

#[cfg(any(feature = "mmu", feature = "kvm", feature = "cheri"))]
fn main() {
    // `cargo bench` passes `--bench`, any other non-flag argument is used as a name filter
    let filter = std::env::args().skip(1).find(|arg| !arg.starts_with('-'));
    let benches: [(&str, fn()); 3] = [
        ("fan_out", composition_stress::bench_fan_out),
        ("fan_out_fan_in", composition_stress::bench_fan_out_fan_in),
        ("chain", composition_stress::bench_chain),
    ];
    for (name, bench) in benches {
        if filter
            .as_ref()
            .is_none_or(|filter| name.contains(filter.as_str()))
        {
            bench();
        }
    }
}

#[cfg(not(any(feature = "mmu", feature = "kvm", feature = "cheri")))]
fn main() {
    println!("Enable one of the engine features (mmu, kvm, cheri) to run this benchmark");
}

//! End-to-end stress tests for the composition scheduling.
//!
//! The tests start a server and run compositions with very wide fan-outs and very long chains of
//! the `test_{name}_basic` function to measure the scheduling overhead.
//!
//! As they take a while, they are ignored by default. Run them and see the runtimes with:
//! `cargo test --features <engine> --test composition_stress_tests -- --ignored --nocapture`.

#[cfg(any(feature = "mmu", feature = "kvm", feature = "cheri"))]
mod composition_stress_tests {
    use dandelion_server::{DandelionDeserializeResponse, DandelionRequest, InputItem, InputSet};
    use reqwest::blocking::Client;
    use serde::Serialize;
    use serial_test::serial;
    use std::{
        io::{BufRead, BufReader},
        process::{Child, Command, Stdio},
        thread::spawn,
        time::{Duration, Instant},
    };

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

    #[derive(Serialize)]
    struct RegisterFunctionLocal {
        name: String,
        context_size: u64,
        engine_type: String,
        local_path: String,
        binary: Vec<u8>,
        input_sets: Vec<(String, Option<Vec<(String, Vec<u8>)>>)>,
        output_sets: Vec<String>,
    }

    #[derive(Serialize)]
    struct RegisterChain {
        composition: String,
    }

    /// Kills the server when dropped, so it is also cleaned up when a test panics.
    struct ServerKiller {
        server: Child,
    }

    impl ServerKiller {
        fn start() -> Self {
            let mut server = Command::new(assert_cmd::cargo::cargo_bin!())
                .stdout(Stdio::piped())
                // the logs go to stderr, which are not of interest here and would distort timings
                .stderr(Stdio::null())
                .env("RUST_LOG", "error")
                .spawn()
                .unwrap();
            let mut stdout = BufReader::new(server.stdout.take().unwrap());
            loop {
                let mut buf = String::new();
                let len = stdout.read_line(&mut buf).unwrap();
                assert_ne!(len, 0, "Server exited unexpectedly");
                print!("Server out: {}", buf);
                if buf.contains("Socket ready") {
                    break;
                }
            }
            // keep draining stdout so the server never blocks on a full pipe
            spawn(move || {
                let mut buf = String::new();
                while stdout.read_line(&mut buf).is_ok_and(|len| len > 0) {
                    print!("Server out: {}", buf);
                    buf.clear();
                }
            });
            Self { server }
        }

        fn assert_running(mut self) {
            let status = self.server.try_wait().unwrap();
            assert_eq!(status, None, "Server exited unexpectedly");
        }
    }

    impl Drop for ServerKiller {
        fn drop(&mut self) {
            Command::new("kill")
                .args(["-s", "TERM", &self.server.id().to_string()])
                .status()
                .unwrap();
            let _ = self.server.wait();
        }
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
        let register_request = RegisterFunctionLocal {
            name: String::from(FUNCTION_NAME),
            context_size: CONTEXT_SIZE,
            engine_type,
            local_path: basic_path,
            binary: Vec::new(),
            input_sets: vec![(String::from("A"), None)],
            output_sets: vec![String::from("Out")],
        };
        let resp = client
            .post("http://localhost:8080/register/function")
            .body(bson::to_vec(&register_request).unwrap())
            .send()
            .unwrap();
        assert!(resp.status().is_success());
    }

    fn register_composition(client: &Client, composition: String) {
        let resp = client
            .post("http://localhost:8080/register/composition")
            .body(bson::to_vec(&RegisterChain { composition }).unwrap())
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
        let data: Vec<Vec<u8>> = (0..num_items as u64)
            .map(|i| i.to_le_bytes().to_vec())
            .collect();
        let request = DandelionRequest {
            name: String::from(composition),
            sets: vec![InputSet {
                identifier: String::from(""),
                items: data
                    .iter()
                    .enumerate()
                    .map(|(key, data)| InputItem {
                        identifier: String::from(""),
                        key: key as u32,
                        data,
                    })
                    .collect(),
            }],
        };
        let body = bson::to_vec(&request).unwrap();

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
        let response: DandelionDeserializeResponse = bson::from_slice(&resp_body).unwrap();
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

    #[test]
    #[serial]
    #[ignore = "stress test, run with --ignored"]
    fn stress_fan_out() {
        let server = ServerKiller::start();
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

    #[test]
    #[serial]
    #[ignore = "stress test, run with --ignored"]
    fn stress_fan_out_fan_in() {
        let server = ServerKiller::start();
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

    #[test]
    #[serial]
    #[ignore = "stress test, run with --ignored"]
    fn stress_chain() {
        let server = ServerKiller::start();
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

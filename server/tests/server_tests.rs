#[cfg(any(feature = "mmu", feature = "kvm", feature = "cheri"))]
mod server_tests {

    use byteorder::{LittleEndian, ReadBytesExt};
    use dandelion_server::{DandelionDeserializeResponse, DandelionRequest, InputItem, InputSet};
    use reqwest::blocking::Client;
    use serde::Serialize;
    use serial_test::serial;
    use std::{
        io::{BufRead, BufReader, Cursor, Read},
        process::{Child, Command, Stdio},
    };

    #[derive(Serialize)]
    struct RegisterFunction {
        name: String,
        context_size: u64,
        engine_type: String,
        binary: Vec<u8>,
        input_sets: Vec<(String, Option<Vec<(String, Vec<u8>)>>)>,
        output_sets: Vec<String>,
    }

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

    struct ServerKiller {
        name: &'static str,
        server: Child,
    }

    impl ServerKiller {
        fn check_for_start(&mut self) {
            let mut reader = BufReader::new(self.server.stdout.take().unwrap());
            loop {
                let mut buf = String::new();
                let len = reader.read_line(&mut buf).unwrap();
                assert_ne!(len, 0, "Server exited unexpectedly");
                if buf.contains("Socket ready") {
                    break;
                } else {
                    print!("{} out: {}", self.name, buf);
                }
            }
            let _ = self.server.stdout.insert(reader.into_inner());
        }

        /// Reads the server's stderr (where the logs are written) until each of `markers`
        /// has appeared, in order, printing every line on the way. This is used both to
        /// synchronize the test with the server's progress and to assert that the expected
        /// log lines are produced. A single reader is kept for the whole scan so no buffered
        /// log lines are lost between markers. Panics if the process exits before all markers
        /// are seen.
        fn wait_for_stderr(&mut self, markers: &[&str]) {
            let mut reader = BufReader::new(self.server.stderr.take().unwrap());
            let mut next = 0;
            while next < markers.len() {
                let mut buf = String::new();
                let len = reader.read_line(&mut buf).unwrap();
                assert_ne!(
                    len, 0,
                    "{} exited before logging {:?}",
                    self.name, markers[next]
                );
                print!("{} err: {}", self.name, buf);
                if buf.contains(markers[next]) {
                    next += 1;
                }
            }
            let _ = self.server.stderr.insert(reader.into_inner());
        }
    }

    impl Drop for ServerKiller {
        fn drop(&mut self) {
            let mut kill = Command::new("kill")
                .stdout(Stdio::piped())
                .args(["-s", "TERM", &self.server.id().to_string()])
                .spawn()
                .unwrap();
            kill.wait().unwrap();

            if let Some(mut child_stdout) = self.server.stdout.take() {
                let mut outbuf = Vec::new();
                let _ = child_stdout
                    .read_to_end(&mut outbuf)
                    .expect("should be able to read child output after killing it");
                print!(
                    "{} output:\n{}",
                    self.name,
                    String::from_utf8(outbuf)
                        .expect("Should be able to convert child stdout to string")
                );
            }
            let mut errbuf = Vec::new();
            let _ = self
                .server
                .stderr
                .take()
                .expect("Should have stderr pipe for child")
                .read_to_end(&mut errbuf)
                .expect("Should be able to read child stderr");
            print!(
                "{} stderr:\n{}",
                self.name,
                String::from_utf8(errbuf).expect("Server stderr should be string")
            )
        }
    }

    fn send_matrix_request(
        endpoint: &str,
        function_name: String,
        chain: bool,
        http_version: reqwest::Version,
        client: Client,
    ) {
        let matrix_dim = 3;
        let expected_checksum = if chain {
            matrix_dim * matrix_dim * matrix_dim
        } else {
            matrix_dim
        };
        send_matrix_request_expect_checksum(
            endpoint,
            function_name,
            matrix_dim,
            expected_checksum,
            http_version,
            client,
        );
    }

    fn send_matrix_request_expect_checksum(
        endpoint: &str,
        function_name: String,
        matrix_dim: u64,
        expected_checksum: u64,
        http_version: reqwest::Version,
        client: Client,
    ) {
        // call into function
        let mut data = Vec::new();
        // Use a matrix big enough to potentially get split into multiple frames
        data.extend_from_slice(&u64::to_le_bytes(matrix_dim));
        for _ in 0..matrix_dim * matrix_dim {
            data.extend_from_slice(&u64::to_le_bytes(1));
        }
        let mat_request = DandelionRequest {
            name: function_name,
            sets: vec![InputSet {
                identifier: String::from(""),
                items: vec![InputItem {
                    identifier: String::from(""),
                    key: 0,
                    data: &data,
                }],
            }],
        };

        let resp = client
            .post(endpoint)
            .version(http_version)
            .body(bson::to_vec(&mat_request).unwrap())
            .send()
            .unwrap();
        assert!(resp.status().is_success());

        let body = resp.bytes().unwrap();
        let response: DandelionDeserializeResponse = bson::from_slice(&body).unwrap();
        assert_ne!(0, response.invocation_id);
        #[cfg(feature = "timestamp")]
        println!("{}", response.timestamps);
        assert_eq!(1, response.sets.len());
        assert_eq!(1, response.sets[0].items.len());
        let response_data = response.sets[0].items[0].data;
        assert_eq!(
            (matrix_dim * matrix_dim + 1) as usize * size_of::<u64>(),
            response_data.len()
        );
        let mut reader = Cursor::new(response_data);
        let mat_size = reader.read_u64::<LittleEndian>().unwrap();
        assert_eq!(matrix_dim, mat_size);
        let checksum = reader.read_u64::<LittleEndian>().unwrap();
        assert_eq!(expected_checksum, checksum);
    }

    fn register_and_request(http_version: reqwest::Version, client: Client, local: bool) {
        // register function
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
        let matmul_path = format!(
            "{}/../machine_interface/tests/data/test_{}_matmul",
            env!("CARGO_MANIFEST_DIR"),
            version,
        );

        let version_string = match http_version {
            reqwest::Version::HTTP_09 => "0_9",
            reqwest::Version::HTTP_10 => "1_0",
            reqwest::Version::HTTP_11 => "1_1",
            reqwest::Version::HTTP_2 => "2_0",
            reqwest::Version::HTTP_3 => "3_0",
            _ => panic!("Unkown http version: {:?}", http_version),
        };

        let function_name = format!("matmul_{}", version_string);
        let register_request = if local {
            bson::to_vec(&RegisterFunctionLocal {
                name: function_name.clone(),
                context_size: 0x802_0000,
                local_path: matmul_path,
                binary: Vec::new(),
                engine_type,
                input_sets: vec![(String::from("InMats"), None)],
                output_sets: vec![String::from("OutMats")],
            })
            .unwrap()
        } else {
            bson::to_vec(&RegisterFunction {
                name: function_name.clone(),
                context_size: 0x802_0000,
                binary: std::fs::read(matmul_path).unwrap(),
                engine_type,
                input_sets: vec![(String::from("InMats"), None)],
                output_sets: vec![String::from("OutMats")],
            })
            .unwrap()
        };
        let registration_resp = client
            .post("http://localhost:8080/register/function")
            .version(http_version)
            .body(register_request)
            .send()
            .unwrap();
        assert!(registration_resp.status().is_success());

        let chain_name = format!("chain_{}", version_string);
        let chain_request = RegisterChain {
            composition: format!(
                r#"
                function {function} (InMats) => (OutMats);
                composition {chain} (CompInMats) => (CompOutMats) {{
                    {function} (InMats = all CompInMats) => (InterMat = OutMats);
                    {function} (InMats = all InterMat) => (CompOutMats = OutMats);
                }}
            "#,
                function = function_name,
                chain = chain_name,
            ),
        };

        let chain_resp = client
            .post("http://localhost:8080/register/composition")
            .version(http_version)
            .body(bson::to_vec(&chain_request).unwrap())
            .send()
            .unwrap();
        assert!(chain_resp.status().is_success());

        send_matrix_request(
            "http://localhost:8080/hot/matmul",
            function_name,
            false,
            http_version,
            client.clone(),
        );
        send_matrix_request(
            "http://localhost:8080/hot/matmul",
            chain_name,
            true,
            http_version,
            client,
        );
    }

    struct MultinodeServers {
        master: ServerKiller,
        worker: ServerKiller,
    }

    impl MultinodeServers {
        fn assert_running(mut self) {
            let status_result = self.master.server.try_wait();
            drop(self.master);
            let status = status_result.unwrap();
            assert_eq!(status, None, "Server exited unexpectedly");

            let status_result = self.worker.server.try_wait();
            drop(self.worker);
            let status = status_result.unwrap();
            assert_eq!(status, None, "Server exited unexpectedly");
        }
    }

    fn multinode_preload_path() -> String {
        let version;
        #[cfg(feature = "mmu")]
        {
            version = format!("process_{}", std::env::consts::ARCH);
        }
        #[cfg(feature = "kvm")]
        {
            version = format!("kvm_{}", std::env::consts::ARCH);
        }
        #[cfg(feature = "cheri")]
        {
            version = "cheri".to_string();
        }
        format!(
            "{}/tests/preload_files/preload_{}.json",
            env!("CARGO_MANIFEST_DIR"),
            version
        )
    }

    fn multinode_config_path() -> String {
        format!(
            "{}/tests/manifests/multinode_config.json",
            env!("CARGO_MANIFEST_DIR"),
        )
    }

    fn start_master() -> ServerKiller {
        let preload_path = multinode_preload_path();
        let multinode_config = multinode_config_path();

        let mut master_cmd = Command::new(assert_cmd::cargo::cargo_bin!());
        let master_server = master_cmd
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env("RUST_LOG", "debug,multinode=trace")
            .arg("--bin-preload-path")
            .arg(&preload_path)
            .arg("--total-cores")
            .arg("1")
            .arg("--test-mode")
            .arg("no-compute")
            .arg("--node-id")
            .arg("0")
            .arg("--multinode-config")
            .arg(&multinode_config)
            .spawn()
            .unwrap();
        let mut master = ServerKiller {
            name: "Master",
            server: master_server,
        };
        master.check_for_start();
        master
    }

    fn start_worker() -> ServerKiller {
        let preload_path = multinode_preload_path();
        let multinode_config = multinode_config_path();

        let remote_port = 8081;
        let mut worker_cmd = Command::new(assert_cmd::cargo::cargo_bin!());
        let worker_server = worker_cmd
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env("RUST_LOG", "debug,multinode=trace")
            .arg("--bin-preload-path")
            .arg(&preload_path)
            .arg("--port")
            .arg(remote_port.to_string())
            .arg("--node-id")
            .arg("1")
            .arg("--multinode-config")
            .arg(&multinode_config)
            .spawn()
            .unwrap();
        let mut worker = ServerKiller {
            name: "Worker",
            server: worker_server,
        };
        worker.check_for_start();
        worker
    }

    fn start_multinode_servers() -> MultinodeServers {
        println!("Preload_path: {}", multinode_preload_path());
        let master = start_master();
        let worker = start_worker();
        MultinodeServers { master, worker }
    }

    fn multinode_client() -> Client {
        Client::builder()
            .timeout(Some(std::time::Duration::from_secs(5)))
            .build()
            .unwrap()
    }

    #[test]
    #[serial]
    fn serve_matmul_http_2() {
        let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!());
        let server = cmd
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut server_killer = ServerKiller {
            name: "Server",
            server,
        };
        server_killer.check_for_start();

        let client = reqwest::blocking::Client::builder()
            .http2_prior_knowledge()
            .build()
            .unwrap();
        register_and_request(reqwest::Version::HTTP_2, client, false);

        let status_result = server_killer.server.try_wait();
        drop(server_killer);
        let status = status_result.unwrap();
        assert_eq!(status, None, "Server exited unexpectedly");
    }

    #[test]
    #[serial]
    fn serve_matmul_http_2_local() {
        let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!());
        let server = cmd
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut server_killer = ServerKiller {
            name: "Server",
            server,
        };
        server_killer.check_for_start();

        let client = reqwest::blocking::Client::builder()
            .http2_prior_knowledge()
            .build()
            .unwrap();
        register_and_request(reqwest::Version::HTTP_2, client, true);

        let status_result = server_killer.server.try_wait();
        drop(server_killer);
        let status = status_result.unwrap();
        assert_eq!(status, None, "Server exited unexpectedly");
    }

    #[test]
    #[serial]
    fn serve_matmul_http_1_1() {
        let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!());
        let server = cmd
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut server_killer = ServerKiller {
            name: "Server",
            server,
        };
        server_killer.check_for_start();

        let client = reqwest::blocking::Client::new();
        register_and_request(reqwest::Version::HTTP_11, client, false);

        let status_result = server_killer.server.try_wait();
        drop(server_killer);
        let status = status_result.unwrap();
        assert_eq!(status, None, "Server exited unexpectedly");
    }

    #[test]
    #[serial]
    fn serve_multinode() {
        let servers = start_multinode_servers();

        // perform the request
        send_matrix_request(
            "http://localhost:8080/hot/matmul",
            String::from("matmul"),
            false,
            reqwest::Version::HTTP_11,
            multinode_client(),
        );

        servers.assert_running();
    }

    #[test]
    #[serial]
    fn serve_multinode_reuses_output() {
        let servers = start_multinode_servers();
        let client = multinode_client();

        let composition_request = RegisterChain {
            composition: String::from(
                r#"
                function matmul(matrix_in) => (matrix_out);
                function matmac(mul_left, add_left, add_right) => (matrix_out);
                composition reuse_output(CompIn) => (CompOut) {
                    matmul(matrix_in = all CompIn) => (MatrixA = matrix_out);
                    matmul(matrix_in = all MatrixA) => (MatrixB = matrix_out);
                    matmac(add_left = all MatrixA, add_right = all MatrixB) => (CompOut = matrix_out);
                }
            "#,
            ),
        };
        let composition_resp = client
            .post("http://localhost:8080/register/composition")
            .version(reqwest::Version::HTTP_11)
            .body(bson::to_vec(&composition_request).unwrap())
            .send()
            .unwrap();
        assert!(composition_resp.status().is_success());

        // The composition is A(input), B(A), C(A, B), so A's output is consumed twice.
        // For an all-ones 3x3 matrix, A produces 3s, B produces 27s, and C adds A+B.
        send_matrix_request_expect_checksum(
            "http://localhost:8080/hot/matmul",
            String::from("reuse_output"),
            3,
            30,
            reqwest::Version::HTTP_11,
            client,
        );

        servers.assert_running();
    }

    /// Checks that a worker node which loses its connection to the master node notices the
    /// loss, cleans up after it, and automatically re-establishes the connection once the
    /// master is reachable again, all without either node panicking.
    #[test]
    #[serial]
    fn serve_multinode_reconnect() {
        let mut servers = start_multinode_servers();

        // Make sure the cluster has actually formed before we tear it down: the worker
        // connects to the master and the master registers the worker with its cores.
        servers
            .worker
            .wait_for_stderr(&["Established connection to master node"]);
        servers
            .master
            .wait_for_stderr(&["Established connection to worker node"]);

        // Drop the master to sever the worker's connection (also prints the master's output).
        drop(servers.master);

        // Bring the master back up. The worker should notice the lost connection and keep
        // retrying until it can reconnect to the new master.
        let mut master = start_master();

        // The worker logs that it lost the connection (the code path that also clears its
        // ExportRegistry) and then that it re-established it, confirming the reconnect loop.
        servers.worker.wait_for_stderr(&[
            "Lost connection to master node",
            "Established connection to master node",
        ]);

        // The (freshly started) master accepts the worker rejoining the cluster.
        master.wait_for_stderr(&["Established connection to worker node"]);

        // Neither node panicked through the disconnect and reconnect.
        let servers = MultinodeServers {
            master,
            worker: servers.worker,
        };
        servers.assert_running();
    }
}

#[cfg(all(
    any(
        feature = "wasm",
        feature = "mmu",
        feature = "cheri",
        feature = "gpu_thread",
        feature = "gpu_process"
    ),
    feature = "reqwest_io"
))]
mod server_tests {

    use assert_cmd::prelude::*;
    use byteorder::{LittleEndian, ReadBytesExt};
    use dandelion_server::{DandelionDeserializeResponse, DandelionRequest, InputItem, InputSet};
    use reqwest::blocking::Client;
    use serde::Serialize;
    use serial_test::serial;
    use std::{
        io::{BufRead, BufReader, Cursor, Read},
        process::{Child, Command, Stdio},
        sync::Mutex,
    };

    // Prevent tests running in parallel to avoid address already in use errors
    lazy_static::lazy_static! {
        static ref TEST_LOCK: Mutex<()> = Mutex::new(());
    }

    struct ServerKiller {
        server: Child,
    }

    #[derive(Serialize)]
    struct RegisterLibrary {
        name: String,
        library: Vec<u8>,
    }

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
    struct RegisterChain {
        composition: String,
    }

    #[derive(Serialize)]
    struct MatrixRequest {
        name: String,
        rows: u64,
        cols: u64,
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
                    "server output:\n{}",
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
                "server stderr:\n{}",
                String::from_utf8(errbuf).expect("Server stderr should be string")
            )
        }
    }

    fn send_matrix_request(
        endpoint: &str,
        function_name: String,
        http_version: reqwest::Version,
        client: Client,
    ) {
        // call into function
        let mut data = Vec::new();
        data.extend_from_slice(&i64::to_le_bytes(1));
        data.extend_from_slice(&i64::to_le_bytes(1));
        #[cfg(feature = "gpu")]
        let cfg = Vec::from(i64::to_le_bytes(1i64)); // GPU specific config input for eg. grid size

        let mut sets = vec![InputSet {
            identifier: String::from("A"),
            items: vec![InputItem {
                identifier: String::from(""),
                key: 0,
                data: &data,
            }],
        }];

        #[cfg(feature = "gpu")]
        sets.push(InputSet {
            identifier: String::from("cfg"),
            items: vec![InputItem {
                identifier: String::from(""),
                key: 0,
                data: &cfg,
            }],
        });
        let mat_request = DandelionRequest {
            name: function_name,
            sets,
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
        assert_eq!(1, response.sets.len());
        assert_eq!(1, response.sets[0].items.len());
        let response_data = response.sets[0].items[0].data;
        assert_eq!(response_data.len(), 16);
        let mut reader = Cursor::new(response_data);
        let mat_size = reader.read_u64::<LittleEndian>().unwrap();
        assert_eq!(1, mat_size);
        let checksum = reader.read_u64::<LittleEndian>().unwrap();
        assert_eq!(1, checksum);
    }

    fn register_and_request(http_version: reqwest::Version, client: Client) {
        let version: String;
        let mut engine_type;
        #[cfg(feature = "wasm")]
        {
            version = format!("sysld_wasm_{}", std::env::consts::ARCH);
            engine_type = String::from("RWasm");
        }
        #[cfg(feature = "mmu")]
        {
            version = format!("elf_mmu_{}", std::env::consts::ARCH);
            engine_type = String::from("Process");
        }
        #[cfg(feature = "cheri")]
        {
            version = "elf_cheri";
            engine_type = String::from("Cheri");
        }
        let matmul_path;
        #[cfg(any(feature = "wasm", feature = "mmu", feature = "cheri"))]
        {
            matmul_path = format!(
                "{}/../machine_interface/tests/data/test_{}_matmul",
                env!("CARGO_MANIFEST_DIR"),
                version,
            );
        }
        // TODO: unify with other engines
        #[cfg(feature = "gpu")]
        {
            matmul_path = format!(
                "{}/../machine_interface/tests/data/test_gpu_matmul_para.json",
                env!("CARGO_MANIFEST_DIR"),
            );
            #[cfg(feature = "gpu_thread")]
            {
                engine_type = String::from("GpuThread");
            }
            #[cfg(feature = "gpu_process")]
            {
                engine_type = String::from("GpuProcess");
            }
        }

        // Register GPU kernel library
        #[cfg(feature = "gpu")]
        {
            let register_library = RegisterLibrary {
                name: String::from("mlops.hsaco"),
                library: std::fs::read(format!(
                    "{}/../machine_interface/tests/libs/mlops.hsaco",
                    env!("CARGO_MANIFEST_DIR")
                ))
                .unwrap(),
            };

            let library_client = reqwest::blocking::Client::new();
            let library_resp = library_client
                .post("http://localhost:8080/register/library")
                .body(bson::to_vec(&register_library).unwrap())
                .send()
                .unwrap();
            assert!(library_resp.status().is_success());
        }

        let version_string = match http_version {
            reqwest::Version::HTTP_09 => "0_9",
            reqwest::Version::HTTP_10 => "1_0",
            reqwest::Version::HTTP_11 => "1_1",
            reqwest::Version::HTTP_2 => "2_0",
            reqwest::Version::HTTP_3 => "3_0",
            _ => panic!("Unkown http version: {:?}", http_version),
        };

        let function_name = format!("matmul_{}", version_string);
        let register_request = RegisterFunction {
            name: function_name.clone(),
            context_size: 0x802_0000,
            binary: std::fs::read(matmul_path).unwrap(),
            engine_type,
            input_sets: vec![(String::from("A"), None)],
            output_sets: vec![String::from("B")],
        };
        let registration_resp = client
            .post("http://localhost:8080/register/function")
            .version(http_version)
            .body(bson::to_vec(&register_request).unwrap())
            .send()
            .unwrap();
        assert!(registration_resp.status().is_success());

        let chain_name = format!("chain_{}", version_string);
        let chain_request = RegisterChain {
            #[cfg(not(feature = "gpu"))]
            composition: format!(
                r#"
                (:function {function} (InMats) -> (OutMats))
                (:composition {chain} (CompInMats) -> (CompOutMats) (
                    ({function} ((:all InMats <- CompInMats)) => ((InterMat := OutMats)))
                    ({function} ((:all InMats <- InterMat)) => ((CompOutMats := OutMats)))
                ))
            "#,
                function = function_name,
                chain = chain_name,
            ),
            #[cfg(feature = "gpu")]
            composition: format!(
                r#"
                (:function {function} (InMats Config) -> (OutMats))
                (:composition {chain} (CompInMats CompConfig) -> (CompOutMats) (
                    ({function} ((:all InMats <- CompInMats) (:all Config <- CompConfig)) => ((InterMat := OutMats)))
                    ({function} ((:all InMats <- InterMat) (:all Config <- CompConfig)) => ((CompOutMats := OutMats)))
                ))
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
            http_version,
            client.clone(),
        );
        send_matrix_request(
            "http://localhost:8080/hot/matmul",
            chain_name,
            http_version,
            client,
        );
    }

    #[test]
    #[serial]
    fn serve_matmul_http_2() {
        let mut cmd = Command::cargo_bin("dandelion_server").unwrap();
        let server = cmd
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env("DANDELION_LIBRARY_PATH", "/tmp/dandelion_server/libs/")
            .spawn()
            .unwrap();
        let mut server_killer = ServerKiller { server };
        let mut reader = BufReader::new(server_killer.server.stdout.take().unwrap());
        loop {
            let mut buf = String::new();
            let len = reader.read_line(&mut buf).unwrap();
            assert_ne!(len, 0, "Server exited unexpectedly");
            if buf.contains("Server start") {
                break;
            } else {
                print!("{}", buf);
            }
        }
        let _ = server_killer.server.stdout.insert(reader.into_inner());

        let client = reqwest::blocking::Client::builder()
            .http2_prior_knowledge()
            .build()
            .unwrap();
        register_and_request(reqwest::Version::HTTP_2, client);

        let status_result = server_killer.server.try_wait();
        drop(server_killer);
        let status = status_result.unwrap();
        assert_eq!(status, None, "Server exited unexpectedly");
    }

    #[test]
    #[serial]
    fn serve_matmul_http_1_1() {
        let mut cmd = Command::cargo_bin("dandelion_server").unwrap();
        let server = cmd
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env("DANDELION_LIBRARY_PATH", "/tmp/dandelion_server/libs/")
            .spawn()
            .unwrap();
        let mut server_killer = ServerKiller { server };
        let mut reader = BufReader::new(server_killer.server.stdout.take().unwrap());
        loop {
            let mut buf = String::new();
            let len = reader.read_line(&mut buf).unwrap();
            assert_ne!(len, 0, "Server exited unexpectedly");
            if buf.contains("Server start") {
                break;
            } else {
                print!("{}", buf);
            }
        }
        let _ = server_killer.server.stdout.insert(reader.into_inner());

        let client = reqwest::blocking::Client::new();
        register_and_request(reqwest::Version::HTTP_11, client);

        let status_result = server_killer.server.try_wait();
        drop(server_killer);
        let status = status_result.unwrap();
        assert_eq!(status, None, "Server exited unexpectedly");
    }

    fn send_inference_request(endpoint: &str, function_name: String) {
        // call into function
        let mut matrix_data = Vec::new();
        matrix_data.extend_from_slice(&f32::to_le_bytes(224f32));
        matrix_data.extend_from_slice(&f32::to_le_bytes(224f32));
        for i in 0..(224 * 224) {
            matrix_data.extend_from_slice(&f32::to_le_bytes(i as f32));
        }

        let mut kernel_data = Vec::new();
        kernel_data.extend_from_slice(&f32::to_le_bytes(5f32));
        kernel_data.extend_from_slice(&f32::to_le_bytes(5f32));
        for i in 0..(5 * 5) {
            kernel_data.extend_from_slice(&f32::to_le_bytes(i as f32));
        }

        let mut cfg = Vec::new();
        cfg.extend_from_slice(&i64::to_le_bytes((112 * 112 + 2) * 4));
        cfg.extend_from_slice(&i64::to_le_bytes((224 + 31) / 32));
        cfg.extend_from_slice(&i64::to_le_bytes((112 + 31) / 32));
        cfg.extend_from_slice(&i64::to_le_bytes(500));

        let sets = vec![
            InputSet {
                identifier: String::from("A"),
                items: vec![InputItem {
                    identifier: String::from(""),
                    key: 0,
                    data: &matrix_data,
                }],
            },
            InputSet {
                identifier: String::from("B"),
                items: vec![InputItem {
                    identifier: String::from(""),
                    key: 0,
                    data: &kernel_data,
                }],
            },
            InputSet {
                identifier: String::from("cfg"),
                items: vec![InputItem {
                    identifier: String::from(""),
                    key: 0,
                    data: &cfg,
                }],
            },
        ];

        let mat_request = DandelionRequest {
            name: function_name,
            sets,
        };

        let client = reqwest::blocking::Client::new();
        let resp = client
            .post(endpoint)
            .body(bson::to_vec(&mat_request).unwrap())
            .send()
            .unwrap();
        assert!(resp.status().is_success());

        let body = resp.bytes().unwrap();
        let response: DandelionDeserializeResponse = bson::from_slice(&body).unwrap();
        assert_eq!(1, response.sets.len());
        assert_eq!(1, response.sets[0].items.len());
        let response_data = response.sets[0].items[0].data;
        assert_eq!(response_data.len(), (112 * 112 + 2) * 4);
    }

    #[cfg(any(feature = "gpu", feature = "mmu"))]
    #[test]
    #[serial]
    fn serve_inference() {
        let mut cmd = Command::cargo_bin("dandelion_server").unwrap();
        let mut server = cmd
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env("DANDELION_LIBRARY_PATH", "/tmp/dandelion_server/libs/")
            .spawn()
            .unwrap();
        let mut reader = BufReader::new(server.stdout.take().unwrap());
        loop {
            let mut buf = String::new();
            let len = reader.read_line(&mut buf).unwrap();
            assert_ne!(len, 0, "Server exited unexpectedly");
            if buf.contains("Server start") {
                break;
            }
        }
        let _ = server.stdout.insert(reader.into_inner());
        let mut server_killer = ServerKiller { server };

        // register function
        let engine_type;
        #[cfg(feature = "gpu")]
        let inference_path = format!(
            "{}/../machine_interface/tests/data/test_gpu_inference.json",
            env!("CARGO_MANIFEST_DIR"),
        );
        #[cfg(feature = "mmu")]
        let inference_path = format!(
            "{}/../machine_interface/tests/data/test_elf_mmu_x86_64_inference",
            env!("CARGO_MANIFEST_DIR"),
        );
        #[cfg(feature = "gpu_thread")]
        {
            engine_type = String::from("GpuThread");
        }
        #[cfg(feature = "gpu_process")]
        {
            engine_type = String::from("GpuProcess");
        }
        #[cfg(feature = "mmu")]
        {
            engine_type = String::from("Process");
        }

        // Register GPU kernel library
        #[cfg(feature = "gpu")]
        {
            let register_library = RegisterLibrary {
                name: String::from("mlops.hsaco"),
                library: std::fs::read(format!(
                    "{}/../machine_interface/tests/libs/mlops.hsaco",
                    env!("CARGO_MANIFEST_DIR")
                ))
                .unwrap(),
            };

            let library_client = reqwest::blocking::Client::new();
            let library_resp = library_client
                .post("http://localhost:8080/register/library")
                .body(bson::to_vec(&register_library).unwrap())
                .send()
                .unwrap();
            assert!(library_resp.status().is_success());
        }

        let register_request = RegisterFunction {
            name: String::from("inference"),
            context_size: 0x802_0000,
            binary: std::fs::read(inference_path).unwrap(),
            engine_type,
            input_sets: vec![
                (String::from("A"), None),
                (String::from("B"), None),
                (String::from("cfg"), None),
            ],
            output_sets: vec![String::from("D")],
        };
        let registration_client = reqwest::blocking::Client::new();
        let registration_resp = registration_client
            .post("http://localhost:8080/register/function")
            .body(bson::to_vec(&register_request).unwrap())
            .send()
            .unwrap();
        assert!(registration_resp.status().is_success());

        let chain_request = RegisterChain {
            composition: String::from(
                r#"
                (:function inference (A B cfg) -> (out))
                (:composition chain (img kern func_cfg) -> (img_out) (
                    (inference ((:all A <- img) (:all B <- kern) (:all cfg <- func_cfg)) => ((img_out := out)))
                ))
            "#,
            ),
        };
        let chain_client = reqwest::blocking::Client::new();
        let chain_resp = chain_client
            .post("http://localhost:8080/register/composition")
            .body(bson::to_vec(&chain_request).unwrap())
            .send()
            .unwrap();
        assert!(chain_resp.status().is_success());

        send_inference_request(
            "http://localhost:8080/hot/inference",
            String::from("inference"),
        );
        send_inference_request("http://localhost:8080/hot/inference", String::from("chain"));

        let status_result = server_killer.server.try_wait();
        drop(server_killer);
        let status = status_result.unwrap();
        assert_eq!(status, None, "Server exited unexpectedly");
    }
}

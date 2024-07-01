#[cfg(all(
    any(feature = "wasm", feature = "mmu", feature = "cheri"),
    feature = "reqwest_io"
))]
mod server_tests {

    use assert_cmd::prelude::*;
    use byteorder::{LittleEndian, ReadBytesExt};
    use dandelion_server::{DandelionDeserializeResponse, DandelionRequest, InputItem, InputSet};
    use serde::Serialize;
    use std::{
        io::{BufRead, BufReader, Cursor, Read},
        process::{Child, Command, Stdio},
    };

    struct ServerKiller {
        server: Child,
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

    fn send_matrix_request(endpoint: &str, function_name: String) {
        // call into function
        let mut data = Vec::new();
        data.extend_from_slice(&i64::to_le_bytes(1));
        data.extend_from_slice(&i64::to_le_bytes(1));
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
        assert_eq!(response_data.len(), 16);
        let mut reader = Cursor::new(response_data);
        let mat_size = reader.read_u64::<LittleEndian>().unwrap();
        assert_eq!(1, mat_size);
        let checksum = reader.read_u64::<LittleEndian>().unwrap();
        assert_eq!(1, checksum);
    }

    #[test]
    fn serve_matmul() {
        let mut cmd = Command::cargo_bin("dandelion_server").unwrap();
        let server = cmd
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
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

        // register function
        let version;
        let engine_type;
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
        let matmul_path = format!(
            "{}/../machine_interface/tests/data/test_{}_matmul",
            env!("CARGO_MANIFEST_DIR"),
            version,
        );

        let register_request = RegisterFunction {
            name: String::from("matmul"),
            context_size: 0x802_0000,
            binary: std::fs::read(matmul_path).unwrap(),
            engine_type,
            input_sets: vec![(String::from(""), None)],
            output_sets: vec![String::from("")],
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
                (:function matmul (InMats) -> (OutMats))
                (:composition chain (CompInMats) -> (CompOutMats) (
                    (matmul ((:all InMats <- CompInMats)) => ((InterMat := OutMats)))
                    (matmul ((:all InMats <- InterMat)) => ((CompOutMats := OutMats)))
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

        send_matrix_request("http://localhost:8080/hot/matmul", String::from("matmul"));
        send_matrix_request("http://localhost:8080/hot/matmul", String::from("chain"));

        let status_result = server_killer.server.try_wait();
        drop(server_killer);
        let status = status_result.unwrap();
        assert_eq!(status, None, "Server exited unexpectedly");
    }
}

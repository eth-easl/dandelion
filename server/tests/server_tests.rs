#[cfg(all(any(feature = "mmu", feature = "cheri"), feature = "hyper_io"))]
mod server_tests {

    use assert_cmd::prelude::*;
    use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
    use std::{
        io::{BufRead, BufReader, Cursor},
        process::{Command, Stdio},
    };

    #[test]
    fn serve_matmul() {
        let mut cmd = Command::cargo_bin("dandelion_server").unwrap();
        let mut server = cmd
            .stdout(Stdio::piped())
            .env("NUM_COLD", "1")
            .spawn()
            .unwrap();
        let mut reader = BufReader::new(server.stdout.take().unwrap());
        loop {
            let mut buf = String::new();
            let len = reader.read_line(&mut buf).unwrap();
            assert_ne!(len, 0, "Server exited unexpectedly");
            if buf.contains("Hello, World") {
                break;
            }
        }

        let mat_size = 1;
        let mut body = Vec::new();
        let mut writer = Cursor::new(&mut body);
        writer.write_u64::<BigEndian>(mat_size).unwrap();
        writer.write_u64::<BigEndian>(mat_size).unwrap();

        let client = reqwest::blocking::Client::new();
        let resp = client
            .post("http://localhost:8080/hot/matmul")
            .body(body)
            .send()
            .unwrap();
        assert!(resp.status().is_success());

        let body = resp.bytes().unwrap();
        assert_eq!(body.len(), 8);
        let mut reader = Cursor::new(body);
        let checksum = reader.read_u64::<BigEndian>().unwrap();
        assert_eq!(checksum, 1);

        let status = server.try_wait().unwrap();
        assert_eq!(status, None, "Server exited unexpectedly");

        // Instead of sigkill, send sigterm for clean shutdown
        let mut kill = Command::new("kill")
            .stdout(Stdio::piped())
            .args(["-s", "TERM", &server.id().to_string()])
            .spawn()
            .unwrap();

        kill.wait().unwrap();
    }
}

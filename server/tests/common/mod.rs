//! Helpers shared between the server integration tests.

use std::{
    io::{pipe, BufRead, BufReader, PipeReader, PipeWriter, Read, Write},
    process::{Child, Command, Stdio},
    thread::spawn,
};

/// Kills the server when dropped, so it is also cleaned up when a test panics.
pub struct ServerKiller {
    pub name: &'static str,
    pub server: Child,
    stderr: BufReader<PipeReader>,
    stdout: BufReader<PipeReader>,
}

fn tee(name: &'static str, mut reader: impl BufRead, mut writer: PipeWriter) {
    loop {
        let mut buf = String::new();
        match reader.read_line(&mut buf) {
            Err(err) => panic!("{}", err),
            Ok(0) => break,
            _ => (),
        }
        print!("{} out: {}", name, buf);
        writer.write_all(buf.as_bytes()).unwrap();
    }
}

impl ServerKiller {
    pub fn new(name: &'static str, mut server: Child) -> Self {
        let stdout = BufReader::new(server.stdout.take().unwrap());
        let stderr = BufReader::new(server.stderr.take().unwrap());
        // create two new pipes, could be channels for better performance, using pipe for convenience
        let (out_reader, out_writer) = pipe().unwrap();
        spawn(move || tee(name, stdout, out_writer));
        let (err_reader, err_writer) = pipe().unwrap();
        spawn(move || tee(name, stderr, err_writer));
        Self {
            name,
            server,
            stdout: BufReader::new(out_reader),
            stderr: BufReader::new(err_reader),
        }
    }

    pub fn assert_running(mut self) {
        let status = self.server.try_wait().unwrap();
        assert_eq!(status, None, "Server exited unexpectedly");
    }

    pub fn check_for_start(&mut self) {
        loop {
            let mut buf = String::new();
            let len = self.stdout.read_line(&mut buf).unwrap();
            assert_ne!(len, 0, "Server exited unexpectedly");
            if buf.contains("Socket ready") {
                break;
            } else {
                print!("{} out: {}", self.name, buf);
            }
        }
    }

    #[allow(dead_code)] // only used by some of the tests
    pub fn wait_for_stderr(&mut self, markers: &[&str]) {
        let mut next = 0;
        while next < markers.len() {
            let mut buf = String::new();
            let len = self.stderr.read_line(&mut buf).unwrap();
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

        let mut outbuf = Vec::new();
        let _ = self
            .stdout
            .read_to_end(&mut outbuf)
            .expect("should be able to read child output after killing it");
        print!(
            "{} output:\n{}",
            self.name,
            String::from_utf8(outbuf).expect("Should be able to convert child stdout to string")
        );

        let mut errbuf = Vec::new();
        let _ = self
            .stderr
            .read_to_end(&mut errbuf)
            .expect("Should be able to read child stderr");
        print!(
            "{} stderr:\n{}",
            self.name,
            String::from_utf8(errbuf).expect("Server stderr should be string")
        )
    }
}

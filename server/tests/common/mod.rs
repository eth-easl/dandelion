//! Helpers shared between the server integration tests.

use std::{
    io::{pipe, BufRead, BufReader, PipeReader, PipeWriter, Write},
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
        println!("Starting to kill {}", self.name);
        let mut kill = Command::new("kill")
            .stdout(Stdio::piped())
            .args(["-s", "TERM", &self.server.id().to_string()])
            .spawn()
            .unwrap();
        kill.wait().unwrap();

        // try to read lines from stderr until we finish
        loop {
            let mut line_buffer = String::new();
            let chars = self.stderr.read_line(&mut line_buffer).unwrap();
            if chars == 0 {
                break;
            }
            print!("{} err: {}", self.name, line_buffer);
            std::io::stdout().flush().unwrap();
        }

        loop {
            let mut line_buffer = String::new();
            let chars = self.stdout.read_line(&mut line_buffer).unwrap();
            if chars == 0 {
                break;
            }
            print!("{} out: {}", self.name, line_buffer);
            std::io::stdout().flush().unwrap();
        }

        println!("{} shut down", self.name);
    }
}

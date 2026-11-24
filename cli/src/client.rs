use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};

use crate::config::Settings;
use crate::error::{ApiError, ConnectionError};
use crate::protocol::{self, proto};

/// The server does not look at the content type; this just labels the body as binary.
pub const CONTENT_TYPE: &str = "application/octet-stream";

// ---------------------------------------------------------------------------------------------
// Data model

#[derive(Debug, Clone, PartialEq)]
pub struct Item {
    pub identifier: String,
    pub key: u32,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Set {
    pub items: Vec<Item>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct InvokeResponse {
    pub sets: Vec<Set>,
    /// JSON-encoded trace, passed through as received.
    pub trace: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InputSet {
    pub name: String,
    pub static_items: Option<Vec<Item>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Executable {
    /// Path to a binary that is already on the server's disk.
    LocalPath(String),
    /// Binary uploaded with the request.
    Binary(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct FunctionRegistration {
    pub name: String,
    pub context_size: u64,
    pub engine_type: String,
    pub executable: Executable,
    pub input_sets: Vec<InputSet>,
    pub output_sets: Vec<String>,
    pub min_set_bytes: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum InvokeTarget {
    Name(String),
    Composition(String),
}

// ---------------------------------------------------------------------------------------------
// Encoding

fn data_set(items: &[Item]) -> proto::DataSet {
    proto::DataSet {
        items: items
            .iter()
            .map(|item| proto::DataItem {
                identifier: item.identifier.clone(),
                key: item.key,
                payload_length: item.data.len() as u64,
            })
            .collect(),
    }
}

pub fn encode_function(reg: &FunctionRegistration) -> Vec<u8> {
    let (executable, binary) = match &reg.executable {
        Executable::LocalPath(path) => (
            proto::register_function::Executable::LocalPath(path.clone()),
            b"".as_slice(),
        ),
        Executable::Binary(bytes) => (
            proto::register_function::Executable::BinaryLength(bytes.len() as u64),
            bytes.as_slice(),
        ),
    };
    let metadata = proto::RegisterFunction {
        name: reg.name.clone(),
        context_size: reg.context_size,
        engine_type: reg.engine_type.clone(),
        executable: Some(executable),
        input_sets: reg
            .input_sets
            .iter()
            .map(|set| proto::RegistrationSet {
                identifier: set.name.clone(),
                set: set.static_items.as_deref().map(data_set),
            })
            .collect(),
        output_sets: reg.output_sets.clone(),
        min_set_bytes: reg.min_set_bytes.clone(),
    };
    let statics = reg
        .input_sets
        .iter()
        .flat_map(|set| set.static_items.iter().flatten())
        .map(|item| item.data.as_slice());
    protocol::encode(&metadata, std::iter::once(binary).chain(statics))
}

pub fn encode_composition(text: &str) -> Vec<u8> {
    let metadata = proto::RegisterComposition {
        composition: text.to_string(),
        // NOTE: The server does not support static sets for compositions yet.
        input_sets: Vec::new(),
    };
    protocol::encode(&metadata, [])
}

pub fn encode_invoke(target: &InvokeTarget, sets: &[Set]) -> Vec<u8> {
    let target = match target {
        InvokeTarget::Name(n) => proto::dandelion_request::Target::Name(n.clone()),
        InvokeTarget::Composition(c) => proto::dandelion_request::Target::Composition(c.clone()),
    };
    let metadata = proto::DandelionRequest {
        target: Some(target),
        sets: sets.iter().map(|s| data_set(&s.items)).collect(),
    };
    let payloads = sets
        .iter()
        .flat_map(|s| &s.items)
        .map(|i| i.data.as_slice());
    protocol::encode(&metadata, payloads)
}

// ---------------------------------------------------------------------------------------------
// Decoding

pub fn decode_invoke_response(body: &[u8]) -> Result<InvokeResponse> {
    let (metadata, mut payload) = protocol::decode::<proto::DandelionResponse>(body)?;
    let mut sets = Vec::with_capacity(metadata.sets.len());
    for (si, set) in metadata.sets.into_iter().enumerate() {
        let mut items = Vec::with_capacity(set.items.len());
        for (ii, item) in set.items.into_iter().enumerate() {
            let len = usize::try_from(item.payload_length)
                .ok()
                .filter(|len| *len <= payload.len())
                .ok_or_else(|| {
                    anyhow!(
                        "response set {si} item {ii} declares {} bytes, but only {} bytes are left",
                        item.payload_length,
                        payload.len()
                    )
                })?;
            let (data, rest) = payload.split_at(len);
            payload = rest;
            items.push(Item {
                identifier: item.identifier,
                key: item.key,
                data: data.to_vec(),
            });
        }
        sets.push(Set { items });
    }
    if !payload.is_empty() {
        bail!(
            "response has {} bytes of payload that belong to no item",
            payload.len()
        );
    }
    Ok(InvokeResponse {
        sets,
        trace: metadata.trace,
    })
}

pub fn decode_status(body: &[u8]) -> Result<String> {
    let (status, payload) = protocol::decode::<proto::StatusResponse>(body)?;
    if !payload.is_empty() {
        bail!(
            "status response has {} unexpected payload bytes",
            payload.len()
        );
    }
    Ok(status.message)
}

// ---------------------------------------------------------------------------------------------
// Transport

pub struct Client {
    http: reqwest::blocking::Client,
    base_url: String,
    verbose: bool,
}

impl Client {
    pub fn new(settings: &Settings) -> Result<Self> {
        reqwest::Url::parse(&settings.url)
            .with_context(|| format!("invalid server URL `{}`", settings.url))?;
        let connect_timeout = settings
            .timeout
            .map_or(Duration::from_secs(10), |t| t.min(Duration::from_secs(10)));
        let http = reqwest::blocking::Client::builder()
            // reqwest's blocking client defaults to 30s; invocations can take longer, so only
            // apply a total timeout when one is configured.
            .timeout(settings.timeout)
            .connect_timeout(connect_timeout)
            .user_agent(concat!("dandelion-cli/", env!("CARGO_PKG_VERSION")))
            .build()
            .context("cannot create HTTP client")?;
        Ok(Client {
            http,
            base_url: settings.url.trim_end_matches('/').to_string(),
            verbose: settings.verbose,
        })
    }

    pub fn url_for(&self, path: &str) -> String {
        format!("{}/{}", self.base_url, path.trim_start_matches('/'))
    }

    /// POSTs a protocol body and returns the response body of a successful request.
    pub fn post(&self, path: &str, body: Vec<u8>) -> Result<Vec<u8>> {
        let sent = body.len();
        let req = self
            .http
            .post(self.url_for(path))
            .header(reqwest::header::CONTENT_TYPE, CONTENT_TYPE)
            .body(body);
        let (status, bytes, _) = self.send(req, "POST", path, sent)?;
        check_status(status, &bytes)?;
        Ok(bytes)
    }

    /// POSTs a request that the server answers with a status message, and returns the message.
    pub fn post_for_status(&self, path: &str, body: Vec<u8>) -> Result<String> {
        decode_status(&self.post(path, body)?)
    }

    /// GETs a path and returns the status and the latency. The status is not checked.
    pub fn get(&self, path: &str) -> Result<(reqwest::StatusCode, Vec<u8>, Duration)> {
        let req = self.http.get(self.url_for(path));
        self.send(req, "GET", path, 0)
    }

    fn send(
        &self,
        req: reqwest::blocking::RequestBuilder,
        method: &str,
        path: &str,
        sent: usize,
    ) -> Result<(reqwest::StatusCode, Vec<u8>, Duration)> {
        let url = self.url_for(path);
        let start = Instant::now();
        let resp = req.send().map_err(|e| connection_error(&url, e))?;
        let status = resp.status();
        let bytes = resp
            .bytes()
            .map_err(|e| connection_error(&url, e))?
            .to_vec();
        let elapsed = start.elapsed();
        if self.verbose {
            eprintln!(
                "{method} {url} -> {status} in {} (sent {}, received {})",
                format_duration(elapsed),
                format_size(sent as u64),
                format_size(bytes.len() as u64),
            );
        }
        Ok((status, bytes, elapsed))
    }
}

pub fn format_size(n: u64) -> String {
    if n < 1024 {
        format!("{n} B")
    } else {
        bytesize::ByteSize::b(n).display().iec().to_string()
    }
}

pub fn format_duration(d: Duration) -> String {
    let ms = d.as_secs_f64() * 1000.0;
    if ms < 1000.0 {
        format!("{ms:.1} ms")
    } else {
        format!("{:.2} s", ms / 1000.0)
    }
}

fn connection_error(url: &str, e: reqwest::Error) -> anyhow::Error {
    let what = if e.is_timeout() {
        "request timed out"
    } else if e.is_connect() {
        "cannot connect"
    } else {
        "request failed"
    };
    // Include the root cause; reqwest's top-level message alone is rarely informative.
    let mut cause = String::new();
    let mut src: Option<&dyn std::error::Error> = std::error::Error::source(&e);
    while let Some(s) = src {
        cause = s.to_string();
        src = s.source();
    }
    let msg = if cause.is_empty() {
        format!("{what}: {url}")
    } else {
        format!("{what}: {url}: {cause}")
    };
    anyhow::Error::new(ConnectionError(msg))
}

pub fn check_status(status: reqwest::StatusCode, body: &[u8]) -> Result<()> {
    if status.is_success() {
        return Ok(());
    }
    let message =
        decode_status(body).unwrap_or_else(|_| String::from_utf8_lossy(body).into_owned());
    Err(anyhow::Error::new(ApiError {
        status: status.as_u16(),
        body: message,
    }))
}

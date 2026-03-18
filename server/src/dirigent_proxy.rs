use crate::dirigent_service::DirigentService;
use crate::request_parser;
use crate::request_parser::http2_initial_parsing;
use crate::DispatcherCommand;
use bytes::Bytes;
use dandelion_commons::records::Recorder;
use dandelion_server::{
    DandelionBody, DandelionDeserializeResponse, DandelionRequest, InputItem, InputSet,
};
use dispatcher::dispatcher::DispatcherInput;
use hyper::Response;
use hyper_util::client::legacy::connect::proxy;
use k8s_openapi::api::authorization;
use log::trace;
use log::{debug, error, info, warn};
use machine_interface::{
    composition::CompositionSet,
    function_driver::Metadata,
    machine_config::EngineType,
    memory_domain::Context,
    memory_domain::{bytes_context::BytesContext, read_only::ReadOnlyContext},
    DataItem, DataSet, Position,
};
use serde::Deserialize;
use serde::Serialize;
use std::io::{BufReader, Cursor, Error, Read};
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread;
use std::{
    collections::HashMap,
    convert::Infallible,
    future::Future,
    io::{ErrorKind, Write},
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
    time::Instant,
};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::{Builder, Runtime};
use tokio::signal::unix::SignalKind;
use tokio::sync::{mpsc, oneshot};
use tokio::time::sleep;
use tokio::time::{self, Duration};
use tokio::{io, try_join};
use x509_parser::nom::number;

// Stuff needed for mTLS
use anyhow::{anyhow, bail, Context as AnyhowContext, Result};
use bytes::{Buf, BytesMut};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};
use std::{fs::File, path::Path};
use tokio::io::AsyncReadExt;
use tokio_rustls::{server::TlsStream, TlsAcceptor};
use x509_parser::{extensions::GeneralName, parse_x509_certificate};

// Stuff needed for rate limiting
use redis::{aio::MultiplexedConnection, Client, Script};
use std::time::{SystemTime, UNIX_EPOCH};

use core_affinity::{self, CoreId};
use dandelion_server::config::DandelionConfig;

const HTTP2_MAGIC_PACKET: [u8; 24] = [
    0x50, 0x52, 0x49, 0x20, 0x2a, 0x20, 0x48, 0x54, 0x54, 0x50, 0x2f, 0x32, 0x2e, 0x30, 0x0d, 0x0a,
    0x0d, 0x0a, 0x53, 0x4d, 0x0d, 0x0a, 0x0d, 0x0a,
];

const FUNCTION_FOLDER_PATH: &str = "/tmp/dandelion_server";

const PROXY_TLS_WORKER_CERT_FILENAME: &str = "worker.crt.pem";
const PROXY_TLS_WORKER_KEY_FILENAME: &str = "worker.key.pem";
const PROXY_TLS_CA_BUNDLE_FILENAME: &str = "ca.crt.pem";
const PROXY_TLS_EXPECTED_SPIFFE_FILENAME: &str = "expected_data_plane_spiffe_id.txt";
const ZIPKIN_CHANNEL_CAPACITY: usize = 4096;
const ZIPKIN_SERVICE_NAME: &str = "anakonda-proxy";
const ZIPKIN_SPAN_NAME: &str = "proxy.request_event";

// ********* mTLS related functions *************
// ************************************************
// ************************************************
fn load_certs(path: &str) -> Result<Vec<CertificateDer<'static>>> {
    let mut rd = BufReader::new(File::open(path).context("open server cert")?);
    let certs = rustls_pemfile::certs(&mut rd).collect::<Result<Vec<_>, _>>()?;
    if certs.is_empty() {
        bail!("no certs found in {}", path);
    }
    Ok(certs)
}

fn load_key(path: &str) -> Result<PrivateKeyDer<'static>> {
    let mut rd = BufReader::new(File::open(path).context("open server key")?);
    rustls_pemfile::private_key(&mut rd)?
        .ok_or_else(|| anyhow::anyhow!("no private key found in {}", path))
}

fn load_client_roots(path: &str) -> Result<RootCertStore> {
    let mut rd = BufReader::new(File::open(path).context("open client CA bundle")?);
    let certs = rustls_pemfile::certs(&mut rd).collect::<Result<Vec<_>, _>>()?;
    let mut roots = RootCertStore::empty();
    let (added, _ignored) = roots.add_parsable_certificates(certs);
    if added == 0 {
        bail!("no valid CA certs in {}", path);
    }
    Ok(roots)
}

fn build_mtls_acceptor(
    server_cert: &str,
    server_key: &str,
    client_ca: &str,
) -> Result<TlsAcceptor> {
    let cert_chain = load_certs(server_cert)?;
    let key = load_key(server_key)?;
    let roots = load_client_roots(client_ca)?;
    let verifier = WebPkiClientVerifier::builder(Arc::new(roots)).build()?;

    let mut cfg = ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(cert_chain, key)?;

    cfg.alpn_protocols = vec![b"h2".to_vec()]; // apparently this is recommended for HTTP/2 over TLS

    Ok(TlsAcceptor::from(Arc::new(cfg)))
}

fn extract_spiffe_id(tls_stream: &TlsStream<TcpStream>) -> Result<String> {
    let (_, server_conn) = tls_stream.get_ref();

    let certs = server_conn
        .peer_certificates()
        .ok_or_else(|| anyhow!("no peer certificates"))?;
    let leaf = certs
        .first()
        .ok_or_else(|| anyhow!("empty peer cert chain"))?;

    let (_, cert) =
        parse_x509_certificate(leaf.as_ref()).map_err(|e| anyhow!("bad cert DER: {e}"))?;

    let san = cert
        .subject_alternative_name()
        .map_err(|e| anyhow!("failed to read SAN: {e}"))?
        .ok_or_else(|| anyhow!("SAN extension not found"))?;

    san.value
        .general_names
        .iter()
        .find_map(|gn| match gn {
            GeneralName::URI(uri) if uri.starts_with("spiffe://") => Some(uri.to_string()),
            _ => None,
        })
        .ok_or_else(|| anyhow!("no SPIFFE URI found in SAN"))
}

struct ProxyMTLSMaterialPaths {
    server_cert_path: String,
    server_key_path: String,
    client_ca_bundle_path: String,
    expected_data_plane_spiffe_id_path: String,
}

fn build_proxy_mtls_material_paths(material_dir: &str) -> ProxyMTLSMaterialPaths {
    let dir = Path::new(material_dir);
    ProxyMTLSMaterialPaths {
        server_cert_path: dir
            .join(PROXY_TLS_WORKER_CERT_FILENAME)
            .to_string_lossy()
            .into_owned(),
        server_key_path: dir
            .join(PROXY_TLS_WORKER_KEY_FILENAME)
            .to_string_lossy()
            .into_owned(),
        client_ca_bundle_path: dir
            .join(PROXY_TLS_CA_BUNDLE_FILENAME)
            .to_string_lossy()
            .into_owned(),
        expected_data_plane_spiffe_id_path: dir
            .join(PROXY_TLS_EXPECTED_SPIFFE_FILENAME)
            .to_string_lossy()
            .into_owned(),
    }
}

fn load_expected_data_plane_spiffe_id(path: &str) -> Result<String> {
    let expected = std::fs::read_to_string(path)
        .context("failed to read expected data-plane SPIFFE ID file")?;

    let trimmed = expected.trim().to_string();
    if trimmed.is_empty() {
        bail!("expected data-plane SPIFFE ID file is empty: {}", path);
    }

    Ok(trimmed)
}

//  We implement the "DpStream" as a wrapper around the TlsStream<TcpStream> and implement the required interface
//  such that our workers can read/write to it in the same way they read/write to a normal TcpStream.
struct DpStream {
    inner: TlsStream<TcpStream>,
    local_addr: SocketAddr,
    peer_addr: SocketAddr,
    read_cache: BytesMut,
    eof: bool,
}

impl DpStream {
    fn new(inner: TlsStream<TcpStream>, local_addr: SocketAddr, peer_addr: SocketAddr) -> Self {
        Self {
            inner,
            local_addr,
            peer_addr,
            read_cache: BytesMut::new(),
            eof: false,
        }
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.local_addr)
    }
    fn peer_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.peer_addr)
    }

    async fn readable(&mut self) -> io::Result<()> {
        if !self.read_cache.is_empty() || self.eof {
            return Ok(());
        }
        let mut tmp = [0u8; 16 * 1024];
        let n = self.inner.read(&mut tmp).await?;
        if n == 0 {
            self.eof = true;
        } else {
            self.read_cache.extend_from_slice(&tmp[..n]);
        }
        Ok(())
    }

    fn try_read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if self.eof && self.read_cache.is_empty() {
            return Ok(0);
        }
        if self.read_cache.is_empty() {
            return Err(io::Error::new(
                ErrorKind::WouldBlock,
                "no decrypted bytes ready",
            ));
        }
        let n = out.len().min(self.read_cache.len());
        out[..n].copy_from_slice(&self.read_cache[..n]);
        self.read_cache.advance(n);
        Ok(n)
    }

    async fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.inner.write_all(buf).await
    }

    async fn shutdown(&mut self) -> io::Result<()> {
        self.inner.shutdown().await
    }
}

trait ProxyStream {
    fn local_addr(&self) -> io::Result<SocketAddr>;
    fn peer_addr(&self) -> io::Result<SocketAddr>;
    async fn wait_readable(&mut self) -> io::Result<()>;
    fn read_nonblocking(&mut self, out: &mut [u8]) -> io::Result<usize>;
    async fn send_all(&mut self, buf: &[u8]) -> io::Result<()>;
    async fn close_stream(&mut self) -> io::Result<()>;
}

impl ProxyStream for TcpStream {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.local_addr()
    }
    fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.peer_addr()
    }
    async fn wait_readable(&mut self) -> io::Result<()> {
        self.readable().await
    }
    fn read_nonblocking(&mut self, out: &mut [u8]) -> io::Result<usize> {
        self.try_read(out)
    }
    async fn send_all(&mut self, buf: &[u8]) -> io::Result<()> {
        tokio::io::AsyncWriteExt::write_all(self, buf).await
    }
    async fn close_stream(&mut self) -> io::Result<()> {
        tokio::io::AsyncWriteExt::shutdown(self).await
    }
}

impl ProxyStream for DpStream {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        DpStream::local_addr(self)
    }
    fn peer_addr(&self) -> io::Result<SocketAddr> {
        DpStream::peer_addr(self)
    }
    async fn wait_readable(&mut self) -> io::Result<()> {
        DpStream::readable(self).await
    }
    fn read_nonblocking(&mut self, out: &mut [u8]) -> io::Result<usize> {
        DpStream::try_read(self, out)
    }
    async fn send_all(&mut self, buf: &[u8]) -> io::Result<()> {
        DpStream::write_all(self, buf).await
    }
    async fn close_stream(&mut self) -> io::Result<()> {
        DpStream::shutdown(self).await
    }
}

// ********* rate_limiting related functions *************
// ************************************************
// ************************************************
async fn connect_redis(
    redis_ip: &str,
    redis_port: u16,
    redis_password: &str,
) -> redis::RedisResult<MultiplexedConnection> {
    //  Assumes that password is URL-safe. If it contains special chars like @ or :, percent-encode it first
    let redis_url = format!("redis://:{}@{}:{}/", redis_password, redis_ip, redis_port);

    let client = Client::open(redis_url)?;
    client.get_multiplexed_async_connection().await
}

#[derive(Clone)]
struct RateLimitRedisCtx {
    conn: MultiplexedConnection, // clone per command call/task
    incr_with_expire: Script,    // immutable shared script
    safe_rollback: Script,       // immutable shared script
}

fn now_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn compute_bucket_start_and_end(
    now_secs: u64,
    rate_limiting_time_unit_in_seconds: u32,
) -> (u64, u64) {
    let bucket_size = u64::from(rate_limiting_time_unit_in_seconds);
    assert!(bucket_size > 0, "time bucket size must be > 0");

    let bucket_start = now_secs - (now_secs % bucket_size);
    let bucket_end = bucket_start + bucket_size;

    (bucket_start, bucket_end)
}

//  we prefix the keys with "rate_limiting" in case we end up using the same Redis instance for other purposes in the future
fn make_rate_limiting_key(function_name: &String, bucket_start: u64) -> String {
    format!("rate_limiting:{}:{}", function_name, bucket_start)
}

#[derive(Clone)]
struct ZipkinCtx {
    tx: mpsc::Sender<ZipkinEvent>,
}

#[derive(Clone)]
struct ZipkinEvent {
    stream_id: i32,
    outcome: String,
    ts_unix_us: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ZipkinEndpoint {
    service_name: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ZipkinSpan {
    trace_id: String,
    id: String,
    name: String,
    timestamp: u64,
    duration: u64,
    local_endpoint: ZipkinEndpoint,
    tags: HashMap<String, String>,
}

fn now_unix_microseconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

fn emit_zipkin_event(
    zipkin_ctx: Option<&Arc<ZipkinCtx>>,
    stream_id: i32,
    outcome: impl Into<String>,
) {
    let Some(ctx) = zipkin_ctx else {
        return;
    };
    let outcome = outcome.into();

    let event = ZipkinEvent {
        stream_id,
        outcome: outcome.clone(),
        ts_unix_us: now_unix_microseconds(),
    };

    if let Err(send_err) = ctx.tx.try_send(event) {
        match send_err {
            tokio::sync::mpsc::error::TrySendError::Full(_) => {
                debug!(
                    "Dropping Zipkin event because queue is full (stream_id={}, outcome={})",
                    stream_id, outcome
                );
            }
            tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                debug!(
                    "Dropping Zipkin event because publisher channel is closed (stream_id={}, outcome={})",
                    stream_id, outcome
                );
            }
        }
    }
}

fn maybe_emit_zipkin_event(
    should_emit_zipkin: bool,
    zipkin_ctx: Option<&Arc<ZipkinCtx>>,
    stream_id: i32,
    outcome: impl Into<String>,
) {
    if should_emit_zipkin {
        emit_zipkin_event(zipkin_ctx, stream_id, outcome);
    }
}

// utility that generates a traceID per event based on the event timestamp, stream_id and a sequence number.
fn make_zipkin_ids(event: &ZipkinEvent, seq: u64) -> (String, String) {
    let stream_bits = u64::from(event.stream_id as u32);
    let lo = event.ts_unix_us ^ stream_bits.rotate_left(13) ^ seq;
    let hi = event.ts_unix_us.rotate_left(29) ^ stream_bits.rotate_left(7) ^ seq.rotate_left(3);
    (format!("{:016x}{:016x}", hi, lo), format!("{:016x}", lo))
}

async fn flush_zipkin_batch(
    client: &reqwest::Client,
    target_url: &str,
    batch: &mut Vec<ZipkinEvent>,
    span_seq: &mut u64,
) {
    if batch.is_empty() {
        return;
    }

    let mut spans: Vec<ZipkinSpan> = Vec::with_capacity(batch.len());
    for event in batch.drain(..) {
        let this_seq = *span_seq;
        *span_seq = (*span_seq).wrapping_add(1);
        let (trace_id, id) = make_zipkin_ids(&event, this_seq);
        let mut tags = HashMap::new();
        tags.insert("stream_id".to_string(), event.stream_id.to_string());
        tags.insert("outcome".to_string(), event.outcome.to_string());

        spans.push(ZipkinSpan {
            trace_id,
            id, //  this is unfortunately imposed by the zipkin api. At the moment we are not really benefitting from cluttering the span with this, but we must respect the API requirements.
            name: ZIPKIN_SPAN_NAME.to_string(),
            timestamp: event.ts_unix_us,
            duration: 1, //  this is apparently the standard to model "point events" in Zipkin
            local_endpoint: ZipkinEndpoint {
                service_name: ZIPKIN_SERVICE_NAME.to_string(),
            },
            tags,
        });
    }

    match client.post(target_url).json(&spans).send().await {
        Ok(resp) => {
            if !resp.status().is_success() {
                warn!(
                    "Zipkin batch POST returned non-success status: {}",
                    resp.status()
                );
            }
        }
        Err(err) => {
            warn!("Zipkin batch POST failed: {}", err);
        }
    }
}

async fn zipkin_batch_publisher(
    mut rx: mpsc::Receiver<ZipkinEvent>,
    zipkin_addr: String,
    zipkin_port: u16,
    zipkin_batch_size: usize,
    zipkin_flush_interval_ms: u64,
) {
    let batch_size = zipkin_batch_size.max(1);
    let target_url = format!("http://{}:{}/api/v2/spans", zipkin_addr, zipkin_port);
    let client = reqwest::Client::new();
    let mut ticker = time::interval(Duration::from_millis(zipkin_flush_interval_ms.max(1)));
    ticker.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    let mut batch: Vec<ZipkinEvent> = Vec::with_capacity(batch_size);
    let mut span_seq: u64 = 1;

    loop {
        tokio::select! {
            maybe_event = rx.recv() => {
                match maybe_event {
                    Some(event) => {
                        batch.push(event);
                        if batch.len() >= batch_size {
                            flush_zipkin_batch(&client, &target_url, &mut batch, &mut span_seq).await;
                        }
                    }
                    None => {
                        flush_zipkin_batch(&client, &target_url, &mut batch, &mut span_seq).await;
                        debug!("Zipkin publisher exiting after channel close");
                        return;
                    }
                }
            }
            _ = ticker.tick() => {
                if !batch.is_empty() {
                    flush_zipkin_batch(&client, &target_url, &mut batch, &mut span_seq).await;
                }
            }
        }
    }
}

// ********* Register/invoke Dandelion functions*************
// ************************************************
// ************************************************

// Used by the client side. But for the simplicity, we use it for register_function_local
// to achieve minimal changes when compared with the original register_function
#[derive(Serialize, Deserialize)]
struct RegisterFunctionLocal {
    name: String,
    context_size: u64,
    engine_type: String,
    local_path: String,
    binary: Vec<u8>,
    input_sets: Vec<(String, Option<Vec<(String, Vec<u8>)>>)>,
    output_sets: Vec<String>,
}

// To register a function directly with local fun bin files
async fn register_function_local(
    func_bin_path: String,
    func_name: String,
    engine_type: String,
    dispatcher: mpsc::Sender<DispatcherCommand>,
) -> Result<Response<DandelionBody>, Infallible> {
    // let bytes = req
    //     .collect()
    //     .await
    //     .expect("Failed to extract body from function registration")
    //     .to_bytes();

    // *** Currently the only parts different from the original register_function() ***
    let register_request = bson::to_vec(&RegisterFunctionLocal {
        name: func_name.clone(),
        context_size: 0x802_0000,
        local_path: func_bin_path,
        binary: Vec::new(),
        engine_type,
        input_sets: vec![(String::from(""), None); 256], // set to 256 input sets
        output_sets: vec![String::from(""); 256],        // set to 256 output sets
    })
    .unwrap();
    let bytes = Bytes::from(register_request);
    // *** Difference ends ***

    // find first line end character
    let request_map: RegisterFunctionLocal =
        bson::from_slice(&bytes).expect("Should be able to deserialize request");
    // if local is present ignore the binary
    let path_string = if !request_map.local_path.is_empty() {
        // check that file exists
        if let Err(err) = std::fs::File::open(&request_map.local_path) {
            error!("[register_function_local]: open local file error!!!");
            let err_message = format!(
                "Tried to register function with local path, but failed to open file with error {}",
                err
            );
            return Ok::<_, Infallible>(Response::new(DandelionBody::from_vec(
                err_message.as_bytes().to_vec(),
            )));
        };
        request_map.local_path
    } else {
        // write function to file
        std::fs::create_dir_all(FUNCTION_FOLDER_PATH).unwrap();
        let mut path_buff = PathBuf::from(FUNCTION_FOLDER_PATH);
        path_buff.push(request_map.name.clone());
        let mut function_file = std::fs::File::create(path_buff.clone())
            .expect("Failed to create file for registering function");
        function_file
            .write_all(&request_map.binary)
            .expect("Failed to write file with content for registering");
        path_buff.to_str().unwrap().to_string()
    };

    let engine_type = match request_map.engine_type.as_str() {
        #[cfg(feature = "wasm")]
        "RWasm" => EngineType::RWasm,
        #[cfg(feature = "mmu")]
        "Process" => EngineType::Process,
        #[cfg(feature = "kvm")]
        "Kvm" => EngineType::Kvm,
        #[cfg(feature = "cheri")]
        "Cheri" => EngineType::Cheri,
        unkown => panic!("Unkown engine type string {}", unkown),
    };
    let input_sets = request_map
        .input_sets
        .into_iter()
        .map(|(name, data)| {
            if let Some(static_data) = data {
                let data_contexts = static_data
                    .into_iter()
                    .map(|(item_name, data_vec)| {
                        let item_size = data_vec.len();
                        let mut new_context =
                            ReadOnlyContext::new(data_vec.into_boxed_slice()).unwrap();
                        new_context.content.push(Some(DataSet {
                            ident: name.clone(),
                            buffers: vec![DataItem {
                                ident: item_name,
                                data: Position {
                                    offset: 0,
                                    size: item_size,
                                },
                                key: 0,
                            }],
                        }));
                        Arc::new(new_context)
                    })
                    .collect();
                let composition_set = CompositionSet::from((0, data_contexts));
                (name, Some(composition_set))
            } else {
                (name, None)
            }
        })
        .collect();
    let (callback, confirmation) = oneshot::channel();
    let metadata = Metadata {
        input_sets: input_sets,
        output_sets: request_map.output_sets,
    };
    dispatcher
        .send(DispatcherCommand::FunctionRegistration {
            name: request_map.name,
            engine_type,
            context_size: request_map.context_size as usize,
            path: path_string,
            metadata,
            callback,
        })
        .await
        .unwrap();
    confirmation
        .await
        .unwrap()
        .expect("Should be able to insert function");
    return Ok::<_, Infallible>(Response::new(DandelionBody::from_vec(
        "Function registered".as_bytes().to_vec(),
    )));
}

// To invoke a dandelion function
async fn invoke_dandelion_function(
    function_name: String,
    input_sets: Vec<InputSet<'_>>,
    request_sender: mpsc::Sender<DispatcherCommand>,
) -> (Vec<Option<CompositionSet>>, Recorder) {
    info!(
        "About to invoke the Dandelion Function with name: {}",
        function_name
    );

    let nghttp2_codec_request = DandelionRequest {
        name: function_name,
        sets: input_sets,
    };

    let nghttp2_codec_request_bytes = bson::to_vec(&nghttp2_codec_request).unwrap();
    let total_size = nghttp2_codec_request_bytes.len();
    let frame_data = vec![Bytes::from(nghttp2_codec_request_bytes)];

    // from context from frame bytes
    let request_context_result = BytesContext::from_bytes_vec(frame_data, total_size).await;
    if request_context_result.is_err() {
        warn!("request parsing failed with: {:?}", request_context_result);
    }
    let (function_name, request_context) = request_context_result.unwrap();
    debug!("finished creating request context");

    // TODO match set names to assign sets to composition sets
    // map sets in the order they are in the request
    let request_number = request_context.content.len();
    debug!("Request number of request_context: {}", request_number);
    let request_arc = Arc::new(request_context);
    let inputs = (0..request_number)
        .map(|set_id| {
            DispatcherInput::Set(CompositionSet::from((set_id, vec![request_arc.clone()])))
        })
        .collect::<Vec<_>>();

    // want a 1 to 1 mapping of all outputs the functions gives as long as we don't add user input on what they want
    let is_cold: bool = false;
    let start_time = Instant::now();

    let (callback, output_recevier) = tokio::sync::oneshot::channel();
    request_sender
        .send(DispatcherCommand::FunctionRequest {
            name: function_name,
            inputs,
            is_cold,
            start_time: start_time.clone(),
            callback,
        })
        .await
        .unwrap();
    let (function_output, recorder) = output_recevier
        .await
        .unwrap()
        .expect("Should get result from function");

    (function_output, recorder)
}

// ********* Invoke authentication policy func; Parse its output *************

fn make_single_item_context(
    set_index: usize,
    set_ident: &str,
    item_ident: &str,
    key: u32,
    data: Box<[u8]>,
) -> Arc<machine_interface::memory_domain::Context> {
    let data_len = data.len();
    let mut ctx = ReadOnlyContext::new(data).expect("Failed to create ReadOnlyContext");
    if ctx.content.len() <= set_index {
        ctx.content.resize_with(set_index + 1, || None);
    }
    ctx.content[set_index] = Some(DataSet {
        ident: set_ident.to_string(),
        buffers: vec![DataItem {
            ident: item_ident.to_string(),
            data: Position {
                offset: 0,
                size: data_len,
            },
            key,
        }],
    });
    Arc::new(ctx)
}

async fn invoke_dandelion_function_direct(
    function_name: String,
    input_sets: Vec<Option<CompositionSet>>,
    request_sender: mpsc::Sender<DispatcherCommand>,
) -> (Vec<Option<CompositionSet>>, Recorder) {
    let inputs = input_sets
        .into_iter()
        .map(|set| match set {
            Some(s) => DispatcherInput::Set(s),
            None => DispatcherInput::None,
        })
        .collect::<Vec<_>>();

    let is_cold: bool = false;
    let start_time = Instant::now();
    let (callback, output_recevier) = tokio::sync::oneshot::channel();
    request_sender
        .send(DispatcherCommand::FunctionRequest {
            name: function_name,
            inputs,
            is_cold,
            start_time: start_time.clone(),
            callback,
        })
        .await
        .unwrap();

    let (function_output, recorder) = output_recevier
        .await
        .unwrap()
        .expect("Should get result from function");

    (function_output, recorder)
}

async fn prepare_input_and_invoke_jwt_policy(
    jwt_policy_func_name: String,
    jwt_pem_context: Arc<Context>,
    request_sender: mpsc::Sender<DispatcherCommand>,
    header_pairs: &Vec<(String, String)>,
) -> u8 {
    let mut contexts: Vec<Arc<Context>> = Vec::new();
    contexts.push(jwt_pem_context);

    let mut found_token_in_header = false;
    for (header_name, header_value) in header_pairs {
        if header_name == "authorization" {
            let header_ctx = make_single_item_context(
                0,
                "",
                "",
                1,
                header_value.clone().into_bytes().into_boxed_slice(),
            );
            contexts.push(header_ctx);
            found_token_in_header = true;
        }
    }
    if !found_token_in_header {
        debug!("No authorization header found in caller's request!");
        return 0;
    }

    let set0 = CompositionSet::from((0, contexts));
    let input_sets: Vec<Option<CompositionSet>> = vec![Some(set0)];

    let (function_output, recorder) = invoke_dandelion_function_direct(
        String::from(jwt_policy_func_name),
        input_sets,
        request_sender.clone(),
    )
    .await;

    let response_dandelion_body = dandelion_server::DandelionBody::new(function_output, &recorder);
    let body: Bytes = response_dandelion_body.into_bytes();
    let response: DandelionDeserializeResponse = bson::from_slice(&body).unwrap();
    response.sets[0].items[0].data[0]
}

// ********* Invoke authorization policy func; Parse its output *************
async fn prepare_input_and_invoke_authorization_policy(
    authorization_policy_func_name: String,
    request_sender: mpsc::Sender<DispatcherCommand>,
    header_pairs: &Vec<(String, String)>,
) -> u8 {
    let mut input_set0_items = Vec::new();

    for (header_name, header_value) in header_pairs {
        if header_name == ":method" {
            let header_value_bytes = header_value.as_bytes();
            input_set0_items.push(InputItem {
                identifier: String::from(""),
                key: 0,
                data: &header_value_bytes,
            });
            // debug!("Authorization policy input :method: {}", header_value);
        }
    }

    let input_sets: Vec<InputSet> = vec![InputSet {
        identifier: String::from(""),
        items: input_set0_items,
    }];

    let (function_output, recorder) = invoke_dandelion_function(
        String::from(authorization_policy_func_name),
        input_sets,
        request_sender.clone(),
    )
    .await;

    let response_dandelion_body = dandelion_server::DandelionBody::new(function_output, &recorder);

    let body: Bytes = response_dandelion_body.into_bytes();
    let response: DandelionDeserializeResponse = bson::from_slice(&body).unwrap();

    let authorization_verdict = response.sets[0].items[0].data[0];

    // debug!("Authorization policy function output: {:?}", function_output);
    authorization_verdict
}

async fn prepare_input_and_invoke_rate_limiting_policy(
    rate_limiting_policy_func_name: String,
    request_sender: mpsc::Sender<DispatcherCommand>,
    number_of_requests_in_current_time_window: u32,
    rate_limiting_requests_per_time_unit: u32,
) -> u8 {
    // For simplicity, we only pass the ":path" header to the rate limiting function as input for now
    let mut input_set0_items = Vec::new();

    let number_of_requests_in_current_time_window =
        number_of_requests_in_current_time_window.to_ne_bytes();
    let rate_limiting_requests_per_time_unit = rate_limiting_requests_per_time_unit.to_ne_bytes();
    input_set0_items.push(InputItem {
        identifier: String::from(""),
        key: 0,
        data: &number_of_requests_in_current_time_window,
    });
    input_set0_items.push(InputItem {
        identifier: String::from(""),
        key: 1,
        data: &rate_limiting_requests_per_time_unit,
    });

    let input_sets: Vec<InputSet> = vec![InputSet {
        identifier: String::from(""),
        items: input_set0_items,
    }];

    let (function_output, recorder) = invoke_dandelion_function(
        String::from(rate_limiting_policy_func_name),
        input_sets,
        request_sender.clone(),
    )
    .await;

    let response_dandelion_body = dandelion_server::DandelionBody::new(function_output, &recorder);

    let body: Bytes = response_dandelion_body.into_bytes();
    let response: DandelionDeserializeResponse = bson::from_slice(&body).unwrap();

    let rate_limiting_verdict = response.sets[0].items[0].data[0];

    // debug!("Rate limiting policy function output: {:?}", function_output);
    rate_limiting_verdict
}

async fn prepare_input_and_invoke_telemetry_policy(
    telemetry_policy_func_name: String,
    request_sender: mpsc::Sender<DispatcherCommand>,
    authorization_policy_start_ts_us: u64,
    authorization_policy_end_ts_us: u64,
    jwt_policy_start_ts_us: u64,
    jwt_policy_end_ts_us: u64,
    rate_limiting_policy_start_ts_us: u64,
    rate_limiting_policy_end_ts_us: u64,
    execution_engine_roundtrip_start_ts_us: u64,
    execution_engine_roundtrip_end_ts_us: u64,
) -> String {
    let mut input_set0_items = Vec::new();

    let authorization_policy_start_ts_us = authorization_policy_start_ts_us.to_ne_bytes();
    let authorization_policy_end_ts_us = authorization_policy_end_ts_us.to_ne_bytes();
    let jwt_policy_start_ts_us = jwt_policy_start_ts_us.to_ne_bytes();
    let jwt_policy_end_ts_us = jwt_policy_end_ts_us.to_ne_bytes();
    let rate_limiting_policy_start_ts_us = rate_limiting_policy_start_ts_us.to_ne_bytes();
    let rate_limiting_policy_end_ts_us = rate_limiting_policy_end_ts_us.to_ne_bytes();
    let execution_engine_roundtrip_start_ts_us =
        execution_engine_roundtrip_start_ts_us.to_ne_bytes();
    let execution_engine_roundtrip_end_ts_us = execution_engine_roundtrip_end_ts_us.to_ne_bytes();

    input_set0_items.push(InputItem {
        identifier: String::from(""),
        key: 0,
        data: &authorization_policy_start_ts_us,
    });
    input_set0_items.push(InputItem {
        identifier: String::from(""),
        key: 1,
        data: &authorization_policy_end_ts_us,
    });
    input_set0_items.push(InputItem {
        identifier: String::from(""),
        key: 2,
        data: &jwt_policy_start_ts_us,
    });
    input_set0_items.push(InputItem {
        identifier: String::from(""),
        key: 3,
        data: &jwt_policy_end_ts_us,
    });
    input_set0_items.push(InputItem {
        identifier: String::from(""),
        key: 4,
        data: &rate_limiting_policy_start_ts_us,
    });
    input_set0_items.push(InputItem {
        identifier: String::from(""),
        key: 5,
        data: &rate_limiting_policy_end_ts_us,
    });
    input_set0_items.push(InputItem {
        identifier: String::from(""),
        key: 6,
        data: &execution_engine_roundtrip_start_ts_us,
    });
    input_set0_items.push(InputItem {
        identifier: String::from(""),
        key: 7,
        data: &execution_engine_roundtrip_end_ts_us,
    });

    let input_sets: Vec<InputSet> = vec![InputSet {
        identifier: String::from(""),
        items: input_set0_items,
    }];

    let (function_output, recorder) = invoke_dandelion_function(
        String::from(telemetry_policy_func_name),
        input_sets,
        request_sender.clone(),
    )
    .await;

    let response_dandelion_body = dandelion_server::DandelionBody::new(function_output, &recorder);

    let body: Bytes = response_dandelion_body.into_bytes();
    let response: DandelionDeserializeResponse = bson::from_slice(&body).unwrap();

    let telemetry_message = response.sets[0].items[0]
        .data
        .iter()
        .map(|b| *b as char)
        .collect::<String>();

    telemetry_message
}

// ********* Invoke nghttp2 code func; Parse its output *************
// ******************************************************************
// ******************************************************************

// Prepare the input to the nghttp2_codec function and invokes it
async fn prepare_input_and_invoke_nghttp2_codec(
    nghttp2_codec_func_name: String,
    request_sender: mpsc::Sender<DispatcherCommand>,
    is_server: i8,
    first_create: i8,
    mut size_of_session_state: usize,
    size_of_data_to_read: usize,
    num_req_resp_to_send: usize,
    num_req_resp_to_receive: usize,
    session_state: &Vec<u8>,
    data_to_read: &Vec<u8>,
    stream_id_to_send_response_list: &Vec<i32>,
    num_of_headers_to_send_list: &Vec<usize>,
    trailer_offset_to_send_list: &Vec<i32>,
    num_of_trailers_to_send_list: &Vec<usize>,
    size_of_headers_to_send_list: &Vec<usize>,
    size_of_body_to_send_list: &Vec<usize>,
    headers_to_send_list: &Vec<Vec<u8>>,
    body_to_send_list: &Vec<Vec<u8>>,
) -> (Vec<Option<CompositionSet>>, Recorder) {
    // *** Prepare Input to the nghttp2 codec *****
    // **** input set items to the ngtthp2 codec; ****

    // **input set 0 **
    let mut input_set0_items = Vec::new();

    let input_nghttp2_is_server = is_server.to_ne_bytes();
    let input_nghttp2_first_create = first_create.to_ne_bytes();
    if size_of_session_state > 0 {
        size_of_session_state = size_of_session_state - 8; // // The first 8 bytes are the size, not the actual session state
    }
    let input_nghttp2_size_of_session_state = (size_of_session_state).to_ne_bytes(); // Actually this input is not needed by the codec. So we don't track it
    let input_size_of_data_to_read = size_of_data_to_read.to_ne_bytes();
    let input_num_req_resp_to_send = num_req_resp_to_send.to_ne_bytes();
    let input_num_req_resp_to_receive = num_req_resp_to_receive.to_ne_bytes();
    debug!("nghttp2_codec INPUT set0 is_server: {}", is_server);
    debug!("nghttp2_codec INPUT set0 first_create: {}", first_create);
    debug!(
        "nghttp2_codec INPUT set0 size_of_session_state: {}",
        size_of_session_state
    );
    debug!(
        "nghttp2_codec INPUT set0 session_state.len(): {}",
        session_state.len()
    );
    debug!(
        "nghttp2_codec INPUT set0 size_of_data_to_read: {}",
        size_of_data_to_read
    );
    debug!(
        "nghttp2_codec INPUT set0 num_req_resp_to_send: {}",
        num_req_resp_to_send
    );
    debug!(
        "nghttp2_codec INPUT set0 num_req_resp_to_receive: {}",
        num_req_resp_to_receive
    );

    input_set0_items.push(InputItem {
        identifier: String::from(""),
        key: 0,
        data: &input_nghttp2_is_server,
    });

    input_set0_items.push(InputItem {
        identifier: String::from(""),
        key: 1,
        data: &input_nghttp2_first_create,
    });

    input_set0_items.push(InputItem {
        identifier: String::from(""),
        key: 2,
        data: &input_nghttp2_size_of_session_state,
    });

    input_set0_items.push(InputItem {
        identifier: String::from(""),
        key: 3,
        data: &session_state[8..], // The first 8 bytes are the size, not the actual session state
    });

    input_set0_items.push(InputItem {
        identifier: String::from(""),
        key: 4,
        data: &input_size_of_data_to_read,
    });

    input_set0_items.push(InputItem {
        identifier: String::from(""),
        key: 5,
        data: &data_to_read[..size_of_data_to_read],
    });

    input_set0_items.push(InputItem {
        identifier: String::from(""),
        key: 6,
        data: &input_num_req_resp_to_send,
    });

    input_set0_items.push(InputItem {
        identifier: String::from(""),
        key: 7,
        data: &input_num_req_resp_to_receive,
    });

    let mut input_sets: Vec<InputSet> = Vec::with_capacity(num_req_resp_to_send + 1);
    input_sets.push(InputSet {
        identifier: String::from(""),
        items: input_set0_items,
    });

    // ** input set 1 ~ set n **
    // Transfer the input to [u8]
    let mut input_stream_id_to_send_response_list = Vec::with_capacity(num_req_resp_to_send);
    let mut input_num_of_headers_to_send_list = Vec::with_capacity(num_req_resp_to_send);
    let mut input_trailer_offset_to_send_list = Vec::with_capacity(num_req_resp_to_send);
    let mut input_num_of_trailers_to_send_list = Vec::with_capacity(num_req_resp_to_send);
    let mut input_size_of_headers_to_send_list = Vec::with_capacity(num_req_resp_to_send);
    let mut input_size_of_body_to_send_list = Vec::with_capacity(num_req_resp_to_send);
    for input_set_idx in 1..(1 + num_req_resp_to_send) {
        let stream_id_to_send_response = stream_id_to_send_response_list[input_set_idx - 1];
        let num_of_headers_to_send = num_of_headers_to_send_list[input_set_idx - 1];
        let trailer_offset_to_send = trailer_offset_to_send_list[input_set_idx - 1];
        let num_of_trailers_to_send = num_of_trailers_to_send_list[input_set_idx - 1];
        let size_of_headers_to_send = size_of_headers_to_send_list[input_set_idx - 1];
        let size_of_body_to_send = size_of_body_to_send_list[input_set_idx - 1];

        let input_stream_id_to_send_response = stream_id_to_send_response.to_ne_bytes();
        let input_num_of_headers_to_send = num_of_headers_to_send.to_ne_bytes();
        let input_trailer_offset_to_send = trailer_offset_to_send.to_ne_bytes();
        let input_num_of_trailers_to_send = num_of_trailers_to_send.to_ne_bytes();
        let input_size_of_headers_to_send = size_of_headers_to_send.to_ne_bytes();
        let input_size_of_body_to_send = size_of_body_to_send.to_ne_bytes();

        input_stream_id_to_send_response_list.push(input_stream_id_to_send_response);
        input_num_of_headers_to_send_list.push(input_num_of_headers_to_send);
        input_trailer_offset_to_send_list.push(input_trailer_offset_to_send);
        input_num_of_trailers_to_send_list.push(input_num_of_trailers_to_send);
        input_size_of_headers_to_send_list.push(input_size_of_headers_to_send);
        input_size_of_body_to_send_list.push(input_size_of_body_to_send);

        debug!("nghttp2_codec INPUT set {}", input_set_idx);
        debug!(
            "nghttp2_codec INPUT stream_id_to_send_response: {}",
            stream_id_to_send_response
        );
        debug!(
            "nghttp2_codec INPUT size_of_headers_to_send: {}",
            size_of_headers_to_send
        );
        debug!(
            "nghttp2_codec INPUT size_of_body_to_send: {}",
            size_of_body_to_send
        );
    }

    for input_set_idx in 1..(1 + num_req_resp_to_send) {
        let mut input_setn_items = Vec::new();

        let input_stream_id_to_send_response =
            &input_stream_id_to_send_response_list[input_set_idx - 1];
        let input_num_of_headers_to_send = &input_num_of_headers_to_send_list[input_set_idx - 1];
        let input_trailer_offset_to_send = &input_trailer_offset_to_send_list[input_set_idx - 1];
        let input_num_of_trailers_to_send = &input_num_of_trailers_to_send_list[input_set_idx - 1];
        let input_size_of_headers_to_send = &input_size_of_headers_to_send_list[input_set_idx - 1];
        let input_size_of_body_to_send = &input_size_of_body_to_send_list[input_set_idx - 1];
        let headers_to_send = &headers_to_send_list[input_set_idx - 1];
        let body_to_send = &body_to_send_list[input_set_idx - 1];

        input_setn_items.push(InputItem {
            identifier: String::from(""),
            key: 6,
            data: input_stream_id_to_send_response,
        });

        input_setn_items.push(InputItem {
            identifier: String::from(""),
            key: 7,
            data: input_num_of_headers_to_send,
        });

        input_setn_items.push(InputItem {
            identifier: String::from(""),
            key: 8,
            data: input_size_of_headers_to_send,
        });

        input_setn_items.push(InputItem {
            identifier: String::from(""),
            key: 9,
            data: headers_to_send,
        });

        input_setn_items.push(InputItem {
            identifier: String::from(""),
            key: 10,
            data: input_size_of_body_to_send,
        });

        input_setn_items.push(InputItem {
            identifier: String::from(""),
            key: 11,
            data: body_to_send,
        });

        input_setn_items.push(InputItem {
            identifier: String::from(""),
            key: 12,
            data: input_trailer_offset_to_send,
        });

        input_setn_items.push(InputItem {
            identifier: String::from(""),
            key: 13,
            data: input_num_of_trailers_to_send,
        });

        input_sets.push(InputSet {
            identifier: String::from(""),
            items: input_setn_items,
        })
    }

    // Invoke the function
    let (function_output, recorder) = invoke_dandelion_function(
        String::from(nghttp2_codec_func_name),
        input_sets,
        request_sender.clone(),
    )
    .await;

    (function_output, recorder)
}

// parse nghttp2_codec output
// *** TO DO: Currently the way to get the func output involves unnessary serialization/deserialization and data copying ****
fn parse_nghttp2_codec_output(
    function_output: Vec<Option<CompositionSet>>,
    recorder: Recorder,
) -> (
    // set 0
    Vec<u8>, // session state
    usize,   // num_of_req_or_res_received
    usize,   // num_of_req_or_resp_sent
    Vec<u8>, // stream_id_list_sent_requests
    Vec<u8>, // data_to_send
    // set 1 ~ n
    Vec<i32>,     // stream_id_list
    Vec<usize>,   // num_of_headers_list
    Vec<i32>,     // trailer_offset_list
    Vec<usize>,   // num_of_trailers_list
    Vec<Vec<u8>>, // headers_received_list
    Vec<Vec<u8>>, // body_received_list
    Vec<String>,  // header_authority_value_string_list
    Vec<String>,  // header_x_anakonda_forward_value_string_list
    Vec<String>,  // header_status_string_list
) {
    // ************************
    // *** parse the output ***

    let response_dandelion_body = dandelion_server::DandelionBody::new(function_output, &recorder);

    // ***TO DO***: currently we first transfer the reponse_dandelion_body into Bytes and then Deserialize it.
    // Can we directly get the reponse from the response_dandelion_body (DandelionBody)
    let body = response_dandelion_body.into_bytes(); // tmp, should have zero-copy in the future
    let response: DandelionDeserializeResponse = bson::from_slice(&body).unwrap();

    info!("response.sets.len(): {}", response.sets.len());

    // ***TO DO****: currently we copy data from the output_item.data (&[u8]). Can we avoid this data copying?
    // *** set 0 ***
    assert_eq!(5, response.sets[0].items.len());
    info!("output set 0");
    let mut session_state: Vec<u8> = Vec::new();
    let mut num_of_req_or_res_received: usize = 0;
    let mut num_of_req_or_resp_sent: usize = 0;
    let mut stream_id_list_sent_requests: Vec<u8> = Vec::new();
    let mut data_to_send: Vec<u8> = Vec::new();
    for output_item in &(response.sets[0].items) {
        let output_item_key = output_item.key;
        let output_item_data = output_item.data;

        match output_item_key {
            0 => {
                session_state.clear();
                session_state.extend_from_slice(output_item_data);
                info!("codec OUTPUT session state len: {}", session_state.len());
            }
            1 => {
                let arr: [u8; size_of::<usize>()] =
                    output_item_data.try_into().expect("wrong length");
                num_of_req_or_res_received = usize::from_ne_bytes(arr);

                info!(
                    "codec OUTPUT num_of_req_or_res_received: {}",
                    num_of_req_or_res_received
                );
            }
            2 => {
                let arr: [u8; size_of::<usize>()] =
                    output_item_data.try_into().expect("wrong length");
                num_of_req_or_resp_sent = usize::from_ne_bytes(arr);

                info!(
                    "codec OUTPUT num_of_req_or_resp_sent: {}",
                    num_of_req_or_resp_sent
                );
            }
            3 => {
                stream_id_list_sent_requests.clear();
                stream_id_list_sent_requests.extend_from_slice(output_item_data);
            }
            4 => {
                data_to_send.clear();
                data_to_send.extend_from_slice(output_item_data);
            }

            _ => {}
        }
    }

    // *** Set 1 ~ n***
    // *** Each set is one resp or req (received)
    let mut stream_id_list: Vec<i32> = Vec::new();
    let mut num_of_headers_list: Vec<usize> = Vec::new();
    let mut trailer_offset_list: Vec<i32> = Vec::new();
    let mut num_of_trailers_list: Vec<usize> = Vec::new();
    let mut headers_received_list: Vec<Vec<u8>> = Vec::new();
    let mut body_received_list: Vec<Vec<u8>> = Vec::new();
    let mut header_authority_value_string_list: Vec<String> = Vec::new();
    let mut header_x_anakonda_forward_value_string_list: Vec<String> = Vec::new();
    let mut header_status_value_string_list: Vec<String> = Vec::new();
    for idx_req_resp in 0..num_of_req_or_res_received {
        let set_idx = idx_req_resp + 1;

        let mut stream_id: i32 = -1;
        let mut num_of_headers: usize = 0;
        let mut trailer_offset: i32 = -1;
        let mut num_of_trailers: usize = 0;
        let mut headers_received: Vec<u8> = Vec::new();
        let mut body_received: Vec<u8> = Vec::new();
        let mut header_authority_value: Vec<u8> = Vec::new();
        let header_authority_value_string;
        let mut header_x_anakonda_forward_value: Vec<u8> = Vec::new();
        let header_x_anakonda_forward_value_string;
        let mut header_status_value: Vec<u8> = Vec::new();
        let header_status_value_string;

        assert_eq!(10, response.sets[set_idx].items.len());

        debug!("output set {}", set_idx);
        for output_item in &(response.sets[set_idx].items) {
            let output_item_key = output_item.key;
            let output_item_data = output_item.data;

            match output_item_key {
                0 => {
                    let arr: [u8; size_of::<i32>()] =
                        output_item_data.try_into().expect("wrong length");
                    stream_id = i32::from_ne_bytes(arr);

                    debug!("codec OUTPUT stream id: {}", stream_id);
                }
                1 => {
                    let arr: [u8; size_of::<usize>()] =
                        output_item_data.try_into().expect("wrong length");
                    num_of_headers = usize::from_ne_bytes(arr);

                    debug!(" codec OUTPUT num_of_headers: {}", num_of_headers);
                }
                2 => {
                    headers_received.clear();
                    headers_received.extend_from_slice(output_item_data);

                    debug!(
                        "codec OUTPUT headers_received.len(): {}",
                        headers_received.len()
                    );
                }
                3 => {
                    body_received.clear();
                    body_received.extend_from_slice(output_item_data);

                    debug!("codec OUTPUT body_received.len(): {}", body_received.len());
                }
                4 => {
                    header_authority_value.clear();
                    header_authority_value.extend_from_slice(output_item_data);

                    debug!(
                        "codec header_authority_value length: {}",
                        header_authority_value.len()
                    );
                }
                6 => {
                    header_status_value.clear();
                    header_status_value.extend_from_slice(output_item_data);

                    debug!(
                        "codec header_status_value length: {}",
                        header_status_value.len()
                    );
                }
                7 => {
                    header_x_anakonda_forward_value.clear();
                    header_x_anakonda_forward_value.extend_from_slice(output_item_data);

                    debug!(
                        "codec header_x_anakonda_forward_value length: {}",
                        header_x_anakonda_forward_value.len()
                    );
                }
                8 => {
                    let arr: [u8; size_of::<i32>()] =
                        output_item_data.try_into().expect("wrong length");
                    trailer_offset = i32::from_ne_bytes(arr);

                    debug!(" codec OUTPUT trailer_offset: {}", trailer_offset);
                }
                9 => {
                    let arr: [u8; size_of::<usize>()] =
                        output_item_data.try_into().expect("wrong length");
                    num_of_trailers = usize::from_ne_bytes(arr);

                    debug!(" codec OUTPUT num_of_trailers: {}", num_of_trailers);
                }
                _ => {}
            }
        }

        stream_id_list.push(stream_id);
        num_of_headers_list.push(num_of_headers);
        trailer_offset_list.push(trailer_offset);
        num_of_trailers_list.push(num_of_trailers);
        headers_received_list.push(headers_received);
        body_received_list.push(body_received);

        header_authority_value_string = String::from_utf8(header_authority_value).unwrap();
        debug!(
            "codec header_authority_value: {}",
            header_authority_value_string
        );
        header_authority_value_string_list.push(header_authority_value_string);

        header_x_anakonda_forward_value_string =
            String::from_utf8(header_x_anakonda_forward_value).unwrap();
        debug!(
            "codec header_x_anakonda_forward_value: {}",
            header_x_anakonda_forward_value_string
        );
        header_x_anakonda_forward_value_string_list.push(header_x_anakonda_forward_value_string);

        header_status_value_string = String::from_utf8(header_status_value).unwrap();
        debug!("codec header_status_value: {}", header_status_value_string);
        header_status_value_string_list.push(header_status_value_string);
    }

    // debug!("headers_received_list: {:?}", headers_received_list);

    for header in &headers_received_list {
        debug!(
            "headers_received: {}",
            String::from_utf8(header.to_vec()).unwrap()
        );
    }

    (
        // set 0
        session_state,
        num_of_req_or_res_received,
        num_of_req_or_resp_sent,
        stream_id_list_sent_requests,
        data_to_send,
        // set 1 ~ n
        stream_id_list,
        num_of_headers_list,
        trailer_offset_list,
        num_of_trailers_list,
        headers_received_list,
        body_received_list,
        header_authority_value_string_list,
        header_x_anakonda_forward_value_string_list,
        header_status_value_string_list,
    )
}

// ********* Different worker threads *************
// ************************************************
// ************************************************

// Define message to communicate between different worker threads
// *** TO DO: The payload filed now is actually not used but just for printing out some info. Should remove it ***

#[derive(Debug)]
struct StreamWorkerToRouterReq {
    payload: String,
    header_authority_value_string: String,
    header_x_anakonda_forward_value_string: String,
    router_to_stream_worker_tx: oneshot::Sender<RouterToStreamWorkerResp>,
}

#[derive(Debug)]
struct StreamWorkerToRouterReport5xxErrorReq {
    header_x_anakonda_forward_value_string: String, // The url
}

#[derive(Debug)]
struct RouterToStreamWorkerResp {
    payload: String,
    stream_worker_to_func_connection_worker_tx: mpsc::Sender<StreamWorkerToFuncConnReq>,
    circuit_break: bool,
    cb_response_status: String,
    cb_response_body: String,
}

struct FunctionConnToStreamWorkerResp {
    payload: String,
    resp_status: String,
    num_of_headers_to_send: usize,
    trailer_offset: i32,
    num_of_trailers_to_send: usize,
    headers: Vec<u8>,
    body_to_send: Vec<u8>,
}

struct StreamWorkerToFuncConnReq {
    payload: String,
    stream_id: i32, // The stream_id of that stream worker
    num_of_headers_to_send: usize,
    trailer_offset: i32,
    num_of_trailers_to_send: usize,
    headers: Vec<u8>,
    body: Vec<u8>,
    function_connection_worker_to_stream_worker_tx: mpsc::Sender<FunctionConnToStreamWorkerResp>,
}

struct StreamWorkerToDpConnReq {
    payload: String,
    stream_id_to_send_response: i32,
    num_of_headers_to_send: usize,
    trailer_offset: i32,
    num_of_trailers_to_send: usize,
    headers: Vec<u8>,
    body_to_send: Vec<u8>,
}

impl Default for StreamWorkerToDpConnReq {
    fn default() -> Self {
        Self {
            payload: String::new(),
            stream_id_to_send_response: -1,
            num_of_headers_to_send: 0,
            trailer_offset: -1,
            num_of_trailers_to_send: 0,
            headers: Vec::new(),
            body_to_send: Vec::new(),
        }
    }
}

struct RouterToFuncConnReq {
    function_connection_worker_to_router_tx: oneshot::Sender<usize>,
}

struct FuncConnTuple {
    conn_id: u32, // used to distinguish connections to the same sandbox
    stream_worker_to_func_connection_worker_tx: mpsc::Sender<StreamWorkerToFuncConnReq>,
    router_to_func_connection_worker_tx: mpsc::Sender<RouterToFuncConnReq>,
    num_pending_reqs: usize,
}

// a helper func used by the worker to read from a tcp stream until blocking
async fn read_from_tcp_stream_until_blocking(
    stream: &mut TcpStream,
    data_to_read: &mut Vec<u8>,
) -> bool {
    // The returned value indicates if the connection is closed by the peer or there is a connection error

    // Scratch buffer for each try_read call
    let mut buf = vec![0u8; 16 * 1024];

    loop {
        match stream.try_read(&mut buf) {
            Ok(0) => {
                // Peer closed the connection
                let _ = stream.shutdown().await;

                return true;
            }
            Ok(n) => {
                data_to_read.extend_from_slice(&buf[..n]);

                // Keep draining until the kernel says "no more right now"
                continue;
            }
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                // No more bytes currently available; process what we have

                return false;
            }
            Err(err) => {
                let _ = stream.shutdown().await;
                return true;
            }
        }
    }
}

// a helper func used by the worker to read from a ProxyStream (which can be either a TCP or TLS stream) until blocking
async fn read_from_stream_until_blocking<S: ProxyStream>(
    stream: &mut S,
    data_to_read: &mut Vec<u8>,
) -> bool {
    // The returned value indicates if the connection is closed by the peer or there is a connection error

    // Scratch buffer for each try_read call
    let mut buf = vec![0u8; 16 * 1024];

    loop {
        match stream.read_nonblocking(&mut buf) {
            Ok(0) => {
                // Peer closed the connection
                let _ = stream.close_stream().await;

                return true;
            }
            Ok(n) => {
                data_to_read.extend_from_slice(&buf[..n]);

                // Keep draining until the kernel says "no more right now"
                continue;
            }
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                // No more bytes currently available; process what we have

                return false;
            }
            Err(err) => {
                let _ = stream.close_stream().await;
                return true;
            }
        }
    }
}

fn generate_circuit_break_retry_resp(
    cb_resp_status: String,
    cb_resp_body: String,
) -> (Vec<u8>, Vec<u8>) {
    let mut headers: Vec<u8> = Vec::new();
    let mut body_to_send: Vec<u8> = Vec::new();

    let header_status_name_string = ":status\0";
    let header_stauts_name_len = header_status_name_string.len();
    let header_stauts_name_len_bytes = header_stauts_name_len.to_ne_bytes();
    let header_status_value_string = cb_resp_status;
    let header_stauts_value_len = header_status_value_string.len();
    let header_stauts_value_len_bytes = header_stauts_value_len.to_ne_bytes();
    headers.extend_from_slice(&header_stauts_name_len_bytes);
    headers.extend_from_slice(header_status_name_string.as_bytes());
    headers.extend_from_slice(&header_stauts_value_len_bytes);
    headers.extend_from_slice(header_status_value_string.as_bytes());

    let body_to_send_string = cb_resp_body;
    body_to_send.extend_from_slice(body_to_send_string.as_bytes());

    return (headers, body_to_send);
}

// This handles the TCP/HTTP connection with user function container
// Thus, it acts as a HTTP client
async fn func_connection_worker3(
    nghttp2_codec_func_name: String,
    mut stream: TcpStream,
    request_sender: mpsc::Sender<DispatcherCommand>,
    mut stream_worker_to_func_connection_worker_rx: mpsc::Receiver<StreamWorkerToFuncConnReq>,
    mut router_to_func_connection_worker_rx: mpsc::Receiver<RouterToFuncConnReq>,
) {
    let tcp_conn_local_addr = stream.local_addr().unwrap();
    let tcp_conn_peer_addr = stream.peer_addr().unwrap();
    info!(
        "[func_connection_worker3. Local Addr: {}; Peer Addr: {}] A new user function connection",
        tcp_conn_local_addr, tcp_conn_peer_addr
    );

    // Hash Map
    // Key: Stream id of the sent request
    // Value: Channel to the stream worker (to send the corresponding response)
    let mut stream_worker_map: HashMap<i32, mpsc::Sender<FunctionConnToStreamWorkerResp>> =
        HashMap::new();

    // Some numbers to track
    let mut num_pending_reqs: usize = 0;

    // Output (set_0) from the nghttp2 codec (other sets 1~n use local vars; each set is for one req or resp)
    let mut session_state: Vec<u8> = vec![0u8; 20 * 1024]; // Also the input
    let mut num_of_req_or_res_received: usize;
    let mut num_of_req_or_resp_sent: usize;
    let mut data_to_send: Vec<u8>;
    let mut stream_id_list_sent_requests: Vec<u8>;

    // input to the nghttp2 codec
    let is_server: i8 = 0;
    let mut first_create: i8;
    let mut size_of_session_state: usize = 0;
    let mut data_to_read = Vec::new();
    let mut size_of_data_to_read: usize;
    let mut num_req_resp_to_send: usize;
    let mut num_of_req_or_resp_to_receive: usize;
    let mut stream_id_to_send_response_list: Vec<i32>;
    let mut num_of_headers_to_send_list: Vec<usize>;
    let mut trailer_offset_to_send_list: Vec<i32>;
    let mut num_of_trailers_to_send_list: Vec<usize>;
    let mut size_of_headers_to_send_list: Vec<usize>;
    let mut headers_to_send_list: Vec<Vec<u8>>;
    let mut body_to_send_list: Vec<Vec<u8>>;
    let mut size_of_body_to_send_list: Vec<usize>;

    // The main logic loop (it stops until the other side closes the connection)
    let mut loop_idx = -1;
    loop {
        loop_idx = loop_idx + 1;

        // *** first_create ***
        if loop_idx == 0 {
            first_create = 1;
        } else {
            first_create = 0;
        }

        // Re-initialize

        // Re-initialize the data to read
        size_of_data_to_read = 0;
        num_of_req_or_resp_to_receive = 0;

        // Re-initialize the data to send
        num_req_resp_to_send = 0;
        stream_id_to_send_response_list = Vec::new();
        num_of_headers_to_send_list = Vec::new();
        trailer_offset_to_send_list = Vec::new();
        num_of_trailers_to_send_list = Vec::new();
        headers_to_send_list = Vec::new();
        body_to_send_list = Vec::new();
        size_of_headers_to_send_list = Vec::new();
        size_of_body_to_send_list = Vec::new();

        let mut channel_to_send_back_resp_list: Vec<
            Option<mpsc::Sender<FunctionConnToStreamWorkerResp>>,
        > = Vec::new();

        // ****** If first create, we directly call the codec (as a client to initialize the connection) ******
        // ****** Otherwise, block until one of the two events happens
        if first_create == 0 {
            tokio::select! {
                stream_readable = stream.readable() => { // 1) Data received at the socket
                    match stream_readable {
                        Ok(()) => {
                            // println!("stream is now readable");
                        }
                        Err(err) => {
                            error!("[func_connection_worker3. Local Addr: {}; Peer Addr: {}] readable() failed: {:?}", tcp_conn_local_addr, tcp_conn_peer_addr, err);
                            continue;
                        }
                    }


                    // read data from the socket until
                    // There is no data in the scoket AND There is no truncated frame/ unended stream in the received data.
                    loop {
                        let peer_closed = read_from_tcp_stream_until_blocking(&mut stream, &mut data_to_read).await;
                        if peer_closed == true {
                            info!("[func_connection_worker3. Local Addr: {}; Peer Addr: {}] The dp connection peer has closed the connection", tcp_conn_local_addr, tcp_conn_peer_addr);
                            // Peer closed the connection
                            let _ = stream.shutdown().await;
                            return;
                        }

                        // Check if we have truncated frame or incomplete header or unended streams
                        let initial_parse_result: i32;
                        (num_of_req_or_resp_to_receive, initial_parse_result) = http2_initial_parsing(&data_to_read);
                        if initial_parse_result == 0 {
                            size_of_data_to_read = data_to_read.len();
                            break;
                        }
                        else {
                            debug!("[func_connection_worker3. Local Addr: {}; Peer Addr: {}] currently received data contains truncated frame or incomplete req/res. Thus, waiting for more data to arrive", tcp_conn_local_addr, tcp_conn_peer_addr);
                            let stream_readable_again = stream.readable().await;
                            match stream_readable_again {
                                Ok(()) => {
                                    debug!("[func_connection_worker3. Local Addr: {}; Peer Addr: {}] More data arrives", tcp_conn_local_addr, tcp_conn_peer_addr);
                                    continue;
                                }
                                Err(err) => {
                                    panic!("[func_connection_worker3. Local Addr: {}; Peer Addr: {}] readable() failed: {:?}", tcp_conn_local_addr, tcp_conn_peer_addr, err);
                                }
                            }
                        }

                    }

                    if data_to_read.is_empty() {
                        if first_create == 1 {
                            loop_idx = -1;
                        }
                        continue;
                    }
                }
                Some(req) = stream_worker_to_func_connection_worker_rx.recv() => { // 2) These is a response to send (from a stream worker). We continue this iteration
                    debug!("[func_connection_worker3. Local Addr: {}; Peer Addr: {}] gets request from stream worker", tcp_conn_local_addr, tcp_conn_peer_addr);

                    num_req_resp_to_send = num_req_resp_to_send + 1;
                    stream_id_to_send_response_list.push(-1); // meaningless for the client
                    num_of_headers_to_send_list.push(req.num_of_headers_to_send);
                    trailer_offset_to_send_list.push(req.trailer_offset);
                    num_of_trailers_to_send_list.push(req.num_of_trailers_to_send);
                    size_of_headers_to_send_list.push(req.headers.len());
                    headers_to_send_list.push(req.headers);
                    size_of_body_to_send_list.push(req.body.len());
                    body_to_send_list.push(req.body);

                    channel_to_send_back_resp_list.push(Some(req.function_connection_worker_to_stream_worker_tx));

                    // deplete the channel (send batching)
                    while let Ok(req) = stream_worker_to_func_connection_worker_rx.try_recv() {
                        num_req_resp_to_send = num_req_resp_to_send + 1;
                        stream_id_to_send_response_list.push(-1); // meaningless for the client
                        num_of_headers_to_send_list.push(req.num_of_headers_to_send);
                        trailer_offset_to_send_list.push(req.trailer_offset);
                        num_of_trailers_to_send_list.push(req.num_of_trailers_to_send);
                        size_of_headers_to_send_list.push(req.headers.len());
                        headers_to_send_list.push(req.headers);
                        size_of_body_to_send_list.push(req.body.len());
                        body_to_send_list.push(req.body);

                        channel_to_send_back_resp_list.push(Some(req.function_connection_worker_to_stream_worker_tx));
                    }
                }
                Some(req) = router_to_func_connection_worker_rx.recv() => { // 3) Req from the router to know the num of pending requests
                    let _ = req.function_connection_worker_to_router_tx.send(num_pending_reqs);
                    continue;
                }


            }
        }

        // ****** Prepare input and call the nghttp2 codec ******
        debug!("[func_connection_worker3. Local Addr: {}; Peer Addr: {}] prepare input and call the nghttp2 codec", tcp_conn_local_addr, tcp_conn_peer_addr);
        let (function_output, recorder) = prepare_input_and_invoke_nghttp2_codec(
            nghttp2_codec_func_name.clone(),
            request_sender.clone(),
            is_server,
            first_create,
            size_of_session_state,
            size_of_data_to_read,
            num_req_resp_to_send,
            num_of_req_or_resp_to_receive,
            &session_state,
            &data_to_read,
            &stream_id_to_send_response_list,
            &num_of_headers_to_send_list,
            &trailer_offset_to_send_list,
            &num_of_trailers_to_send_list,
            &size_of_headers_to_send_list,
            &size_of_body_to_send_list,
            &headers_to_send_list,
            &body_to_send_list,
        )
        .await;
        data_to_read.drain(..size_of_data_to_read);

        // ****** Parse the output of the codec ******
        debug!("[func_connection_worker3. Local Addr: {}; Peer Addr: {}] parse the output of the nghttp2 codec", tcp_conn_local_addr, tcp_conn_peer_addr);

        // for output set 1 ~ n
        let stream_id_list: Vec<i32>;
        let num_of_headers_list: Vec<usize>;
        let trailer_offset_list: Vec<i32>;
        let num_of_trailers_list: Vec<usize>;
        let mut headers_received_list: Vec<Vec<u8>>;
        let mut body_received_list: Vec<Vec<u8>>;
        let mut header_authority_value_string_list: Vec<String>;
        let mut header_x_anakonda_forward_value_string_list: Vec<String>;
        let mut header_status_value_string_list: Vec<String>;

        (
            // set 0
            session_state,
            num_of_req_or_res_received,
            num_of_req_or_resp_sent,
            stream_id_list_sent_requests,
            data_to_send,
            // set 1 ~ n
            stream_id_list,
            num_of_headers_list,
            trailer_offset_list,
            num_of_trailers_list,
            headers_received_list,
            body_received_list,
            header_authority_value_string_list,
            header_x_anakonda_forward_value_string_list,
            header_status_value_string_list,
        ) = parse_nghttp2_codec_output(function_output, recorder);
        size_of_session_state = session_state.len();

        // ****** Operations triggered by output set 0 *******
        num_pending_reqs = num_pending_reqs + num_of_req_or_resp_sent - num_of_req_or_res_received;
        debug!(
            "[func_connection_worker3. Local Addr: {}; Peer Addr: {}] num_pending_reqs {}",
            tcp_conn_local_addr, tcp_conn_peer_addr, num_pending_reqs
        );

        let mut stream_id_list_sent_requests_i32: Vec<i32> = Vec::new();
        for i in 0..num_of_req_or_resp_sent {
            // let arr: [u8; size_of::<usize>()]  =
            let u8_bytes =
                &stream_id_list_sent_requests[i * (size_of::<i32>())..(i + 1) * (size_of::<i32>())];

            let arr: [u8; size_of::<i32>()] = u8_bytes.try_into().expect("wrong length");
            stream_id_list_sent_requests_i32.push(i32::from_ne_bytes(arr));
        }

        // 1) If we have sent request(s)
        // Insert (stream_id, stream_worker_channel) into the hash map
        for i in 0..num_of_req_or_resp_sent {
            let stream_id_sent_request = stream_id_list_sent_requests_i32[i];
            let channel_to_send_back_resp = channel_to_send_back_resp_list.remove(0);

            info!("[func_connection_worker3. Local Addr: {}; Peer Addr: {}] send a request to uc with stream id: {}", tcp_conn_local_addr, tcp_conn_peer_addr, stream_id_sent_request);

            match channel_to_send_back_resp {
                Some(c) => {
                    stream_worker_map.insert(stream_id_sent_request, c);
                }
                None => {
                    error!("[func_connection_worker3. Local Addr: {}; Peer Addr: {}] does not find the channel to send back resp to stream worker", tcp_conn_local_addr, tcp_conn_peer_addr)
                }
            }
        }

        // 2) Send data outout
        // *** If there is data needed to be sent ***
        if data_to_send.len() > 0 {
            if let Err(err) = stream.write_all(&data_to_send).await {
                error!("[func_connection_worker3. Local Addr: {}; Peer Addr: {}]: tcp stream write_all failed: {:?}", tcp_conn_local_addr, tcp_conn_peer_addr, err);
                let _ = stream.shutdown().await;
                return;
            } else {
                debug!("[func_connection_worker3. Local Addr: {}; Peer Addr: {}] TCP stream send finished successfully", tcp_conn_local_addr, tcp_conn_peer_addr);
            }
        }

        // ******* Operations triggered by output set 1 ~ n
        // *** If we receive resps from the user func container ****
        // Send it to the corresponding stream worker
        for i in 0..num_of_req_or_res_received {
            let stream_id: i32 = stream_id_list[i];
            let num_of_headers: usize = num_of_headers_list[i];
            let trailer_offset: i32 = trailer_offset_list[i];
            let num_of_trailers: usize = num_of_trailers_list[i];
            let headers_received: Vec<u8> = headers_received_list.remove(0);
            let body_received: Vec<u8> = body_received_list.remove(0);
            let resp_status = header_status_value_string_list.remove(0);
            let channel_to_stream_worker = stream_worker_map.remove(&stream_id);

            info!("[func_connection_worker3. Local Addr: {}; Peer Addr: {}] receives a resp from uc with stream id: {}", tcp_conn_local_addr, tcp_conn_peer_addr, stream_id);

            match channel_to_stream_worker {
                Some(c) => {
                    // debug!("[func conn worker] sends response to stream worker {}", stream_id);

                    let _ = c
                        .send(FunctionConnToStreamWorkerResp {
                            payload: format!("get resp from user func"),
                            resp_status: resp_status,
                            num_of_headers_to_send: num_of_headers,
                            trailer_offset: trailer_offset,
                            num_of_trailers_to_send: num_of_trailers,
                            headers: headers_received,
                            body_to_send: body_received,
                        })
                        .await;
                }
                None => {
                    error!("[func_connection_worker3. Local Addr: {}; Peer Addr: {}] There is no channel in the hasp map to send back resp to stream worker! The send request (to the uc) stream id: {}", tcp_conn_local_addr, tcp_conn_peer_addr, stream_id);
                }
            }
        }
    }
}

// It receives request from the stream_worker
// AND give back a channel to a user_func_conn worker
// It would query the Dirigent Service to get the url of user func containers.
// Based on the url, it might use the existing connection or create a new one.
async fn router(
    nghttp2_codec_func_name: String,
    request_sender: mpsc::Sender<DispatcherCommand>,
    mut stream_worker_to_router_rx: mpsc::Receiver<StreamWorkerToRouterReq>,
    mut stream_worker_to_router_report_5xx_error_rx: mpsc::Receiver<
        StreamWorkerToRouterReport5xxErrorReq,
    >,
) {
    let config = dandelion_server::config::DandelionConfig::get_config();
    let max_tcp_connections = config.max_tcp_connections;
    let max_http2_pending_requests = config.max_http2_pending_requests;
    let max_consecutive_5xx_errors = config.consecutive_5xx_errors;
    let outlier_check_interval = config.outlier_check_interval;
    let base_ejection_time = config.base_ejection_time;
    info!("[Router] max_tcp_connections: {}, max_http2_pending_requests: {}, max_consecutive_5xx_errors: {}", max_tcp_connections, max_http2_pending_requests, max_consecutive_5xx_errors);

    // key: url; value: (ejected, already_achieve_max_num_consecutive_5xx_errors, previous_result_is_5xx, num_consecutive_5xx_errors, num_pending_reqs)
    let mut uc_connections2: HashMap<String, (bool, bool, bool, usize, u32, Vec<FuncConnTuple>)> =
        HashMap::new();

    // If we receive sth from this channel, it's the time to check the outlier; (used within the router)
    // If we have achieved the max num of consecutive 5xx errors in the last interval, eject the sandbox/url
    let (time_to_check_outlier_tx, mut time_to_check_outlier_check_rx) = mpsc::channel::<u8>(32);
    let time_to_check_outlier_tx_clone = time_to_check_outlier_tx.clone();
    tokio::spawn(async move {
        sleep(Duration::from_secs(outlier_check_interval)).await;
        let _ = time_to_check_outlier_tx_clone.send(0).await;
    });

    // If we receive sth from this channel, put back the ejected url (used within the router)
    let (put_back_ejected_url_tx, mut put_back_ejected_url_rx) = mpsc::channel::<String>(32);

    // Process requests from stream workers
    loop {
        tokio::select! {
            Some(_) = time_to_check_outlier_check_rx.recv() => {
                // Check if we have achieved the max num of consecutive 5xx errors in the last interval
                // itereate through the hashmap; check all the urls
                for (destination_url_string, value) in uc_connections2.iter_mut() {
                    let (
                        ejected,
                        already_achieve_max_num_consecutive_5xx_errors,
                        _,
                        _,
                        _,
                        _,
                    ) = value;

                    if *ejected == false && *already_achieve_max_num_consecutive_5xx_errors == true {
                        *ejected = true;

                        info!("[Router] Eject the url: {}", destination_url_string);

                        let destination_url_string_clone = (*destination_url_string).clone();
                        let put_back_ejected_url_tx_clone = put_back_ejected_url_tx.clone();
                        tokio::spawn(async move {
                            sleep(Duration::from_secs(base_ejection_time)).await;
                            let _ = put_back_ejected_url_tx_clone.send(destination_url_string_clone).await;
                        });

                    }
                }


                let time_to_check_outlier_tx_clone = time_to_check_outlier_tx.clone();
                tokio::spawn(async move {
                    sleep(Duration::from_secs(outlier_check_interval)).await;
                    let _ = time_to_check_outlier_tx_clone.send(0).await;
                });
            }
            Some(destination_url_string) = put_back_ejected_url_rx.recv() => {
                let (ejected, already_achieve_max_num_consecutive_5xx_errors, previous_result_is_5xx, current_num_consecutive_errors, _, _) = uc_connections2
                    .get_mut(&destination_url_string)
                    .unwrap();
                *ejected = false;
                *already_achieve_max_num_consecutive_5xx_errors = false;
                *previous_result_is_5xx = false;
                *current_num_consecutive_errors = 0;
                *current_num_consecutive_errors = 0;

                info!("[Router] put back the ejected url {}", destination_url_string);
            }
            Some(req) = stream_worker_to_router_rx.recv() => {
                info!(
                    "[Router] receives the request with header authority: {} {}",
                    req.header_authority_value_string, req.header_x_anakonda_forward_value_string,
                );

                // TODO: need to make sure that the user codec does not alter this header --> security concern
                // ****** Based on the header_authority_value_sting, query the dirigent service to get the url ******
                // ****** Based on the url, pick one connection and its corresponding func_conn_worker ******
                let destination_url_string = req.header_x_anakonda_forward_value_string;
                debug!(
                    "[Router] gets the url: {} from the dirigent service",
                    destination_url_string
                );

                let stream_worker_to_func_connection_worker_tx: mpsc::Sender<StreamWorkerToFuncConnReq>;
                let stream_worker_to_func_connection_worker_rx: mpsc::Receiver<StreamWorkerToFuncConnReq>;

                // *** Check if there is a need to create a new connection ***
                let mut need_to_create_a_new_connection: bool = false;
                let mut circuit_break: bool = false;
                let mut circuit_break_resp_status: String = String::from("");
                let mut circuit_break_resp_body: String = String::from("");
                let mut selected_conn_idx: usize = 0;
                if uc_connections2.contains_key(&destination_url_string) {

                    let (ejected, already_achieve_max_num_consecutive_5xx_errors, _, num_consecutive_errors, _, vec_func_conn_tuple) = uc_connections2
                        .get_mut(&destination_url_string)
                        .unwrap();

                    if *ejected == true { // If ejected
                        info!("[Router] circuit breaking (url: {}), exceed max num of consecutive 5xx errors",
                            destination_url_string,
                        );
                        need_to_create_a_new_connection = false;
                        circuit_break = true;
                        circuit_break_resp_status = String::from("504\0");
                        circuit_break_resp_body = String::from("Service Unavailable: exceed max num of consecutive 5xx errors");
                    }
                    else {
                        let mut i = 0;

                        // Iterate through the current connection pool
                        while i < vec_func_conn_tuple.len() {
                            // if the conn is already closed, remove it
                            if vec_func_conn_tuple[i].stream_worker_to_func_connection_worker_tx.is_closed() {
                                info!("[Router] The func connection(url: {}, conn_id: {}) is already closed. Remove it from the pool", destination_url_string, vec_func_conn_tuple[i].conn_id);
                                vec_func_conn_tuple.remove(i);
                                continue;
                            }

                            // Check the num of pending reqs on ths conn
                            let (func_conn_worker_to_router_tx, func_conn_worker_to_router_rx) =
                                oneshot::channel::<usize>();

                            let _  = vec_func_conn_tuple[i].router_to_func_connection_worker_tx.send(
                                RouterToFuncConnReq {
                                    function_connection_worker_to_router_tx: func_conn_worker_to_router_tx
                                }
                            ).await;

                            match func_conn_worker_to_router_rx.await {
                                Ok(num) => {
                                    vec_func_conn_tuple[i].num_pending_reqs = num;
                                    debug!(
                                        "[Router] gets the numbder of pending requests on func conn (url: {}, conn_id: {}): {}",
                                        destination_url_string,
                                        vec_func_conn_tuple[i].conn_id,
                                        vec_func_conn_tuple[i].num_pending_reqs
                                    );
                                }
                                Err(e) => {
                                    error!(
                                        "[Router] fails to get the num of pending requests on func conn (url: {}, conn_id: {}) with error: {}",
                                        destination_url_string,
                                        vec_func_conn_tuple[i].conn_id,
                                        e
                                    );
                                }
                            }

                            if vec_func_conn_tuple[i].num_pending_reqs < max_http2_pending_requests { // we pick this conn if the num of pending requests is less than the threshold
                                selected_conn_idx = i;
                                break;
                            }

                            i += 1;
                        }

                        // all connections in the pool has a max num of pending reqs
                        if i >= vec_func_conn_tuple.len() {
                            need_to_create_a_new_connection = true;
                        }

                        // Check if we have already achieved the max num of connections
                        if need_to_create_a_new_connection == true && vec_func_conn_tuple.len() >= max_tcp_connections {
                            info!("[Router] circuit breaking (url: {}), exceed max num of pending requests and connections",
                                destination_url_string,
                            );
                            need_to_create_a_new_connection = false;
                            circuit_break = true;
                            circuit_break_resp_status = String::from("503\0");
                            circuit_break_resp_body = String::from("Service Unavailable: exceed max num of pending requests and connections");
                        }

                    }
                } else {
                    need_to_create_a_new_connection = true;
                    uc_connections2.insert(destination_url_string.clone(), (false, false, false, 0, 0, Vec::new()));
                }

                if need_to_create_a_new_connection == true {
                    let (_,_,  _, _, new_conn_id, vec_func_conn_tuple) = uc_connections2
                        .get_mut(&destination_url_string)
                        .unwrap();

                    (
                        stream_worker_to_func_connection_worker_tx,
                        stream_worker_to_func_connection_worker_rx,
                    ) = mpsc::channel::<StreamWorkerToFuncConnReq>(32);

                    let (
                        router_to_func_connection_worker_tx,
                        router_to_func_connection_worker_rx,
                    ) = mpsc::channel::<RouterToFuncConnReq>(32);

                    match TcpStream::connect(&destination_url_string).await {
                        Ok(s) => {
                            info!(
                                "[Router] creates a new connection to {}",
                                destination_url_string
                            );
                            let stream = s;

                            tokio::spawn(func_connection_worker3(
                                nghttp2_codec_func_name.clone(),
                                stream,
                                request_sender.clone(),
                                stream_worker_to_func_connection_worker_rx,
                                router_to_func_connection_worker_rx,
                            ));

                            vec_func_conn_tuple.push(FuncConnTuple {
                                conn_id: *new_conn_id,
                                stream_worker_to_func_connection_worker_tx: stream_worker_to_func_connection_worker_tx.clone(),
                                router_to_func_connection_worker_tx: router_to_func_connection_worker_tx.clone(),
                                num_pending_reqs: 0,
                            });
                            *new_conn_id = *new_conn_id + 1;
                        }
                        Err(e) => {
                            error!(
                                "[Router] establish connection to {} with  error: {}",
                                destination_url_string, e
                            );
                        }
                    }
                }
                else {
                    debug!(
                        "[Router] already has a connection to {}",
                        destination_url_string
                    );

                    let (_, _, _, _, _, vec_func_conn_tuple) = uc_connections2
                        .get_mut(&destination_url_string)
                        .unwrap();

                    stream_worker_to_func_connection_worker_tx =
                        vec_func_conn_tuple[selected_conn_idx].stream_worker_to_func_connection_worker_tx.clone();
                }

                // Send the answer to the stream worker
                let router_to_stream_worker_reply = RouterToStreamWorkerResp {
                    payload: format!("router processed the request: {}", req.payload),
                    stream_worker_to_func_connection_worker_tx: stream_worker_to_func_connection_worker_tx
                        .clone(),
                    circuit_break: circuit_break,
                    cb_response_status: circuit_break_resp_status,
                    cb_response_body: circuit_break_resp_body,
                };
                let _ = req
                    .router_to_stream_worker_tx
                    .send(router_to_stream_worker_reply);
            }
            Some(req) = stream_worker_to_router_report_5xx_error_rx.recv() => {
                let destination_url_string = req.header_x_anakonda_forward_value_string;
                if uc_connections2.contains_key(&destination_url_string) {
                    let (ejected, already_achieve_max_num_consecutive_5xx_errors, previous_resp_is_5xx, current_num_consecutive_5xx_errors, _, _) = uc_connections2
                        .get_mut(&destination_url_string)
                        .unwrap();

                    if *ejected == false && *already_achieve_max_num_consecutive_5xx_errors == false { // The sandbox is not ejected; we havn't achieved the max num of consecutive 5xx errors in the last interval
                        if *previous_resp_is_5xx == true {
                            *current_num_consecutive_5xx_errors = *current_num_consecutive_5xx_errors + 1;
                        }
                        else {
                            *previous_resp_is_5xx = true;
                            *current_num_consecutive_5xx_errors = 1;
                        }

                        if *current_num_consecutive_5xx_errors >= max_consecutive_5xx_errors { // circuit break
                            *already_achieve_max_num_consecutive_5xx_errors = true;

                            debug!("[Router] url {} achieves the max consecutive 5xx errors", destination_url_string);
                        }
                    }
                } else {
                    error!("[Router] does not has the connection pool to the {}", destination_url_string);
                }


            }
        }
    }

    debug!("[Router] all channels to the router are closed; Router stops working");
}

fn read_u64_le(cur: &mut Cursor<&[u8]>) -> Result<u64, String> {
    let mut b = [0u8; 8];
    std::io::Read::read_exact(cur, &mut b).map_err(|_| "EOF while reading u64".to_string())?;
    Ok(u64::from_le_bytes(b))
}

/// Read exactly `len` bytes, then optionally consume a single trailing NUL (0x00) if present.
/// Also strips a trailing NUL from the returned payload (in case `len` includes it).
fn read_len_prefixed_field(
    cur: &mut Cursor<&[u8]>,
    len: usize,
    what: &'static str,
) -> Result<Vec<u8>, String> {
    // Read payload
    let mut v = vec![0u8; len];
    std::io::Read::read_exact(cur, &mut v)
        .map_err(|_| format!("EOF while reading {what} payload ({len} bytes)"))?;

    // Variant A: payload ends with NUL and len included it -> strip
    if v.last() == Some(&0) {
        v.pop();
        return Ok(v);
    }

    // Variant B: payload is followed by an external NUL separator -> consume if present
    let pos = cur.position() as usize;
    let buf = cur.get_ref();
    if pos < buf.len() && buf[pos] == 0 {
        cur.set_position((pos + 1) as u64);
    }

    Ok(v)
}

/// Robust parsing for:
/// [u64 klen][k bytes][optional 0] [u64 vlen][v bytes][optional 0] ...
pub fn parse_headers(buf: &[u8]) -> Result<Vec<(String, String)>, String> {
    let mut cur = Cursor::new(buf);
    let mut out = Vec::new();

    while (cur.position() as usize) < buf.len() {
        let klen = read_u64_le(&mut cur)? as usize;
        let kbytes = read_len_prefixed_field(&mut cur, klen, "key")?;

        let vlen = read_u64_le(&mut cur)? as usize;
        let vbytes = read_len_prefixed_field(&mut cur, vlen, "value")?;

        let key = String::from_utf8(kbytes).map_err(|e| format!("key not utf8: {e}"))?;
        let val = String::from_utf8(vbytes).map_err(|e| format!("value not utf8: {e}"))?;

        out.push((key, val));
    }

    Ok(out)
}

// Check is the :status string is a 5xx error
fn is_5xx(s: &str) -> bool {
    let bytes = s.as_bytes();
    bytes.len() == 3 && bytes[0] == b'5' && bytes[1].is_ascii_digit() && bytes[2].is_ascii_digit()
}

// It handles one HTTP2 req-resp pair
// It is launched by the dp_conn worker
// It receives the HTTP2 Request from the dp_conn_worker, query the router and forwards it to the func_conn_worker.
// Later, it would reiceves the HTTP2 response from the func_conn_worker and forwards it back to the dp_conn_worker
// *** TO DO: it should also execute the request-level network filters ***
async fn stream_worker(
    num_of_headers_received: usize,
    headers_received: Vec<u8>,
    trailer_offset: i32,
    num_of_trailers_received: usize,
    header_authority_value_string: String,
    header_x_anakonda_forward_value_string: String,
    body_received: Vec<u8>,
    stream_worker_to_dp_connection_worker_tx: mpsc::Sender<StreamWorkerToDpConnReq>,
    stream_worker_to_router_tx: mpsc::Sender<StreamWorkerToRouterReq>,
    stream_worker_to_router_report_5xx_error_tx: mpsc::Sender<
        StreamWorkerToRouterReport5xxErrorReq,
    >,
    stream_id: i32,
    tcp_conn_local_addr: SocketAddr, // just for log
    tcp_conn_peer_addr: SocketAddr,  // just for log
    request_sender: mpsc::Sender<DispatcherCommand>,
    authorization_policy_func_name: String,
    jwt_policy_func_name: String,
    jwt_pem_context: Arc<Context>,
    enable_authorization_policy: bool,
    enable_jwt_policy: bool,
    rate_limiting_policy_func_name: String,
    rate_limit_ctx: Option<Arc<RateLimitRedisCtx>>,
    zipkin_ctx: Option<Arc<ZipkinCtx>>,
    rate_limiting_requests_per_time_unit: u32,
    rate_limiting_time_unit_in_seconds: u32,
    telemetry_policy_name: String,
    telemetry_policy_bin_local_path: String,
) {
    info!(
        "[stream_worker. Local Addr: {}; Peer Addr: {}; Stream ID:{}] A new stream worker",
        tcp_conn_local_addr, tcp_conn_peer_addr, stream_id
    );
    let should_emit_zipkin = zipkin_ctx.is_some();
    maybe_emit_zipkin_event(
        should_emit_zipkin,
        zipkin_ctx.as_ref(),
        stream_id,
        "request entered the stream_worker in anakonda proxy",
    );

    let mut authorization_policy_start_ts_us: u64 = 0;
    let mut authorization_policy_end_ts_us: u64 = 0;
    let mut jwt_policy_start_ts_us: u64 = 0;
    let mut jwt_policy_end_ts_us: u64 = 0;
    let mut rate_limiting_policy_start_ts_us: u64 = 0;
    let mut rate_limiting_policy_end_ts_us: u64 = 0;
    let mut execution_engine_roundtrip_start_ts_us: u64 = 0;
    let mut execution_engine_roundtrip_end_ts_us: u64 = 0;

    // config (retry)
    let config = dandelion_server::config::DandelionConfig::get_config();
    let max_retry_times = config.max_retry_times;
    let per_retry_timeout = config.per_retry_timeout;

    let header_pairs: Vec<(String, String)> = match parse_headers(&headers_received) {
        Ok(v) => {
            // debug!("headers parsed: {:?}", v);
            v
        }
        Err(e) => {
            error!("failed to parse headers: {}", e);
            Vec::new() // or handle the error as needed
        }
    };

    // debug!("header pairs: {:?}", header_pairs);

    //  First policy that we want to check: authorization
    if enable_authorization_policy {
        authorization_policy_start_ts_us = now_unix_microseconds();
        let authorization_verdict = prepare_input_and_invoke_authorization_policy(
            authorization_policy_func_name,
            request_sender.clone(),
            &header_pairs,
        )
        .await;
        authorization_policy_end_ts_us = now_unix_microseconds();

        if authorization_verdict == 0 {
            error!(
                "[stream_worker. Local Addr: {}; Peer Addr: {}; Stream ID:{}] The authorization policy returns deny. Thus, the stream worker stops working",
                tcp_conn_local_addr, tcp_conn_peer_addr, stream_id
            );
            let _ = stream_worker_to_dp_connection_worker_tx
                .send(StreamWorkerToDpConnReq {
                    payload: format!(
                        "[stream worker:{}] The authorization policy returns deny. Thus, the stream worker stops working",
                        stream_id
                    ),
                    stream_id_to_send_response: stream_id,
                    num_of_headers_to_send: 0,
                    num_of_trailers_to_send: num_of_trailers_received,
                    trailer_offset: trailer_offset,
                    headers: Vec::new(),
                    body_to_send: Vec::new(),
                })
                .await;
            maybe_emit_zipkin_event(
                should_emit_zipkin,
                zipkin_ctx.as_ref(),
                stream_id,
                "authorization_denied",
            );
            return;
        }
    }

    //  Second policy that we want to check: authentication via JWT
    if enable_jwt_policy {
        jwt_policy_start_ts_us = now_unix_microseconds();
        let jwt_verdict = prepare_input_and_invoke_jwt_policy(
            jwt_policy_func_name,
            jwt_pem_context,
            request_sender.clone(),
            &header_pairs,
        )
        .await;
        jwt_policy_end_ts_us = now_unix_microseconds();

        if jwt_verdict == 0 {
            error!(
                "[stream_worker. Local Addr: {}; Peer Addr: {}; Stream ID:{}] The jwt policy returns deny. Thus, the stream worker stops working",
                tcp_conn_local_addr, tcp_conn_peer_addr, stream_id
            );
            let _ = stream_worker_to_dp_connection_worker_tx
                .send(StreamWorkerToDpConnReq {
                    payload: format!(
                        "[stream worker:{}] The jwt policy returns deny. Thus, the stream worker stops working",
                        stream_id
                    ),
                    stream_id_to_send_response: stream_id,
                    num_of_headers_to_send: 0,
                    num_of_trailers_to_send: num_of_trailers_received,
                    trailer_offset: trailer_offset,
                    headers: Vec::new(),
                    body_to_send: Vec::new(),
                })
                .await;
            maybe_emit_zipkin_event(
                should_emit_zipkin,
                zipkin_ctx.as_ref(),
                stream_id,
                "jwt_denied",
            );
            return;
        }
    }

    //  If we have a rate_limit_ctx, we want to apply rate limiting
    if let Some(rate_limit_ctx) = rate_limit_ctx.as_ref() {
        rate_limiting_policy_start_ts_us = now_unix_microseconds();
        // unpack variables from redis context
        let mut redis_con = rate_limit_ctx.conn.clone(); // cheap clone of multiplexed conn
        let incr_script = &rate_limit_ctx.incr_with_expire;
        let rollback_script = &rate_limit_ctx.safe_rollback;

        // compute buckets and rate_limiting_key
        let now_secs = now_unix_seconds();
        let (bucket_start, bucket_end) =
            compute_bucket_start_and_end(now_secs, rate_limiting_time_unit_in_seconds);

        let mut function_name: String = "".to_string();
        for (header_name, header_value) in header_pairs {
            if header_name == "function" {
                function_name = header_value;
                break;
            }
        }

        let rate_limiting_key = make_rate_limiting_key(&function_name, bucket_start);

        let current_counter_value: u32 = match incr_script
            .key(&rate_limiting_key)
            .arg(bucket_end as i64)
            .invoke_async(&mut redis_con)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                error!("Redis INCR script failed: {}", e);
                0 // If we don't manage to query redis, we decide to let the request go (i.e. assume that the request count for this function is 0)
            }
        };

        let rate_limiting_verdict = prepare_input_and_invoke_rate_limiting_policy(
            rate_limiting_policy_func_name,
            request_sender.clone(),
            current_counter_value,
            rate_limiting_requests_per_time_unit,
        )
        .await;
        rate_limiting_policy_end_ts_us = now_unix_microseconds();

        if rate_limiting_verdict == 0 {
            //  We want to rollback the counter in redis
            match rollback_script
                .key(&rate_limiting_key)
                .invoke_async::<Option<i64>>(&mut redis_con)
                .await
            {
                Ok(_) => {
                    debug!(
                        "Redis rollback script succeeded for key: {}",
                        rate_limiting_key
                    );
                }
                Err(e) => {
                    error!(
                        "Redis rollback script failed for key: {} with error: {}",
                        rate_limiting_key, e
                    );
                }
            }

            error!(
                "[stream_worker. Local Addr: {}; Peer Addr: {}; Stream ID:{}] The rate limiting policy returns deny. Thus, the stream worker stops working",
                tcp_conn_local_addr, tcp_conn_peer_addr, stream_id
            );
            let _ = stream_worker_to_dp_connection_worker_tx
                .send(StreamWorkerToDpConnReq {
                    payload: format!(
                        "[stream worker:{}] The rate limiting policy returns deny. Thus, the stream worker stops working",
                        stream_id
                    ),
                    stream_id_to_send_response: stream_id,
                    num_of_headers_to_send: 0,
                    num_of_trailers_to_send: num_of_trailers_received,
                    trailer_offset: trailer_offset,
                    headers: Vec::new(),
                    body_to_send: Vec::new(),
                })
                .await;
            maybe_emit_zipkin_event(
                should_emit_zipkin,
                zipkin_ctx.as_ref(),
                stream_id,
                "rate_limited",
            );
            return;
        }
    }

    // If we receive sth from this channel, it's the time to retry
    let mut current_retry_times: usize = 0;
    let (time_to_retry_tx, mut time_to_retry_rx) = mpsc::channel::<usize>(32);

    // Used to get resp(s) back from the func_conn_worker
    let (
        function_connection_worker_to_stream_worker_tx,
        mut function_connection_worker_to_stream_worker_rx,
    ) = mpsc::channel::<FunctionConnToStreamWorkerResp>(32);

    // The retry loop
    loop {
        // The stream worker would query the router to get the channel to the user_func_conn_worker
        // The channel used by the router to send resp back
        let (router_to_stream_worker_tx, router_to_stream_worker_rx) =
            oneshot::channel::<RouterToStreamWorkerResp>();

        // ****** send request to the router ******
        if let Err(e) = stream_worker_to_router_tx
            .send(StreamWorkerToRouterReq {
                payload: format!("request from stream worker: {}", stream_id),
                header_authority_value_string: header_authority_value_string.clone(),
                header_x_anakonda_forward_value_string: header_x_anakonda_forward_value_string
                    .clone(),
                router_to_stream_worker_tx: router_to_stream_worker_tx,
            })
            .await
        {
            error!("[stream_worker. Local Addr: {}; Peer Addr: {}; Stream ID:{}] failed to send to router: {}", tcp_conn_local_addr, tcp_conn_peer_addr, stream_id, e);

            // If we fail to send req to the router
            let _ = stream_worker_to_dp_connection_worker_tx
                .send(StreamWorkerToDpConnReq {
                    payload: format!(
                        "[stream worker:{}] failed to send to router: {}",
                        stream_id, e
                    ),
                    ..Default::default()
                })
                .await;

            execution_engine_roundtrip_start_ts_us = 0;
            execution_engine_roundtrip_end_ts_us = 0;
            maybe_emit_zipkin_event(
                should_emit_zipkin,
                zipkin_ctx.as_ref(),
                stream_id,
                "router_send_failed",
            );
            return;
        }

        // ****** Wait for router's resp ******
        match router_to_stream_worker_rx.await {
            Ok(resp) => {
                debug!("[stream_worker. Local Addr: {}; Peer Addr: {}; Stream ID:{}] gets resp from the router", tcp_conn_local_addr, tcp_conn_peer_addr, stream_id);

                // From the router, we know if there is a circuit breaking
                if resp.circuit_break == true {
                    // If there is, directly return a error response
                    let (cb_resp_headers, cb_resp_body) = generate_circuit_break_retry_resp(
                        resp.cb_response_status,
                        resp.cb_response_body,
                    );
                    let _ = stream_worker_to_dp_connection_worker_tx
                        .send(StreamWorkerToDpConnReq {
                            payload: format!("[stream worker:{}] circuit breaking", stream_id,),
                            stream_id_to_send_response: stream_id,
                            num_of_headers_to_send: 1,
                            trailer_offset: -1,
                            num_of_trailers_to_send: 0,
                            headers: cb_resp_headers,
                            body_to_send: cb_resp_body,
                        })
                        .await;
                    return;
                }

                // From the router, we now know which user-func connection to use
                let stream_worker_to_func_connection_worker_tx =
                    resp.stream_worker_to_func_connection_worker_tx;

                // send request to the func_conn worker
                if let Err(e) = stream_worker_to_func_connection_worker_tx
                    .send(StreamWorkerToFuncConnReq {
                        payload: format!("request from stream worker: {}", stream_id),
                        stream_id: stream_id,
                        num_of_headers_to_send: num_of_headers_received,
                        trailer_offset: trailer_offset,
                        num_of_trailers_to_send: num_of_trailers_received,
                        headers: headers_received.clone(),
                        body: body_received.clone(),
                        function_connection_worker_to_stream_worker_tx:
                            function_connection_worker_to_stream_worker_tx.clone(),
                    })
                    .await
                {
                    error!("[stream_worker. Local Addr: {}; Peer Addr: {}; Stream ID:{}] failed to send to user func conn worker: {}", tcp_conn_local_addr, tcp_conn_peer_addr, stream_id, e);

                    // If we fail to send to user func conn worker
                    let _ = stream_worker_to_dp_connection_worker_tx
                        .send(StreamWorkerToDpConnReq {
                            payload: format!(
                                "[stream worker:{}] failed to send to user func conn worker: {}",
                                stream_id, e
                            ),
                            ..Default::default()
                        })
                        .await;
                    execution_engine_roundtrip_end_ts_us = now_unix_microseconds();
                    maybe_emit_zipkin_event(
                        should_emit_zipkin,
                        zipkin_ctx.as_ref(),
                        stream_id,
                        "func_conn_send_failed",
                    );
                    return;
                }

                // Reset the retry timeout timer
                let time_to_retry_tx_clone = time_to_retry_tx.clone();
                tokio::spawn(async move {
                    sleep(Duration::from_secs(per_retry_timeout)).await;
                    let _ = time_to_retry_tx_clone.send(current_retry_times).await;
                });

                // *** Wait for the resp from the user func conn worker ***
                tokio::select! {
                    Some(resp) = function_connection_worker_to_stream_worker_rx.recv() => { // 1) If we receive the resp from the function connection worker
                        execution_engine_roundtrip_end_ts_us = now_unix_microseconds();
                        debug!("[stream_worker. Local Addr: {}; Peer Addr: {}; Stream ID:{}] gets resp from the user func conn worker with status {}", tcp_conn_local_addr, tcp_conn_peer_addr, stream_id, &resp.resp_status);

                        // Check the resp status: is it a 5xx error?
                        let resp_status_is_5xx = is_5xx(&resp.resp_status);

                        if resp_status_is_5xx == true {
                            // Report the 5xx error to the router
                            let _ = stream_worker_to_router_report_5xx_error_tx
                                .send(StreamWorkerToRouterReport5xxErrorReq {
                                    header_x_anakonda_forward_value_string: header_x_anakonda_forward_value_string.clone()
                                })
                                .await;

                            if current_retry_times < max_retry_times {
                                current_retry_times = current_retry_times + 1;
                                debug!("[stream_worker. Local Addr: {}; Peer Addr: {}; Stream ID:{}] retry because receives the 5xx error. Already retry times: {}", tcp_conn_local_addr, tcp_conn_peer_addr, stream_id, current_retry_times);
                                continue; // start the next loop
                            }
                            else { // return an error response
                                let (cb_resp_headers, cb_resp_body) = generate_circuit_break_retry_resp(String::from("505\0"), String::from("Service Unavailable: Exceed Max Retry Times"));
                                let _ = stream_worker_to_dp_connection_worker_tx
                                    .send(StreamWorkerToDpConnReq {
                                        payload: format!(
                                            "[stream worker:{}] Exceed Max Retry Times",
                                            stream_id,
                                        ),
                                        stream_id_to_send_response: stream_id,
                                        num_of_headers_to_send: 1,
                                        trailer_offset: -1,
                                        num_of_trailers_to_send: 0,
                                        headers: cb_resp_headers,
                                        body_to_send: cb_resp_body,
                                    })
                                    .await;
                                return;

                            }
                        }

                        //  if we have zipkin enabled, we want to call the telemetry function and create the string log
                        //  then we want to make the call to zipkin to log it
                        if should_emit_zipkin {
                            let string_log_for_zipkin = prepare_input_and_invoke_telemetry_policy(
                                telemetry_policy_name.clone(),
                                request_sender.clone(),
                                authorization_policy_start_ts_us,
                                authorization_policy_end_ts_us,
                                jwt_policy_start_ts_us,
                                jwt_policy_end_ts_us,
                                rate_limiting_policy_start_ts_us,
                                rate_limiting_policy_end_ts_us,
                                execution_engine_roundtrip_start_ts_us,
                                execution_engine_roundtrip_end_ts_us,
                            )
                                .await;

                            maybe_emit_zipkin_event(
                                should_emit_zipkin,
                                zipkin_ctx.as_ref(),
                                stream_id,
                                string_log_for_zipkin,
                            );
                        }

                        let _ = stream_worker_to_dp_connection_worker_tx
                            .send(StreamWorkerToDpConnReq {
                                payload: format!(
                                    "[stream worker:{}] final response: {} ",
                                    stream_id, resp.payload
                                ),
                                stream_id_to_send_response: stream_id,
                                num_of_headers_to_send: resp.num_of_headers_to_send,
                                trailer_offset: resp.trailer_offset,
                                num_of_trailers_to_send: resp.num_of_trailers_to_send,
                                headers: resp.headers,
                                body_to_send: resp.body_to_send,
                            })
                            .await;
                    }
                    Some(n) = time_to_retry_rx.recv() => {
                        let next_retry_times = n + 1;

                        if current_retry_times < next_retry_times {
                            if current_retry_times < max_retry_times {
                                current_retry_times = current_retry_times + 1;
                                debug!("[stream_worker. Local Addr: {}; Peer Addr: {}; Stream ID:{}] retry because of the timeout. Already retry times: {}", tcp_conn_local_addr, tcp_conn_peer_addr, stream_id, current_retry_times);
                                continue; // start the next loop
                            }
                            else { // return an error response
                                let (cb_resp_headers, cb_resp_body) = generate_circuit_break_retry_resp(String::from("505\0"), String::from("Service Unavailable: Exceed Max Retry Times"));
                                let _ = stream_worker_to_dp_connection_worker_tx
                                    .send(StreamWorkerToDpConnReq {
                                        payload: format!(
                                            "[stream worker:{}] Exceed Max Retry Times",
                                            stream_id,
                                        ),
                                        stream_id_to_send_response: stream_id,
                                        num_of_headers_to_send: 1,
                                        trailer_offset: -1,
                                        num_of_trailers_to_send: 0,
                                        headers: cb_resp_headers,
                                        body_to_send: cb_resp_body,
                                    })
                                    .await;
                                return;

                            }
                        }
                    }
                }
            }
            Err(e) => {
                execution_engine_roundtrip_start_ts_us = 0;
                execution_engine_roundtrip_end_ts_us = 0;
                error!("[stream_worker. Local Addr: {}; Peer Addr: {}; Stream ID:{}] failed to receive from the router: {}", tcp_conn_local_addr, tcp_conn_peer_addr, stream_id, e);

                let _ = stream_worker_to_dp_connection_worker_tx
                    .send(StreamWorkerToDpConnReq {
                        payload: format!(
                            "[stream worker:{}] failed to receive from router: {}",
                            stream_id, e
                        ),
                        ..Default::default()
                    })
                    .await;
            }
        }

        debug!(
            "[stream_worker. Local Addr: {}; Peer Addr: {}; Stream ID:{}] stops working!",
            tcp_conn_local_addr, tcp_conn_peer_addr, stream_id
        );
    }
}

// The task to handle one TCP/HTTP connection with the data plane
// Thus, it acts as a HTTP server
async fn dp_connection_worker3<S: ProxyStream>(
    nghttp2_codec_func_name: String,
    mut stream: S,
    request_sender: mpsc::Sender<DispatcherCommand>,
    stream_worker_to_router_tx: mpsc::Sender<StreamWorkerToRouterReq>,
    stream_worker_to_router_report_5xx_error_tx: mpsc::Sender<
        StreamWorkerToRouterReport5xxErrorReq,
    >,
    authorization_policy_func_name: String,
    jwt_policy_func_name: String,
    jwt_pem_context: Arc<Context>,
    enable_authorization_policy: bool,
    enable_jwt_policy: bool,
    rate_limiting_policy_func_name: String,
    rate_limit_ctx: Option<Arc<RateLimitRedisCtx>>,
    zipkin_ctx: Option<Arc<ZipkinCtx>>,
    rate_limiting_requests_per_time_unit: u32,
    rate_limiting_time_unit_in_seconds: u32,
    telemetry_policy_name: String,
    telemetry_policy_bin_local_path: String,
) where
    S: ProxyStream + Send + Unpin + 'static,
{
    let tcp_conn_local_addr = stream.local_addr().unwrap();
    let tcp_conn_peer_addr = stream.peer_addr().unwrap();
    info!(
        "[dp_connection_worker3. Local Addr: {}; Peer Addr: {}] A new dp connection",
        tcp_conn_local_addr, tcp_conn_peer_addr
    );

    // Create channel to receive req from the stream worker
    let (stream_worker_to_dp_connection_worker_tx, mut stream_worker_to_dp_connection_worker_rx) =
        mpsc::channel::<StreamWorkerToDpConnReq>(32);

    let mut num_of_completed_stream_worker = 0; // Just for logging

    // Output (set_0) from the nghttp2 codec (other sets 1~n use local vars; each set is for one req or resp)
    let mut session_state: Vec<u8> = vec![0u8; 20 * 1024]; // Also the input
    let mut num_of_req_or_res_received: usize;
    let mut num_of_req_or_resp_sent: usize;
    let mut data_to_send: Vec<u8>;
    let mut stream_id_list_sent_requests: Vec<u8>; // not used by the dp_connection_worker

    // *** input to the nghttp2 codec ***
    let is_server: i8 = 1;
    let mut first_create: i8;
    let mut size_of_session_state: usize = 0; // actually not used
    let mut data_to_read = Vec::new();
    let mut size_of_data_to_read: usize;
    let mut num_req_resp_to_send: usize;
    let mut num_of_req_or_resp_to_receive: usize;
    let mut stream_id_to_send_response_list: Vec<i32>;
    let mut num_of_headers_to_send_list: Vec<usize>;
    let mut trailer_offset_to_send_list: Vec<i32>;
    let mut num_of_trailers_to_send_list: Vec<usize>;
    let mut size_of_headers_to_send_list: Vec<usize>;
    let mut headers_to_send_list: Vec<Vec<u8>>;
    let mut body_to_send_list: Vec<Vec<u8>>;
    let mut size_of_body_to_send_list: Vec<usize>;

    // The main logic loop (it stops until the other side closes the connection)
    let mut loop_idx = -1;
    loop {
        loop_idx = loop_idx + 1;

        // *** first_create ***
        if loop_idx == 0 {
            first_create = 1;
        } else {
            first_create = 0;
        }

        // If first create, wait until the socket is readable (maybe not necessary)
        if first_create == 1 {
            // Wait until the socket becomes readable
            if let Err(err) = stream.wait_readable().await {
                error!("[dp_connection_worker3. Local Addr: {}; Peer Addr: {}] tcp stream readable() failed: {:?}", tcp_conn_local_addr, tcp_conn_peer_addr, err);
                break;
            }
        }

        // Re-initialize

        // Re-initialize the data to read
        size_of_data_to_read = 0;
        num_of_req_or_resp_to_receive = 0;

        // Re-initialize the data to send
        num_req_resp_to_send = 0;
        stream_id_to_send_response_list = Vec::new();
        num_of_headers_to_send_list = Vec::new();
        trailer_offset_to_send_list = Vec::new();
        num_of_trailers_to_send_list = Vec::new();
        headers_to_send_list = Vec::new();
        body_to_send_list = Vec::new();
        size_of_headers_to_send_list = Vec::new();
        size_of_body_to_send_list = Vec::new();

        // ****** Block until one of the two events happpen *******
        tokio::select! {
            stream_readable = stream.wait_readable() => { // 1) Data received at the socket.
                match stream_readable {
                    Ok(()) => {
                        // println!("stream is now readable");
                    }
                    Err(err) => {
                        error!("[dp_connection_worker3. Local Addr: {}; Peer Addr: {}] readable() failed: {:?}", tcp_conn_local_addr, tcp_conn_peer_addr, err);
                        continue;
                    }
                }

                // read data from the socket until
                // There is no data in the scoket AND There is no truncated frame/ unended stream in the received data.
                loop {
                    let peer_closed = read_from_stream_until_blocking(&mut stream, &mut data_to_read).await;
                    if peer_closed == true {
                        info!("[dp_connection_worker3. Local Addr: {}; Peer Addr: {}] The dp connection peer has closed the connection", tcp_conn_local_addr, tcp_conn_peer_addr);
                        // Peer closed the connection
                        let _ = stream.close_stream().await;
                        return;
                    }

                    // Check if we have truncated frame or incomplete header or unended streams
                    let initial_parse_result: i32;
                    if first_create == 1 {
                        if data_to_read.len() < 24 { // if what we receive is even less than the len of MAGIC interface
                            num_of_req_or_resp_to_receive = 0;
                            initial_parse_result = -1;
                        }
                        else {
                            if data_to_read[..24] == HTTP2_MAGIC_PACKET.to_vec() {
                                (num_of_req_or_resp_to_receive, initial_parse_result) = http2_initial_parsing(&data_to_read[24..]);
                            } else {
                                error!("Protocol not supported!");
                                // TODO: terminate connection
                                num_of_req_or_resp_to_receive = 0;
                                initial_parse_result = 0;
                            }
                        }
                    }
                    else {
                        (num_of_req_or_resp_to_receive, initial_parse_result) = http2_initial_parsing(&data_to_read);
                    }

                    if initial_parse_result == 0 {
                        size_of_data_to_read = data_to_read.len();
                        break;
                    }
                    else {
                        debug!("[dp_connection_worker3. Local Addr: {}; Peer Addr: {}] currently received data contains truncated frame or incomplete req/res. Thus, waiting for more data to arrive", tcp_conn_local_addr, tcp_conn_peer_addr);
                        let stream_readable_again = stream.wait_readable().await;
                        match stream_readable_again {
                            Ok(()) => {
                                debug!("[dp_connection_worker3. Local Addr: {}; Peer Addr: {}] More data arrives", tcp_conn_local_addr, tcp_conn_peer_addr);
                                continue;
                            }
                            Err(err) => {
                                panic!("[dp_connection_worker3. Local Addr: {}; Peer Addr: {}] readable() failed: {:?}", tcp_conn_local_addr, tcp_conn_peer_addr, err);
                            }
                        }
                    }
                }

                if data_to_read.is_empty() {
                    if first_create == 1 {
                        loop_idx = -1;
                    }
                    continue;
                }
            }

            Some(req) = stream_worker_to_dp_connection_worker_rx.recv() => { // 2) These is a response to send. We continue this iteration
                info!("[dp_connection_worker3. Local Addr: {}; Peer Addr: {}]  From the stream worker, get the response to stream {}", tcp_conn_local_addr, tcp_conn_peer_addr, req.stream_id_to_send_response);

                if req.stream_id_to_send_response > 0 {
                    num_of_completed_stream_worker = num_of_completed_stream_worker + 1;

                    num_req_resp_to_send = num_req_resp_to_send + 1;
                    stream_id_to_send_response_list.push(req.stream_id_to_send_response);
                    num_of_headers_to_send_list.push(req.num_of_headers_to_send);
                    trailer_offset_to_send_list.push(req.trailer_offset);
                    num_of_trailers_to_send_list.push(req.num_of_trailers_to_send);
                    size_of_headers_to_send_list.push(req.headers.len());
                    headers_to_send_list.push(req.headers);
                    size_of_body_to_send_list.push(req.body_to_send.len());
                    body_to_send_list.push(req.body_to_send);
                }

                // deplete the channel (send batching)
                while let Ok(req) = stream_worker_to_dp_connection_worker_rx.try_recv() {
                    if req.stream_id_to_send_response > 0 {
                        num_of_completed_stream_worker = num_of_completed_stream_worker + 1;

                        num_req_resp_to_send = num_req_resp_to_send + 1;
                        stream_id_to_send_response_list.push(req.stream_id_to_send_response);
                        num_of_headers_to_send_list.push(req.num_of_headers_to_send);
                        trailer_offset_to_send_list.push(req.trailer_offset);
                        num_of_trailers_to_send_list.push(req.num_of_trailers_to_send);
                        size_of_headers_to_send_list.push(req.headers.len());
                        headers_to_send_list.push(req.headers);
                        size_of_body_to_send_list.push(req.body_to_send.len());
                        body_to_send_list.push(req.body_to_send);
                    }
                }

            }
        }

        debug!("[dp_connection_worker3. Local Addr: {}; Peer Addr: {}] num_of_completed_stream_worker {}", tcp_conn_local_addr, tcp_conn_peer_addr, num_of_completed_stream_worker);

        // ****** Prepare input and call the nghttp2 codec ******
        debug!("[dp_connection_worker3. Local Addr: {}; Peer Addr: {}] prepare input and call the nghttp2 codec", tcp_conn_local_addr, tcp_conn_peer_addr);
        let (function_output, recorder) = prepare_input_and_invoke_nghttp2_codec(
            nghttp2_codec_func_name.clone(),
            request_sender.clone(),
            is_server,
            first_create,
            size_of_session_state,
            size_of_data_to_read,
            num_req_resp_to_send,
            num_of_req_or_resp_to_receive,
            &session_state,
            &data_to_read,
            &stream_id_to_send_response_list,
            &num_of_headers_to_send_list,
            &trailer_offset_to_send_list,
            &num_of_trailers_to_send_list,
            &size_of_headers_to_send_list,
            &size_of_body_to_send_list,
            &headers_to_send_list,
            &body_to_send_list,
        )
        .await;

        data_to_read.drain(..size_of_data_to_read);

        // ****** Parse the output of the codec ******
        debug!("[dp_connection_worker3. Local Addr: {}; Peer Addr: {}] parse the output of the nghttp2 codec", tcp_conn_local_addr, tcp_conn_peer_addr);
        // for output set 1 ~ n
        let stream_id_list: Vec<i32>;
        let num_of_headers_list: Vec<usize>;
        let trailer_offset_list: Vec<i32>;
        let num_of_trailers_list: Vec<usize>;
        let mut headers_received_list: Vec<Vec<u8>>;
        let mut body_received_list: Vec<Vec<u8>>;
        let mut header_authority_value_string_list: Vec<String>;
        let mut header_x_anakonda_forward_value_string_list: Vec<String>;
        let mut header_status_value_string_list: Vec<String>;

        (
            // set 0
            session_state,
            num_of_req_or_res_received,
            num_of_req_or_resp_sent,
            stream_id_list_sent_requests,
            data_to_send,
            // set 1 ~ n
            stream_id_list,
            num_of_headers_list,
            trailer_offset_list,
            num_of_trailers_list,
            headers_received_list,
            body_received_list,
            header_authority_value_string_list,
            header_x_anakonda_forward_value_string_list,
            header_status_value_string_list,
        ) = parse_nghttp2_codec_output(function_output, recorder);

        size_of_session_state = session_state.len();

        // ****** Operations triggered by output set 0 *******
        // 1) Send data outout
        // *** If there is data needed to be sent
        if data_to_send.len() > 0 {
            if let Err(err) = stream.send_all(&data_to_send).await {
                error!("[dp_connection_worker3. Local Addr: {}; Peer Addr: {}] tcp stream write_all failed: {:?}", tcp_conn_local_addr, tcp_conn_peer_addr, err);
                let _ = stream.close_stream().await;
                return;
            } else {
                debug!("[dp_connection_worker3. Local Addr: {}; Peer Addr: {}] TCP stream send finished successfully", tcp_conn_local_addr, tcp_conn_peer_addr);
            }
        }

        // ******* Operations triggered by output set 1 ~ n
        // *** If we receive reqs from the data plane ***

        for i in 0..num_of_req_or_res_received {
            let stream_id: i32 = stream_id_list[i];
            let num_of_headers: usize = num_of_headers_list[i];
            let trailer_offset: i32 = trailer_offset_list[i];
            let num_of_trailers: usize = num_of_trailers_list[i];
            let headers_received: Vec<u8> = headers_received_list.remove(0);
            let body_received: Vec<u8> = body_received_list.remove(0);
            let header_authority_value_string: String =
                header_authority_value_string_list.remove(0);
            let header_x_anakonda_forward_value_string: String =
                header_x_anakonda_forward_value_string_list.remove(0);
            let jwt_pem_context = Arc::clone(&jwt_pem_context);

            info!("[dp_connection_worker3. Local Addr: {}; Peer Addr: {}] Receive req with stream id {}. Spawn a stream worker.", tcp_conn_local_addr, tcp_conn_peer_addr, stream_id);
            tokio::spawn(stream_worker(
                num_of_headers,
                headers_received,
                trailer_offset,
                num_of_trailers,
                header_authority_value_string,
                header_x_anakonda_forward_value_string,
                body_received,
                stream_worker_to_dp_connection_worker_tx.clone(),
                stream_worker_to_router_tx.clone(),
                stream_worker_to_router_report_5xx_error_tx.clone(),
                stream_id,
                stream.local_addr().unwrap(),
                stream.peer_addr().unwrap(),
                request_sender.clone(),
                authorization_policy_func_name.clone(),
                jwt_policy_func_name.clone(),
                jwt_pem_context,
                enable_authorization_policy,
                enable_jwt_policy,
                rate_limiting_policy_func_name.clone(),
                rate_limit_ctx.clone(),
                zipkin_ctx.clone(),
                rate_limiting_requests_per_time_unit,
                rate_limiting_time_unit_in_seconds,
                telemetry_policy_name.clone(),
                telemetry_policy_bin_local_path.clone(),
            ));
        }
    }
}

// ********* To create and start the dirigent proxy *************
// ******************************************************************
// ******************************************************************

async fn create_proxy_server2(
    nghttp2_codec_func_name: String,
    nghttp2_codec_bin_local_path: String,
    request_sender: mpsc::Sender<DispatcherCommand>,
    port: u16,
    proxy_tls_material_dir: String,
    authorization_policy_func_name: String,
    authorization_policy_bin_local_path: String,
    jwt_policy_func_name: String,
    jwt_policy_bin_local_path: String,
    jwt_pem_context: Arc<Context>,
    enable_authorization_policy: bool,
    enable_jwt_policy: bool,
    enable_mtls: bool,
    enable_rate_limiting: bool,
    rate_limiting_policy_func_name: String,
    rate_limiting_policy_bin_local_path: String,
    rate_limiting_redis_addr: String,
    rate_limiting_redis_port: u16,
    rate_limiting_redis_pass: String,
    rate_limiting_requests_per_time_unit: u32,
    rate_limiting_time_unit_in_seconds: u32,
    enable_zipkin_telemetry: bool,
    zipkin_addr: String,
    zipkin_port: u16,
    zipkin_batch_size: usize,
    zipkin_flush_interval_ms: u64,
    telemetry_policy_name: String,
    telemetry_policy_bin_local_path: String,
) {
    // ****** Before the loop actually starts, register some functions ******

    let engine_type: String = if cfg!(feature = "kvm") {
        "Kvm".to_string()
    } else if cfg!(feature = "mmu") {
        "Process".to_string()
    } else {
        panic!("No valid feature selected: expected `kvm` or `mmu`. Other engine type currently still untested");
    };

    // register the nghttp2 codec
    let _ = register_function_local(
        nghttp2_codec_bin_local_path.clone(),
        nghttp2_codec_func_name.clone(),
        engine_type.clone(),
        request_sender.clone(),
    )
    .await
    .unwrap();

    if enable_authorization_policy {
        // register the authorization policy function
        let _ = register_function_local(
            authorization_policy_bin_local_path.clone(),
            authorization_policy_func_name.clone(),
            engine_type.clone(),
            request_sender.clone(),
        )
        .await
        .unwrap();
    }

    if enable_jwt_policy {
        // register the jwt policy function
        let _ = register_function_local(
            jwt_policy_bin_local_path.clone(),
            jwt_policy_func_name.clone(),
            engine_type.clone(),
            request_sender.clone(),
        )
        .await
        .unwrap();
    }

    if enable_rate_limiting {
        //  register the rate_limiting policy function
        let _ = register_function_local(
            rate_limiting_policy_bin_local_path.clone(),
            rate_limiting_policy_func_name.clone(),
            engine_type.clone(),
            request_sender.clone(),
        )
        .await
        .unwrap();
    }

    if enable_zipkin_telemetry {
        //  register the telemetry function
        let _ = register_function_local(
            telemetry_policy_bin_local_path.clone(),
            telemetry_policy_name.clone(),
            engine_type.clone(),
            request_sender.clone(),
        )
        .await
        .unwrap();
    }

    let rate_limit_ctx: Option<Arc<RateLimitRedisCtx>> = if enable_rate_limiting {
        let conn = connect_redis(
            &rate_limiting_redis_addr,
            rate_limiting_redis_port,
            &rate_limiting_redis_pass,
        )
        .await
        .expect("failed to connect to the rate-limiting redis instance");

        let incr_with_expire = Script::new(
            r#"
            local v = redis.call("INCR", KEYS[1])
            if v == 1 then
                redis.call("EXPIREAT", KEYS[1], tonumber(ARGV[1]))
            end
            return v
        "#,
        );

        let safe_rollback = Script::new(
            r#"
            local v = redis.call("GET", KEYS[1])
            if not v then
                return nil
            end
            v = tonumber(v)
            if v <= 0 then
                return v
            end
            return redis.call("DECR", KEYS[1])
        "#,
        );

        Some(Arc::new(RateLimitRedisCtx {
            conn,
            incr_with_expire,
            safe_rollback,
        }))
    } else {
        None
    };

    let zipkin_ctx: Option<Arc<ZipkinCtx>> = if enable_zipkin_telemetry {
        let (zipkin_tx, zipkin_rx) = mpsc::channel::<ZipkinEvent>(ZIPKIN_CHANNEL_CAPACITY);
        tokio::spawn(zipkin_batch_publisher(
            zipkin_rx,
            zipkin_addr,
            zipkin_port,
            zipkin_batch_size,
            zipkin_flush_interval_ms,
        ));
        Some(Arc::new(ZipkinCtx { tx: zipkin_tx }))
    } else {
        None
    };

    // ***** spawn the router ******
    let (stream_worker_to_router_tx, stream_worker_to_func_router_rx) =
        mpsc::channel::<StreamWorkerToRouterReq>(32);

    let (
        stream_worker_to_router_report_5xx_error_tx,
        stream_worker_to_func_router_report_5xx_error_rx,
    ) = mpsc::channel::<StreamWorkerToRouterReport5xxErrorReq>(32);

    tokio::spawn(router(
        nghttp2_codec_func_name.clone(),
        request_sender.clone(),
        stream_worker_to_func_router_rx,
        stream_worker_to_func_router_report_5xx_error_rx,
    ));

    // ****** The listening socket (for the data plane connections)******
    let addr: SocketAddr = SocketAddr::from(([0, 0, 0, 0], port)); // The dirigent proxy port
    let listener = TcpListener::bind(addr).await.unwrap();

    // signal handlers for gracefull shutdown
    let mut sigterm_stream = tokio::signal::unix::signal(SignalKind::terminate()).unwrap();
    let mut sigint_stream = tokio::signal::unix::signal(SignalKind::interrupt()).unwrap();
    let mut sigquit_stream = tokio::signal::unix::signal(SignalKind::quit()).unwrap();

    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("install rustls crypto provider (aws-lc-rs)");

    // ****** Start the dirigent proxy ******
    loop {
        tokio::select! {
            connection_pair = listener.accept() => {
                info!("A new DP connection!");
                let (tcp_stream, peer_addr) = connection_pair.unwrap();
                let local_addr = tcp_stream.local_addr().unwrap();
                let loop_dispatcher = request_sender.clone();
                let proxy_tls_material_dir = proxy_tls_material_dir.clone();

                let stream_worker_to_router_tx_clone = stream_worker_to_router_tx.clone();
                let stream_worker_to_router_report_5xx_error_tx_clone = stream_worker_to_router_report_5xx_error_tx.clone();

                // for each new dp connection spawn a dp connection worker
                let func_name = nghttp2_codec_func_name.clone();
                let authorization_policy_func_name = authorization_policy_func_name.clone();
                let jwt_policy_func_name = jwt_policy_func_name.clone();
                let jwt_pem_context = Arc::clone(&jwt_pem_context);
                let rate_limiting_policy_func_name = rate_limiting_policy_func_name.clone();
                let telemetry_policy_name = telemetry_policy_name.clone();
                let telemetry_policy_bin_local_path = telemetry_policy_bin_local_path.clone();

                if enable_mtls {
                    let rate_limit_ctx = rate_limit_ctx.clone();
                    let zipkin_ctx = zipkin_ctx.clone();

                    tokio::spawn(async move {
                        let mtls_paths = build_proxy_mtls_material_paths(&proxy_tls_material_dir);
                        let expected_data_plane_spiffe_id = match load_expected_data_plane_spiffe_id(
                            &mtls_paths.expected_data_plane_spiffe_id_path,
                        ) {
                            Ok(spiffe_id) => spiffe_id,
                            Err(err) => {
                                warn!(
                                    "Cannot load expected data-plane SPIFFE ID from '{}': {:?}",
                                    mtls_paths.expected_data_plane_spiffe_id_path,
                                    err,
                                );
                                return;
                            }
                        };

                        let tls_acceptor = match build_mtls_acceptor(
                            &mtls_paths.server_cert_path,
                            &mtls_paths.server_key_path,
                            &mtls_paths.client_ca_bundle_path,
                        ) {
                            Ok(acceptor) => acceptor,
                            Err(err) => {
                                warn!(
                                    "Cannot configure mTLS acceptor from material directory '{}': {:?}",
                                    proxy_tls_material_dir,
                                    err,
                                );
                                return;
                            }
                        };

                        match tls_acceptor.accept(tcp_stream).await {
                            Ok(tls_stream) => {
                                match extract_spiffe_id(&tls_stream) {
                                    Ok(spiffe_id) => {
                                        if spiffe_id != expected_data_plane_spiffe_id {
                                            warn!(
                                                "mTLS handshake rejected from {} due to SPIFFE mismatch. expected={}, actual={}",
                                                peer_addr,
                                                expected_data_plane_spiffe_id,
                                                spiffe_id,
                                            );
                                            return;
                                        }

                                        debug!("mTLS handshake successful. Peer SPIFFE ID: {}", spiffe_id);
                                    }
                                    Err(e) => {
                                        warn!("mTLS handshake rejected from {}: failed to extract SPIFFE ID: {:?}", peer_addr, e);
                                        return;
                                    }
                                }

                                let stream = DpStream::new(tls_stream, local_addr, peer_addr);
                                let service_dispatcher_ptr = loop_dispatcher.clone();
                                dp_connection_worker3(func_name, stream, service_dispatcher_ptr.clone(), stream_worker_to_router_tx_clone.clone(), stream_worker_to_router_report_5xx_error_tx_clone.clone(), authorization_policy_func_name, jwt_policy_func_name, jwt_pem_context, enable_authorization_policy, enable_jwt_policy, rate_limiting_policy_func_name,rate_limit_ctx, zipkin_ctx, rate_limiting_requests_per_time_unit, rate_limiting_time_unit_in_seconds, telemetry_policy_name, telemetry_policy_bin_local_path).await;
                            }
                            Err (err) => {
                                warn!("mTLS handshake failed from {}: {:?}", peer_addr, err);
                            }
                        }
                    });
                }
                else {
                    let rate_limit_ctx = rate_limit_ctx.clone();
                    let zipkin_ctx = zipkin_ctx.clone();
                    tokio::spawn(async move {
                        let service_dispatcher_ptr = loop_dispatcher.clone();
                        dp_connection_worker3(func_name, tcp_stream, service_dispatcher_ptr.clone(), stream_worker_to_router_tx_clone.clone(), stream_worker_to_router_report_5xx_error_tx_clone.clone(), authorization_policy_func_name, jwt_policy_func_name, jwt_pem_context, enable_authorization_policy, enable_jwt_policy, rate_limiting_policy_func_name,rate_limit_ctx, zipkin_ctx, rate_limiting_requests_per_time_unit, rate_limiting_time_unit_in_seconds, telemetry_policy_name, telemetry_policy_bin_local_path).await;
                    });
                }


            }
            _ = sigterm_stream.recv() => return,
            _ = sigint_stream.recv() => return,
            _ = sigquit_stream.recv() => return,
        }
    }
}

pub fn start_proxy_server2(
    nghttp2_codec_func_name: String,
    nghttp2_codec_bin_local_path: String,
    proxy_cores: Vec<u8>,
    request_sender: mpsc::Sender<DispatcherCommand>,
    port: u16,
    proxy_tls_material_dir: String,
    authorization_policy_func_name: String,
    authorization_policy_bin_local_path: String,
    jwt_policy_func_name: String,
    jwt_policy_bin_local_path: String,
    jwt_policy_pem_file_local_path: String,
    enable_authorization_policy: bool,
    enable_jwt_policy: bool,
    enable_mtls: bool,
    enable_rate_limiting: bool,
    rate_limiting_policy_func_name: String,
    rate_limiting_policy_bin_local_path: String,
    rate_limiting_redis_addr: String,
    rate_limiting_redis_port: u16,
    rate_limiting_redis_pass: String,
    rate_limiting_requests_per_time_unit: u32,
    rate_limiting_time_unit_in_seconds: u32,
    enable_zipkin_telemetry: bool,
    zipkin_addr: String,
    zipkin_port: u16,
    zipkin_batch_size: usize,
    zipkin_flush_interval_ms: u64,
    telemetry_policy_name: String,
    telemetry_policy_bin_local_path: String,
) {
    let runtime = if cfg!(feature = "unpin_proxy") {
        Runtime::new().unwrap() // The default runtime. Use all the cores it could use
    } else {
        // make multithreaded dirigent proxy runtime
        // set up tokio runtime, need io in any case
        let mut runtime_builder = Builder::new_multi_thread();
        runtime_builder.enable_io();
        runtime_builder.enable_time();
        runtime_builder.worker_threads(proxy_cores.len());
        // Pin each Tokio worker thread to a specific core
        let cores = proxy_cores.clone(); // move into closure
        runtime_builder.on_thread_start(move || {
            // Each worker thread calls this once.
            // Need a way to pick which core this thread should use.

            // One simple approach: assign cores in a round-robin based on thread name/id
            // (Tokio doesn't expose a stable "worker index" here), so use a global counter:
            static NEXT_DIRIGENT_PROXY_CORE: AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

            let i = NEXT_DIRIGENT_PROXY_CORE.fetch_add(1, Ordering::Relaxed);
            let core = cores[i % cores.len()] as usize;

            // core_affinity expects core_affinity::CoreId
            let core_id = CoreId { id: core };
            let _ok = core_affinity::set_for_current(core_id);
        });
        // runtime_builder.global_queue_interval(10);
        // runtime_builder.event_interval(10);

        runtime_builder.build().unwrap()
    };

    let jwt_pem_bytes =
        std::fs::read(&jwt_policy_pem_file_local_path).expect("Failed to read JWT policy PEM file");
    let jwt_pem_context = make_single_item_context(0, "", "", 0, jwt_pem_bytes.into_boxed_slice());
    thread::spawn(move || {
        runtime.block_on(async {
            create_proxy_server2(
                nghttp2_codec_func_name,
                nghttp2_codec_bin_local_path,
                request_sender,
                port,
                proxy_tls_material_dir,
                authorization_policy_func_name,
                authorization_policy_bin_local_path,
                jwt_policy_func_name,
                jwt_policy_bin_local_path,
                jwt_pem_context,
                enable_authorization_policy,
                enable_jwt_policy,
                enable_mtls,
                enable_rate_limiting,
                rate_limiting_policy_func_name,
                rate_limiting_policy_bin_local_path,
                rate_limiting_redis_addr,
                rate_limiting_redis_port,
                rate_limiting_redis_pass,
                rate_limiting_requests_per_time_unit,
                rate_limiting_time_unit_in_seconds,
                enable_zipkin_telemetry,
                zipkin_addr,
                zipkin_port,
                zipkin_batch_size,
                zipkin_flush_interval_ms,
                telemetry_policy_name,
                telemetry_policy_bin_local_path,
            )
            .await;
        });
    });
}

use std::{
    convert::Infallible, io::Write, net::SocketAddr, path::PathBuf, sync::Arc, time::Instant,
};

use bytes::Bytes;
use dandelion_commons::{
    err_dandelion, records::Recorder, CompositionError, DandelionError, DandelionResult,
    FrontendError, FunctionRegistryError,
};
use dandelion_server::{
    serialize_bson_response, AsyncInvocationAcceptedResponse, AsyncInvocationState, DandelionBody,
};
use dispatcher::dispatcher::Dispatcher;
use http_body_util::BodyExt;
use hyper::{
    body::{Body, Incoming},
    service::service_fn,
    Request, Response, StatusCode,
};
use log::{debug, error, warn};
use machine_interface::{
    composition::{CompositionSet, LocalCompositionSet},
    function_driver::Metadata,
    machine_config::EngineType,
    memory_domain::bytes_context::BytesContext,
};
use serde::Deserialize;
use tokio::{net::TcpListener, signal::unix::SignalKind};

fn default_path() -> String {
    String::new()
}

//----------------------------------------
// user function/composition registration

/// Struct containing registration information for new function
#[derive(Debug, Deserialize)]
struct RegisterFunction {
    /// String name of the function
    name: String,
    /// Default size for context to allocate to execute function
    context_size: u64,
    /// Which engine the function should be executed on
    engine_type: String,
    /// Optional local path to the binary if it is already on local disc
    #[serde(default = "default_path")]
    local_path: String,
    /// Binary representation of the function, ignored if a local path is given
    binary: Vec<u8>,
    /// Metadata for the sets and optionally static items to pass into the function for that set
    input_sets: Vec<(String, Option<Vec<(String, Vec<u8>)>>)>,
    /// output set names
    output_sets: Vec<String>,
    /// The minimum size the largest set of a group of any sets should have. If given (i.e. has a
    /// value of > 0) the JoinIterator will combine any sets to achieve this size best-effort.
    #[serde(default)]
    min_set_bytes: Vec<usize>,
}

async fn handle_function_registration(
    req: Request<Incoming>,
    dispatcher: &'static Dispatcher,
    folder_path: &'static str,
) -> DandelionResult<DandelionBody> {
    let bytes = req
        .collect()
        .await
        .expect("Failed to extract body from function registration")
        .to_bytes();

    // find first line end character
    let request_map: RegisterFunction =
        bson::from_slice(&bytes).expect("Should be able to deserialize request");

    // if local is present ignore the binary
    let path_string = if !request_map.local_path.is_empty() {
        // check that file exists
        if let Err(err) = std::fs::File::open(&request_map.local_path) {
            return err_dandelion!(DandelionError::RequestError(FrontendError::InvalidRequest(
                format!("Tried to register function with local path, but failed to open file with error {}",
                err),
            )));
        };
        request_map.local_path
    } else {
        // write function to file
        std::fs::create_dir_all(folder_path).unwrap();
        let mut path_buff = PathBuf::from(folder_path);
        path_buff.push(request_map.name.clone());
        let mut function_file = std::fs::File::create(path_buff.clone())
            .expect("Failed to create file for registering function");
        function_file
            .write_all(&request_map.binary)
            .expect("Failed to write file with content for registering");
        path_buff.to_str().unwrap().to_string()
    };

    // TODO: move to machine config
    let engine_type = match request_map.engine_type.as_str() {
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
            (
                name,
                data.and_then(|static_data| {
                    Some(LocalCompositionSet::from_byte_items(static_data))
                }),
            )
        })
        .collect();

    let metadata = Metadata {
        input_sets: input_sets,
        output_sets: request_map.output_sets,
        min_set_bytes: request_map.min_set_bytes,
    };
    match dispatcher
        .insert_function(
            request_map.name,
            engine_type,
            request_map.context_size as usize,
            path_string,
            metadata,
        ) {
        Ok(()) => Ok(DandelionBody::from_vec(
            "Function registered".as_bytes().to_vec(),
        )),
        Err(insertion_err) => Err(insertion_err),
    }
}

#[derive(Debug, Deserialize)]
struct RegisterChain {
    composition: String,
}

async fn handle_composition_registration(
    req: Request<Incoming>,
    dispatcher: &'static Dispatcher,
) -> DandelionResult<DandelionBody> {
    let bytes = req
        .collect()
        .await
        .expect("Failed to extract body from function registration")
        .to_bytes();

    // find first line end character
    let request_map: RegisterChain =
        bson::from_slice(&bytes).expect("Should be able to deserialize request");

    match dispatcher.insert_compositions(request_map.composition) {
        Ok(()) => Ok(DandelionBody::from_vec(
            "Function registered".as_bytes().to_vec(),
        )),
        Err(insertion_err) => Err(insertion_err),
    }
}

//----------------
// user invoction

struct ParsedInvocationRequest {
    had_function_name: bool,
    function_id: Arc<String>,
    composition: Option<String>,
    inputs: Vec<Option<CompositionSet>>,
    recorder: Recorder,
    raw_request_bytes: Vec<u8>,
}

fn build_parsed_invocation_request(
    function_name: Option<String>,
    composition: Option<String>,
    request_context: machine_interface::memory_domain::Context,
    invocation_id: dandelion_commons::InvocationId,
    start_time: Instant,
    raw_request_bytes: Vec<u8>,
) -> ParsedInvocationRequest {
    let had_function_name = function_name.is_some();
    let function_id = Arc::new(function_name.unwrap_or_else(|| String::from("Composition")));
    let mut recorder = Recorder::new(invocation_id, function_id.clone(), start_time);
    recorder.record(dandelion_commons::records::RecordPoint::DeserializationEnd);
    debug!("finished creating request context");

    // TODO match set names to assign sets to composition sets
    // map sets in the order they are in the request
    let request_number = request_context.content.len();
    debug!("Request number of request_context: {}", request_number);
    let inputs = CompositionSet::from_context(request_context)
        .into_iter()
        .collect::<Vec<_>>();

    ParsedInvocationRequest {
        had_function_name,
        function_id,
        composition,
        inputs,
        recorder,
        raw_request_bytes,
    }
}

async fn parse_invocation_request_bytes(
    raw_request_bytes: Vec<u8>,
    invocation_id: dandelion_commons::InvocationId,
) -> DandelionResult<ParsedInvocationRequest> {
    let start_time = Instant::now();

    // from context from frame bytes
    let (function_name, composition, request_context) =
        match BytesContext::from_bytes_vec(
            vec![Bytes::from(raw_request_bytes.clone())],
            raw_request_bytes.len(),
        )
        .await
        {
            Ok(parsed_request) => parsed_request,
            Err(parse_error) => {
                warn!("request parsing failed with: {:?}", parse_error);
                return Err(parse_error);
            }
        };
    Ok(build_parsed_invocation_request(
        function_name,
        composition,
        request_context,
        invocation_id,
        start_time,
        raw_request_bytes,
    ))
}

async fn parse_invocation_request(req: Request<Incoming>) -> DandelionResult<ParsedInvocationRequest> {
    debug!("Starting to serve request");
    // UUID v7 is roughly time-ordered and lexicographically sortable
    let invocation_id = dandelion_commons::InvocationId::now_v7();
    let start_time = Instant::now();

    // pull all frames from the network
    let mut incomming = req.into_body();
    let mut body_pin = std::pin::Pin::new(&mut incomming);
    let mut frame_data = Vec::new();
    let mut total_size = 0usize;
    loop {
        if let Some(frame_result) =
            futures::future::poll_fn(|cx| body_pin.as_mut().poll_frame(cx)).await
        {
            let data_frame = frame_result.unwrap().into_data().unwrap();
            total_size += data_frame.len();
            frame_data.push(data_frame);
        } else if body_pin.is_end_stream() {
            break;
        }
    }

    let (function_name, composition, request_context) =
        match BytesContext::from_bytes_vec(frame_data, total_size).await {
            Ok(parsed_request) => parsed_request,
            Err(parse_error) => {
                warn!("request parsing failed with: {:?}", parse_error);
                return Err(parse_error);
            }
        };

    Ok(build_parsed_invocation_request(
        function_name,
        composition,
        request_context,
        invocation_id,
        start_time,
        Vec::new(),
    ))
}

async fn parse_async_invocation_request(
    req: Request<Incoming>,
) -> DandelionResult<ParsedInvocationRequest> {
    debug!("Starting to serve request");
    let raw_request_bytes = req
        .collect()
        .await
        .expect("Failed to extract body from invocation request")
        .to_bytes()
        .to_vec();
    // UUID v7 is roughly time-ordered and lexicographically sortable
    let invocation_id = dandelion_commons::InvocationId::now_v7();
    parse_invocation_request_bytes(raw_request_bytes, invocation_id).await
}

async fn dispatch_invocation(
    is_cold: bool,
    parsed: ParsedInvocationRequest,
    dispatcher: &'static Dispatcher,
) -> DandelionResult<(Vec<Option<LocalCompositionSet>>, Recorder)> {
    let ParsedInvocationRequest {
        had_function_name,
        function_id,
        composition,
        inputs,
        recorder,
        raw_request_bytes: _,
    } = parsed;

    // want a 1 to 1 mapping of all outputs the functions gives as long as we don't add user input on what they want
    if had_function_name {
        dispatcher
            .queue_function_by_name(function_id, inputs, is_cold, recorder)
            .await
    } else {
        dispatcher
            .queue_unregistered_composition(
                composition
                    .expect("Did not get a service name nor a composition description in request"),
                inputs,
                is_cold,
                recorder,
            )
            .await
    }
}

async fn handle_request(
    is_cold: bool,
    req: Request<Incoming>,
    dispatcher: &'static Dispatcher,
) -> DandelionResult<DandelionBody> {
    let parsed = parse_invocation_request(req).await?;
    let (function_output, recorder) = dispatch_invocation(is_cold, parsed, dispatcher).await?;

    let response_body = dandelion_server::DandelionBody::new(function_output, &recorder);

    debug!("finished creating response body");
    #[cfg(feature = "archive")]
    TRACING_ARCHIVE.get().unwrap().insert_recorder(recorder);

    Ok(response_body)
}

async fn handle_async_request(
    is_cold: bool,
    req: Request<Incoming>,
    dispatcher: &'static Dispatcher,
) -> DandelionResult<DandelionBody> {
    let parsed = parse_async_invocation_request(req).await?;
    let invocation_id = parsed.recorder.invocation_id();

    crate::async_invocation::persist_submitted(invocation_id, &parsed.raw_request_bytes)?;
    spawn_async_invocation(dispatcher, is_cold, parsed);

    Ok(DandelionBody::from_vec(serialize_bson_response(
        &AsyncInvocationAcceptedResponse {
            invocation_id,
            state: AsyncInvocationState::Running,
        },
    )))
}

fn parse_invocation_id(path: &str) -> DandelionResult<dandelion_commons::InvocationId> {
    dandelion_commons::InvocationId::parse_str(path).map_err(|_| {
        dandelion_commons::dandelion_err!(DandelionError::RequestError(
            FrontendError::InvalidRequest(format!("Invalid invocation id {}", path))
        ))
    })
}

async fn handle_async_status(path: &str) -> DandelionResult<DandelionBody> {
    let invocation_id = parse_invocation_id(path)?;
    let status = crate::async_invocation::load_status(invocation_id)?;
    Ok(DandelionBody::from_vec(serialize_bson_response(&status)))
}

async fn handle_async_result(path: &str) -> DandelionResult<(StatusCode, DandelionBody)> {
    let invocation_id = parse_invocation_id(path)?;
    match crate::async_invocation::load_result(invocation_id)? {
        Some(result) => Ok((StatusCode::OK, DandelionBody::from_vec(result))),
        None => {
            let status = crate::async_invocation::load_status(invocation_id)?;
            Ok((
                StatusCode::ACCEPTED,
                DandelionBody::from_vec(serialize_bson_response(&status)),
            ))
        }
    }
}

fn spawn_async_invocation(
    dispatcher: &'static Dispatcher,
    is_cold: bool,
    parsed: ParsedInvocationRequest,
) {
    let invocation_id = parsed.recorder.invocation_id();
    machine_interface::function_driver::system_driver::recovery_log::activate_async_invocation_logging(
        invocation_id,
    );
    tokio::spawn(async move {
        let dispatch_result = dispatch_invocation(is_cold, parsed, dispatcher).await;
        match dispatch_result {
            Ok((function_output, recorder)) => {
                let response_bytes =
                    dandelion_server::DandelionBody::new(function_output, &recorder).into_bytes();
                if let Err(err) =
                    crate::async_invocation::persist_completed(invocation_id, &response_bytes)
                {
                    error!(
                        "Failed to persist completed async invocation {}: {}",
                        invocation_id, err
                    );
                    let _ = crate::async_invocation::persist_failed(
                        invocation_id,
                        format!("Failed to persist async result: {}", err),
                    );
                }
            }
            Err(err) => {
                if let Err(persist_err) =
                    crate::async_invocation::persist_failed(invocation_id, format!("{}", err))
                {
                    error!(
                        "Failed to persist failed async invocation {}: {}",
                        invocation_id, persist_err
                    );
                }
            }
        }

        machine_interface::function_driver::system_driver::recovery_log::deactivate_async_invocation_logging(invocation_id);
        machine_interface::function_driver::system_driver::recovery_log::clear_recovered_io(invocation_id);
    });
}

pub async fn resume_recoverable_invocations(dispatcher: &'static Dispatcher) -> DandelionResult<()> {
    for recoverable in crate::async_invocation::list_recoverable_invocations()? {
        let invocation_id = recoverable.invocation_id;
        let parsed = parse_invocation_request_bytes(recoverable.request_bytes, invocation_id).await?;
        let recovered_io = machine_interface::function_driver::system_driver::recovery_log::load_io_completion_records(invocation_id)?;
        // add the recovered io to the cache
        machine_interface::function_driver::system_driver::recovery_log::install_recovered_io_records(
            invocation_id,
            recovered_io,
        )?;
        spawn_async_invocation(dispatcher, false, parsed);
    }
    Ok(())
}

//-----------------------
// main service function

async fn service(
    req: Request<Incoming>,
    dispatcher: &'static Dispatcher,
    folder_path: &'static str,
) -> Result<Response<DandelionBody>, Infallible> {
    // handle request
    let path = req.uri().path().to_string();
    let res = match path.as_str() {
        "/register/function" => handle_function_registration(req, dispatcher, folder_path)
            .await
            .map(|body| (StatusCode::OK, body)),
        "/register/composition" => handle_composition_registration(req, dispatcher)
            .await
            .map(|body| (StatusCode::OK, body)),
        // TODO: rename to cold func and hot func, remove matmul, compute, io
        "/cold/matmul"
        | "/cold/matmulstore"
        | "/cold/compute"
        | "/cold/io"
        | "/cold/chain_scaling"
        | "/cold/middleware_app"
        | "/cold/compression_app"
        | "/cold/python_app" => handle_request(true, req, dispatcher)
            .await
            .map(|body| (StatusCode::OK, body)),
        "/hot/matmul"
        | "/hot/matmulstore"
        | "/hot/compute"
        | "/hot/io"
        | "/hot/chain_scaling"
        | "/hot/middleware_app"
        | "/hot/compression_app"
        | "/hot/python_app" => handle_request(false, req, dispatcher)
            .await
            .map(|body| (StatusCode::OK, body)),
        "/async" => handle_async_request(false, req, dispatcher)
            .await
            .map(|body| (StatusCode::ACCEPTED, body)),
        other_uri => {
            if let Some(invocation_path) = other_uri.strip_prefix("/async/invocation/") {
                if let Some(invocation_id) = invocation_path.strip_suffix("/result") {
                    handle_async_result(invocation_id).await
                } else {
                    handle_async_status(invocation_path)
                        .await
                        .map(|body| (StatusCode::OK, body))
                }
            } else {
                debug!("Received request on {}", other_uri);
                Ok((
                    StatusCode::OK,
                    DandelionBody::from_vec(format!("Hello, World\n").into_bytes()),
                ))
            }
        }
    };

    // create response
    match res {
        Ok((status_code, body)) => {
            let mut response = Response::new(body);
            *response.status_mut() = status_code;
            Ok::<_, Infallible>(response)
        }
        Err(err) => {
            warn!("Failed to serve request: {}", err);
            // for all other requests set response status to something not ok and write the error in the response body
            let mut response = Response::new(DandelionBody::from_vec(
                format!("Failed to serve request: {}", err).into_bytes(),
            ));
            *response.status_mut() = match err.error {
                DandelionError::RequestError(
                    FrontendError::InvalidRequest(_)
                    | FrontendError::StreamEnd
                    | FrontendError::ViolatedSpec
                    | FrontendError::MalformedMessage,
                )
                | DandelionError::FunctionRegistry(
                    FunctionRegistryError::DuplicateInsert(_)
                    | FunctionRegistryError::TypeConflictInsert(_)
                    | FunctionRegistryError::InvalidSystemInsert(_)
                    | FunctionRegistryError::InvalidUserInsert(_)
                    | FunctionRegistryError::UnknownFunction(_)
                    | FunctionRegistryError::BinaryNotFound,
                )
                | DandelionError::Composition(
                    CompositionError::DuplicateIdentifier(_)
                    | CompositionError::UnknownFunction(_)
                    | CompositionError::UndefinedDataSet(_)
                    | CompositionError::InvalidFunctionApplication(_)
                    | CompositionError::DuplicateSetName(_)
                    | CompositionError::InvalidFunctionDeclaration(_),
                ) => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            Ok::<_, Infallible>(response)
        }
    }
}

pub async fn service_loop(dispatcher: &'static Dispatcher, folder_path: &'static str, port: u16) {
    // socket to listen to
    let addr: SocketAddr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr).await.unwrap();
    println!("Socket ready");

    // signal handlers for gracefull shutdown
    let mut sigterm_stream = tokio::signal::unix::signal(SignalKind::terminate()).unwrap();
    let mut sigint_stream = tokio::signal::unix::signal(SignalKind::interrupt()).unwrap();
    let mut sigquit_stream = tokio::signal::unix::signal(SignalKind::quit()).unwrap();

    loop {
        tokio::select! {
            connection_pair = listener.accept() => {
                let (stream,_) = connection_pair.unwrap();
                let io = hyper_util::rt::TokioIo::new(stream);
                tokio::task::spawn(async move {
                    if let Err(err) = hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new())
                        .serve_connection_with_upgrades(
                            io,
                            service_fn(|req| service(req, dispatcher, folder_path)),
                        )
                        .await
                    {
                        error!("Request serving failed with error: {:?}", err);
                    }
                });
            }
            _ = sigterm_stream.recv() => return,
            _ = sigint_stream.recv() => return,
            _ = sigquit_stream.recv() => return,
        }
    }
}

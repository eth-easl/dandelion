use dandelion_commons::{
    dandelion_err, err_dandelion,
    records::{RecordPoint, Recorder},
    DandelionError, DandelionResult, FrontendError,
};
use dandelion_server::protocol::{
    DandelionBody, DandelionRequest, Executable, InvocationTarget, RegisterChain, RegisterFunction,
};
use dispatcher::dispatcher::Dispatcher;
use http_body_util::BodyExt;
use hyper::{body::Incoming, service::service_fn, Request, Response, StatusCode};
use log::{debug, error, info, trace, warn};
use machine_interface::machine_config::EngineType;
use memory::{
    context::read_only::ReadOnlyContext, data::Metadata, DataItem, DataSet, LocalData, Position,
};
use std::{
    convert::Infallible, io::Write, net::SocketAddr, path::PathBuf, println, sync::Arc,
    time::Instant,
};
use tokio::{
    net::TcpListener,
    signal::unix::SignalKind,
    sync::{oneshot, watch},
};

//----------------------------------------
// user function/composition registration

async fn handle_function_registration(
    req: Request<Incoming>,
    dispatcher: &'static Dispatcher,
    folder_path: &'static str,
) -> DandelionResult<DandelionBody> {
    let request_map = RegisterFunction::decode(read_body(req).await?)?;
    let path_string = match request_map.executable {
        Executable::LocalPath(local_path) => {
            // check that file exists
            if let Err(err) = std::fs::File::open(&local_path) {
                return err_dandelion!(DandelionError::RequestError(FrontendError::InvalidRequest(
                    format!("Tried to register function with local path, but failed to open file with error {}",
                    err),
                )));
            };
            local_path
        }
        Executable::Binary(binary) => {
            // write function to file
            std::fs::create_dir_all(folder_path).unwrap();
            let mut path_buff = PathBuf::from(folder_path);
            path_buff.push(request_map.name.clone());
            let mut function_file = std::fs::File::create(path_buff.clone())
                .expect("Failed to create file for registering function");
            function_file
                .write_all(&binary)
                .expect("Failed to write file with content for registering");
            path_buff.to_str().unwrap().to_string()
        }
    };

    // TODO: move to machine config
    let engine_type = match request_map.engine_type.as_str() {
        #[cfg(feature = "mmu")]
        "Process" => EngineType::Process,
        #[cfg(feature = "kvm")]
        "Kvm" => EngineType::Kvm,
        #[cfg(feature = "cheri")]
        "Cheri" => EngineType::Cheri,
        unknown => panic!("Unkown engine type string {}", unknown),
    };

    let input_sets = request_map
        .input_sets
        .into_iter()
        .map(|(name, data)| (name, data.map(static_data_set)))
        .collect();

    let metadata = Metadata {
        input_sets,
        output_sets: request_map.output_sets,
        min_set_bytes: request_map.min_set_bytes,
    };
    dispatcher
        .insert_function(
            request_map.name,
            engine_type,
            request_map.context_size,
            path_string,
            metadata,
        )
        .expect("Should be able to insert function");
    Ok(DandelionBody::status("Function registered"))
}

/// Builds a set out of static items given at function registration, each backed by its own context.
fn static_data_set(items: Vec<(String, Vec<u8>)>) -> DataSet {
    let items = items
        .into_iter()
        .map(|(name, data)| {
            let size = data.len();
            let ctx = ReadOnlyContext::from_boxed(data.into_boxed_slice())
                .expect("Should be able to create read only context for static data");
            DataItem::new_local(
                name,
                0,
                LocalData {
                    ctx: Arc::new(ctx),
                    pos: Position { offset: 0, size },
                },
            )
        })
        .collect();
    DataSet::from_items(Arc::new(items))
}

async fn handle_composition_registration(
    req: Request<Incoming>,
    dispatcher: &'static Dispatcher,
) -> DandelionResult<DandelionBody> {
    let request_map = RegisterChain::decode(read_body(req).await?)?;
    match dispatcher.insert_compositions(request_map.composition) {
        Ok(()) => Ok(DandelionBody::status("Function registered")),
        Err(insertion_err) => err_dandelion!(DandelionError::RequestError(
            FrontendError::InternalError(format!(
                "Failed to insert composition into dispatcher: {:?}",
                insertion_err
            ),)
        )),
    }
}

//----------------
// user invoction

async fn handle_request(
    is_cold: bool,
    req: Request<Incoming>,
    dispatcher: &'static Dispatcher,
) -> DandelionResult<DandelionBody> {
    debug!("Starting to serve request");

    let start_time = Instant::now();
    let DandelionRequest {
        target,
        context: request_context,
    } = DandelionRequest::decode(read_body(req).await?)?;
    let deserialization_end = Instant::now();
    let new_recorder = |function_id| {
        let mut recorder = Recorder::new(function_id, start_time);
        recorder.prerecorded(RecordPoint::DeserializationEnd, deserialization_end);
        recorder
    };
    debug!("finished creating request context");

    // TODO match set names to assign sets to composition sets
    // map sets in the order they are in the request
    let request_number = request_context.content.len();
    debug!("Request number of request_context: {}", request_number);
    let inputs = DataSet::from_context(request_context);

    // want a 1 to 1 mapping of all outputs the functions gives as long as we don't add user input on what they want

    let (function_output, recorder) = match target {
        InvocationTarget::Name(name) => {
            let function_id = Arc::new(name);
            dispatcher
                .queue_function_by_name(
                    function_id.clone(),
                    inputs,
                    is_cold,
                    new_recorder(function_id),
                )
                .await
        }
        InvocationTarget::Composition(composition) => {
            dispatcher
                .queue_unregistered_composition(
                    composition,
                    inputs,
                    !is_cold,
                    new_recorder(Arc::new(String::from("Composition"))),
                )
                .await
        }
    }?;

    debug!("finished creating response body");
    #[cfg(feature = "archive")]
    TRACING_ARCHIVE.get().unwrap().insert_recorder(recorder);

    DandelionBody::new(function_output, &recorder)
}

async fn read_body(req: Request<Incoming>) -> DandelionResult<Vec<bytes::Bytes>> {
    let mut body = req.into_body();
    let mut frames = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_| {
            dandelion_err!(DandelionError::RequestError(
                FrontendError::FailledToGetFrames
            ))
        })?;
        if let Ok(data) = frame.into_data() {
            frames.push(data);
        }
    }
    Ok(frames)
}

fn http_response(body: DandelionBody, status: StatusCode) -> Response<DandelionBody> {
    let mut response = Response::new(body);
    *response.status_mut() = status;
    response
}

//-----------------------
// main service function

async fn service(
    req: Request<Incoming>,
    dispatcher: &'static Dispatcher,
    folder_path: &'static str,
    request_counter: std::sync::Weak<()>,
) -> Result<Response<DandelionBody>, Infallible> {
    let _running_guard = match request_counter.upgrade() {
        Some(guard) => guard,
        None => {
            return Ok(http_response(
                DandelionBody::status("Server is shutting down"),
                StatusCode::SERVICE_UNAVAILABLE,
            ));
        }
    };

    // handle request
    let res = match req.uri().path() {
        "/register/function" => handle_function_registration(req, dispatcher, folder_path).await,
        "/register/composition" => handle_composition_registration(req, dispatcher).await,
        // TODO: rename to cold func and hot func, remove matmul, compute, io
        "/cold/matmul"
        | "/cold/matmulstore"
        | "/cold/compute"
        | "/cold/io"
        | "/cold/chain_scaling"
        | "/cold/middleware_app"
        | "/cold/compression_app"
        | "/cold/python_app" => handle_request(true, req, dispatcher).await,
        "/hot/matmul"
        | "/hot/matmulstore"
        | "/hot/compute"
        | "/hot/io"
        | "/hot/chain_scaling"
        | "/hot/middleware_app"
        | "/hot/compression_app"
        | "/hot/python_app" => handle_request(false, req, dispatcher).await,
        other_uri => {
            debug!("Received request on {}", other_uri);
            Ok(DandelionBody::status("Hello, World\n"))
        }
    };

    match res {
        Ok(body) => Ok(http_response(body, StatusCode::OK)),
        Err(err) => {
            warn!("Failed to serve request: {}", err);
            let status = match err.error {
                DandelionError::RequestError(FrontendError::InternalError(_)) => {
                    StatusCode::INTERNAL_SERVER_ERROR
                }
                DandelionError::RequestError(_) => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            Ok(http_response(
                DandelionBody::status(format!("Failed to serve request: {}", err.message())),
                status,
            ))
        }
    }
}

pub async fn service_loop(
    dispatcher: &'static Dispatcher,
    folder_path: &'static str,
    port: u16,
    client_shutdown_option: Option<(watch::Sender<bool>, oneshot::Receiver<()>)>,
) {
    // socket to listen to
    let addr: SocketAddr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr).await.unwrap();
    println!("Socket ready");
    let running_requests = Arc::new(());

    // signal handlers for gracefull shutdown
    let mut sigterm_stream = tokio::signal::unix::signal(SignalKind::terminate()).unwrap();
    let mut sigint_stream = tokio::signal::unix::signal(SignalKind::interrupt()).unwrap();
    let mut sigquit_stream = tokio::signal::unix::signal(SignalKind::quit()).unwrap();

    loop {
        tokio::select! {
            connection_pair = listener.accept() => {
                let (stream,_) = connection_pair.unwrap();
                let io = hyper_util::rt::TokioIo::new(stream);
                let request_counter = Arc::downgrade(&running_requests);
                tokio::task::spawn(async move {
                    let counter_clone = request_counter.clone();
                    if let Err(err) = hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new())
                        .serve_connection_with_upgrades(
                            io,
                            service_fn(|req| service(req, dispatcher, folder_path, counter_clone.clone())),
                        )
                        .await
                    {
                        error!("Request serving failed with error: {:?}", err);
                    }
                });
            }
            _ = sigterm_stream.recv() => break,
            _ = sigint_stream.recv() => break,
            _ = sigquit_stream.recv() => break,
        }
    }

    drop(listener);
    let running_checker = Arc::downgrade(&running_requests);
    drop(running_requests);

    info!("Broke frontend loop, startig graceful shutdown");

    // if there is a remote client, stop receiving work
    if let Some((shutdown_sender, completion_receiver)) = client_shutdown_option {
        shutdown_sender.send_replace(true);
        // await completion, receiving completion implies, that all remote requests have returned and the export registry is empty
        completion_receiver.await.unwrap();
    }
    // remote work has already drained, if there was work submitted to this node directly, wait for that to drain
    loop {
        let remaining_requests = running_checker.strong_count();
        trace!(
            "finished waiting for remote, {} requests remaining from local frontend",
            remaining_requests
        );
        if remaining_requests == 0 {
            break;
        } else {
            // make sure this does not hog a core that would be necessary to make progress on the shutdown.
            tokio::task::yield_now().await;
        }
    }
}

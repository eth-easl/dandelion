use dandelion_commons::{
    err_dandelion, records::Recorder, DandelionError, DandelionResult, FrontendError,
};
use dandelion_server::protocol::{
    DandelionBody, DandelionRequest, InvocationTarget, RegisterChain, RegisterFunction,
};
use dispatcher::dispatcher::Dispatcher;
use http_body_util::BodyExt;
use hyper::{body::Incoming, service::service_fn, Request, Response, StatusCode};
use log::{debug, error, info, trace, warn};
use machine_interface::{
    composition::{CompositionSet, LocalCompositionSet},
    function_driver::Metadata,
    machine_config::EngineType,
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
        unknown => panic!("Unkown engine type string {}", unknown),
    };

    let input_sets = request_map
        .input_sets
        .into_iter()
        .map(|(name, data)| (name, data.map(LocalCompositionSet::from_byte_items)))
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
    let function_id = Arc::new(match &target {
        InvocationTarget::Name(name) => name.clone(),
        InvocationTarget::Composition(_) => String::from("Composition"),
    });
    let mut recorder = Recorder::new(function_id.clone(), start_time);
    recorder.record(dandelion_commons::records::RecordPoint::DeserializationEnd);
    debug!("finished creating request context");

    // TODO match set names to assign sets to composition sets
    // map sets in the order they are in the request
    let request_number = request_context.content.len();
    debug!("Request number of request_context: {}", request_number);
    let inputs = CompositionSet::from_context(request_context)
        .into_iter()
        .collect::<Vec<_>>();

    // want a 1 to 1 mapping of all outputs the functions gives as long as we don't add user input on what they want

    let (function_output, recorder) = match target {
        InvocationTarget::Name(_) => {
            dispatcher
                .queue_function_by_name(function_id, inputs, is_cold, recorder)
                .await
        }
        InvocationTarget::Composition(composition) => {
            dispatcher
                .queue_unregistered_composition(composition, inputs, !is_cold, recorder)
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
        let frame = frame.or_else(|err| {
            err_dandelion!(DandelionError::RequestError(FrontendError::InvalidRequest(
                format!("Failed to read request body: {err}")
            )))
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
                DandelionError::RequestError(FrontendError::InvalidRequest(_)) => {
                    StatusCode::BAD_REQUEST
                }
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            Ok(http_response(
                DandelionBody::status(format!("Failed to serve request: {}", err)),
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

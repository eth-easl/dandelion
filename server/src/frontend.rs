use std::{
    collections::BTreeMap,
    convert::Infallible,
    io::Write,
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Instant,
};

use crate::{DispatcherCommand, TRACING_ARCHIVE};
use dandelion_commons::{
    err_dandelion, records::Recorder, DandelionError, DandelionResult, FrontendError,
};
use dandelion_server::DandelionBody;
use dispatcher::{
    dispatcher::DispatcherInput,
    queue::{get_engine_flag, WorkQueue},
};
use http_body_util::BodyExt;
use hyper::{
    body::{Body, Incoming},
    service::service_fn,
    Request, Response, StatusCode,
};
use log::{debug, error, warn};
use machine_interface::{
    composition::CompositionSet,
    function_driver::{Metadata, WorkDone, WorkToDo},
    machine_config::EngineType,
    memory_domain::{bytes_context::BytesContext, read_only::ReadOnlyContext},
    DataItem, DataSet, Position,
};
use multinode::{
    proto::{Engine, NodeInfo},
    util::{composition_sets_to_proto, engine_type_ptod, proto_data_sets_to_composition_sets},
};
use serde::Deserialize;
use tokio::{
    net::TcpListener,
    signal::unix::SignalKind,
    sync::{mpsc, oneshot},
};

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
}

async fn handle_function_registration(
    req: Request<Incoming>,
    dispatcher: mpsc::Sender<DispatcherCommand>,
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
    return Ok(DandelionBody::from_vec(
        "Function registered".as_bytes().to_vec(),
    ));
}

#[derive(Debug, Deserialize)]
struct RegisterChain {
    composition: String,
}

async fn handle_composition_registration(
    req: Request<Incoming>,
    dispatcher: mpsc::Sender<DispatcherCommand>,
) -> DandelionResult<DandelionBody> {
    let bytes = req
        .collect()
        .await
        .expect("Failed to extract body from function registration")
        .to_bytes();

    // find first line end character
    let request_map: RegisterChain =
        bson::from_slice(&bytes).expect("Should be able to deserialize request");

    let (callback, confirmation) = oneshot::channel();
    dispatcher
        .send(DispatcherCommand::CompositionRegistration {
            composition: request_map.composition,
            callback,
        })
        .await
        .unwrap();

    if let Err(insertion_err) = confirmation.await.unwrap() {
        return err_dandelion!(DandelionError::RequestError(FrontendError::InternalError(
            format!(
                "Failed to insert composition into dispatcher: {:?}",
                insertion_err
            ),
        )));
    }

    return Ok(DandelionBody::from_vec(
        "Function registered".as_bytes().to_vec(),
    ));
}

//----------------
// user invoction

async fn handle_request(
    is_cold: bool,
    req: Request<Incoming>,
    dispatcher: mpsc::Sender<DispatcherCommand>,
) -> DandelionResult<DandelionBody> {
    debug!("Starting to serve request");

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
        } else {
            if body_pin.is_end_stream() {
                break;
            } else {
                continue;
            }
        }
    }

    // from context from frame bytes
    let request_context_result = BytesContext::from_bytes_vec(frame_data, total_size).await;
    if request_context_result.is_err() {
        warn!("request parsing failed with: {:?}", request_context_result);
    }

    // TODO make single enum, so we cannot have the None None or Some Some case
    let (function_name, composition, request_context) = request_context_result.unwrap();
    let had_function_name = function_name.is_some();
    let function_id = Arc::new(function_name.unwrap_or_else(|| String::from("Composition")));
    let mut recorder = Recorder::new(function_id.clone(), start_time);
    recorder.record(dandelion_commons::records::RecordPoint::DeserializationEnd);
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

    let (callback, output_recevier) = tokio::sync::oneshot::channel();
    if had_function_name {
        dispatcher
            .send(DispatcherCommand::FunctionRequest {
                function_id,
                inputs,
                is_cold,
                recorder,
                callback,
            })
            .await
            .unwrap();
    } else {
        dispatcher
            .send(DispatcherCommand::CompositionRequest {
                composition: composition
                    .expect("Did not get a service name nor a composition description in request"),
                inputs,
                is_cold,
                recorder,
                callback,
            })
            .await
            .unwrap();
    }
    let (function_output, recorder) = output_recevier
        .await
        .unwrap()
        .expect("Should get result from function");

    let response_body = dandelion_server::DandelionBody::new(function_output, &recorder);

    debug!("finished creating response body");
    #[cfg(feature = "archive")]
    TRACING_ARCHIVE.get().unwrap().insert_recorder(recorder);

    Ok(response_body)
}

async fn handle_stats_collection(_req: Request<Incoming>) -> DandelionResult<DandelionBody> {
    let archive_ref = TRACING_ARCHIVE.get().unwrap();
    let response = DandelionBody::from_vec(archive_ref.get_summary().into_bytes());
    archive_ref.reset();
    Ok(response)
}

//-----------
// multinode

async fn remote_work_queue(
    mut deregister: oneshot::Receiver<()>,
    work_queue: WorkQueue,
    engines: Vec<Engine>,
    host: String,
    port: u16,
    remote_timeout: u64,
) {
    // create remote node
    let remote_node = multinode::client::create_client(host, port, remote_timeout).await;
    // poll work queue and relay work
    // TODO add logic for waiting for all engine types at the same time and book keeping the available resources
    let engine_type = engine_type_ptod(engines[1].engine_type()).unwrap();
    let engine_flags = get_engine_flag(engine_type);
    loop {
        //  check if we need to stop using that node
        match deregister.try_recv() {
            // If there is nothing to receive continue working
            Err(oneshot::error::TryRecvError::Empty) => (),
            // if there is a shutdown message or the channel broke, stop sending requests
            Ok(()) | Err(oneshot::error::TryRecvError::Closed) => break,
        }
        // check if work is available
        if let Some((work, debt)) = work_queue.try_get_work(engine_flags, engine_type) {
            match work {
                // TODO try to recover from remote failing if it is not a function failure
                WorkToDo::FunctionArguments {
                    function_id,
                    function_alternatives: _,
                    caching: _,
                    input_sets,
                    metadata: _,
                    recorder,
                } => {
                    let results = remote_node
                        .invoke_function(function_id, &input_sets)
                        .await
                        .and_then(|context| Ok(WorkDone::Context(context)));
                    drop(recorder);
                    debt.fulfill(results);
                }
                WorkToDo::Shutdown(_) => panic!("Remote engine should not get shutdown request"),
            };
        } else {
            tokio::task::yield_now().await;
        }
    }
}

async fn handle_remote_node_registration(
    req: Request<Incoming>,
    dispatcher: mpsc::Sender<DispatcherCommand>,
    remote_nodes: Arc<Mutex<BTreeMap<(String, u16), oneshot::Sender<()>>>>,
    remote_timeout: u64,
) -> DandelionResult<DandelionBody> {
    debug!("Received remote node registration");

    let req_bytes = req
        .collect()
        .await
        .expect("Failed to extract body from function registration")
        .to_bytes();
    let node_info = match multinode::deserialize_node_info(req_bytes) {
        Ok(node_info) => node_info,
        Err(err) => {
            return err_dandelion!(DandelionError::RequestError(FrontendError::InvalidRequest(
                format!("Failed to register remote node: {:?}", err),
            )));
        }
    };

    debug!(
        "Handling remote registration from host={}, port={} with engines {:?}",
        node_info.host, node_info.port, node_info.engines,
    );

    let NodeInfo {
        host,
        port,
        engines,
    } = node_info;

    let (callback, receiver) = tokio::sync::oneshot::channel();
    dispatcher
        .send(DispatcherCommand::RemoteRegistration { callback })
        .await
        .unwrap();

    let action_status = match receiver.await.unwrap() {
        Ok(work_queue) => {
            // create shutdown channel
            let (deregister_sender, deregister_receiver) = tokio::sync::oneshot::channel();
            tokio::spawn(remote_work_queue(
                deregister_receiver,
                work_queue,
                engines,
                host.clone(),
                port as u16,
                remote_timeout,
            ));
            remote_nodes
                .lock()
                .unwrap()
                .insert((host, port as u16), deregister_sender);
            multinode::proto::ActionStatus {
                success: true,
                message: "".to_string(),
            }
        }
        Err(err) => multinode::proto::ActionStatus {
            success: false,
            message: format!("Failed to register remote node: {}", err),
        },
    };
    Ok(DandelionBody::from_vec(
        multinode::serialize_action_status(action_status).to_vec(),
    ))
}

async fn handle_remote_node_deregistration(
    req: Request<Incoming>,
    remote_nodes: Arc<Mutex<BTreeMap<(String, u16), oneshot::Sender<()>>>>,
) -> DandelionResult<DandelionBody> {
    debug!("Received remote node registration");

    let req_bytes = req
        .collect()
        .await
        .expect("Failed to extract body from function registration")
        .to_bytes();
    let node_info = match multinode::deserialize_node_info(req_bytes) {
        Ok(node_info) => node_info,
        Err(err) => {
            return err_dandelion!(DandelionError::RequestError(FrontendError::InvalidRequest(
                format!("Failed to register remote node: {:?}", err),
            )));
        }
    };

    let host_port = (node_info.host, node_info.port as u16);
    let action_status =
        if let Some(running_remote) = remote_nodes.lock().unwrap().remove(&host_port) {
            running_remote.send(()).unwrap();
            multinode::proto::ActionStatus {
                success: true,
                message: "".to_string(),
            }
        } else {
            multinode::proto::ActionStatus {
                success: false,
                message: format!(
                    "Failed to deregister remote node: {}:{}, not a registered remote node",
                    host_port.0, host_port.1
                ),
            }
        };

    Ok(DandelionBody::from_vec(
        multinode::serialize_action_status(action_status).to_vec(),
    ))
}

async fn handle_remote_node_request(
    req: Request<Incoming>,
    dispatcher: mpsc::Sender<DispatcherCommand>,
) -> DandelionResult<DandelionBody> {
    debug!("Parsing remote execution request");
    let start_time = Instant::now();

    let req_bytes = req
        .collect()
        .await
        .expect("Failed to extract body from task request")
        .to_bytes();

    let multinode::proto::InvocationRequest {
        function_id,
        data_sets,
    } = match multinode::deserialize_invocation_request(req_bytes.clone()) {
        Ok(task_info) => task_info,
        Err(err) => {
            return err_dandelion!(DandelionError::RequestError(FrontendError::InvalidRequest(
                format!("Failed to deserialize invocation request: {:?}", err),
            )));
        }
    };

    let function_id = Arc::new(function_id);

    let mut recorder = Recorder::new(function_id.clone(), start_time);
    recorder.record(dandelion_commons::records::RecordPoint::DeserializationEnd);

    let input_sets = proto_data_sets_to_composition_sets(data_sets);

    debug!("remote input sets: {:?}", input_sets);

    let (callback, output_recevier) = tokio::sync::oneshot::channel();
    dispatcher
        .send(DispatcherCommand::RemoteFunctionRequest {
            function_id: function_id,
            inputs: input_sets,
            recorder,
            callback,
        })
        .await
        .unwrap();

    debug!("Remote function finished execution");

    let invocation_response = match output_recevier.await.unwrap() {
        // TODO: handle recorder
        Ok((out_sets, _)) => multinode::proto::InvocationResponse {
            success: true,
            error_msg: "".to_string(),
            data_sets: composition_sets_to_proto(&out_sets),
        },
        Err(err) => multinode::proto::InvocationResponse {
            success: false,
            error_msg: format!("Failed to get result from function: {:?}", err),
            data_sets: vec![],
        },
    };
    Ok(DandelionBody::from_vec(
        multinode::serialize_invocation_response(invocation_response).to_vec(),
    ))
}

//-----------------------
// main service function

async fn service(
    req: Request<Incoming>,
    dispatcher: mpsc::Sender<DispatcherCommand>,
    folder_path: &'static str,
) -> Result<Response<DandelionBody>, Infallible> {
    // handle request
    let mut is_multinode_request = false;
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
        "/stats" => handle_stats_collection(req).await,
        other_uri => {
            debug!("Received request on {}", other_uri);
            Ok(DandelionBody::from_vec(
                format!("Hello, World\n").into_bytes(),
            ))
        }
    };

    // create response
    match res {
        Ok(body) => Ok::<_, Infallible>(Response::new(body)),
        Err(err) => {
            warn!("Failed to serve request: {}", err);
            let response = if is_multinode_request {
                // for multinode requests return ActionStatus
                let action_status = multinode::proto::ActionStatus {
                    success: false,
                    message: format!("{}", err),
                };
                Response::new(DandelionBody::from_vec(
                    multinode::serialize_action_status(action_status).to_vec(),
                ))
            } else {
                // for all other requests set response status to something not ok and write the error in the response body
                let mut response = Response::new(DandelionBody::from_vec(
                    format!("Failed to serve request: {}", err).into_bytes(),
                ));
                *response.status_mut() = match err.error {
                    DandelionError::RequestError(FrontendError::InvalidRequest(_)) => {
                        StatusCode::BAD_REQUEST
                    }
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                };
                response
            };
            Ok::<_, Infallible>(response)
        }
    }
}

pub async fn service_loop(
    request_sender: mpsc::Sender<DispatcherCommand>,
    folder_path: &'static str,
    port: u16,
) {
    // socket to listen to
    let addr: SocketAddr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr).await.unwrap();

    // signal handlers for gracefull shutdown
    let mut sigterm_stream = tokio::signal::unix::signal(SignalKind::terminate()).unwrap();
    let mut sigint_stream = tokio::signal::unix::signal(SignalKind::interrupt()).unwrap();
    let mut sigquit_stream = tokio::signal::unix::signal(SignalKind::quit()).unwrap();

    loop {
        tokio::select! {
            connection_pair = listener.accept() => {
                let (stream,_) = connection_pair.unwrap();
                let loop_dispatcher = request_sender.clone();
                let io = hyper_util::rt::TokioIo::new(stream);
                tokio::task::spawn(async move {
                    let service_dispatcher_ptr = loop_dispatcher.clone();
                    if let Err(err) = hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new())
                        .serve_connection_with_upgrades(
                            io,
                            service_fn(|req| service(req, service_dispatcher_ptr.clone(), folder_path)),
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

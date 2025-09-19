use http_body_util::BodyExt;
use hyper::{
    body::{Body, Incoming},
    Request, Response, StatusCode,
};
use log::{debug, trace, warn};
use multinode::proto::TaskResult;
use serde::Deserialize;
use std::{convert::Infallible, io::Write, path::PathBuf, sync::Arc, time::Instant};
use tokio::sync::{mpsc, oneshot};

use super::{DispatcherCommand, FUNCTION_FOLDER_PATH, TRACING_ARCHIVE};
use dandelion_commons::{DandelionError, DandelionResult, FrontendError};
use dandelion_server::DandelionBody;
use dispatcher::{
    composition::{
        convert_composition_sets_to_protobuf, convert_protobuf_to_composition_sets, CompositionSet,
    },
    dispatcher::DispatcherInput,
    function_registry::Metadata,
};
use machine_interface::{
    machine_config::{i32_to_engine_type, EngineType},
    memory_domain::{bytes_context::BytesContext, read_only::ReadOnlyContext},
    DataItem, DataSet, Position,
};

fn default_path() -> String {
    String::new()
}

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
) -> DandelionResult<DandelionBody> {
    let bytes = req
        .collect()
        .await
        .expect("Failed to extract body from function registration")
        .to_bytes(); // TODO: return error

    // find first line end character
    let request_map: RegisterFunction =
        bson::from_slice(&bytes).expect("Should be able to deserialize request"); // TODO: return error

    // if local is present ignore the binary
    let path_string = if !request_map.local_path.is_empty() {
        // check that file exists
        if let Err(err) = std::fs::File::open(&request_map.local_path) {
            return Err(DandelionError::RequestError(FrontendError::InvalidRequest(
                format!("Tried to register function with local path, but failed to open file with error {}",
                err),
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

    // TODO: move to machine config
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
        input_sets: Arc::new(input_sets),
        output_sets: Arc::new(request_map.output_sets),
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

    if let Err(insertion_err) = confirmation.await.unwrap() {
        return Err(DandelionError::RequestError(FrontendError::InternalError(
            format!(
                "Failed to insert function into dispatcher: {:?}",
                insertion_err
            ),
        )));
    };

    Ok(DandelionBody::from_vec(
        "Function registered".as_bytes().to_vec(),
    ))
}

#[derive(Debug, Deserialize)]
struct RegisterChain {
    composition: String,
}

async fn handle_composition_registeration(
    req: Request<Incoming>,
    dispatcher: mpsc::Sender<DispatcherCommand>,
) -> DandelionResult<DandelionBody> {
    let bytes = req
        .collect()
        .await
        .expect("Failed to extract body from composition registration")
        .to_bytes(); // TODO: return error

    // find first line end character
    let request_map: RegisterChain =
        bson::from_slice(&bytes).expect("Should be able to deserialize request"); // TODO: return error

    let (callback, confirmation) = oneshot::channel();
    dispatcher
        .send(DispatcherCommand::CompositionRegistration {
            composition: request_map.composition,
            callback,
        })
        .await
        .unwrap();

    if let Err(insertion_err) = confirmation.await.unwrap() {
        return Err(DandelionError::RequestError(FrontendError::InternalError(
            format!(
                "Failed to insert composition into dispatcher: {:?}",
                insertion_err
            ),
        )));
    };

    Ok(DandelionBody::from_vec(
        "Composition registered".as_bytes().to_vec(),
    ))
}

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
        return Err(DandelionError::RequestError(FrontendError::InvalidRequest(
            format!("request parsing failed with: {:?}", request_context_result),
        )));
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

    let (callback, output_recevier) = tokio::sync::oneshot::channel();
    dispatcher
        .send(DispatcherCommand::FunctionRequest {
            name: function_name,
            inputs,
            is_cold,
            start_time: start_time.clone(),
            callback,
        })
        .await
        .unwrap();
    let (function_output, recorder) = match output_recevier.await.unwrap() {
        Ok(x) => x,
        Err(err) => {
            return Err(DandelionError::RequestError(FrontendError::InternalError(
                format!("Failed to get result from function: {}", err),
            )))
        }
    };

    let response_body = dandelion_server::DandelionBody::new(function_output, &recorder);

    debug!("finished creating response body");
    #[cfg(feature = "archive")]
    TRACING_ARCHIVE.get().unwrap().insert_recorder(recorder);

    Ok(response_body)
}

// multinode

async fn handle_remote_node_registration(
    req: Request<Incoming>,
    dispatcher: mpsc::Sender<DispatcherCommand>,
) -> DandelionResult<DandelionBody> {
    println!("Received remote node registration");
    let req_bytes = req
        .collect()
        .await
        .expect("Failed to extract body from function registration")
        .to_bytes();

    let node_info = match multinode::deserialize_node_info(req_bytes) {
        Ok(node_info) => node_info,
        Err(err) => {
            return Err(DandelionError::RequestError(FrontendError::InvalidRequest(
                format!("Failed to register remote node: {:?}", err),
            )));
        }
    };

    let engine_type = match i32_to_engine_type(node_info.engine_type) {
        Some(t) => t,
        None => {
            warn!("Failed to register remote node: Invalid engine type");
            return Err(DandelionError::RequestError(FrontendError::InvalidRequest(
                format!("Failed to register remote node: Invalid engine type"),
            )));
        }
    };
    dispatcher
        .send(DispatcherCommand::RemoteRegistration {
            name: node_info.name,
            host: node_info.host,
            port: node_info.port as u16,
            engine_type: engine_type,
            engine_cap: node_info.engine_capacity,
        })
        .await
        .unwrap();

    let response = multinode::serialize_action_status(multinode::proto::ActionStatus {
        success: true,
        message: "".to_string(),
    });
    Ok(DandelionBody::from_vec(response.to_vec()))
}

async fn handle_remote_node_deregistration(
    req: Request<Incoming>,
    dispatcher: mpsc::Sender<DispatcherCommand>,
) -> DandelionResult<DandelionBody> {
    println!("Received remote node registration");
    let req_bytes = req
        .collect()
        .await
        .expect("Failed to extract body from function registration")
        .to_bytes();

    let node_info = match multinode::deserialize_node_info(req_bytes) {
        Ok(node_info) => node_info,
        Err(err) => {
            return Err(DandelionError::RequestError(FrontendError::InvalidRequest(
                format!("Failed to register remote node: {:?}", err),
            )));
        }
    };

    dispatcher
        .send(DispatcherCommand::RemoteDeregistration {
            name: node_info.name,
        })
        .await
        .unwrap();

    let response = multinode::serialize_action_status(multinode::proto::ActionStatus {
        success: true,
        message: "".to_string(),
    });
    Ok(DandelionBody::from_vec(response.to_vec()))
}

async fn handle_remote_node_request(
    req: Request<Incoming>,
    dispatcher: mpsc::Sender<DispatcherCommand>,
) -> DandelionResult<DandelionBody> {
    let start_time = Instant::now();

    let req_bytes = req
        .collect()
        .await
        .expect("Failed to extract body from task request")
        .to_bytes();

    let task_info = match multinode::deserialize_task(req_bytes.clone()) {
        Ok(task_info) => task_info,
        Err(err) => {
            return Err(DandelionError::RequestError(FrontendError::InvalidRequest(
                format!("Failed to deserialize task request: {:?}", err),
            )));
        }
    };

    let input_sets = convert_protobuf_to_composition_sets(task_info.data_sets, req_bytes);
    let (callback, output_recevier) = tokio::sync::oneshot::channel();
    dispatcher
        .send(DispatcherCommand::RemoteTask {
            function_id: task_info.function_id,
            inputs: input_sets,
            start_time,
            callback,
        })
        .await
        .unwrap();

    let response = match output_recevier.await.unwrap() {
        Ok(out_sets) => multinode::serialize_task_result(TaskResult {
            success: true,
            error_msg: String::new(),
            data_sets: convert_composition_sets_to_protobuf(out_sets),
        }),
        Err(err) => multinode::serialize_task_result(TaskResult {
            success: false,
            error_msg: format!("Failed to get result from function: {:?}", err),
            data_sets: vec![],
        }),
    };
    Ok(DandelionBody::from_vec(response.to_vec()))
}

// stats

async fn handle_stats_collection(_req: Request<Incoming>) -> DandelionResult<DandelionBody> {
    let archive_ref = TRACING_ARCHIVE.get().unwrap();
    let response = DandelionBody::from_vec(archive_ref.get_summary().into_bytes());
    archive_ref.reset();
    Ok(response)
}

// main service function

pub async fn service(
    req: Request<Incoming>,
    dispatcher: mpsc::Sender<DispatcherCommand>,
) -> Result<Response<DandelionBody>, Infallible> {
    // handle request
    let mut is_multinode_request = false;
    let res = match req.uri().path() {
        "/register/function" => handle_function_registration(req, dispatcher).await,
        "/register/composition" => handle_composition_registeration(req, dispatcher).await,
        "/multinode/register" => {
            is_multinode_request = true;
            handle_remote_node_registration(req, dispatcher).await
        }
        "/multinode/deregister" => {
            is_multinode_request = true;
            handle_remote_node_deregistration(req, dispatcher).await
        }
        "/multinode/schedule" => {
            is_multinode_request = true;
            handle_remote_node_request(req, dispatcher).await
        }
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
            trace!("Received request on {}", other_uri);
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
                let response = multinode::serialize_action_status(multinode::proto::ActionStatus {
                    success: false,
                    message: format!("{}", err),
                });
                Response::new(DandelionBody::from_vec(response.to_vec()))
            } else {
                // for all other requests set response status to something not ok and write the error in the response body
                let mut response = Response::new(DandelionBody::from_vec(
                    format!("Failed to serve request: {}", err).into_bytes(),
                ));
                *response.status_mut() = match err {
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

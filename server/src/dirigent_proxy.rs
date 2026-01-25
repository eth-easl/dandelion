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
use log::trace;
use log::{debug, error, info, warn};
use machine_interface::{
    composition::CompositionSet,
    function_driver::Metadata,
    machine_config::EngineType,
    memory_domain::{bytes_context::BytesContext, read_only::ReadOnlyContext},
    DataItem, DataSet, Position,
};
use serde::Deserialize;
use serde::Serialize;
use std::io::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread;
use std::{
    collections::HashMap,
    convert::Infallible,
    io::{ErrorKind, Write},
    path::PathBuf,
    time::Instant,
    sync::atomic::{AtomicUsize, Ordering},
};
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::{Runtime, Builder};
use tokio::signal::unix::SignalKind;
use tokio::sync::{mpsc, oneshot};
use tokio::{io, try_join};

use core_affinity::{self, CoreId};
use dandelion_server::config::DandelionConfig;

const HTTP2_MAGIC_PACKET: [u8; 24] = [
    0x50, 0x52, 0x49, 0x20, 0x2a, 0x20, 0x48, 0x54, 0x54, 0x50, 0x2f, 0x32, 0x2e, 0x30, 0x0d, 0x0a,
    0x0d, 0x0a, 0x53, 0x4d, 0x0d, 0x0a, 0x0d, 0x0a,
];

pub async fn proxy_to_uc(
    request: Vec<u8>,
    mut client_recv: tokio::net::tcp::ReadHalf<'_>,
    mut client_send: tokio::net::tcp::WriteHalf<'_>,
    mut destination: TcpStream,
) -> Result<(), Box<dyn std::error::Error>> {
    // By the model of: https://anirudhsingh.dev/blog/2021/07/writing-a-tcp-proxy-in-rust/
    let (mut server_recv, mut server_send) = destination.split();

    let destination_to_source = async {
        let val = io::copy(&mut server_recv, &mut client_send).await.unwrap();
        trace!("SERVER -> CLIENT: server_recv -> client_send {}", val);

        Ok::<u64, Error>(val)
    };

    let source_to_destination = async {
        let val = server_send.write(request.as_slice()).await;
        trace!(
            "CLIENT -> SERVER: client_recv -> server_send {}",
            val.unwrap()
        );

        let res = io::copy(&mut client_recv, &mut server_send).await.unwrap();
        trace!("CLIENT -> SERVER: client_recv -> server_send {}", res);

        Ok::<u64, Error>(res)
    };

    let _ = try_join!(destination_to_source, source_to_destination);

    Ok(())
}

async fn create_proxy_server(port: u16, dg_svc: Arc<DirigentService>) -> io::Result<()> {
    let addr: SocketAddr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr).await?;

    tracing::info!("Listening on {}", addr);

    loop {
        let (mut raw_stream, _) = listener.accept().await?;
        let dg_svc_clone = Arc::clone(&dg_svc);

        tokio::task::spawn(async move {
            let (mut client_recv, client_send) = raw_stream.split();
            let mut stream = BufReader::new(&mut client_recv);

            match request_parser::parse_request(&mut stream).await {
                Ok(request) => {
                    tracing::info!(?request, "incoming request");

                    // NOTE: in curl set -H 'Connection: close' to force connection closure
                    let function_raw = request.headers.get("Host");
                    if function_raw.is_none() {
                        tracing::error!("Header 'Host' not found. Aborting request.");

                        return Err::<(), Error>(Error::new(
                            io::ErrorKind::Other,
                            "Header 'Host' not found. Aborting request.",
                        ));
                    }

                    let function = function_raw.unwrap();
                    let destination_url = dg_svc_clone.choose_on_endpoint(&function);
                    if destination_url.is_none() {
                        tracing::error!("Path not found '{}'. Aborting request.", &function);

                        return Err::<(), Error>(Error::new(
                            io::ErrorKind::Other,
                            format!("function not found for the given path {}", function),
                        ));
                    }

                    let destination = TcpStream::connect(destination_url.clone().unwrap())
                        .await
                        .unwrap();

                    // TODO: share socket towards the destination URL between multiple threads
                    let _ =
                        proxy_to_uc(request.raw_data, client_recv, client_send, destination).await;

                    trace!("Connection to {} closed\n", destination_url.unwrap());
                }
                Err(e) => {
                    tracing::info!(?e, "failed to parse request");
                    return Err::<(), Error>(Error::new(
                        io::ErrorKind::Other,
                        "failed to parse request",
                    ));
                }
            }

            return Ok::<_, Error>(());
        });
    }
}

pub fn start_proxy_server(port: u16, dg_svc: Arc<DirigentService>) {
    let runtime = Runtime::new().unwrap();

    thread::spawn(move || {
        runtime.block_on(async {
            create_proxy_server(port, dg_svc).await.unwrap();
        });
    });
}

const FUNCTION_FOLDER_PATH: &str = "/tmp/dandelion_server";

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

// ********* Invoke nghttp2 code func; Parse its output *************
// ******************************************************************
// ******************************************************************

// Prepare the input to the nghttp2_codec function and invokes it
async fn prepare_input_and_invoke_nghttp2_codec(
    nghttp2_codec_func_name : String,
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
    let mut input_size_of_headers_to_send_list = Vec::with_capacity(num_req_resp_to_send);
    let mut input_size_of_body_to_send_list = Vec::with_capacity(num_req_resp_to_send);
    for input_set_idx in 1..(1 + num_req_resp_to_send) {
        let stream_id_to_send_response = stream_id_to_send_response_list[input_set_idx - 1];
        let num_of_headers_to_send = num_of_headers_to_send_list[input_set_idx - 1];
        let size_of_headers_to_send = size_of_headers_to_send_list[input_set_idx - 1];
        let size_of_body_to_send = size_of_body_to_send_list[input_set_idx - 1];

        let input_stream_id_to_send_response = stream_id_to_send_response.to_ne_bytes();
        let input_num_of_headers_to_send = num_of_headers_to_send.to_ne_bytes();
        let input_size_of_headers_to_send = size_of_headers_to_send.to_ne_bytes();
        let input_size_of_body_to_send = size_of_body_to_send.to_ne_bytes();

        input_stream_id_to_send_response_list.push(input_stream_id_to_send_response);
        input_num_of_headers_to_send_list.push(input_num_of_headers_to_send);
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
    Vec<Vec<u8>>, // headers_received_list
    Vec<Vec<u8>>, // body_received_list
    Vec<String>,  // header_authority_value_string_list
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
    let mut headers_received_list: Vec<Vec<u8>> = Vec::new();
    let mut body_received_list: Vec<Vec<u8>> = Vec::new();
    let mut header_authority_value_string_list: Vec<String> = Vec::new();
    for idx_req_resp in 0..num_of_req_or_res_received {
        let set_idx = idx_req_resp + 1;

        let mut stream_id: i32 = -1;
        let mut num_of_headers: usize = 0;
        let mut headers_received: Vec<u8> = Vec::new();
        let mut body_received: Vec<u8> = Vec::new();
        let mut header_authority_value: Vec<u8> = Vec::new();
        let header_authority_value_string;

        assert_eq!(5, response.sets[set_idx].items.len());

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
                _ => {}
            }
        }

        stream_id_list.push(stream_id);
        num_of_headers_list.push(num_of_headers);
        headers_received_list.push(headers_received);
        body_received_list.push(body_received);

        header_authority_value_string = String::from_utf8(header_authority_value).unwrap();
        debug!(
            "codec header_authority_value: {}",
            header_authority_value_string
        );
        header_authority_value_string_list.push(header_authority_value_string);
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
        headers_received_list,
        body_received_list,
        header_authority_value_string_list,
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
    router_to_stream_worker_tx: oneshot::Sender<RouterToStreamWorkerResp>,
}

#[derive(Debug)]
struct RouterToStreamWorkerResp {
    payload: String,
    stream_worker_to_func_connection_worker_tx: mpsc::Sender<StreamWorkerToFuncConnReq>,
}

struct FunctionConnToStreamWorkerResp {
    payload: String,
    num_of_headers_to_send: usize,
    headers: Vec<u8>,
    body_to_send: Vec<u8>,
}

struct StreamWorkerToFuncConnReq {
    payload: String,
    stream_id: i32, // The stream_id of that stream worker
    num_of_headers_to_send: usize,
    headers: Vec<u8>,
    body: Vec<u8>,
    function_connection_worker_to_stream_worker_tx: oneshot::Sender<FunctionConnToStreamWorkerResp>,
}

struct StreamWorkerToDpConnReq {
    payload: String,
    stream_id_to_send_response: i32,
    num_of_headers_to_send: usize,
    headers: Vec<u8>,
    body_to_send: Vec<u8>,
}

impl Default for StreamWorkerToDpConnReq {
    fn default() -> Self {
        Self {
            payload: String::new(),
            stream_id_to_send_response: -1,
            num_of_headers_to_send: 0,
            headers: Vec::new(),
            body_to_send: Vec::new(),
        }
    }
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

// This handles the TCP/HTTP connection with user function container
// Thus, it acts as a HTTP client
async fn func_connection_worker3(
    nghttp2_codec_func_name : String,
    mut stream: TcpStream,
    request_sender: mpsc::Sender<DispatcherCommand>,
    mut stream_worker_to_func_connection_worker_rx: mpsc::Receiver<StreamWorkerToFuncConnReq>,
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
    let mut stream_worker_map: HashMap<i32, oneshot::Sender<FunctionConnToStreamWorkerResp>> =
        HashMap::new();

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
        headers_to_send_list = Vec::new();
        body_to_send_list = Vec::new();
        size_of_headers_to_send_list = Vec::new();
        size_of_body_to_send_list = Vec::new();

        let mut channel_to_send_back_resp_list: Vec<
            Option<oneshot::Sender<FunctionConnToStreamWorkerResp>>,
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
                        size_of_headers_to_send_list.push(req.headers.len());
                        headers_to_send_list.push(req.headers);
                        size_of_body_to_send_list.push(req.body.len());
                        body_to_send_list.push(req.body);

                        channel_to_send_back_resp_list.push(Some(req.function_connection_worker_to_stream_worker_tx));
                    }
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
        let mut headers_received_list: Vec<Vec<u8>>;
        let mut body_received_list: Vec<Vec<u8>>;
        let mut header_authority_value_string_list: Vec<String>;

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
            headers_received_list,
            body_received_list,
            header_authority_value_string_list,
        ) = parse_nghttp2_codec_output(function_output, recorder);
        size_of_session_state = session_state.len();

        // ****** Operations triggered by output set 0 *******
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
            let headers_received: Vec<u8> = headers_received_list.remove(0);
            let body_received: Vec<u8> = body_received_list.remove(0);
            let channel_to_stream_worker = stream_worker_map.remove(&stream_id);

            info!("[func_connection_worker3. Local Addr: {}; Peer Addr: {}] receives a resp from uc with stream id: {}", tcp_conn_local_addr, tcp_conn_peer_addr, stream_id);

            match channel_to_stream_worker {
                Some(c) => {
                    // debug!("[func conn worker] sends response to stream worker {}", stream_id);

                    c.send(FunctionConnToStreamWorkerResp {
                        payload: format!("get resp from user func"),
                        num_of_headers_to_send: num_of_headers,
                        headers: headers_received,
                        body_to_send: body_received,
                    });
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
    nghttp2_codec_func_name : String,
    dg_svc: Arc<DirigentService>,
    request_sender: mpsc::Sender<DispatcherCommand>,
    mut stream_worker_to_router_rx: mpsc::Receiver<StreamWorkerToRouterReq>,
) {
    // ***TMP and TO DO***: currently we assume that for each user container/url, there will only be one connection
    // key: url; value: the channel to the func_conn_worker, which handles the connection to that func container
    let mut uc_connections: HashMap<String, mpsc::Sender<StreamWorkerToFuncConnReq>> =
        HashMap::new();

    // Process requests from stream workers
    while let Some(req) = stream_worker_to_router_rx.recv().await {
        info!(
            "[Router] receives the request with header authority: {}",
            req.header_authority_value_string
        );

        // TODO: use X-Anakonda-Forward for routing
        // ****** Based on the header_authority_value_sting, query the dirigent service to get the url ******
        // ****** Based on the url, pick one connection and its corresponding func_conn_worker ******
        let destination_url = dg_svc.choose_on_endpoint(&req.header_authority_value_string);
        if destination_url.is_none() {
            error!(
                "[Router] the dirigent service does not the find the url of function {}",
                req.header_authority_value_string
            );
        } else {
            let destination_url_string = destination_url.unwrap();
            debug!(
                "[Router] gets the url: {} from the dirigent service",
                destination_url_string
            );

            let stream_worker_to_func_connection_worker_tx: mpsc::Sender<
                StreamWorkerToFuncConnReq,
            >;
            let stream_worker_to_func_connection_worker_rx: mpsc::Receiver<
                StreamWorkerToFuncConnReq,
            >;

            // *** Check if there is a need to create a new connection ***
            let mut need_to_create_a_new_connection: bool = false;
            if uc_connections.contains_key(&destination_url_string) {
                if uc_connections
                    .get(&destination_url_string)
                    .unwrap()
                    .is_closed()
                {
                    info!("[Router] The connection stored in the map is closed. Need to create a new connection to the uc");
                    need_to_create_a_new_connection = true;
                    uc_connections.remove(&destination_url_string);
                }
            } else {
                need_to_create_a_new_connection = true;
            }

            if need_to_create_a_new_connection == true {
                (
                    stream_worker_to_func_connection_worker_tx,
                    stream_worker_to_func_connection_worker_rx,
                ) = mpsc::channel::<StreamWorkerToFuncConnReq>(256);

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
                        ));

                        uc_connections.insert(
                            destination_url_string,
                            stream_worker_to_func_connection_worker_tx.clone(),
                        );
                    }
                    Err(e) => {
                        error!(
                            "[Router] establish connection to {} with  error: {}",
                            destination_url_string, e
                        );
                    }
                }
            } else {
                debug!(
                    "[Router] already has a connection to {}",
                    destination_url_string
                );
                stream_worker_to_func_connection_worker_tx =
                    uc_connections.get(&destination_url_string).unwrap().clone();
            }

            // Send the answer to the stream worker
            let router_to_stream_worker_reply = RouterToStreamWorkerResp {
                payload: format!("router processed the request: {}", req.payload),
                stream_worker_to_func_connection_worker_tx:
                    stream_worker_to_func_connection_worker_tx.clone(),
            };
            let _ = req
                .router_to_stream_worker_tx
                .send(router_to_stream_worker_reply);
        }
    }

    debug!("[Router] all channels to the router are closed; Router stops working");
}

// It handles one HTTP2 req-resp pair
// It is launched by the dp_conn worker
// It receives the HTTP2 Request from the dp_conn_worker, query the router and forwards it to the func_conn_worker.
// Later, it would reiceves the HTTP2 response from the func_conn_worker and forwards it back to the dp_conn_worker
// *** TO DO: it should also execute the request-level network filters ***
async fn stream_worker(
    num_of_headers_received: usize,
    headers_received: Vec<u8>,
    header_authority_value_string: String,
    body_received: Vec<u8>,
    stream_worker_to_dp_connection_worker_tx: mpsc::Sender<StreamWorkerToDpConnReq>,
    stream_worker_to_router_tx: mpsc::Sender<StreamWorkerToRouterReq>,
    stream_id: i32,
    tcp_conn_local_addr: SocketAddr, // just for log
    tcp_conn_peer_addr: SocketAddr,  // just for log
) {
    info!(
        "[stream_worker. Local Addr: {}; Peer Addr: {}; Stream ID:{}] A new stream worker",
        tcp_conn_local_addr, tcp_conn_peer_addr, stream_id
    );

    // The stream worker would query the router to get the channel to the user_func_conn_worker
    // The channel used by the router to send resp back
    let (router_to_stream_worker_tx, router_to_stream_worker_rx) =
        oneshot::channel::<RouterToStreamWorkerResp>();

    // ****** send request to the router ******
    if let Err(e) = stream_worker_to_router_tx
        .send(StreamWorkerToRouterReq {
            payload: format!("request from stream worker: {}", stream_id),
            header_authority_value_string: header_authority_value_string,
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
        return;
    }

    // ****** Wait for router's resp ******
    match router_to_stream_worker_rx.await {
        Ok(resp) => {
            debug!("[stream_worker. Local Addr: {}; Peer Addr: {}; Stream ID:{}] gets resp from the router", tcp_conn_local_addr, tcp_conn_peer_addr, stream_id);

            // From the router, we now know which user-func connection to use
            let stream_worker_to_func_connection_worker_tx =
                resp.stream_worker_to_func_connection_worker_tx;

            // Used to get the resp back from the func_conn_worker
            let (
                function_connection_worker_to_stream_worker_tx,
                function_connection_worker_to_stream_worker_rx,
            ) = oneshot::channel::<FunctionConnToStreamWorkerResp>();
            // send request to the func_conn worker
            if let Err(e) = stream_worker_to_func_connection_worker_tx
                .send(StreamWorkerToFuncConnReq {
                    payload: format!("request from stream worker: {}", stream_id),
                    stream_id: stream_id,
                    num_of_headers_to_send: num_of_headers_received,
                    headers: headers_received,
                    body: body_received,
                    function_connection_worker_to_stream_worker_tx:
                        function_connection_worker_to_stream_worker_tx,
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
                return;
            }

            // *** Wait for the resp from the user func conn worker ***
            match function_connection_worker_to_stream_worker_rx.await {
                Ok(resp) => {
                    info!("[stream_worker. Local Addr: {}; Peer Addr: {}; Stream ID:{}] gets resp from the user func conn worker", tcp_conn_local_addr, tcp_conn_peer_addr, stream_id);

                    let _ = stream_worker_to_dp_connection_worker_tx
                        .send(StreamWorkerToDpConnReq {
                            payload: format!(
                                "[stream worker:{}] final response: {} ",
                                stream_id, resp.payload
                            ),
                            stream_id_to_send_response: stream_id,
                            num_of_headers_to_send: resp.num_of_headers_to_send,
                            headers: resp.headers,
                            body_to_send: resp.body_to_send,
                        })
                        .await;
                }
                Err(e) => {
                    error!("[stream_worker. Local Addr: {}; Peer Addr: {}; Stream ID:{}] fails to get resp from the func conn worker: {}", tcp_conn_local_addr, tcp_conn_peer_addr, stream_id, e);

                    let _ = stream_worker_to_dp_connection_worker_tx
                        .send(
                            StreamWorkerToDpConnReq {
                                payload: format!("[stream worker:{}] fails to receive from the user func conn worker: {} ", stream_id, e),
                                ..Default::default()
                            }
                        )
                        .await;
                }
            }
        }
        Err(e) => {
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

// The task to handle one TCP/HTTP connection with the data plane
// Thus, it acts as a HTTP server
async fn dp_connection_worker3(
    nghttp2_codec_func_name: String,
    mut stream: TcpStream,
    request_sender: mpsc::Sender<DispatcherCommand>,
    stream_worker_to_router_tx: mpsc::Sender<StreamWorkerToRouterReq>,
) {
    let tcp_conn_local_addr = stream.local_addr().unwrap();
    let tcp_conn_peer_addr = stream.peer_addr().unwrap();
    info!(
        "[dp_connection_worker3. Local Addr: {}; Peer Addr: {}] A new dp connection",
        tcp_conn_local_addr, tcp_conn_peer_addr
    );

    // Create channel to receive req from the stream worker
    let (stream_worker_to_dp_connection_worker_tx, mut stream_worker_to_dp_connection_worker_rx) =
        mpsc::channel::<StreamWorkerToDpConnReq>(256);

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
            if let Err(err) = stream.readable().await {
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
        headers_to_send_list = Vec::new();
        body_to_send_list = Vec::new();
        size_of_headers_to_send_list = Vec::new();
        size_of_body_to_send_list = Vec::new();

        // ****** Block until one of the two events happpen *******
        tokio::select! {
            stream_readable = stream.readable() => { // 1) Data received at the socket.
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
                    let peer_closed = read_from_tcp_stream_until_blocking(&mut stream, &mut data_to_read).await;
                    if peer_closed == true {
                        info!("[dp_connection_worker3. Local Addr: {}; Peer Addr: {}] The dp connection peer has closed the connection", tcp_conn_local_addr, tcp_conn_peer_addr);
                        // Peer closed the connection
                        let _ = stream.shutdown().await;
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
                        let stream_readable_again = stream.readable().await;
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
        let mut headers_received_list: Vec<Vec<u8>>;
        let mut body_received_list: Vec<Vec<u8>>;
        let mut header_authority_value_string_list: Vec<String>;
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
            headers_received_list,
            body_received_list,
            header_authority_value_string_list,
        ) = parse_nghttp2_codec_output(function_output, recorder);

        size_of_session_state = session_state.len();

        // ****** Operations triggered by output set 0 *******
        // 1) Send data outout
        // *** If there is data needed to be sent
        if data_to_send.len() > 0 {
            if let Err(err) = stream.write_all(&data_to_send).await {
                error!("[dp_connection_worker3. Local Addr: {}; Peer Addr: {}] tcp stream write_all failed: {:?}", tcp_conn_local_addr, tcp_conn_peer_addr, err);
                let _ = stream.shutdown().await;
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
            let headers_received: Vec<u8> = headers_received_list.remove(0);
            let body_received: Vec<u8> = body_received_list.remove(0);
            let header_authority_value_string: String =
                header_authority_value_string_list.remove(0);

            info!("[dp_connection_worker3. Local Addr: {}; Peer Addr: {}] Receive req with stream id {}. Spawn a stream worker.", tcp_conn_local_addr, tcp_conn_peer_addr, stream_id);
            tokio::spawn(stream_worker(
                num_of_headers,
                headers_received,
                header_authority_value_string,
                body_received,
                stream_worker_to_dp_connection_worker_tx.clone(),
                stream_worker_to_router_tx.clone(),
                stream_id,
                stream.local_addr().unwrap(),
                stream.peer_addr().unwrap(),
            ));
        }
    }
}

// ********* To create and start the dirigent proxy *************
// ******************************************************************
// ******************************************************************

async fn create_proxy_server2(
    nghttp2_codec_func_name : String,
    nghttp2_codec_bin_local_path : String,
    request_sender: mpsc::Sender<DispatcherCommand>,
    port: u16,
    dg_svc: Arc<DirigentService>,
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
        engine_type,
        request_sender.clone(),
    )
    .await
    .unwrap();

    // ***** spawn the router ******
    let (stream_worker_to_router_tx, stream_worker_to_func_router_rx) =
        mpsc::channel::<StreamWorkerToRouterReq>(256);
    let dg_svc_clone = Arc::clone(&dg_svc);

    tokio::spawn(router(
        nghttp2_codec_func_name.clone(),
        dg_svc_clone,
        request_sender.clone(),
        stream_worker_to_func_router_rx,
    ));

    // ****** The listening socket (for the data plane connections)******
    let addr: SocketAddr = SocketAddr::from(([0, 0, 0, 0], port)); // The dirigent proxy port
    let listener = TcpListener::bind(addr).await.unwrap();

    // signal handlers for gracefull shutdown
    let mut sigterm_stream = tokio::signal::unix::signal(SignalKind::terminate()).unwrap();
    let mut sigint_stream = tokio::signal::unix::signal(SignalKind::interrupt()).unwrap();
    let mut sigquit_stream = tokio::signal::unix::signal(SignalKind::quit()).unwrap();

    // ****** Start the dirigent proxy ******
    loop {
        tokio::select! {
            connection_pair = listener.accept() => {
                info!("A new DP connection!");
                let (stream,_) = connection_pair.unwrap();
                let loop_dispatcher = request_sender.clone();

                let stream_worker_to_router_tx_clone = stream_worker_to_router_tx.clone();

                // for each new dp connection spawn a dp connection worker
                let func_name = nghttp2_codec_func_name.clone();
                tokio::spawn(async move {
                    let service_dispatcher_ptr = loop_dispatcher.clone();
                    dp_connection_worker3(func_name, stream, service_dispatcher_ptr.clone(), stream_worker_to_router_tx_clone.clone()).await;
                });
            }
            _ = sigterm_stream.recv() => return,
            _ = sigint_stream.recv() => return,
            _ = sigquit_stream.recv() => return,
        }
    }
}

pub fn start_proxy_server2(
    nghttp2_codec_func_name : String,
    nghttp2_codec_bin_local_path : String,
    proxy_cores: Vec<u8>,
    request_sender: mpsc::Sender<DispatcherCommand>,
    port: u16,
    dg_svc: Arc<DirigentService>,
) {

    let runtime = 
        if cfg!(feature = "unpin_proxy") {
            Runtime::new().unwrap() // The default runtime. Use all the cores it could use
        }
        else {
            // make multithreaded dirigent proxy runtime
            // set up tokio runtime, need io in any case
            let mut runtime_builder = Builder::new_multi_thread();
            runtime_builder.enable_io();
            runtime_builder.worker_threads(proxy_cores.len());
            // Pin each Tokio worker thread to a specific core
            let cores = proxy_cores.clone(); // move into closure
            runtime_builder.on_thread_start(move || {
                // Each worker thread calls this once.
                // Need a way to pick which core this thread should use.

                // One simple approach: assign cores in a round-robin based on thread name/id
                // (Tokio doesn't expose a stable "worker index" here), so use a global counter:
                static NEXT_DIRIGENT_PROXY_CORE: AtomicUsize =
                    std::sync::atomic::AtomicUsize::new(0);

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


    thread::spawn(move || {
        runtime.block_on(async {
            create_proxy_server2(nghttp2_codec_func_name, nghttp2_codec_bin_local_path, request_sender, port, dg_svc).await;
        });
    });
}

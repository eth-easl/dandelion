use crate::dirigent_service::DirigentService;
use crate::request_parser;
use log::trace;
use std::io::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread;
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Runtime;
use tokio::{io, try_join};

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

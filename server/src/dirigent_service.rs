use bytes::Bytes;
use http_body_util::Full;
use hyper::server::conn::http1;
use hyper::service::Service;
use hyper::{Method, Request, Response};
use log::debug;
use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::thread;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;

#[derive(Debug, Clone)]
struct ExternalSandbox {
    sandbox_id: String,
    function: String,
    url: String,
}

#[derive(Debug, Clone)]
pub struct DirigentService {
    data: Arc<Mutex<HashMap<String, ExternalSandbox>>>,
}

pub fn new_dirigent_service() -> DirigentService {
    DirigentService {
        data: Arc::new(Mutex::new(HashMap::new())),
    }
}

impl Service<Request<hyper::body::Incoming>> for DirigentService {
    type Response = Response<Full<Bytes>>;
    type Error = hyper::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn call(&self, req: Request<hyper::body::Incoming>) -> Self::Future {
        fn mk_response(s: String) -> Result<Response<Full<Bytes>>, hyper::Error> {
            Ok(Response::builder().body(Full::new(Bytes::from(s))).unwrap())
        }

        let cpy = Arc::clone(&self.data);

        let res = match (req.method(), req.uri().path()) {
            (&Method::GET, "/add") => {
                let sandbox_id = req.headers().get("sandbox_id").unwrap().to_str().unwrap();
                let function = req.headers().get("function").unwrap().to_str().unwrap();
                let url = req.headers().get("url").unwrap().to_str().unwrap();

                if !process_add_action(
                    cpy,
                    sandbox_id.to_string(),
                    function.to_string(),
                    url.to_string(),
                ) {
                    mk_response("Successful ADD operation".parse().unwrap())
                } else {
                    mk_response("Successful UPDATE operation".parse().unwrap())
                }
            }
            (&Method::GET, "/remove") => {
                let sandbox_id = req
                    .headers()
                    .get("sandbox_id")
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_string();

                if process_remove_action(cpy, &*sandbox_id) {
                    mk_response("Successful REMOVE operation".parse().unwrap())
                } else {
                    mk_response("REMOVE operation failed - no key found".parse().unwrap())
                }
            }
            (&Method::GET, "/list_sandboxes") => {
                let res = process_list_action(cpy);

                mk_response(format!("{:?}", res))
            }
            _ => mk_response("Invalid request type/method".into()),
        };

        Box::pin(async { res })
    }
}

fn process_add_action(
    hm: Arc<Mutex<HashMap<String, ExternalSandbox>>>,
    sandbox_id: String,
    function: String,
    url: String,
) -> bool {
    let new_sandbox = ExternalSandbox {
        sandbox_id: sandbox_id.clone(),
        function: function.clone(),
        url: url.clone(),
    };

    let mut cpy = hm.lock().unwrap();
    let exists = cpy.contains_key(&sandbox_id.clone());

    cpy.insert(sandbox_id.clone(), new_sandbox);
    debug!(
        "Successful ADD/UPDATE action for sandbox_id: {}, function: {}, url: {}",
        sandbox_id, function, url
    );

    exists
}

fn process_remove_action(
    hm: Arc<Mutex<HashMap<String, ExternalSandbox>>>,
    sandbox_id: &str,
) -> bool {
    match hm.lock().unwrap().remove(sandbox_id) {
        Some(..) => {
            debug!("Successful REMOVE action for {}", sandbox_id);
            true
        }
        None => {
            debug!("Invalid REMOVE action received for {}", sandbox_id);
            false
        }
    }
}

fn process_list_action(hm: Arc<Mutex<HashMap<String, ExternalSandbox>>>) -> String {
    let mut builder = String::new();

    for (_key, val) in hm.lock().unwrap().iter().clone() {
        builder.push_str(format!(
            "{}, {}, {}\n",
            val.function, val.sandbox_id, val.url
        ).as_str());
    }

    debug!("Successful LIST action for");

    builder
}

async fn create_dirigent_server(port: u16) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr).await?;

    let dirigent_service = new_dirigent_service();

    println!("Listening on {}", addr);

    loop {
        let (stream, _) = listener.accept().await?;
        let io = hyper_util::rt::TokioIo::new(stream);
        let dg_svc_clone = dirigent_service.clone();

        tokio::task::spawn(async move {
            if let Err(err) = http1::Builder::new()
                .serve_connection(io, dg_svc_clone)
                .await
            {
                println!("Error serving connection: {:?}", err);
            }
        });
    }
}

pub fn start_dirigent_server(port: u16) {
    let runtime = Runtime::new().unwrap();

    thread::spawn(move || {
        runtime.block_on(async {
            create_dirigent_server(port).await.unwrap();
        });
    });
}

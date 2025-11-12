use bytes::Bytes;
use http_body_util::Full;
use hyper::server::conn::http1;
use hyper::service::Service;
use hyper::{Method, Request, Response};
use log::debug;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::thread;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;

const STATUS_OK: u16 = 200;
const STATUS_BAD_REQUEST: u16 = 400;

type DgSvcMap = HashMap<String, HashSet<ExternalSandbox>>;

#[derive(Clone, Debug)]
pub struct ExternalSandbox {
    pub sandbox_id: String,
    pub url: String,
}

impl PartialEq for ExternalSandbox {
    fn eq(&self, other: &Self) -> bool {
        self.sandbox_id == other.sandbox_id
    }
}

impl Eq for ExternalSandbox {}

impl Hash for ExternalSandbox {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.sandbox_id.hash(state);
    }
}

#[derive(Clone, Debug)]
pub struct DirigentService {
    data: Arc<Mutex<DgSvcMap>>,
}

pub fn new_dirigent_service() -> DirigentService {
    DirigentService {
        data: Arc::new(Mutex::new(HashMap::new())),
    }
}

impl DirigentService {
    pub fn choose_on_endpoint(&self, path: &String) -> Option<String> {
        let eps = self.data.lock().unwrap().get(path).cloned();
        if eps.is_some() {
            let list: Vec<ExternalSandbox> = eps.unwrap().into_iter().collect();
            let index: usize = rand::random_range(0..list.len());

            Some(list.get(index)?.url.clone())
        } else {
            None
        }
    }
}

impl Service<Request<hyper::body::Incoming>> for DirigentService {
    type Response = Response<Full<Bytes>>;
    type Error = hyper::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn call(&self, req: Request<hyper::body::Incoming>) -> Self::Future {
        fn mk_response(s: String, code: u16) -> Result<Response<Full<Bytes>>, hyper::Error> {
            Ok(Response::builder()
                .status(code)
                .body(Full::new(Bytes::from(s)))
                .unwrap())
        }

        let cpy = Arc::clone(&self.data);

        let res = match (req.method(), req.uri().path()) {
            (&Method::POST, "/add_external_sandbox") => {
                if req.headers().get("sandbox_id").is_none()
                    || req.headers().get("function").is_none()
                    || req.headers().get("url").is_none()
                {
                    mk_response("Invalid arguments".parse().unwrap(), 400)
                } else {
                    let function = req.headers().get("function").unwrap().to_str().unwrap();
                    let sandbox_id = req.headers().get("sandbox_id").unwrap().to_str().unwrap();
                    let url = req.headers().get("url").unwrap().to_str().unwrap();

                    if !process_add_action(
                        cpy,
                        function.to_string(),
                        sandbox_id.to_string(),
                        url.to_string(),
                    ) {
                        mk_response("Successful ADD operation".parse().unwrap(), STATUS_OK)
                    } else {
                        mk_response("Successful UPDATE operation".parse().unwrap(), STATUS_OK)
                    }
                }
            }
            (&Method::POST, "/remove_external_sandbox") => {
                if req.headers().get("sandbox_id").is_none()
                    || req.headers().get("function").is_none()
                {
                    mk_response("Invalid arguments".parse().unwrap(), 400)
                } else {
                    let function = req.headers().get("function").unwrap().to_str().unwrap();
                    let sandbox_id = req.headers().get("sandbox_id").unwrap().to_str().unwrap();

                    if process_remove_action(cpy, function.to_string(), sandbox_id.to_string()) {
                        mk_response("Successful REMOVE operation".parse().unwrap(), STATUS_OK)
                    } else {
                        mk_response(
                            "REMOVE operation failed - no key found".parse().unwrap(),
                            STATUS_OK,
                        )
                    }
                }
            }
            (&Method::POST, "/list_external_sandboxes") => {
                let res = process_list_action(cpy);

                mk_response(format!("{:?}", res), STATUS_OK)
            }
            _ => mk_response("Invalid request type/method".into(), STATUS_BAD_REQUEST),
        };

        Box::pin(async { res })
    }
}

fn process_add_action(
    hm: Arc<Mutex<DgSvcMap>>,
    function: String,
    sandbox_id: String,
    url: String,
) -> bool {
    let new_sandbox = ExternalSandbox {
        sandbox_id: sandbox_id.clone(),
        url: url.clone(),
    };

    let mut main_struct = hm.lock().unwrap();

    match main_struct.get(&function.clone()) {
        Some(v) => {
            let mut set = v.clone();

            set.replace(new_sandbox);
            main_struct.insert(function.clone(), set);

            debug!(
                "Successful UPDATE action for sandbox_id: {}, function: {}, url: {}",
                sandbox_id, function, url
            );

            true
        }
        None => {
            let mut set = HashSet::new();

            set.insert(new_sandbox);
            main_struct.insert(function.clone(), set);

            debug!(
                "Successful ADD action for sandbox_id: {}, function: {}, url: {}",
                sandbox_id, function, url
            );

            false
        }
    }
}

fn process_remove_action(hm: Arc<Mutex<DgSvcMap>>, function: String, sandbox_id: String) -> bool {
    let mut main_struct = hm.lock().unwrap();

    match main_struct.get_mut(&function) {
        Some(set) => {
            let to_remove = ExternalSandbox {
                sandbox_id: sandbox_id.clone(),
                url: "".to_string(),
            };

            match set.remove(&to_remove) {
                true => {
                    debug!("Successful REMOVE action for function: {}, sandbox ID: {}", &function, sandbox_id);
                    true
                }
                false => {
                    debug!("Invalid REMOVE action received for {}", sandbox_id);
                    false
                }
            }
        }
        None => false,
    }
}

fn process_list_action(hm: Arc<Mutex<DgSvcMap>>) -> (String, usize) {
    let mut builder = String::new();
    let mut cnt: usize = 0;

    for (function, val) in hm.lock().unwrap().iter().clone() {
        builder.push_str(format!("{}\n", function).as_str());

        for v in val.iter() {
            builder.push_str(format!("\t{} {}\n", v.sandbox_id, v.url).as_str());
            cnt += 1;
        }
    }

    debug!("Successful LIST action for");

    (builder, cnt)
}

async fn create_dirigent_server(
    port: u16,
    dirigent_service: Arc<DirigentService>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr).await?;

    tracing::info!("Listening on {}", addr);

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

pub fn start_dirigent_server(port: u16) -> Arc<DirigentService> {
    let runtime = Runtime::new().unwrap();
    let dg_svc = Arc::new(new_dirigent_service());
    let dg_svc_clone = Arc::clone(&dg_svc);

    thread::spawn(move || {
        runtime.block_on(async {
            create_dirigent_server(port, dg_svc_clone).await.unwrap();
        });
    });

    dg_svc
}

#[cfg(test)]
mod tests {
    use crate::dirigent_service::{
        DgSvcMap, process_add_action, process_list_action, process_remove_action,
    };
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    #[test]
    fn exploration() {
        let hm: Arc<Mutex<DgSvcMap>> = Arc::new(Mutex::new(HashMap::new()));

        assert!(!process_add_action(
            Arc::clone(&hm),
            "function-123".to_string(),
            "sandbox-1".to_string(),
            "localhost:3000".to_string()
        ));
        assert_eq!(process_list_action(Arc::clone(&hm)).1, 1);
        assert!(process_add_action(
            Arc::clone(&hm),
            "function-123".to_string(),
            "sandbox-2".to_string(),
            "localhost:3001".to_string()
        ));
        assert_eq!(process_list_action(Arc::clone(&hm)).1, 2);
        assert!(!process_add_action(
            Arc::clone(&hm),
            "function-234".to_string(),
            "sandbox-3".to_string(),
            "localhost:3002".to_string()
        ));
        assert_eq!(process_list_action(Arc::clone(&hm)).1, 3);
        assert!(process_remove_action(
            Arc::clone(&hm),
            "function-234".to_string(),
            "sandbox-3".to_string(),
        ));
        assert_eq!(process_list_action(Arc::clone(&hm)).1, 2);
        assert!(!process_remove_action(
            Arc::clone(&hm),
            "sandbox-3".to_string(),
            "function-234".to_string()
        ));
        assert_eq!(process_list_action(Arc::clone(&hm)).1, 2);
    }
}

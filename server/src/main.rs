use dandelion_commons::{ContextTypeId, EngineTypeId};
use dispatcher::{
    dispatcher::Dispatcher, function_registry::FunctionRegistry, resource_pool::ResourcePool,
};
use futures::{future::BoxFuture, lock::Mutex};
use hyper::{service::Service, Body, Request, Response, Server};
#[cfg(feature = "cheri")]
use machine_interface::{
    function_lib::{
        cheri::{CheriDriver, CheriLoader},
        Driver, DriverFunction, Loader, LoaderFunction,
    },
    memory_domain::{cheri::CheriMemoryDomain, MemoryDomain},
};
use std::{collections::HashMap, convert::Infallible, net::SocketAddr, sync::Arc};
// use std::time::Instant;

const MAT_SIZE: usize = 128;

async fn serve_cold(
    _req: Request<Body>,
    dispatcher: Arc<Dispatcher>,
) -> Result<Response<Body>, Infallible> {
    let inputs = Vec::new();
    let result = dispatcher.queue_function(0, inputs).await;
    let answer = match result {
        Ok(_) => "Dispatcher Ok\n".to_string(),
        Err(err) => format!("Dispatcher error: {:?}\n", err),
    };
    // let answer = "Done: Cold\n";
    Ok::<_, Infallible>(Response::new(answer.into()))
}

async fn serve_hot(
    _req: Request<Body>,
    dispatcher: Arc<Dispatcher>,
) -> Result<Response<Body>, Infallible> {
    let inputs = Vec::new();
    let result = dispatcher.queue_function(0, inputs).await;
    let answer = match result {
        Ok(_) => "Dispatcher Ok\n".to_string(),
        Err(err) => format!("Dispatcher error: {:?}\n", err),
    };
    // let answer = "Done: Hot \n";
    Ok::<_, Infallible>(Response::new(answer.into()))
}

async fn serve_native(_req: Request<Body>) -> Result<Response<Body>, Infallible> {
    let mut in_mat: [u64; MAT_SIZE * MAT_SIZE] = [0; MAT_SIZE * MAT_SIZE];
    let mut out_mat: [u64; MAT_SIZE * MAT_SIZE] = [0; MAT_SIZE * MAT_SIZE];
    for i in 0..in_mat.len() {
        in_mat[i] = i as u64 + 1;
    }
    for i in 0..MAT_SIZE {
        for j in 0..MAT_SIZE {
            for k in 0..MAT_SIZE {
                out_mat[i * MAT_SIZE + j] += in_mat[i * MAT_SIZE + k] * in_mat[j * MAT_SIZE + k];
            }
        }
    }
    Ok::<_, Infallible>(Response::new("Done: Natv\n".into()))
}

async fn service(
    req: Request<Body>,
    dispatcher: Arc<Dispatcher>,
) -> Result<Response<Body>, Infallible> {
    let uri = req.uri().path();
    match uri {
        "/cold" => serve_cold(req, dispatcher).await,
        "/hot" => serve_hot(req, dispatcher).await,
        "/native" => serve_native(req).await,
        _ => Ok::<_, Infallible>(Response::new(
            format!("Hello, World! You asked for: {}\n", uri).into(),
        )),
    }
}

struct DispatcherService {
    dispatcher: Arc<Dispatcher>,
}

impl Service<Request<Body>> for DispatcherService {
    type Response = Response<Body>;
    type Error = Infallible;
    // TODO check if we can get concrete type
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;
    // Pin<Box<dyn futures::future::Future<Output = Result<Self::Response, Self::Error>>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Ok(()).into()
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        Box::pin(service(req, self.dispatcher.clone()))
    }
}

struct ServiceMaker {
    dispatcher: Arc<Dispatcher>,
}

impl<T> Service<T> for ServiceMaker {
    type Response = DispatcherService;
    type Error = std::io::Error;
    type Future = futures::future::Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Ok(()).into()
    }

    fn call(&mut self, _: T) -> Self::Future {
        futures::future::ok(DispatcherService {
            dispatcher: self.dispatcher.clone(),
        })
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> () {
    println!("Server Hello");
    // set up dispatcher configuration basics
    let mut domains = HashMap::new();
    let context_id: ContextTypeId = 0;
    let engine_id: EngineTypeId = 0;
    let mut drivers = HashMap::new();
    let mut type_map = HashMap::new();
    type_map.insert(engine_id, context_id);
    let mut pool_map = HashMap::new();
    pool_map.insert(0, vec![1, 2, 3]);
    let resource_pool = ResourcePool {
        engine_pool: Mutex::new(pool_map),
    };
    let mut registry;
    // insert specific configuration
    #[cfg(feature = "cheri")]
    {
        domains.insert(
            context_id,
            CheriMemoryDomain::init(Vec::new()).expect("Should be able to initialize domain"),
        );
        let driver_func = CheriDriver::start_engine as DriverFunction;
        drivers.insert(engine_id, driver_func);
        let mut loader_map = HashMap::new();
        loader_map.insert(0, CheriLoader::parse_function as LoaderFunction);
        registry = FunctionRegistry::new(loader_map);
        let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("../machine_interface/tests/data/test_elf_aarch64c_basic");
        registry.add_local(0, engine_id, path.to_str().unwrap());
    }
    #[cfg(not(feature = "cheri"))]
    {
        let mut loader_map = HashMap::new();
        registry = FunctionRegistry::new(loader_map);
    }

    let dispatcher = Arc::new(
        Dispatcher::init(domains, drivers, type_map, registry, resource_pool)
            .expect("Should be able to start dispatcher"),
    );

    // ready http endpoint
    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    let server = Server::bind(&addr).serve(ServiceMaker { dispatcher });
    // Run this server for... forever!
    if let Err(e) = server.await {
        eprintln!("server error: {}", e);
    }
}

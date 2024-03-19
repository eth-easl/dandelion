use crate::{
    function_driver::{
        thread_utils::{ThreadCommand, ThreadController, ThreadPayload, ThreadState},
        ComputeResource, Driver, Engine, Function, FunctionConfig, SystemFunction,
    },
    memory_domain::{Context, ContextTrait},
    DataItem, DataSet, Position,
};
use core::{
    future::{ready, Future},
    task::Poll,
};
use core_affinity::set_for_current;
use dandelion_commons::{records::Recorder, DandelionError, DandelionResult};
use hyper::{body::HttpBody, Body, Client, HeaderMap, Method, Request, Version};
use log::error;
use std::{
    pin::Pin,
    sync::{Arc, Mutex},
};
use tokio::runtime::{Builder, Runtime};

struct RequestInformation {
    method: Method,
    uri: String,
    version: Version,
    headermap: Vec<(String, String)>,
    body: Body,
}
struct ResponseInformation {
    status: String,
    headermap: HeaderMap,
    body: Vec<u8>,
}

fn http_setup(context: &Context) -> DandelionResult<RequestInformation> {
    let request_set = match context.content.iter().find(|set_option| {
        if let Some(set) = set_option {
            return set.ident == "request";
        } else {
            return false;
        }
    }) {
        Some(Some(set)) => set,
        _ => {
            return Err(DandelionError::MalformedSystemFuncArg(String::from(
                "No request set",
            )))
        }
    };
    if request_set.buffers.len() != 1 {
        return Err(DandelionError::MalformedSystemFuncArg(String::from(
            "More than one request item in set",
        )));
    }
    let request_item = &request_set.buffers[0];
    let mut request_buffer = Vec::<u8>::with_capacity(request_item.data.size);
    request_buffer.resize(request_item.data.size, 0);
    context.read(request_item.data.offset, &mut request_buffer)?;
    let request_line = match String::from_utf8(request_buffer) {
        Ok(line) => line,
        Err(_) => {
            return Err(DandelionError::InvalidSystemFuncArg(String::from(
                "Request line not utf8",
            )));
        }
    };
    let mut request_iter = request_line.split_ascii_whitespace();

    let method_item = request_iter.next();
    let method = match method_item {
        Some(method_string) if method_string == "GET" => Method::GET,
        Some(method_string) if method_string == "PUT" => Method::PUT,
        Some(method_string) => {
            return Err(DandelionError::InvalidSystemFuncArg(String::from(
                method_string,
            )))
        }
        _ => {
            return Err(DandelionError::MalformedSystemFuncArg(String::from(
                "No method found",
            )))
        }
    };

    let uri = String::from(
        request_iter
            .next()
            .ok_or(DandelionError::MalformedSystemFuncArg(String::from(
                "No uri in request",
            )))?,
    );

    let version = match request_iter.next() {
        Some(version_string) if version_string == "HTTP/0.9" => Version::HTTP_09,
        Some(version_string) if version_string == "HTTP/1.0" => Version::HTTP_10,
        Some(version_string) if version_string == "HTTP/1.1" => Version::HTTP_11,
        Some(version_string) if version_string == "HTTP/2.0" => Version::HTTP_2,
        Some(version_string) if version_string == "HTTP/3.0" => Version::HTTP_3,
        Some(version_string) => {
            return Err(DandelionError::InvalidSystemFuncArg(String::from(
                version_string,
            )))
        }
        None => {
            return Err(DandelionError::MalformedSystemFuncArg(String::from(
                "No http version found",
            )))
        }
    };

    let headermap = if let Some(Some(header_set)) = context.content.iter().find(|&set_option| {
        set_option
            .as_ref()
            .and_then(|set| Some(set.ident == "headers"))
            .unwrap_or(false)
    }) {
        let mut headermap = Vec::<(String, String)>::with_capacity(header_set.buffers.len());
        for header_item in &header_set.buffers {
            let mut header_value = Vec::<u8>::with_capacity(header_item.data.size);
            header_value.resize(header_item.data.size, 0);
            context.read(header_item.data.offset, &mut header_value)?;
            let value_string =
                String::from_utf8(header_value).or(Err(DandelionError::MalformedSystemFuncArg(
                    String::from("header value not utf8 conformant"),
                )))?;
            headermap.push((header_item.ident.clone(), value_string));
        }
        headermap
    } else {
        Vec::new()
    };

    let body = if let Some(Some(body_set)) = context.content.iter().find(|&set_option| {
        set_option
            .as_ref()
            .and_then(|set| Some(set.ident == "body"))
            .unwrap_or(false)
    }) {
        if body_set.buffers.len() < 1 {
            Body::empty()
        } else {
            let body_item = &body_set.buffers[0];
            let mut body_buffer = Vec::<u8>::with_capacity(body_item.data.size);
            body_buffer.resize(body_item.data.size, 0);
            context.read(body_item.data.offset, &mut body_buffer)?;
            Body::from(body_buffer)
        }
    } else {
        Body::empty()
    };

    return Ok(RequestInformation {
        method,
        uri,
        version,
        headermap,
        body,
    });
}

async fn http_request(request_info: RequestInformation) -> DandelionResult<ResponseInformation> {
    let RequestInformation {
        method,
        uri,
        version,
        headermap,
        body,
    } = request_info;

    let mut request_builder = Request::builder()
        .method(method)
        .uri(uri.clone())
        .version(version);
    for (key, value) in headermap {
        request_builder = request_builder.header(key, value);
    }
    let request = match request_builder.body(body) {
        Ok(req) => req,
        Err(http_error) => {
            error!("URI: {}", uri);
            return Err(DandelionError::MalformedSystemFuncArg(format!(
                "{:?}",
                http_error
            )));
        }
    };

    // TODO instead of engine errors on connection issues return the error message(s) as items in a separate set
    let client = Client::new();
    let future = client.request(request).await;
    let (parts, mut body) = match future {
        Ok(body) => body.into_parts(),
        Err(err) => {
            println!("message: {}, err: {}", err.message(), err);
            panic!("Future Error")
        }
    };

    // write the status line
    let status = format!(
        "{:?} {} {}",
        parts.version,
        parts.status.as_str(),
        parts.status.canonical_reason().unwrap_or("")
    );

    // read the content length in the header
    // TODO also accept chunked data
    let content_length = parts
        .headers
        .get("content-length")
        .and_then(|value| value.to_str().ok())
        .and_then(|len_str| len_str.parse::<usize>().ok())
        .ok_or(DandelionError::SystemFuncResponseError)?;

    let mut response_data = Vec::new();
    while let Some(Ok(chunk)) = body.data().await {
        response_data.extend_from_slice(&chunk.slice(0..chunk.len()));
    }
    if content_length == response_data.len() {
        let response_info = ResponseInformation {
            status,
            headermap: parts.headers.to_owned(),
            body: response_data,
        };
        return Ok(response_info);
    } else {
        return Err(DandelionError::SystemFuncResponseError);
    }
}

fn http_context_write(
    context: &mut Context,
    output_set_names: Vec<String>,
    response: ResponseInformation,
) -> DandelionResult<()> {
    context.clear_metadata();
    let mut content = Vec::<Option<DataSet>>::new();
    if content.try_reserve(output_set_names.len()).is_err() {
        return Err(DandelionError::OutOfMemory);
    }
    let ResponseInformation {
        status,
        headermap,
        body,
    } = response;
    if output_set_names.iter().any(|elem| elem == "status") {
        let status_offset = context.get_free_space_and_write_slice(status.as_bytes())? as usize;
        content.push(Some(DataSet {
            ident: String::from("status"),
            buffers: vec![DataItem {
                ident: String::from("status"),
                data: Position {
                    offset: status_offset,
                    size: status.len(),
                },
                key: 0,
            }],
        }));
    }

    if output_set_names.iter().any(|elem| elem == "headers") {
        let mut header_dataset = DataSet {
            ident: String::from("headers"),
            buffers: vec![],
        };
        if header_dataset.buffers.try_reserve(headermap.len()).is_err() {
            return Err(DandelionError::OutOfMemory);
        }
        for (key_opt, header_value) in headermap.into_iter() {
            if let Some(key) = key_opt {
                let value = header_value.as_bytes();
                let value_offset = context.get_free_space_and_write_slice(value)? as usize;
                let item = DataItem {
                    ident: key.to_string(),
                    data: Position {
                        offset: value_offset,
                        size: value.len(),
                    },
                    key: 0,
                };
                header_dataset.buffers.push(item);
            }
        }
        content.push(Some(header_dataset));
    }

    if output_set_names.iter().any(|elem| elem == "body") {
        let body_length = body.len();
        let body_offset = context.get_free_space_and_write_slice(&body)? as usize;
        let body_set = DataSet {
            ident: String::from("body"),
            buffers: vec![DataItem {
                ident: String::from("body"),
                data: Position {
                    offset: body_offset,
                    size: body_length,
                },
                key: 0,
            }],
        };
        content.push(Some(body_set));
    }
    context.content = content;
    return Ok(());
}

fn http_run(
    context_ref: &Arc<Mutex<Option<Context>>>,
    runtime: &Runtime,
    output_set_names: Vec<String>,
) -> DandelionResult<()> {
    let mut context_guard = context_ref.lock().unwrap();
    let mut context = context_guard.take().unwrap();
    let response_result = {
        let _guard = runtime.enter();
        let request = http_setup(&context)?;
        runtime
            .block_on(http_request(request))
            .or(Err(DandelionError::EngineError))
    };
    let response = match response_result {
        Ok(resp) => resp,
        Err(err) => {
            let _ = context_guard.insert(context);
            return Err(err);
        }
    };
    let write_result = http_context_write(&mut context, output_set_names, response);
    let _ = context_guard.insert(context);
    return write_result;
}

struct HyperState {
    runtime: Runtime,
}
impl ThreadState for HyperState {
    fn init(core_id: u8) -> DandelionResult<Box<Self>> {
        let runtime = Builder::new_multi_thread()
            .on_thread_start(move || {
                if !set_for_current(core_affinity::CoreId { id: core_id.into() }) {
                    return;
                }
            })
            .worker_threads(1)
            .enable_all()
            .build()
            .or(Err(DandelionError::EngineError))?;

        return Ok(Box::new(HyperState { runtime }));
    }
}

struct HyperCommand {
    system_function: SystemFunction,
    context: Arc<Mutex<Option<Context>>>,
    output_set_names: Vec<String>,
}
impl ThreadPayload for HyperCommand {
    type State = HyperState;
    fn run(self, state: &mut Self::State) -> DandelionResult<()> {
        return match self.system_function {
            SystemFunction::HTTP => http_run(&self.context, &state.runtime, self.output_set_names),
            _ => Err(DandelionError::MalformedConfig),
        };
    }
}

pub struct HyperEngine {
    thread_controller: ThreadController<HyperCommand>,
}

// TODO check if there is a better way than this, for here and for wasm
// potentially coupling the sending back of the message and dropping the contex
struct HyperFuture<'a> {
    engine: &'a mut HyperEngine,
    context: Arc<Mutex<Option<Context>>>,
}

impl Future for HyperFuture<'_> {
    type Output = (DandelionResult<()>, Context);
    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut futures::task::Context,
    ) -> futures::task::Poll<Self::Output> {
        match self.engine.thread_controller.poll_next(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Some(Ok(()))) => {
                // TODO handle errors (we don't have a context if locking fails)
                let mut guard = self.context.lock().unwrap();
                if !guard.is_some() {
                    return Poll::Pending;
                };
                let context = guard.take().unwrap();
                Poll::Ready((Ok(()), context))
            }
            _ => {
                let context = self.context.lock().unwrap().take().unwrap();
                Poll::Ready((Err(DandelionError::EngineError), context))
            }
        }
    }
}

impl Engine for HyperEngine {
    fn run(
        &mut self,
        config: &FunctionConfig,
        context: Context,
        output_set_names: &Vec<String>,
        mut recorder: Recorder,
    ) -> Pin<Box<dyn Future<Output = (DandelionResult<()>, Context)> + '_ + Send>> {
        let function = match config {
            FunctionConfig::SysConfig(sys_func) => sys_func,
            _ => return Box::pin(ready((Err(DandelionError::ConfigMissmatch), context))),
        };
        if let Err(err) = recorder.record(dandelion_commons::records::RecordPoint::EngineStart) {
            return Box::pin(core::future::ready((Err(err), context)));
        }
        let context_ = Arc::new(Mutex::new(Some(context)));
        let command = ThreadCommand::Run(
            recorder,
            HyperCommand {
                system_function: *function,
                context: context_.clone(),
                output_set_names: output_set_names.clone(),
            },
        );
        match self.thread_controller.send_command(command) {
            Ok(()) => (),
            Err(err) => {
                return Box::pin(futures::future::ready((
                    Err(err),
                    context_.lock().unwrap().take().unwrap(),
                )))
            }
        }
        return Box::pin(HyperFuture {
            engine: self,
            context: context_,
        });
    }

    fn abort(&mut self) -> DandelionResult<()> {
        return Ok(());
    }
}

pub struct HyperDriver {}

impl Driver for HyperDriver {
    fn start_engine(&self, resource: ComputeResource) -> DandelionResult<Box<dyn Engine>> {
        let core_id = match resource {
            ComputeResource::CPU(core) => core,
            _ => return Err(DandelionError::EngineResourceError),
        };
        // check that core is available
        let available_cores = match core_affinity::get_core_ids() {
            None => return Err(DandelionError::EngineResourceError),
            Some(cores) => cores,
        };
        if !available_cores
            .iter()
            .find(|x| x.id == usize::from(core_id))
            .is_some()
        {
            return Err(DandelionError::EngineResourceError);
        }
        return Ok(Box::new(HyperEngine {
            thread_controller: ThreadController::new(core_id),
        }));
    }

    fn parse_function(
        &self,
        function_path: String,
        static_domain: &Box<dyn crate::memory_domain::MemoryDomain>,
    ) -> DandelionResult<Function> {
        if function_path.len() != 0 {
            return Err(DandelionError::CalledSystemFuncParser);
        }
        return Ok(Function {
            requirements: crate::DataRequirementList {
                input_requirements: vec![],
                static_requirements: vec![],
            },
            context: static_domain.acquire_context(0)?,
            config: FunctionConfig::SysConfig(SystemFunction::HTTP),
        });
    }
}

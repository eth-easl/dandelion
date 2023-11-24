use crate::{
    function_driver::{Driver, Engine, Function, FunctionConfig, SystemFunction},
    memory_domain::{Context, ContextTrait},
    DataItem, DataSet, Position,
};
use core::future::{ready, Future};
use core_affinity::set_for_current;
use dandelion_commons::{
    records::{RecordPoint, Recorder},
    DandelionError, DandelionResult,
};
use hyper::{body::HttpBody, Body, Client, HeaderMap, Method, Request, Version};
use std::pin::Pin;
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
            println!("URI: {}", uri);
            return Err(DandelionError::MalformedSystemFuncArg(format!(
                "{:?}",
                http_error
            )));
        }
    };
    let client = Client::new();
    let future = client.request(request).await;
    let mut response = future
        .or(Err(DandelionError::EngineError))
        .expect("response failed");

    // write the status line
    let status = format!(
        "{:?} {} {}",
        response.version(),
        response.status().as_str(),
        response.status().canonical_reason().unwrap_or("")
    );

    // read the content length in the header
    // TODO also accept chunked data
    let content_length = response
        .headers()
        .get("content-length")
        .and_then(|value| value.to_str().ok())
        .and_then(|len_str| len_str.parse::<usize>().ok())
        .ok_or(DandelionError::SystemFuncResponseError)?;

    if let Some(Ok(chunck)) = response.body_mut().data().await {
        // body_buffer.write_all(&chunck);
        if chunck.len() != content_length {
            return Err(DandelionError::SystemFuncResponseError);
        }
        let response_info = ResponseInformation {
            status,
            headermap: response.headers().to_owned(),
            body: chunck.into(),
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

async fn http_wrapper(
    mut context: Context,
    output_set_names: Vec<String>,
    runtime: &Runtime,
    mut recorder: Recorder,
) -> (DandelionResult<()>, Context) {
    let response_future;
    {
        let _guard = runtime.enter();
        let request = match http_setup(&context) {
            Ok(request) => request,
            Err(err) => return (Err(err), context),
        };

        if let Err(err) = recorder.record(RecordPoint::EngineStart) {
            return (Err(err), context);
        }
        response_future = runtime.spawn(http_request(request));
    }
    let response = match response_future.await {
        Ok(Ok(info)) => info,
        Ok(Err(err)) => return (Err(err), context),
        Err(_) => {
            println!("response future failed");
            return (Err(DandelionError::EngineError), context);
        }
    };
    if let Err(err) = recorder.record(RecordPoint::EngineEnd) {
        return (Err(err), context);
    }

    return (
        http_context_write(&mut context, output_set_names, response),
        context,
    );
}

pub struct HyperEngine {
    runtime: Runtime,
}

impl Engine for HyperEngine {
    fn run(
        &mut self,
        config: &FunctionConfig,
        context: Context,
        output_set_names: &Vec<String>,
        recorder: Recorder,
    ) -> Pin<Box<dyn Future<Output = (DandelionResult<()>, Context)> + '_ + Send>> {
        let function = match config {
            FunctionConfig::SysConfig(sys_func) => sys_func,
            _ => return Box::pin(ready((Err(DandelionError::ConfigMissmatch), context))),
        };
        return match function {
            SystemFunction::HTTP => Box::pin(http_wrapper(
                context,
                output_set_names.clone(),
                &self.runtime,
                recorder,
            )),
        };
    }

    fn abort(&mut self) -> DandelionResult<()> {
        return Ok(());
    }
}

pub struct HyperDriver {}

const DEFAULT_HTTP_CONTEXT_SIZE: usize = 0x1000; // 4KiB

impl Driver for HyperDriver {
    fn start_engine(&self, config: Vec<u8>) -> DandelionResult<Box<dyn Engine>> {
        if config.len() != 1 {
            return Err(DandelionError::ConfigMissmatch);
        }
        let core_id = config[0];
        // check that core is available
        let available_cores = match core_affinity::get_core_ids() {
            None => return Err(DandelionError::EngineError),
            Some(cores) => cores,
        };
        if !available_cores
            .iter()
            .find(|x| x.id == usize::from(core_id))
            .is_some()
        {
            return Err(DandelionError::MalformedConfig);
        }

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
        return Ok(Box::new(HyperEngine { runtime }));
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
                size: DEFAULT_HTTP_CONTEXT_SIZE,
                input_requirements: vec![],
                static_requirements: vec![],
            },
            context: static_domain.acquire_context(0)?,
            config: FunctionConfig::SysConfig(SystemFunction::HTTP),
        });
    }
}

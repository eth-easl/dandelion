use crate::{
    function_driver::{Driver, Engine, FunctionConfig, SystemFunction},
    memory_domain::{Context, ContextTrait},
    DataItem, DataSet, Position,
};
use core::future::{ready, Future};
use core_affinity::set_for_current;
use dandelion_commons::{
    records::{RecordPoint, Recorder},
    DandelionError, DandelionResult,
};
use hyper::{body::HttpBody, Client, HeaderMap, Method, Request, Version};
use std::pin::Pin;
use tokio::runtime::{Builder, Runtime};

struct RequestInformation {
    method: Method,
    uri: String,
    version: Version,
}
struct ResponseInformation {
    status: String,
    version: String,
    headermap: HeaderMap,
    body: Vec<u8>,
}

fn http_setup(context: &Context) -> DandelionResult<RequestInformation> {
    let request_line = match context.content.iter().find(|set_option| {
        if let Some(set) = set_option {
            return set.ident == "request";
        } else {
            return false;
        }
    }) {
        Some(Some(set)) => set,
        _ => return Err(DandelionError::MalformedSystemFuncArg),
    };

    let method_item = request_line
        .buffers
        .iter()
        .find(|&buffer| buffer.ident == "method")
        .ok_or(DandelionError::MalformedSystemFuncArg)?;

    let mut method_vec = vec![0u8; method_item.data.size];
    context.read(method_item.data.offset, &mut method_vec)?;
    let method = match String::from_utf8(method_vec) {
        Ok(method_string) if method_string == "GET" => Method::GET,
        Ok(method_string) if method_string == "PUT" => Method::PUT,
        Ok(method_string) => return Err(DandelionError::InvalidSystemFuncArg(method_string)),
        _ => return Err(DandelionError::MalformedSystemFuncArg),
    };

    let uri_item = request_line
        .buffers
        .iter()
        .find(|&buffer| buffer.ident == "uri")
        .ok_or(DandelionError::MalformedSystemFuncArg)?;
    let mut uri_vec = vec![0u8; uri_item.data.size];
    context.read(uri_item.data.offset, &mut uri_vec)?;
    let uri = String::from_utf8(uri_vec).or(Err(DandelionError::MalformedSystemFuncArg))?;

    let version_item = request_line
        .buffers
        .iter()
        .find(|&buffer| buffer.ident == "version")
        .ok_or(DandelionError::MalformedSystemFuncArg)?;
    let mut version_vec = vec![0u8; version_item.data.size];
    context.read(version_item.data.offset, &mut version_vec)?;
    let version = match String::from_utf8(version_vec) {
        Ok(version_string) if version_string == "HTTP/0.9" => Version::HTTP_09,
        Ok(version_string) if version_string == "HTTP/1.0" => Version::HTTP_10,
        Ok(version_string) if version_string == "HTTP/1.1" => Version::HTTP_11,
        Ok(version_string) if version_string == "HTTP/2.0" => Version::HTTP_2,
        Ok(version_string) if version_string == "HTTP/3.0" => Version::HTTP_3,
        Ok(version_string) => return Err(DandelionError::InvalidSystemFuncArg(version_string)),
        Err(_) => return Err(DandelionError::MalformedSystemFuncArg),
    };

    return Ok(RequestInformation {
        method,
        uri,
        version,
    });
}

async fn http_request(request_info: RequestInformation) -> DandelionResult<ResponseInformation> {
    let RequestInformation {
        method,
        uri,
        version,
    } = request_info;

    let request = Request::builder()
        .method(method)
        .uri(uri)
        .version(version)
        .body("".into())
        .or(Err(DandelionError::EngineError))?;
    let client = Client::new();
    let future = client.request(request).await;
    let mut response = future.or(Err(DandelionError::EngineError))?;

    // write the status line
    let version = format!("{:?}", response.version());
    let status = response.status().as_str().to_string();

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
            version,
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
        version,
        headermap,
        body,
    } = response;
    if output_set_names.iter().any(|elem| elem == "status line") {
        let status_offset = context.get_free_space_and_write_slice(status.as_bytes())? as usize;
        let version_offset = context.get_free_space_and_write_slice(version.as_bytes())? as usize;
        content.push(Some(DataSet {
            ident: String::from("status line"),
            buffers: vec![
                DataItem {
                    ident: String::from("status"),
                    data: Position {
                        offset: status_offset,
                        size: status.len(),
                    },
                    key: 0,
                },
                DataItem {
                    ident: String::from("version"),
                    data: Position {
                        offset: version_offset,
                        size: version.len(),
                    },
                    key: 0,
                },
            ],
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
        let method = match http_setup(&context) {
            Ok(method) => method,
            Err(err) => return (Err(err), context),
        };

        if let Err(err) = recorder.record(RecordPoint::EngineStart) {
            return (Err(err), context);
        }
        response_future = runtime.spawn(http_request(method));
    }
    let response = match response_future.await {
        Ok(Ok(info)) => info,
        Ok(Err(err)) => return (Err(err), context),
        Err(_) => return (Err(DandelionError::EngineError), context),
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
            SystemFunction::HTTPS => Box::pin(http_wrapper(
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

impl Driver for HyperDriver {
    fn start_engine(&self, config: Vec<u8>) -> DandelionResult<Box<dyn Engine>> {
        if config.len() != 1 {
            return Err(DandelionError::ConfigMissmatch);
        }
        let core_id = config[0];

        let runtime = Builder::new_multi_thread()
            .on_thread_start(move || {
                set_for_current(core_affinity::CoreId { id: core_id.into() });
            })
            .worker_threads(1)
            .enable_all()
            .build()
            .or(Err(DandelionError::EngineError))?;
        return Ok(Box::new(HyperEngine { runtime }));
    }

    fn parse_function(
        &self,
        _function: Vec<u8>,
        _static_domain: &Box<dyn crate::memory_domain::MemoryDomain>,
    ) -> DandelionResult<crate::function_driver::Function> {
        return Err(DandelionError::CalledSystemFuncParser);
    }
}

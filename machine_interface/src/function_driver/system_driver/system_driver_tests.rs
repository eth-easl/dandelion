use crate::{
    function_driver::{Driver, Engine, FunctionConfig, SystemFunction},
    memory_domain::{Context, ContextTrait, MemoryDomain},
    DataItem, DataSet, Position,
};
use dandelion_commons::{
    records::{Archive, RecordPoint, Recorder},
    DandelionResult,
};

const CONTEXT_SIZE: usize = 2048 * 1024;

fn _write_request_line(
    context: &mut Context,
    method: Vec<u8>,
    uri: Vec<u8>,
    version: Vec<u8>,
) -> DandelionResult<()> {
    let method_length = method.len();
    let method_offset = context.get_free_space(method_length, 8)?;
    context.write(method_offset, method)?;

    let version_length = version.len();
    let version_offset = context.get_free_space(version.len(), 8)?;
    context.write(version_offset, version)?;

    let uri_length = uri.len();
    let uri_offset = context.get_free_space(uri_length, 8)?;
    context.write(uri_offset, uri)?;

    context.content.push(Some(DataSet {
        ident: String::from("request"),
        buffers: vec![
            DataItem {
                ident: String::from("method"),
                data: Position {
                    offset: method_offset,
                    size: method_length,
                },
            },
            DataItem {
                ident: String::from("version"),
                data: Position {
                    offset: version_offset,
                    size: version_length,
                },
            },
            DataItem {
                ident: String::from("uri"),
                data: Position {
                    offset: uri_offset,
                    size: uri_length,
                },
            },
        ],
    }));
    return Ok(());
}

fn _write_headers(context: &mut Context, headers: Vec<(&str, &str)>) -> DandelionResult<()> {
    let mut header_set = DataSet {
        ident: String::from("headers"),
        buffers: vec![],
    };
    for (key, value) in headers {
        let value_vec = value.as_bytes().to_vec();
        let value_length = value_vec.len();
        let value_offset = context.get_free_space(value_length, 8)?;
        context.write(value_offset, value_vec)?;
        header_set.buffers.push(DataItem {
            ident: key.to_string(),
            data: Position {
                offset: value_offset,
                size: value_length,
            },
        });
    }
    context.content.push(Some(header_set));
    return Ok(());
}

fn _get_http<Dom: MemoryDomain, Drv: Driver>(dom_init: Vec<u8>, drv_init: Vec<u8>) -> () {
    let domain = Dom::init(dom_init).expect("Should be able to get domain");
    let mut context = domain
        .acquire_context(CONTEXT_SIZE)
        .expect("Should be able to get context");
    let mut engine = Drv::start_engine(drv_init).expect("Should be able to get engine");
    let config = FunctionConfig::SysConfig(SystemFunction::HTTPS);

    let method = "GET".as_bytes().to_vec();
    let uri = "http://httpbin.org/get".as_bytes().to_vec();
    let version = "HTTP/1.1".as_bytes().to_vec();

    _write_request_line(&mut context, method, uri, version)
        .expect("Should be able to prepare request line");

    let archive = std::sync::Arc::new(std::sync::Mutex::new(Archive::new()));
    let mut recorder = Recorder::new(archive, RecordPoint::TransferEnd);
    let output_set_names = vec![
        String::from("headers"),
        String::from("status line"),
        String::from("body"),
    ];
    let (result, result_context) = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(engine.run(&config, context, &output_set_names, recorder.clone()));
    assert_eq!(Ok(()), result);
    recorder
        .record(RecordPoint::FutureReturn)
        .expect("Should have advanced record");

    let response_line = result_context
        .content
        .iter()
        .find(|set_opt| {
            if let Some(set) = set_opt {
                return set.ident == "status line";
            } else {
                return false;
            }
        })
        .expect("Should have status line set")
        .as_ref()
        .expect("Should have status line set");
    let status_item = response_line
        .buffers
        .iter()
        .find(|item| item.ident == "status")
        .expect("Should have status");
    let status = String::from_utf8(
        result_context
            .read(status_item.data.offset, status_item.data.size)
            .expect("Should be able to read status"),
    )
    .expect("Should have status string");
    assert_eq!("200", status);
    let version_item = response_line
        .buffers
        .iter()
        .find(|item| item.ident == "version")
        .expect("Should have version");
    let version = String::from_utf8(
        result_context
            .read(version_item.data.offset, version_item.data.size)
            .expect("Should be able to read version"),
    )
    .expect("Should have version string");
    assert_eq!("HTTP/1.1", version);
}

fn _put_http<Dom: MemoryDomain, Drv: Driver>(dom_init: Vec<u8>, drv_init: Vec<u8>) -> () {
    let domain = Dom::init(dom_init).expect("Should be able to get domain");
    let mut context = domain
        .acquire_context(CONTEXT_SIZE)
        .expect("Should be able to get context");
    let mut engine = Drv::start_engine(drv_init).expect("Should be able to get engine");
    let config = FunctionConfig::SysConfig(SystemFunction::HTTPS);

    let method = "PUT".as_bytes().to_vec();
    let uri = "http://httpbin.org/put".as_bytes().to_vec();
    let version = "HTTP/1.1".as_bytes().to_vec();

    _write_request_line(&mut context, method, uri, version)
        .expect("Should be able to prepare request line");

    let headers = vec![("Content-Type", "text/plain")];
    _write_headers(&mut context, headers).expect("Should be able to write headers");

    let request_body = "Hello World\n".as_bytes().to_vec();
    let body_size = request_body.len();
    let body_offset = context
        .get_free_space(body_size, 8)
        .expect("Should have space for body");
    context
        .write(body_offset, request_body)
        .expect("Should be able to write body");

    let archive = std::sync::Arc::new(std::sync::Mutex::new(Archive::new()));
    let mut recorder = Recorder::new(archive, RecordPoint::TransferEnd);
    let output_set_names = vec![
        String::from("headers"),
        String::from("status line"),
        String::from("body"),
    ];
    let (result, result_context) = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(engine.run(&config, context, &output_set_names, recorder.clone()));
    assert_eq!(Ok(()), result);
    recorder
        .record(RecordPoint::FutureReturn)
        .expect("Should have advanced record");

    let response_line = result_context
        .content
        .iter()
        .find(|set_opt| {
            if let Some(set) = set_opt {
                return set.ident == "status line";
            } else {
                return false;
            }
        })
        .expect("Should have status line set")
        .as_ref()
        .expect("Should have status line set");
    let status_item = response_line
        .buffers
        .iter()
        .find(|item| item.ident == "status")
        .expect("Should have status");
    let status = String::from_utf8(
        result_context
            .read(status_item.data.offset, status_item.data.size)
            .expect("Should be able to read status"),
    )
    .expect("Should have status string");
    assert_eq!("200", status);
    let version_item = response_line
        .buffers
        .iter()
        .find(|item| item.ident == "version")
        .expect("Should have version");
    let version = String::from_utf8(
        result_context
            .read(version_item.data.offset, version_item.data.size)
            .expect("Should be able to read version"),
    )
    .expect("Should have version string");
    assert_eq!("HTTP/1.1", version);
}

// TODO change to start local http server to check against.
macro_rules! driverTests {
    ($name : ident; $domain: ty; $dom_init: expr; $driver : ty ; $drv_init : expr ) => {
        use super::*;

        #[test]
        fn test_http_get() {
            _get_http::<$domain, $driver>($dom_init, $drv_init);
        }

        #[test]
        fn test_http_put() {
            _put_http::<$domain, $driver>($dom_init, $drv_init);
        }
    };
}

#[cfg(feature = "hyper_io")]
mod hyper_io {
    use crate::function_driver::system_driver::hyper::HyperDriver as driver;
    use crate::memory_domain::malloc::MallocMemoryDomain as domain;
    driverTests!(hyper_io; domain; Vec::new(); driver; vec![1]);
}

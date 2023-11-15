#[cfg(all(test, any(feature = "hyper_io")))]
mod system_driver_tests {
    use crate::{
        function_driver::{
            system_driver::get_system_function_output_sets, Driver, FunctionConfig, SystemFunction,
        },
        memory_domain::{Context, ContextTrait, MemoryDomain},
        DataItem, DataSet, Position,
    };
    use dandelion_commons::{
        records::{Archive, RecordPoint, Recorder},
        DandelionResult,
    };

    const _CONTEXT_SIZE: usize = 2048 * 1024;

    fn write_request_line(context: &mut Context, request: Vec<u8>) -> DandelionResult<()> {
        let request_length = request.len();
        let request_offset = context.get_free_space_and_write_slice(&request)? as usize;

        context.content.push(Some(DataSet {
            ident: String::from("request"),
            buffers: vec![DataItem {
                ident: String::from("request"),
                data: Position {
                    offset: request_offset,
                    size: request_length,
                },
                key: 0,
            }],
        }));
        return Ok(());
    }

    fn write_headers(context: &mut Context, headers: Vec<(&str, &str)>) -> DandelionResult<()> {
        let mut header_set = DataSet {
            ident: String::from("headers"),
            buffers: vec![],
        };
        for (key, value) in headers {
            let value_length = value.len();
            let value_offset = context.get_free_space_and_write_slice(value.as_bytes())? as usize;
            header_set.buffers.push(DataItem {
                ident: key.to_string(),
                data: Position {
                    offset: value_offset,
                    size: value_length,
                },
                key: 0,
            });
        }
        context.content.push(Some(header_set));
        return Ok(());
    }

    fn get_http<Dom: MemoryDomain>(
        dom_init: Vec<u8>,
        driver: Box<dyn Driver>,
        drv_init: Vec<u8>,
    ) -> () {
        let domain = Dom::init(dom_init).expect("Should be able to get domain");
        let mut context = domain
            .acquire_context(_CONTEXT_SIZE)
            .expect("Should be able to get context");
        let mut engine = driver
            .start_engine(drv_init)
            .expect("Should be able to get engine");
        let config = FunctionConfig::SysConfig(SystemFunction::HTTP);

        let request = "GET http://httpbin.org/get HTTP/1.1\n\r"
            .as_bytes()
            .to_vec();

        write_request_line(&mut context, request).expect("Should be able to prepare request line");

        let archive = std::sync::Arc::new(std::sync::Mutex::new(Archive::new()));
        let mut recorder = Recorder::new(archive, RecordPoint::TransferEnd);
        let output_set_names = get_system_function_output_sets(SystemFunction::HTTP);
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
                    return set.ident == "status";
                } else {
                    return false;
                }
            })
            .expect("Should have status set")
            .as_ref()
            .expect("Should have status set");
        assert_eq!(1, response_line.buffers.len());
        let status_item = &response_line.buffers[0];
        let mut status_buffer = Vec::<u8>::new();
        status_buffer.resize(status_item.data.size, 0);
        result_context
            .read(status_item.data.offset, &mut status_buffer)
            .expect("Should be able to read status");
        let status = String::from_utf8(status_buffer).expect("Should have status string");
        assert_eq!("HTTP/1.1 200 OK", status);
    }

    fn put_http<Dom: MemoryDomain>(
        dom_init: Vec<u8>,
        driver: Box<dyn Driver>,
        drv_init: Vec<u8>,
    ) -> () {
        let domain = Dom::init(dom_init).expect("Should be able to get domain");
        let mut context = domain
            .acquire_context(_CONTEXT_SIZE)
            .expect("Should be able to get context");
        let mut engine = driver
            .start_engine(drv_init)
            .expect("Should be able to get engine");
        let config = FunctionConfig::SysConfig(SystemFunction::HTTP);

        let request = "PUT http://httpbin.org/put HTTP/1.1".as_bytes().to_vec();

        write_request_line(&mut context, request).expect("Should be able to prepare request line");

        let headers = vec![("Content-Type", "text/plain")];
        write_headers(&mut context, headers).expect("Should be able to write headers");

        let request_body = "Hello World\n".as_bytes();
        let body_size = request_body.len();
        let body_offset = context
            .get_free_space(body_size, 8)
            .expect("Should have space for body");
        context
            .write(body_offset, request_body)
            .expect("Should be able to write body");

        let archive = std::sync::Arc::new(std::sync::Mutex::new(Archive::new()));
        let mut recorder = Recorder::new(archive, RecordPoint::TransferEnd);
        let output_set_names = get_system_function_output_sets(SystemFunction::HTTP);

        let (result, result_context) = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(engine.run(&config, context, &output_set_names, recorder.clone()));
        assert_eq!(Ok(()), result);
        recorder
            .record(RecordPoint::FutureReturn)
            .expect("Should have advanced record");

        let status_set = result_context
            .content
            .iter()
            .find(|set_opt| {
                if let Some(set) = set_opt {
                    return set.ident == "status";
                } else {
                    return false;
                }
            })
            .expect("Should have status set")
            .as_ref()
            .expect("Should have status set");
        assert_eq!(1, status_set.buffers.len());
        let status_item = &status_set.buffers[0];
        let mut status_buffer = Vec::<u8>::new();
        status_buffer.resize(status_item.data.size, 0);
        result_context
            .read(status_item.data.offset, &mut status_buffer)
            .expect("Should be able to read status");
        let status = String::from_utf8(status_buffer).expect("Should have status string");
        assert_eq!("HTTP/1.1 200 OK", status);
    }

    // TODO change to start local http server to check against.
    macro_rules! driverTests {
        ($name : ident; $domain: ty; $dom_init: expr; $driver : expr ; $drv_init : expr ) => {
            #[test]
            fn test_http_get() {
                let driver = Box::new($driver);
                super::get_http::<$domain>($dom_init, driver, $drv_init);
            }

            #[test]
            fn test_http_put() {
                let driver = Box::new($driver);
                super::put_http::<$domain>($dom_init, driver, $drv_init);
            }
        };
    }

    #[cfg(feature = "hyper_io")]
    mod hyper_io {
        use crate::function_driver::system_driver::hyper::HyperDriver;
        use crate::memory_domain::malloc::MallocMemoryDomain as domain;
        driverTests!(hyper_io; domain; Vec::new(); HyperDriver{}; vec![1]);
    }
}

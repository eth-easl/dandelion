#[cfg(all(test, any(feature = "reqwest_io")))]
mod system_driver_tests {
    use crate::{
        composition::CompositionSet,
        function_driver::{
            functions::FunctionAlternative,
            system_driver::{
                get_system_function_input_sets, get_system_function_output_sets, SystemFunction,
                SYS_FUNC_DEFAULT_CONTEXT_SIZE,
            },
            test_queue::TestQueue,
            ComputeResource, Metadata, WorkToDo,
        },
        machine_config::EngineType,
        memory_domain::{
            read_only::ReadOnlyContext, test_resource::get_resource, ContextTrait, MemoryDomain,
            MemoryResource,
        },
        DataItem, DataSet, Position,
    };
    use dandelion_commons::{records::Recorder, FunctionId};
    use std::{
        process::{Child, Command},
        sync::Arc,
        thread,
        time::{Duration, Instant},
    };

    const _CONTEXT_SIZE: usize = 2048 * 1024;

    #[inline]
    fn zero_id() -> FunctionId {
        Arc::new(0.to_string())
    }

    struct HttpServer {
        proc_child: Child,
    }

    impl HttpServer {
        fn start(port: u16) -> Self {
            let mut py_server_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            py_server_path.pop();
            py_server_path.push("machine_interface/tests/python/server.py");

            let proc_child = Command::new("python3")
                .arg(py_server_path)
                .arg(format!("{}", port))
                .stdout(std::process::Stdio::null())
                .spawn()
                .expect("Failed to start python script");

            // TODO: poll the server to figure out if we're started
            thread::sleep(Duration::from_secs(1));

            HttpServer { proc_child }
        }
    }

    impl Drop for HttpServer {
        fn drop(&mut self) {
            println!("Stopping the python server...");
            let _ = self.proc_child.kill();
            let _ = self.proc_child.wait();
        }
    }

    fn read_status(response_buffer: &Vec<u8>) -> String {
        // find first '\n'
        let status_end = response_buffer
            .iter()
            .position(|character| *character == b'\n')
            .unwrap_or(response_buffer.len());
        return std::str::from_utf8(&response_buffer[0..status_end])
            .expect("request has not valid string status line")
            .to_string();
    }

    fn get_http<Dom: MemoryDomain>(
        dom_init: MemoryResource,
        engine_type: EngineType,
        drv_init: ComputeResource,
        uri: String,
        expected_body_size: usize,
    ) -> () {
        let domain =
            Arc::new(Dom::init(get_resource(dom_init)).expect("Should be able to get domain"));
        let queue = TestQueue::new();
        let _engine = engine_type
            .start_engine(drv_init, queue.clone())
            .expect("Should be able to get engine");
        let function = Arc::new(
            engine_type
                .parse_function(String::from(""), &domain)
                .unwrap(),
        );

        let request = format!("GET {} HTTP/1.1", uri).as_bytes().to_vec();
        let request_length = request.len();
        let mut input_context = ReadOnlyContext::new(request.into_boxed_slice()).unwrap();
        input_context.content.push(Some(DataSet {
            ident: "request".to_string(),
            buffers: vec![DataItem {
                ident: "".to_string(),
                data: Position {
                    offset: 0,
                    size: request_length,
                },
                key: 0,
            }],
        }));
        let input_sets = vec![Some(CompositionSet::from((
            0,
            vec![Arc::new(input_context)],
        )))];

        let recorder = Recorder::new(zero_id(), Instant::now());
        let metadata = Arc::new(Metadata {
            input_sets: get_system_function_input_sets(SystemFunction::HTTP)
                .into_iter()
                .map(|name| (name, None))
                .collect(),
            output_sets: get_system_function_output_sets(SystemFunction::HTTP),
        });
        let function_alternatives = vec![Arc::new(FunctionAlternative::new_loaded(
            engine_type,
            SYS_FUNC_DEFAULT_CONTEXT_SIZE,
            String::new(),
            domain,
            function,
        ))];
        let promise = queue.enqueu(WorkToDo::FunctionArguments {
            function_id: Arc::new(String::new()),
            function_alternatives,
            input_sets,
            metadata,
            caching: true,
            recorder: recorder,
        });
        let result_context = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(promise)
            .expect("Engine should return without error")
            .get_context();
        let header_set = result_context.content[0].as_ref().unwrap();
        assert_eq!(1, header_set.buffers.len());
        let status_item = &header_set.buffers[0];
        let mut response_buffer = Vec::<u8>::new();
        response_buffer.resize(status_item.data.size, 0);
        result_context
            .read(status_item.data.offset, &mut response_buffer)
            .expect("Should be able to read status");
        let status = read_status(&response_buffer);
        assert_eq!("HTTP/1.1 200 OK", status);

        // check body
        let body_set = result_context.content[1].as_ref().unwrap();
        assert_eq!(1, body_set.buffers.len());
        // debug!("expected_body_len: {}", expected_body_len);
        assert_eq!(expected_body_size, body_set.buffers[0].data.size);
    }

    fn post_http<Dom: MemoryDomain>(
        dom_init: MemoryResource,
        engine_type: EngineType,
        drv_init: ComputeResource,
        port: u16,
    ) -> () {
        let queue = TestQueue::new();
        let domain =
            Arc::new(Dom::init(get_resource(dom_init)).expect("Should be able to get domain"));
        let _engine = engine_type
            .start_engine(drv_init, queue.clone())
            .expect("Should be able to get engine");
        let function = Arc::new(
            engine_type
                .parse_function(String::from(""), &domain)
                .unwrap(),
        );

        let request = format!(
            r#"POST http://127.0.0.1:{}/post HTTP/1.1
Content-Type: text/plain

Lorem ipsum dolor sit amet, consetetur sadipscing elitr,
sed diam nonumy eirmod tempor invidunt ut labore et dolore
magna aliquyam erat, sed diam voluptua. At vero eos et
accusam et justo duo dolores et ea rebum. Stet clita kasd
gubergren, no sea takimata sanctus est Lorem ipsum dolor
sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing
elitr, sed diam nonumy eirmod tempor invidunt ut labore et
dolore magna aliquyam erat, sed diam voluptua."#,
            port
        )
        .as_bytes()
        .to_vec();
        let request_length = request.len();
        let mut input_context = ReadOnlyContext::new(request.into_boxed_slice()).unwrap();
        input_context.content.push(Some(DataSet {
            ident: "request".to_string(),
            buffers: vec![DataItem {
                ident: "".to_string(),
                data: Position {
                    offset: 0,
                    size: request_length,
                },
                key: 0,
            }],
        }));
        let input_sets = vec![Some(CompositionSet::from((
            0,
            vec![Arc::new(input_context)],
        )))];

        let recorder = Recorder::new(zero_id(), Instant::now());
        let metadata = Arc::new(Metadata {
            input_sets: get_system_function_input_sets(SystemFunction::HTTP)
                .into_iter()
                .map(|name| (name, None))
                .collect(),
            output_sets: get_system_function_output_sets(SystemFunction::HTTP),
        });
        let function_alternatives = vec![Arc::new(FunctionAlternative::new_loaded(
            engine_type,
            SYS_FUNC_DEFAULT_CONTEXT_SIZE,
            String::new(),
            domain,
            function,
        ))];
        let promise = queue.enqueu(WorkToDo::FunctionArguments {
            function_id: Arc::new(String::new()),
            function_alternatives,
            input_sets,
            metadata,
            caching: true,
            recorder,
        });
        let result_context = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(promise)
            .expect("Engine should not fail")
            .get_context();

        let header_set = result_context.content[0].as_ref().unwrap();
        assert_eq!(1, header_set.buffers.len());
        let header_item = &header_set.buffers[0];
        let mut header_buffer = Vec::<u8>::new();
        header_buffer.resize(header_item.data.size, 0);
        result_context
            .read(header_item.data.offset, &mut header_buffer)
            .expect("Should be able to read status");
        let status = read_status(&header_buffer);
        assert_eq!("HTTP/1.1 200 OK", status);
    }

    macro_rules! driverTests {
        ($name : ident; $domain: ty; $dom_init: expr; $engine_type : expr ; $drv_init : expr ) => {
            #[test_log::test]
            fn test_http_get() {
                let port = 9000;
                let _server = super::HttpServer::start(port);
                super::get_http::<$domain>(
                    $dom_init,
                    $engine_type,
                    $drv_init,
                    format!("http://127.0.0.1:{}/get", port),
                    6,
                );
            }

            #[test_log::test]
            fn test_http_get_large() {
                let port = 9001;
                let _server = super::HttpServer::start(port);
                super::get_http::<$domain>(
                    $dom_init,
                    $engine_type,
                    $drv_init,
                    format!("http://127.0.0.1:{}/get_large", port),
                    8192,
                );
            }

            #[test_log::test]
            fn test_http_post() {
                let port = 9002;
                let _server = super::HttpServer::start(port);
                super::post_http::<$domain>($dom_init, $engine_type, $drv_init, port);
            }
        };
    }

    #[cfg(feature = "reqwest_io")]
    mod reqwest_io {
        use crate::function_driver::ComputeResource;
        use crate::machine_config::EngineType;
        // use crate::memory_domain::malloc::MallocMemoryDomain as domain;
        use crate::memory_domain::system_domain::SystemMemoryDomain as domain;
        // use crate::memory_domain::mmap::MmapMemoryDomain as domain;
        driverTests!(reqwest_io; domain; crate::memory_domain::MemoryResource::Anonymous{size: (2<<22)}; EngineType::Reqwest; ComputeResource::CPU(1));
    }
}

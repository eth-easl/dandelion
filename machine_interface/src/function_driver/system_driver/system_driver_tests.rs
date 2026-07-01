#[cfg(test)]
mod system_driver_tests {
    use crate::{
        composition::CompositionSet,
        function_driver::{
            system_driver::{
                convert_to_references,
                recovery_log::{
                    decode_io_completion_payload, encode_io_completion_payload,
                    IoCompletionItem, IoCompletionOutputSet,
                },
                SystemFunction,
            },
            test_queue::TestQueue,
            ComputeResource, WorkToDo,
        },
        machine_config::EngineType,
        memory_domain::{read_only::ReadOnlyContext, ContextTrait},
        DataItem, DataSet, Position,
    };
    use std::process::{Child, Command};

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
            std::thread::sleep(std::time::Duration::from_secs(1));

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

    fn get_http(
        engine_type: EngineType,
        drv_init: ComputeResource,
        uri: String,
        expected_body_size: usize,
    ) -> () {
        let queue = TestQueue::new();
        let _engine = engine_type
            .start_engine(drv_init, queue.clone())
            .expect("Should be able to get engine");

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

        let input_sets = convert_to_references(
            SystemFunction::HTTP,
            dandelion_commons::InvocationId::nil(),
            None,
            CompositionSet::from_context(input_context),
        )
        .unwrap();

        // let recorder = Recorder::new(zero_id(), Instant::now());

        let promise = queue.enqueu(WorkToDo::SetsToResolve { input_sets });
        let mut result_sets = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(promise)
            .expect("Engine should return without error")
            .get_composition();

        assert_eq!(2, result_sets.len());
        let mut header_set = result_sets[0].take().unwrap().into_local().into_iter();
        let (status_item, status_context) = header_set.next().unwrap();
        assert_eq!(0, header_set.count());
        let mut response_buffer = Vec::<u8>::new();
        response_buffer.resize(status_item.data.size, 0);
        status_context
            .read(status_item.data.offset, &mut response_buffer)
            .expect("Should be able to read status");
        let status = read_status(&response_buffer);
        assert_eq!("HTTP/1.1 200 OK", status);

        // check body
        let mut body_set = result_sets[1].take().unwrap().into_local().into_iter();
        let (body_item, _) = body_set.next().unwrap();
        assert_eq!(0, body_set.count());
        // debug!("expected_body_len: {}", expected_body_len);
        assert_eq!(expected_body_size, body_item.data.size);
    }

    fn post_http(engine_type: EngineType, drv_init: ComputeResource, port: u16) -> () {
        let queue = TestQueue::new();
        let _engine = engine_type
            .start_engine(drv_init, queue.clone())
            .expect("Should be able to get engine");

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

        let input_sets = convert_to_references(
            SystemFunction::HTTP,
            dandelion_commons::InvocationId::nil(),
            None,
            CompositionSet::from_context(input_context),
        )
        .unwrap();

        // let recorder = Recorder::new(zero_id(), Instant::now());

        let promise = queue.enqueu(WorkToDo::SetsToResolve { input_sets });
        let mut result_sets = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(promise)
            .expect("Engine should not fail")
            .get_composition();

        assert_eq!(2, result_sets.len());
        let mut header_set = result_sets[0].take().unwrap().into_local().into_iter();
        let (header_item, header_context) = header_set.next().unwrap();
        assert_eq!(0, header_set.count());
        let mut header_buffer = Vec::<u8>::new();
        header_buffer.resize(header_item.data.size, 0);
        header_context
            .read(header_item.data.offset, &mut header_buffer)
            .expect("Should be able to read status");
        let status = read_status(&header_buffer);
        assert_eq!("HTTP/1.1 200 OK", status);
    }

    #[test]
    fn io_completion_payload_roundtrip() {
        let outputs = vec![
            IoCompletionOutputSet {
                composition_output_set_id: Some(0),
                set_index: 0,
                set_name: "headers".to_string(),
                items: vec![IoCompletionItem {
                    identifier: "".to_string(),
                    key: 0,
                    data: b"HTTP/1.1 200 OK\n".to_vec(),
                }],
            },
            IoCompletionOutputSet {
                composition_output_set_id: Some(1),
                set_index: 1,
                set_name: "bodies".to_string(),
                items: vec![IoCompletionItem {
                    identifier: "body".to_string(),
                    key: 7,
                    data: vec![0, 1, 2, 255],
                }],
            },
        ];

        let payload = encode_io_completion_payload(&outputs).unwrap();
        let decoded = decode_io_completion_payload(&payload).unwrap();

        assert_eq!(2, decoded.len());
        assert_eq!("headers", decoded[0].set_name);
        assert_eq!(b"HTTP/1.1 200 OK\n".to_vec(), decoded[0].items[0].data);
        assert_eq!("bodies", decoded[1].set_name);
        assert_eq!("body", decoded[1].items[0].identifier);
        assert_eq!(7, decoded[1].items[0].key);
        assert_eq!(vec![0, 1, 2, 255], decoded[1].items[0].data);
    }

    macro_rules! driverTests {
        ($name : ident; $engine_type : expr ; $drv_init : expr ) => {
            #[test_log::test]
            fn test_http_get() {
                let port = 9000;
                let _server = super::HttpServer::start(port);
                super::get_http(
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
                super::get_http(
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
                super::post_http($engine_type, $drv_init, port);
            }
        };
    }

    mod reqwest_io {
        use crate::function_driver::ComputeResource;
        use crate::machine_config::EngineType;
        // use crate::memory_domain::malloc::MallocMemoryDomain as domain;
        // use crate::memory_domain::mmap::MmapMemoryDomain as domain;
        driverTests!(reqwest_io; EngineType::System; ComputeResource::CPU(1));
    }
}

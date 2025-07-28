#[cfg(all(test, any(feature = "reqwest_io")))]
#[allow(clippy::module_inception)]
mod system_driver_tests {
    use crate::{
        function_driver::{
            system_driver::get_system_function_output_sets, test_queue::TestQueue, ComputeResource,
            Driver, FunctionConfig, SystemFunction, WorkToDo,
        },
        memory_domain::{
            mmap::MmapMemoryDomain, test_resource::get_resource, transfer_memory, Context,
            ContextTrait, MemoryDomain, MemoryResource,
        },
        DataItem, DataSet, Position,
    };
    use dandelion_commons::{records::Recorder, DandelionResult};
    use std::{sync::Arc, time::Instant};

    const _CONTEXT_SIZE: usize = 2048 * 1024;

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

    fn get_body_size(response_buffer: &Vec<u8>) -> usize {
        // find two consecutive '\n' that implied headers are finished
        let first_endl = response_buffer
            .windows(2)
            .position(|window| window == b"\n\n")
            .unwrap_or(response_buffer.len());
        let body_start = first_endl + 2;
        return if body_start < response_buffer.len() {
            response_buffer.len() - body_start
        } else {
            0
        };
    }

    fn write_request(context: &mut Context, request: Vec<u8>) -> DandelionResult<()> {
        let mmap_domain = MmapMemoryDomain::init(MemoryResource::Anonymous { size: (1 << 22) })
            .expect("Failed to initialize MmapMemoryDomain: Domain Error");
        let mut mmap_context = mmap_domain
            .acquire_context(_CONTEXT_SIZE)
            .expect("Should be able to get context");

        let request_length = request.len();
        let request_offset_mmap = mmap_context.get_free_space_and_write_slice(&request)? as usize;

        let mut response_buffer_mmap = Vec::<u8>::new();
        response_buffer_mmap.resize(request_length, 0);
        mmap_context
            .read(request_offset_mmap, &mut response_buffer_mmap)
            .expect("Should be able to read");
        let status_mmap = read_status(&response_buffer_mmap);

        let source_ctxt = Arc::new(mmap_context);
        let request_offset_ok = context.get_free_space(request_length, 128);

        let request_offset = if let Ok(req) = request_offset_ok {
            req
        } else {
            panic!("offset in write_request is not ok");
        };

        transfer_memory(
            context,
            source_ctxt,
            request_offset,
            request_offset_mmap,
            request_length,
        )
        .expect("Should successfully transfer");

        let mut response_buffer = Vec::<u8>::new();
        response_buffer.resize(request_length, 0);
        context
            .read(request_offset, &mut response_buffer)
            .expect("Should be able to read context");
        let status = read_status(&response_buffer);
        assert_eq!(status_mmap, status);

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

    fn get_http<Dom: MemoryDomain>(
        dom_init: MemoryResource,
        driver: Box<dyn Driver>,
        drv_init: ComputeResource,
    ) -> () {
        let domain = Dom::init(get_resource(dom_init)).expect("Should be able to get domain");
        let queue = Box::new(TestQueue::new());
        let mut context = domain
            .acquire_context(_CONTEXT_SIZE)
            .expect("Should be able to get context");
        let _engine = driver
            .start_engine(drv_init, queue.clone())
            .expect("Should be able to get engine");
        let config = FunctionConfig::SysConfig(SystemFunction::HTTP);

        let request = "GET http://httpbin.org/get HTTP/1.1".as_bytes().to_vec();

        write_request(&mut context, request).expect("Should be able to prepare request line");

        let recorder = Recorder::new(0, Instant::now());
        let output_sets = Arc::new(get_system_function_output_sets(SystemFunction::HTTP));
        let promise = queue.enqueu(WorkToDo::FunctionArguments {
            config,
            context,
            output_sets,
            recorder: recorder,
        });
        let result_context = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(promise)
            .expect("Engine should return without error")
            .get_context();
        let response_set = result_context
            .content
            .iter()
            .find(|set_opt| {
                if let Some(set) = set_opt {
                    return set.ident == "response";
                } else {
                    return false;
                }
            })
            .expect("Should have response set")
            .as_ref()
            .expect("Should have response set");
        assert_eq!(1, response_set.buffers.len());
        let status_item = &response_set.buffers[0];
        let mut response_buffer = Vec::<u8>::new();
        response_buffer.resize(status_item.data.size, 0);
        result_context
            .read(status_item.data.offset, &mut response_buffer)
            .expect("Should be able to read status");
        let status = read_status(&response_buffer);
        assert_eq!("HTTP/1.1 200 OK", status);

        // check body
        let body_set = result_context
            .content
            .iter()
            .find(|set_opt| {
                if let Some(set) = set_opt {
                    return set.ident == "body";
                } else {
                    return false;
                }
            })
            .expect("Should have body set")
            .as_ref()
            .expect("Should have body set");
        assert_eq!(1, body_set.buffers.len());
        let expected_body_len = get_body_size(&response_buffer);
        // debug!("expected_body_len: {}", expected_body_len);
        assert_eq!(expected_body_len, body_set.buffers[0].data.size);
    }

    fn post_http<Dom: MemoryDomain>(
        dom_init: MemoryResource,
        driver: Box<dyn Driver>,
        drv_init: ComputeResource,
    ) -> () {
        let queue = Box::new(TestQueue::new());
        let domain = Dom::init(get_resource(dom_init)).expect("Should be able to get domain");
        let mut context = domain
            .acquire_context(_CONTEXT_SIZE)
            .expect("Should be able to get context");
        let _engine = driver
            .start_engine(drv_init, queue.clone())
            .expect("Should be able to get engine");
        let config = FunctionConfig::SysConfig(SystemFunction::HTTP);

        let request = r#"POST http://httpbin.org/post HTTP/1.1
Content-Type: text/plain

Lorem ipsum dolor sit amet, consetetur sadipscing elitr,
sed diam nonumy eirmod tempor invidunt ut labore et dolore
magna aliquyam erat, sed diam voluptua. At vero eos et
accusam et justo duo dolores et ea rebum. Stet clita kasd
gubergren, no sea takimata sanctus est Lorem ipsum dolor
sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing
elitr, sed diam nonumy eirmod tempor invidunt ut labore et
dolore magna aliquyam erat, sed diam voluptua."#
            .as_bytes()
            .to_vec();

        write_request(&mut context, request).unwrap();

        let recorder = Recorder::new(0, Instant::now());
        let output_sets = Arc::new(get_system_function_output_sets(SystemFunction::HTTP));
        let promise = queue.enqueu(WorkToDo::FunctionArguments {
            config,
            context,
            output_sets,
            recorder,
        });
        let result_context = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(promise)
            .expect("Engine should not fail")
            .get_context();

        let response = result_context
            .content
            .iter()
            .find(|set_opt| {
                if let Some(set) = set_opt {
                    return set.ident == "response";
                } else {
                    return false;
                }
            })
            .expect("Should have response set")
            .as_ref()
            .expect("Should have response set");
        assert_eq!(1, response.buffers.len());
        let response_item = &response.buffers[0];
        let mut response_buffer = Vec::<u8>::new();
        response_buffer.resize(response_item.data.size, 0);
        result_context
            .read(response_item.data.offset, &mut response_buffer)
            .expect("Should be able to read status");
        let status = read_status(&response_buffer);
        assert_eq!("HTTP/1.1 200 OK", status);
    }

    // TODO change to start local http server to check against.
    macro_rules! driverTests {
        ($name : ident; $domain: ty; $dom_init: expr; $driver : expr ; $drv_init : expr ) => {
            #[test_log::test]
            fn test_http_get() {
                let driver = Box::new($driver);
                super::get_http::<$domain>($dom_init, driver, $drv_init);
            }

            #[test_log::test]
            fn test_http_post() {
                let driver = Box::new($driver);
                super::post_http::<$domain>($dom_init, driver, $drv_init);
            }
        };
    }

    #[cfg(feature = "reqwest_io")]
    mod reqwest_io {
        use crate::function_driver::system_driver::reqwest::ReqwestDriver;
        use crate::function_driver::ComputeResource;
        // use crate::memory_domain::malloc::MallocMemoryDomain as domain;
        use crate::memory_domain::system_domain::SystemMemoryDomain as domain;
        // use crate::memory_domain::mmap::MmapMemoryDomain as domain;
        driverTests!(reqwest_io; domain; crate::memory_domain::MemoryResource::Anonymous{size: (2<<22)}; ReqwestDriver{}; ComputeResource::CPU(1));
    }
}

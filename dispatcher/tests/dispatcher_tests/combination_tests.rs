use super::{check_matrix, run_composition, setup_dispatcher};
use machine_interface::{
    function_driver::ComputeResource,
    machine_config::{DomainType, EngineType},
};
use memory::{
    context::{
        read_only::ReadOnlyContext, ContextDataItem, ContextDataSet, MemoryDomain, MemoryResource,
    },
    DataSet, Position,
};

struct HttpServer {
    proc_child: std::process::Child,
}

impl HttpServer {
    fn start(port: u16) -> Self {
        let mut py_server_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        py_server_path.pop();
        py_server_path.push("machine_interface/tests/python/server.py");

        let proc_child = std::process::Command::new("python3")
            .arg(py_server_path)
            .arg(format!("{}", port))
            .stdout(std::process::Stdio::piped())
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
pub fn fetch_compute<Domain: MemoryDomain>(
    memory_resource: (DomainType, MemoryResource),
    relative_path: &str,
    compute_engine_type: EngineType,
    engine_resource: Vec<ComputeResource>,
) {
    let (dispatcher, _) = setup_dispatcher::<Domain>(
        relative_path,
        vec![(String::from("A"), None)],
        vec![String::from("Out")],
        compute_engine_type,
        engine_resource,
        memory_resource,
    );

    let port = 9003;

    // start http server, use a port that is not used in machine interface or server tests
    let keep_alive = HttpServer::start(port);

    // matrix with the first number indicating the number of rows
    let data = format!("GET http://127.0.0.1:{}/matrix HTTP/1.1", port);
    let data_len = data.len();
    let mut in_context = ReadOnlyContext::new(data.into_bytes().into_boxed_slice())
        .expect("Should be able to create read only context");
    let data_size = data_len * core::mem::size_of::<u8>();
    in_context.content = vec![Some(ContextDataSet {
        items: vec![ContextDataItem {
            ident: String::from(""),
            data: Position {
                offset: 0,
                size: data_size,
            },
            key: 0,
        }],
        total_size: data_size,
    })];

    let composition = r#"
        function HTTP(requests) => (headers, bodies);
        function test_function(A) => (Out);
        composition FetchCompute(Requests) => (Result) {
            HTTP(requests = each Requests) => (Bodies = bodies);
            test_function(A = each Bodies) => (Result = Out);
        }
    "#;

    let inputs = DataSet::from_context(in_context);
    let result_sets = run_composition(&dispatcher, composition, inputs);
    assert_eq!(1, result_sets.len());

    // check the matmul output
    assert_eq!(1, result_sets[0].len());
    let mul_item = &result_sets[0][0];
    assert_eq!(0, mul_item.key);
    check_matrix(mul_item, 2, vec![5, 11, 11, 25]);

    drop(keep_alive);
}

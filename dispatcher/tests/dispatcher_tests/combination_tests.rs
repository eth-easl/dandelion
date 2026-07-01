use super::{check_matrix, setup_dispatcher, zero_id};
use dandelion_commons::records::Recorder;
use machine_interface::{
    composition::{
        Composition, CompositionSet, FunctionDependencies, InputSetDescriptor, ShardingMode,
    },
    function_driver::{functions::SystemFunction, ComputeResource},
    machine_config::{DomainType, EngineType},
    memory_domain::{read_only::ReadOnlyContext, MemoryDomain, MemoryResource},
    DataItem, DataSet, Position,
};
use std::sync::Arc;
use std::{collections::BTreeMap, time::Instant};

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
    let (dispatcher, function_id) = setup_dispatcher::<Domain>(
        relative_path,
        vec![(String::from(""), None)],
        vec![String::from("")],
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
    in_context.content = vec![Some(DataSet {
        ident: String::from("request"),
        buffers: vec![DataItem {
            ident: String::from(""),
            data: Position {
                offset: 0,
                size: data_len * core::mem::size_of::<u8>(),
            },
            key: 0,
        }],
    })];

    let composition = Composition {
        dependencies: vec![
            FunctionDependencies {
                function: Arc::new(SystemFunction::HTTP.to_string()),
                join_info: (vec![0], vec![]),
                input_set_ids: vec![Some(InputSetDescriptor {
                    composition_id: 0,
                    sharding: ShardingMode::Each,
                    optional: false,
                })],
                output_set_ids: vec![None, Some(1)],
            },
            FunctionDependencies {
                function: function_id,
                join_info: (vec![0], vec![]),
                input_set_ids: vec![Some(InputSetDescriptor {
                    composition_id: 1,
                    sharding: ShardingMode::Each,
                    optional: false,
                })],
                output_set_ids: vec![Some(2)],
            },
        ],
        output_map: BTreeMap::from([(2, 0)]),
    };

    let recorder = Recorder::new(dandelion_commons::InvocationId::nil(), zero_id(), Instant::now());

    let inputs = CompositionSet::from_context(in_context);
    let mut result_sets = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(dispatcher.queue_composition(composition, inputs, false, recorder, None))
        .unwrap();
    assert_eq!(1, result_sets.len());

    // check the matmul output
    let mul_set = result_sets[0].take().unwrap();
    assert_eq!(1, mul_set.len());
    let (mul_item, mul_data) = mul_set.into_iter().next().unwrap();
    assert_eq!(0, mul_item.key);
    check_matrix(&mul_data, &mul_item, 2, vec![5, 11, 11, 25]);

    drop(keep_alive);
}

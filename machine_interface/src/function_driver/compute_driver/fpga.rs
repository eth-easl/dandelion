use crate::{
    function_driver::{
        thread_utils::{start_thread, EngineLoop},
        ComputeResource, Driver, FpgaConfig, Function, FunctionConfig, WorkQueue,
    },
    interface::{read_output_structs, setup_input_structs},
    memory_domain::{Context, ContextTrait, ContextType, MemoryDomain},
    DataItem, DataSet, Position,
};
use core_affinity::set_for_current;
use dandelion_commons::{DandelionError, DandelionResult};
use libloading::{Library, Symbol};
use log;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::time::Duration;
use std::{os::unix::net::SocketAddr, str::FromStr, sync::Arc, sync::Mutex};
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
//use tokio::net;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::runtime::{Builder, Runtime};

#[derive(Debug, Clone, Copy)]
pub enum SendMessage {
    //TODO: Implement with Struct
    ErrorMessage,
    LoadMessage,
    InputMessage,
}

type FpgaFunctionId = u64;

#[derive(Debug, Clone, Copy)]
pub enum TileFunctionState {
    Computing,
    Done,
}

#[derive(Debug, Clone, Copy)]
pub enum TileState {
    Empty,
    Filled(TileFunctionState, FpgaFunctionId),
}
pub type TileStateCollection = Arc<Mutex<Vec<TileState>>>;

fn create_tile_state_collection(n: usize) -> TileStateCollection {
    let initial_state = vec![TileState::Empty; n]; // Adjust the size as needed
    Arc::new(Mutex::new(initial_state))
}

async fn dummy_send(conn: SocketAddrV4) {
    println!("Connecting to {}", conn);
    match TcpStream::connect(conn).await {
        Ok(mut stream) => {
            let message = "Hello";
            match stream.write_all(message.as_bytes()).await {
                Ok(_) => {
                    println!("Sent: {}", message);
                    let mut buffer = [0; 1024];
                    match stream.read(&mut buffer).await {
                        Ok(_) => {
                            let received_message = String::from_utf8_lossy(&buffer[..]);
                            println!("Received: {}", received_message);
                        }
                        Err(e) => eprintln!("Failed to read message: {}", e),
                    }
                }
                Err(e) => eprintln!("Failed to send message: {}", e),
            }
        }
        Err(e) => eprintln!("Failed to connect: {}", e),
    }
}

pub struct FpgaLoop {
    cpu_slot: u8, //maybe redundant if we have a runtime
    runtime: Runtime,
    //other stuff as well? Like some state keeping
    tiles: TileStateCollection,
}

impl EngineLoop for FpgaLoop {
    fn init(core_id: u8) -> DandelionResult<Box<Self>> {
        println!("Fpga engine init, core_id: {core_id}");
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
        return Ok(Box::new(FpgaLoop {
            cpu_slot: core_id,
            runtime, //where do the configs for stuff go?...
            tiles: create_tile_state_collection(4),
        }));
    }
    fn run(
        &mut self,
        config: FunctionConfig,
        context: Context,
        _output_sets: Arc<Vec<String>>, //_ so compiler doesn't complain for now TODO: vFIX
    ) -> DandelionResult<Context> {
        println!("Fpga engine entered run!");
        //It's actually not good to pass the connection ips in the function config..
        //This should be passed to init like a computing resource,
        //since it's not supposed to change between invokations..

        //for now it'll be a new connection every time, that's ok
        let function_conf = match config {
            //_ so compiler doesn't complain for now TODO: vFIX
            FunctionConfig::FpgaConfig(fpga_func) => fpga_func,
            _ => return Err(DandelionError::ConfigMissmatch),
        };
        //TODO: here should go the running of stuff
        //outsource to different functions
        //first, parse the context to find out function id, input
        self.runtime
            .block_on(dummy_send(function_conf.std_connection));

        println!("returned bs from run");
        return DandelionResult::Ok(context);
    }
}

pub struct FpgaDriver {}

impl Driver for FpgaDriver {
    fn start_engine(
        &self,
        resource: ComputeResource,
        queue: Box<dyn WorkQueue + Send>,
    ) -> DandelionResult<()> {
        println!("Starting FPGA engine");
        let cpu_slot: u8 = match resource {
            ComputeResource::CPU(core_id) => core_id,
            _ => return Err(DandelionError::EngineResourceError),
        };
        // check that core is available
        let available_cores = match core_affinity::get_core_ids() {
            None => return Err(DandelionError::EngineResourceError),
            Some(cores) => cores,
        };
        if !available_cores
            .iter()
            .any(|x| x.id == usize::from(cpu_slot))
        {
            return Err(DandelionError::EngineResourceError);
        }
        start_thread::<FpgaLoop>(cpu_slot, queue);
        return Ok(());
    }

    fn parse_function(
        &self,
        function_path: String,
        static_domain: &Box<dyn crate::memory_domain::MemoryDomain>,
    ) -> DandelionResult<Function> {
        let config = if function_path == "dummy" {
            let dummyconfig = FpgaConfig {
                std_connection: SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 3456),
                special_connection: SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 4567),
            };
            FunctionConfig::FpgaConfig(dummyconfig)
        } else {
            //TODO: implement actual config/function parsing
            println!("Warning, trying to load a real config, NYI!!!!");
            let dummyconfig = FpgaConfig {
                std_connection: SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 3456),
                special_connection: SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 4567),
            };
            FunctionConfig::FpgaConfig(dummyconfig)
        };
        return Ok(Function {
            requirements: crate::DataRequirementList {
                input_requirements: vec![],
                static_requirements: vec![],
            },
            context: static_domain.acquire_context(0)?,
            config,
        });
    }
}

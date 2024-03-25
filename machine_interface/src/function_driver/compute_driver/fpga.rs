use crate::{//u do need interface, dummy //look into hyper.rs// do async, spawn tasks "multithreaded"
    function_driver::{
        thread_utils::{start_thread, EngineLoop},
        ComputeResource, Driver, Function, FunctionConfig, WorkQueue,
    },
    
    memory_domain::{Context, ContextTrait, ContextType, MemoryDomain},
    DataItem, DataRequirement, DataRequirementList, DataSet, Position,
};
use dandelion_commons::{DandelionError, DandelionResult};
use log::{debug, warn};
use std::{io::Write, net::TcpStream};
struct FpgaLoop {
    fpga_slot: u8,
    tcp_address: String,
    tcp_stream: Option<TcpStream>,
}

impl EngineLoop for FpgaLoop {
    fn init(resource: ComputeResource) -> DandelionResult<Box<Self>> {
        let tcp_address = "127.0.0.1:3411";
        let tcp_stream = TcpStream::connect(tcp_address).ok();
        Ok(Box::new(Self {
            fpga_slot: resource.into(),
            tcp_address: tcp_address.into(),
            tcp_stream,
        }))
    }

    fn run(
        &mut self,
        config: FunctionConfig,
        context: Context,
        output_sets: std::sync::Arc<Vec<String>>,
    ) -> DandelionResult<Context> {
        if let Some(tcp_stream) = &mut self.tcp_stream {
            // Send a dummy packet
            let dummy_packet = "Dummy packet";
            let result = tcp_stream.write(dummy_packet.as_bytes());
            tcp_stream.flush();
        }
        Err(DandelionError::NotImplemented)
    }
}

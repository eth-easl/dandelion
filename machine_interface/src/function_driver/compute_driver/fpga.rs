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
use hyper::header;
use libc::uint32_t;
use libloading::{Library, Symbol};
use log;
use serde::Deserialize;
use std::{
    collections::VecDeque,
    fmt::format,
    net::{Ipv4Addr, SocketAddrV4},
    panic,
};

use std::{os::unix::net::SocketAddr, str::FromStr, sync::Arc, sync::Mutex};
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
//use tokio::net;
//use std::future;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::runtime::{Builder, Runtime};
use tokio::time::{timeout, Duration};

use byteorder::*;
use std::boxed::Box;

/*
dandelion side should keep track of the ram in each tile fsm

have end packet





get stuff working on hacc?
*/

const HEADER_BYTE: u8 = 0x80; //1000 0000
const DATA_BYTE: u8 = 0x40; //0100 0000
const STOP_BYTE: u8 = 0x08; //0000 1000
const ERR_BYTE: u8 = 0x07; //0000 0111
const RESPONSE_BYTE: u8 = 0x20; //0010 0000
const FOOTER_BYTE: u8 = 0x10; //0001 0000

const PACKET_SIZE: usize = 1024;
const RESPONSE_DATA_BUF_SIZE: usize = PACKET_SIZE - 2; //also response error buf size
const RESPONSE_FOOTER_BUF_SIZE: usize = PACKET_SIZE - 4;
const REQUEST_HEADER_BUF_SIZE: usize = PACKET_SIZE - 8;
const REQUEST_DATA_BUF_SIZE: usize = PACKET_SIZE - 2;

/*
flag byte (for now independent of sending or receiving)
isHeader    isData      isResponse      isResponseFooter

Stop        ErrC        ErrC                isErr
*/
#[derive(Debug, Clone, Copy)]
struct ResponseFooter {
    flag_byte: u8,  //1
    tile_id: u8,    //1
    data_size: u16, //2  //data of this LAST packet only.
    //should be enough since we only need to know how much of the data left is inside the received buffer.
    data: [u8; RESPONSE_FOOTER_BUF_SIZE], //WILL be used now
}
#[derive(Debug, Clone, Copy)]
struct ResponseData {
    flag_byte: u8, //1
    tile_id: u8,   //1
    data: [u8; RESPONSE_DATA_BUF_SIZE],
}
#[derive(Debug, Clone, Copy)]
struct ResponseError {
    //error has no use for data
    flag_byte: u8, //1
    tile_id: u8,   //1
                   //padding: [u8; RESPONSE_DATA_BUF_SIZE],
}

#[derive(Debug, Clone, Copy)]
enum ResponseMessage {
    Data(ResponseData),
    Footer(ResponseFooter),
    Error(ResponseError),
}
fn deserialize_response_message(buf: &[u8; PACKET_SIZE]) -> Result<ResponseMessage, String> {
    let flag_byte = buf[0];
    let tile_id = buf[1];
    let is_err = flag_byte & ERR_BYTE != 0;
    let is_response = flag_byte & RESPONSE_BYTE != 0;
    let is_response_footer = flag_byte & FOOTER_BYTE != 0;
    if is_err {
        Ok(ResponseMessage::Error(ResponseError { flag_byte, tile_id }))
    } else if is_response_footer {
        let mut data: [u8; PACKET_SIZE - 4] = [0; PACKET_SIZE - 4];
        let data_size = u16::from_be_bytes([buf[2], buf[3]]);
        data.copy_from_slice(&buf[4..PACKET_SIZE]);
        Ok(ResponseMessage::Footer(ResponseFooter {
            flag_byte,
            tile_id,
            data_size,
            data,
        }))
    } else if is_response {
        let mut data: [u8; PACKET_SIZE - 2] = [0; PACKET_SIZE - 2];
        data.copy_from_slice(&buf[2..PACKET_SIZE]);
        Ok(ResponseMessage::Data(ResponseData {
            flag_byte,
            tile_id,
            data,
        }))
    } else {
        println!("flag_byte: {:?}", flag_byte);
        Err("response made no sense!".to_string())
    }
}

#[derive(Debug, Clone, Copy)]
struct RequestHeader {
    flag_byte: u8,                       //1
    tile_id: u8,                         //1
    bitstream_id: u16,                   //2
    data_size: u32,                      //4
    data: [u8; REQUEST_HEADER_BUF_SIZE], //WILL BE USED NOW
}

#[derive(Debug, Clone, Copy)]
struct RequestData {
    flag_byte: u8, //1
    tile_id: u8,   //1
    data: [u8; REQUEST_DATA_BUF_SIZE],
}
#[derive(Debug, Clone, Copy)]
enum RequestMessage {
    Header(RequestHeader),
    Data(RequestData),
}
fn serialize_send_message(msg: RequestMessage) -> [u8; PACKET_SIZE] {
    let mut result: [u8; PACKET_SIZE] = [0; PACKET_SIZE];
    match msg {
        RequestMessage::Header(header) => {
            result[0] = header.flag_byte;
            result[1] = header.tile_id;
            result[2..4].copy_from_slice(&header.bitstream_id.to_be_bytes());
            result[4..8].copy_from_slice(&header.data_size.to_be_bytes());
            result[8..].copy_from_slice(&header.data)
        }
        RequestMessage::Data(data) => {
            result[0] = data.flag_byte;
            result[1] = data.tile_id;
            result[2..].copy_from_slice(&data.data);
        }
    }

    result
}

type InvocationId = u32;
type MessageCount = u32;

/*
a tile from now on should hold a queue of invocations which each hold their own info and states.
at any point in time only the frontmost invocation should have the state "running", and only one
invocation, the frontmost or second to front invocation should be "loading".
there should be a configurable maximum that defines how many functions should ever be in flight for a tile.


If our receivehandler function gets a packet for a tile, there should be a "running" invocation on that tile, that
we can assign our message to. else we should error out.
*/
#[derive(Debug, Clone, Copy)]
enum InvocationState {
    Pending(),
    Requesting(),
    Running(MessageCount),
    Finished(MessageCount),
    //potential state after finished to show that we have given the data away
}

#[derive(Debug, Clone)]
struct Invocation {
    id: InvocationId,
    state: InvocationState,
    send_messages: Vec<RequestMessage>, //for debugging purposes
    receive_messages: Vec<ResponseMessage>,
    data: Vec<u8>, //only the data parts of the messages, concatinated
}

#[derive(Debug, Clone)]
struct Tile {
    unsent_invocations: Arc<Mutex<VecDeque<Invocation>>>,
    sent_invocations: Arc<Mutex<VecDeque<Invocation>>>,
    //pop_front and push_back!

    // procedure:
    // pop form unsent, send the entire thing, then push it into sent invocaitons.
    // once all data packets are received for the current invocation -> pop from sent
}

//should probably return a result
async fn send_msg(stream: &mut TcpStream, message: RequestMessage) -> io::Result<()> {
    let send_buf = serialize_send_message(message);

    stream.write_all(&send_buf).await.unwrap();
    stream.flush().await.expect("DIDNT FLUSH");
    return Ok(());
}

async fn dummy_send(conn: SocketAddrV4) {
    println!("Connecting to {:?}", conn);
    match TcpStream::connect(conn).await {
        Ok(mut stream) => {
            //matrix multiply of [3,3,3,3]
            let dummy_mat: [i64; 5] = [2, 3, 3, 3, 3];

            let mut buf_representation: [u8; 40] = [0; 40];
            BigEndian::write_i64_into(&dummy_mat, &mut buf_representation);
            let mut databuf: [u8; PACKET_SIZE - 8] = [0; PACKET_SIZE - 8];
            databuf[0..40].copy_from_slice(&buf_representation);
            assert!(buf_representation.len() == 8 * 5);
            let mut header = RequestHeader {
                flag_byte: HEADER_BYTE,
                tile_id: 0,
                bitstream_id: 1,
                data_size: 8 * 5,
                data: databuf,
            };

            if let Err(e) = send_msg(&mut stream, RequestMessage::Header(header)).await {
                eprintln!("failed to send!");
            } else {
                println!("Sent: {:?}", header);

                let dur = Duration::from_secs(3);
                let mut buffer: [u8; PACKET_SIZE] = [0; PACKET_SIZE];
                match timeout(dur, stream.read(&mut buffer)).await {
                    Ok(_) => {
                        let received_message = deserialize_response_message(&buffer).unwrap();
                        println!("Responsed: {:?}", received_message);
                    }
                    Err(e) => eprintln!("Failed to read message: {}", e),
                }
            }
        }
        Err(e) => eprintln!("Failed to connect: {}", e),
    }
}
/*
for getting packets i want a function that takes tcpstream and then tries to get response packets from it.
it should call a helper function for each packet that deserializes it and then acts according to what the
output of that interaction was.
this will need access to a reference of our fpgaloop struct, so we have to pass that to the receiver function
as well. Since we have everything we want to write to under locks, this won't be a problem i hope.

so we read a packet into our buffer, handle it accordingly and act on it. we can exit early with an error,
or the loop continues until we received the ResponseFooter or until maybe some amount of memory was hit.
I will omit the memory check for now and worry about that later.

So once we get the footer we know that our current invocation is done. We need to update our
datastructure and then output an ok. the runner should now create the output space...
*/
//first the function that handles each response buffer
//one receiverloop is started for each invocation after the last datapacket has been sent, so we will
//care about the output in the receiverloop -> we need to pass back what exactly happened there, with
//the invocation state.
fn handle_response_buffer(
    fpgaloop: &FpgaLoop,
    tile_id: u8,
    buf: &[u8; 1024],
) -> Result<InvocationState, String> {
    if let Ok(response) = deserialize_response_message(buf) {
        let tile = &fpgaloop.tiles[usize::from(tile_id)];
        let mut lock = tile.sent_invocations.try_lock();
        if let Ok(ref mut invocations) = lock {
            //find correct invocation
            assert!(invocations.len() > 0);
            let first_invocation = &mut invocations[0];
            match first_invocation.state {
                InvocationState::Running(num) => match response {
                    ResponseMessage::Data(data) => {
                        assert!(data.tile_id == tile_id);
                        first_invocation.state = InvocationState::Running(num + 1);
                        first_invocation.receive_messages.push(response);
                        first_invocation.data.extend_from_slice(&data.data);
                        return Ok(InvocationState::Running(num + 1));
                    }
                    ResponseMessage::Footer(footer) => {
                        assert!(footer.tile_id == tile_id);
                        first_invocation.state = InvocationState::Finished(num + 1);
                        first_invocation.receive_messages.push(response);
                        assert!(footer.data_size > 0);
                        assert!(usize::from(footer.data_size) <= footer.data.len());
                        first_invocation
                            .data
                            .extend_from_slice(&footer.data[0..usize::from(footer.data_size)]);
                    }
                    ResponseMessage::Error(err) => {
                        return Err(format!("RESPONSE ERROR: {:?}", err));
                    }
                },
                //only first invocation can be running.
                _ => {
                    return Err(format!(
                        "ERROR: Received wrong state: {:?}",
                        first_invocation.state
                    )
                    .to_string());
                }
            }
        }
        Err("ERROR: failed to acquire lock".to_string())
    } else {
        Err("ERROR: Could not deserialize the response! oof".to_string())
    }
}

pub struct FpgaLoop {
    cpu_slot: u8, //maybe redundant if we have a runtime
    runtime: Runtime,
    std_connection: std::net::SocketAddrV4, //ip/port for normal usage
    special_connection: std::net::SocketAddrV4, //different port for special control stuff
    //TODO: add debt set/hashmap that keeps track of all debts
    //TODO: debug stuff for invocations?
    tiles: [Tile; 4], //hard coded size for now
    //other stuff as well? Like some state keeping
    invocation_counter: Mutex<u32>, //counter to have unique ids
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
        let tiles = std::array::from_fn(|_| Tile {
            unsent_invocations: Arc::new(Mutex::new(VecDeque::new())),
            sent_invocations: Arc::new(Mutex::new(VecDeque::new())),
        });
        return Ok(Box::new(FpgaLoop {
            cpu_slot: core_id,
            runtime, //where do the configs for stuff go?...
            std_connection: SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 3456),
            special_connection: SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 4567),
            tiles,
            invocation_counter: Mutex::new(0),
        }));
    }
    fn run(
        &mut self,
        config: FunctionConfig,
        context: Context,
        _output_sets: Arc<Vec<String>>, //_ so compiler doesn't complain for now TODO: vFIX
    ) -> DandelionResult<Context> {
        println!("Fpga engine entered run!");

        //setup_input_structs TODO:

        /*
        - unpack context
        (there should only be data in item 0 from set 0 at the start)
        - put that in a local buffer (vec of bytes)

        -send that given the function config (bitstream)

        - receive
        -context.clear_metadata();
        -set up metadata yourself for now (or just make new context with a readonlymemory thing, giv it the buffer)$/
        in the readonly context, set the metadata by setting the occupation to the entire thing, and the content to one set with one item */
        /*
        let function_conf = match config {
            //_ so compiler doesn't complain for now TODO: vFIX
            FunctionConfig::FpgaConfig(fpga_func) => fpga_func,
            _ => return Err(DandelionError::ConfigMissmatch),
        };
        */
        //TODO: here should go the running of stuff
        //outsource to different functions
        //first, parse the context to find out function id, input
        self.runtime.block_on(dummy_send(self.std_connection));

        println!("returned bs from run");
        //TODO: read_output_structs
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
            let dummyconfig = FpgaConfig { bitstream_id: 1 };
            FunctionConfig::FpgaConfig(dummyconfig)
        } else {
            //TODO: implement actual config/function parsing
            println!("Warning, trying to load a real config, NYI!!!!");
            let dummyconfig = FpgaConfig { bitstream_id: 1 };
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

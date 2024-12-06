use crate::{
    function_driver::{
        thread_utils::{start_thread, EngineLoop},
        ComputeResource, Driver, FpgaConfig, Function, FunctionConfig, WorkDone, WorkQueue,
        WorkToDo,
    },
    //interface::{read_output_structs, setup_input_structs},
    memory_domain::{Context, ContextState, ContextTrait, ContextType, MemoryDomain},
    promise::Debt,
    DataItem,
    DataSet,
    Position,
};
use core::cmp::min;
use core_affinity::set_for_current;
use dandelion_commons::{DandelionError, DandelionResult};
use futures::SinkExt;
use libc::IW_QUAL_QUAL_INVALID;
use libloading::{Library, Symbol};
use log;
use serde::{de::IntoDeserializer, Deserialize};
use std::{
    collections::VecDeque,
    fmt::format,
    net::{Ipv4Addr, SocketAddrV4},
    panic,
    time::Instant,
};

use std::{os::unix::net::SocketAddr, str::FromStr, sync::Arc, sync::Mutex};
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt},
    time::{sleep, Sleep},
};
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
        let mut data: [u8; RESPONSE_FOOTER_BUF_SIZE] = [0; RESPONSE_FOOTER_BUF_SIZE];
        let data_size = u16::from_be_bytes([buf[2], buf[3]]);
        data.copy_from_slice(&buf[4..PACKET_SIZE]);
        Ok(ResponseMessage::Footer(ResponseFooter {
            flag_byte,
            tile_id,
            data_size,
            data,
        }))
    } else if is_response {
        let mut data: [u8; RESPONSE_DATA_BUF_SIZE] = [0; RESPONSE_DATA_BUF_SIZE];
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
    bitstream_id: BitstreamId,           //2
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

fn get_messages_from_slice(
    input: &[u8],
    tile_id: u8,
    bitstream_id: BitstreamId,
) -> Result<Vec<RequestMessage>, String> {
    let mut result: Vec<RequestMessage> = Vec::new();

    let input_size = input.len();
    let mut data_written = 0;
    //first, let's write it into a header
    let mut headerbuf: [u8; REQUEST_HEADER_BUF_SIZE] = [0; REQUEST_HEADER_BUF_SIZE];
    let header_buflen = min(input_size, REQUEST_HEADER_BUF_SIZE);
    headerbuf.copy_from_slice(&input[data_written..data_written + header_buflen]);
    data_written += header_buflen;
    let header = RequestHeader {
        flag_byte: HEADER_BYTE,
        tile_id,
        bitstream_id,
        data_size: input_size as u32,
        data: headerbuf,
    };
    result.push(RequestMessage::Header(header)); //append to the "back"
    if (data_written == input_size) {
        //we are done after the header
        return Ok(result);
    }
    //else

    while data_written < input_size {
        let mut databuf: [u8; REQUEST_DATA_BUF_SIZE] = [0; REQUEST_DATA_BUF_SIZE]; //don't forget to reset it to 0s every time!
        let data_buflen = min(input_size - data_written, REQUEST_DATA_BUF_SIZE);
        databuf.copy_from_slice(&input[data_written..data_written + data_buflen]);
        data_written += data_buflen;
        let data = RequestData {
            flag_byte: DATA_BYTE,
            tile_id,
            data: databuf,
        };
        result.push(RequestMessage::Data(data));
    }
    if (data_written != input_size) {
        eprintln!("ERROR: failed creating request messages somehow!");
        return Err("ERROR: failed creating request messages somehow!".to_string());
    }

    return Ok(result);
}

type InvocationId = u32;
type MessageCount = u32;
type BitstreamId = u16;

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
    bitstream_id: BitstreamId,
    state: InvocationState,
    send_messages: Vec<RequestMessage>, //for debugging purposes
    send_data: Vec<u8>,
    receive_messages: Vec<ResponseMessage>,
    receive_data: Vec<u8>, //only the data parts of the messages, concatinated
}

enum HandleResponseBufferResult {
    State(InvocationState),
    Invocation(Box<Invocation>),
}

#[derive(Debug, Clone)]
struct Tile {
    unsent_invocations: Arc<Mutex<VecDeque<Box<Invocation>>>>,
    sent_invocations: Arc<Mutex<VecDeque<Box<Invocation>>>>,
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
/**
 * This function gets anboxed invocation struct with empty queues.
 * If successful, it schedules it onto a tile queue and returns a mut pointer to the invocation it created.
 * If unsuccessful, it cleans up after itself and returns an error.
 *
 *
 * The current mode of operation is:
 *  - no knowledge of predicted execution time
 *  - no reordering: a new invocation can only be added to the end of a queue
 *  - if the soft length limit is achieved, exclude tile queue unless all tiles are at that limit
 *  - FOR NOW: only do the soft limit case, error out if all tile queues are full. TODO: VICTOR add this at some point
 *  - if another tile has the same bitstream at the end of the queue, choose that
 *  - else choose the shortest queue, if multiple queues are the shortest queue, choose the first.
 */
async fn schedule(
    fpgadata: &FpgaData,
    config: &FpgaConfig,
    invocation: Box<Invocation>,
) -> Result<u8, String> {
    //get all tile unsent invocation queues quickly
    //get the info we need and dont release the locks so we stay accurate
    let bitstream_id = config.bitstream_id;
    let tiles = &fpgadata.tiles;
    let mut unsent_queues: Vec<std::sync::MutexGuard<VecDeque<Box<Invocation>>>> =
        Vec::with_capacity(tiles.len());
    let mut infovec: Vec<(usize, bool)> = Vec::with_capacity(tiles.len()); //tuples hold length and wether the bitstream_id in the last tile is equal

    for i in 0..tiles.len() {
        //lock everybody and store the queue in unsent_queues
        unsent_queues[i] = tiles[i]
            .unsent_invocations
            .lock()
            .expect("locking failed badly");
    }
    //this part has to run very quick
    let mut chosen_tile: usize = 0;
    let mut found = false;
    //build infovec
    for i in 0..unsent_queues.len() {
        let temp_queue = &mut unsent_queues[i];
        let temp_len = temp_queue.len();
        let temp_bitstream_id = temp_queue[temp_len - 1].bitstream_id;
        infovec[i] = (temp_len, temp_bitstream_id == bitstream_id);
    }
    //now decide, look only at the ones below threshold
    for i in 0..infovec.len() {
        if (infovec[i].0 < config.soft_max_tile_queue_length) && (infovec[i].1) {
            found = true;
            chosen_tile = i;
        }
    }
    if (!found) {
        //now pick first smallest queue
        let mut smallest = usize::MAX;
        let mut smallest_index = usize::MAX;
        for i in 0..infovec.len() {
            if (infovec[i].0 < config.soft_max_tile_queue_length && infovec[i].0 < smallest) {
                smallest = infovec[i].0;
                smallest_index = i;
            }
        }
        if (smallest == usize::MAX) {
            //we didnt find anything:
            return Err("couldn't find something to fit soft requirements, rest nyi".to_string());
        } else {
            found = true;
            chosen_tile = smallest_index;
        }
    }
    unsent_queues[chosen_tile].push_back(invocation);
    for queue in unsent_queues {
        drop(queue);
    }

    return Ok(chosen_tile as u8);
}

async fn dummy_send(
    fpgadata: &FpgaData,
    config: &FpgaConfig,
    invocation_id: InvocationId,
) -> Result<Box<Invocation>, String> {
    println!("Connecting to {:?}", fpgadata.std_connection);
    match TcpStream::connect(fpgadata.std_connection).await {
        Ok(mut stream) => {
            //matrix multiply of [3,3,3,3]
            let dummy_mat: [i64; 5] = [2, 3, 3, 3, 3];

            let mut buf_representation: [u8; 40] = [0; 40];
            LittleEndian::write_i64_into(&dummy_mat, &mut buf_representation);
            let mut databuf: [u8; REQUEST_HEADER_BUF_SIZE] = [0; REQUEST_HEADER_BUF_SIZE];
            databuf[0..40].copy_from_slice(&buf_representation);
            assert!(buf_representation.len() == 8 * 5);
            let header = RequestHeader {
                flag_byte: HEADER_BYTE,
                tile_id: 0,
                bitstream_id: config.bitstream_id,
                data_size: 8 * 5,
                data: databuf,
            };

            if let Err(_e) = send_msg(&mut stream, RequestMessage::Header(header)).await {
                return Err("failed to send!".to_string());
            } else {
                println!("Sent: {:?}", header);

                let dur = Duration::from_secs(3);
                let mut buffer: [u8; PACKET_SIZE] = [0; PACKET_SIZE];
                //we need to create and add our invocation:f
                let inv = Invocation {
                    id: invocation_id,
                    bitstream_id: config.bitstream_id,
                    state: InvocationState::Running(0),
                    send_messages: Vec::new(),
                    send_data: Vec::new(),
                    receive_messages: Vec::new(),
                    receive_data: Vec::new(),
                };
                //separate scope so we dont hold lock across await.. clippy is complaining a lot man
                {
                    let mut sent_queue = fpgadata.tiles[0].sent_invocations.lock().unwrap(); //TODO:clean up
                    sent_queue.push_back(Box::new(inv));
                }

                match timeout(dur, stream.read(&mut buffer)).await {
                    Ok(_) => {
                        //let received_message = deserialize_response_message(&buffer).unwrap();
                        match handle_response_buffer(fpgadata, 0, &buffer) {
                            Ok(HandleResponseBufferResult::Invocation(invocation)) => {
                                //this means we are done
                                println!("we are done, yay!");
                                println!("result: {:?}", invocation.receive_data);
                                return Ok(invocation);
                            }
                            Ok(HandleResponseBufferResult::State(_state)) => {
                                //TODO: have a problem if the number is too high
                                return Err("multireceiving NYI!".to_string());
                            }
                            Err(text) => {
                                return Err(text);
                            }
                        }

                        //println!("Responsed: {:?}", received_message);
                    }
                    Err(e) => Err(e.to_string()),
                }
            }
        }
        Err(e) => Err(format!("Failed to connect: {e:?}").to_string()),
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
//TODO: change so it already receives a boxed invocation to work with
fn handle_response_buffer(
    fpgadata: &FpgaData,
    tile_id: u8,
    buf: &[u8; 1024],
) -> Result<HandleResponseBufferResult, String> {
    if let Ok(response) = deserialize_response_message(buf) {
        let tile = &fpgadata.tiles[usize::from(tile_id)];
        let mut sent_lock = tile.sent_invocations.try_lock();
        if let Ok(ref mut invocations) = sent_lock {
            //find correct invocation

            assert!(invocations.len() > 0);
            let first_invocation = invocations.front_mut().unwrap(); //TODO: clean up
            match first_invocation.state {
                InvocationState::Running(num) => match response {
                    ResponseMessage::Data(data) => {
                        assert!(data.tile_id == tile_id);
                        first_invocation.state = InvocationState::Running(num + 1);
                        first_invocation.receive_messages.push(response);
                        first_invocation.receive_data.extend_from_slice(&data.data);
                        return Ok(HandleResponseBufferResult::State(first_invocation.state));
                    }
                    ResponseMessage::Footer(footer) => {
                        assert!(footer.tile_id == tile_id);
                        first_invocation.state = InvocationState::Finished(num + 1);
                        first_invocation.receive_messages.push(response);
                        assert!(footer.data_size > 0);
                        assert!(usize::from(footer.data_size) <= footer.data.len());
                        first_invocation
                            .receive_data
                            .extend_from_slice(&footer.data[0..usize::from(footer.data_size)]);
                        //we need to pop the invocation from the queue here.
                        let popped_invocation = invocations.pop_front().unwrap();
                        //TODO: clean up
                        return Ok(HandleResponseBufferResult::Invocation(popped_invocation));
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

/*
async fn worker(fpgaloop: Arc<FpgaLoop>, worker_id: usize) {
    let mut last_tile_time = Instant::now();
    let goodbye = Duration::from_secs(20);
    loop {
        println!("hello, slot: {0} worker: {worker_id}", fpgaloop.cpu_slot);
        sleep(Duration::from_secs(2)).await;

        {
            let queue = fpgaloop
                .unscheduled_invocations
                .lock()
                .expect("couldn't get lock for unscheduled invocations");
            if (queue.len() == 0) && (last_tile_time.elapsed() > goodbye) {
                break;
            } else if queue.len() > 0 {
                last_tile_time = Instant::now();
            }
        }
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
    unscheduled_invocations: Arc<Mutex<VecDeque<Box<Invocation>>>>,
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
            .worker_threads(4)
            .enable_all()
            .build()
            .or(Err(DandelionError::EngineError))?;
        let tiles = std::array::from_fn(|_| Tile {
            unsent_invocations: Arc::new(Mutex::new(VecDeque::new())),
            sent_invocations: Arc::new(Mutex::new(VecDeque::new())),
        });
        let fpgaloop = Arc::new(FpgaLoop {
            cpu_slot: core_id,
            runtime, //where do the configs for stuff go?...
            std_connection: SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 3456),
            special_connection: SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 4567),
            tiles,
            unscheduled_invocations: Arc::new(Mutex::new(VecDeque::new())),
            invocation_counter: Mutex::new(0),
        });

        //here i start the main worker threads, not on the tcp_runtime
        for i in 0..2 {
            fpgaloop.runtime.spawn(worker(fpgaloop.clone(), i));
        }

        return Ok(fpgaloop);
    }
    async fn run(
        &self,
        config: FunctionConfig,
        mut context: Context,
        _output_sets: Arc<Vec<String>>, //_ We ignore this because the output context should only contain one set and one item, located at offset 0.
    ) -> DandelionResult<Context> {
        //get id:
        let mut a = self
            .invocation_counter
            .lock()
            .expect("somehting really wrong must have happend with the invocation counter");
        *a += 1;

        let invocation_id: u32 = *a;
        drop(a); // Release the lock on the mutex for other threads

        println!(
            "Fpga engine entered run! invocation_id: {:?}",
            invocation_id
        );

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
            _ => return Err(DandelionError::ConfigMismatch),
        };
        */
        //TODO: here should go the running of stuff
        //outsource to different functions
        //first, parse the context to find out function id, input
        if let FunctionConfig::FpgaConfig(fpgaconfig) = config {
            match fpgaconfig.dummy_func_num {
                0 => {
                    eprintln!("real runner not implemented yet!");
                    return DandelionResult::Err(DandelionError::NotImplemented);
                }

                1 => {
                    println!("testing input through context, matrix locally");
                    match &context.content[0] {
                        None => {
                            eprintln!("ERROR: no content in context received.");
                            return DandelionResult::Err(DandelionError::ContextMismatch);
                        }
                        Some(set) => {
                            println!("found set: {:?}", set);

                            if (fpgaconfig.bitstream_id != 1) {
                                eprintln!(
                                    "this is local, bistream id should be 1! got : {:?}",
                                    fpgaconfig.bitstream_id
                                );
                                return DandelionResult::Err(DandelionError::ContextMismatch);
                            }

                            let input_item = &set.buffers[0];
                            let input_size = input_item.data.size / 8;
                            let mut input_vec: Vec<i64> = vec![0; input_size];

                            if let Err(e) = context.read(input_item.data.offset, &mut input_vec) {
                                eprintln!("couldn't read from context");
                                return DandelionResult::Err(e);
                            }
                            println!("something worked :D input: {:?}", input_vec);

                            //"wipe" context for output
                            context.clear_metadata();
                            let output_offset = context
                                .get_free_space_and_write_slice(&[2i64, 50i64, 50i64, 50i64, 50i64])
                                .expect("should be able to write to context");
                            println!("output offset should be 0, got: {:?}", output_offset);
                            context.content.push(Some(DataSet {
                                ident: "functionOutputSet".to_string(),
                                buffers: vec![DataItem {
                                    ident: "functionOutputItem".to_string(),
                                    data: Position {
                                        offset: output_offset as usize,
                                        size: 40,
                                    },
                                    key: 0,
                                }],
                            }));
                        }
                    }
                }
                2 => {
                    println!("testing input through context, matrix locally");
                    match &context.content[0] {
                        None => {
                            eprintln!("ERROR: no content in context received.");
                            return DandelionResult::Err(DandelionError::ContextMismatch);
                        }
                        Some(set) => {
                            println!("found set: {:?}", set);

                            let output_invocation = self
                                .runtime
                                .block_on(dummy_send(self, &fpgaconfig, invocation_id))
                                .expect("somthing went wrong.");

                            //"wipe" context for output
                            context.clear_metadata();
                            let output_offset = context
                                .get_free_space_and_write_slice(&output_invocation.receive_data)
                                .expect("should be able to write to context");
                            println!("output offset should be 0, got: {:?}", output_offset);
                            context.content.push(Some(DataSet {
                                ident: "functionOutputSet".to_string(),
                                buffers: vec![DataItem {
                                    ident: "functionOutputItem".to_string(),
                                    data: Position {
                                        offset: output_offset as usize,
                                        size: 40,
                                    },
                                    key: 0,
                                }],
                            }));
                        }
                    }
                }
                _ => {
                    eprintln!("other dummies not yet implemented");
                    return DandelionResult::Err(DandelionError::NotImplemented);
                }
            }
        } else {
            eprintln!("received wrong config!");
            return DandelionResult::Err(DandelionError::ConfigMismatch);
        }

        println!("returned from run");
        //TODO: read_output_structs
        return DandelionResult::Ok(context);
    }
}
*/

/*
planning:
I want to have the following mechanism:
there is a flag or condition that gets set when the loop continues adding a new invocation from the queue. This is a TODO for later.
Until then i will just loop through everything and schedule everything.

There, we do the following: we open the next invocation, and parse it. then we schedule it onto a tile.



notes/ideas for sending:
how many tcp sessions per fpga?
just one? could become quite the ordeal.
one per tile?
unlinked from the tiles? with some max

Why the question?
because if we want to load up multiple invocations per tile, so
it can do them in the future, then this becomes finnicky either in
software or in the hardware.
In theory, I think we worked with just one session.
And that is ok but in my software model, we asynchronously have multiple
threads working on here, and talking to the different tiles with different
invocations. having somehow just one open tcp connection
(that needs to be reopened if there is a longer wait time)
and then somehow multiplexing the data to the correct places seems like a bit of a
nightmare.

We also can't just open a new session for each invocation, since the ones in the
future might take very long to actually return to us and by then the session
might have been closed. This is unlikely, but is also a concern.

i propose the following model:
we have a listener thread, that has information about the invocations and parses
the messages, redirecting them to the correct invocations.
This doesn't have to be just one.

When any listener sees the last response in an invocation,
it fulfills the debt.
debt.fulfill(Box::new(Ok(WorkDone::Context(context))
like this.

right now we won't handle errors. but those will also be passed as fulfilled debts

So receiving would be taken care of.
We just need any small amount of receiver threads to be taken care of.

Ultimately, we don't want infinite debts to remain unfulfilled, but
I plan on doing something there.


So receiving is taken care of.
Sending is the problem. Since we now don't care
about anything anymore in terms of connections, I say
we just do a new tcp session every time i send an invocation
over. This makes it just easier to handle.
*/

async fn engine_loop(queue: Box<dyn WorkQueue + Send>, fpgadata: &FpgaData) -> Debt {
    //
    log::debug!("FPGA engine init.");
    println!("FPGA engine init.");

    loop {
        let (args, debt) = queue.get_engine_args();
        println!("got args!");
        match args {
            WorkToDo::FunctionArguments {
                config,
                mut context,
                output_sets,
                recorder: _,
            } => {
                //actual functionality

                /*
                //try to record
                if let Err(err) = recorder.record(RecordPoint::EngineStart) {
                    debt.fulfill(Box::new(Err(err)));
                    continue;
                }
                */
                //recording is another hard thing to do later

                //for now just accept every new function with args

                //get invocation id
                let mut invocation_counter = fpgadata
                    .invocation_counter
                    .lock()
                    .expect("sth wrong happened to the invocation counter");
                *invocation_counter += 1;
                let invocation_id: u32 = *invocation_counter;
                drop(invocation_counter);

                log::debug!("fpga loop entered run, invocation_id: {:?}", invocation_id);
                if let FunctionConfig::FpgaConfig(fpgaconfig) = config {
                    match fpgaconfig.dummy_func_num {
                        0 => {
                            eprintln!("ERROR: full func NYI");
                            debt.fulfill(Box::new(Err(DandelionError::NotImplemented)));
                            continue;
                        }
                        1 => {
                            eprintln!("ERROR: NYI");
                            debt.fulfill(Box::new(Err(DandelionError::NotImplemented)));
                            continue;
                        }
                        2 => {
                            //test mode only one we implement rn
                            log::debug!("testing input through context, local only");

                            match &context.content[0] {
                                None => {
                                    eprintln!("ERROR: no content in context received.");
                                    debt.fulfill(Box::new(Err(DandelionError::ContextMismatch)));
                                }
                                Some(set) => {
                                    log::debug!("found set: {:?}", set);

                                    let input_item = &set.buffers[0];
                                    let input_size = input_item.data.size / 8;
                                    let mut input_vec: Vec<i64> = vec![0; input_size];

                                    if let Err(e) =
                                        context.read(input_item.data.offset, &mut input_vec)
                                    {
                                        eprintln!("ERROR: couldn't read from context");
                                        debt.fulfill(Box::new(DandelionResult::Err(e)));
                                        continue;
                                    }
                                    log::debug!("something worked :D input: {:?}", input_vec);

                                    //"wipe" context for output
                                    context.clear_metadata();
                                    let output_offset = context
                                        .get_free_space_and_write_slice(&[
                                            2i64, 50i64, 50i64, 50i64, 50i64,
                                        ])
                                        .expect("should be able to write to context");
                                    println!("output offset should be 0, got: {:?}", output_offset);
                                    context.content.push(Some(DataSet {
                                        ident: "functionOutputSet".to_string(),
                                        buffers: vec![DataItem {
                                            ident: "functionOutputItem".to_string(),
                                            data: Position {
                                                offset: output_offset as usize,
                                                size: 40,
                                            },
                                            key: 0,
                                        }],
                                    }));
                                    context.state = ContextState::Run(0);
                                    let results = Box::new(Ok(WorkDone::Context(context)));
                                    debt.fulfill(results);
                                    continue;
                                }
                            }
                        }
                        _ => {
                            eprintln!("ERROR: BEYOND NYI");
                            debt.fulfill(Box::new(Err(DandelionError::NotImplemented)));
                            continue;
                        }
                    }
                } else {
                    eprintln!("ERROR: received wrong config!");
                    debt.fulfill(Box::new(Err(DandelionError::ConfigMismatch)));
                    continue;
                }
            }
            WorkToDo::TransferArguments {
                destination: _,
                source: _,
                destination_set_index: _,
                destination_allignment: _,
                destination_item_index: _,
                destination_set_name: _,
                source_set_index: _,
                source_item_index: _,
                recorder: _,
            } => {
                debt.fulfill(Box::new(Err(DandelionError::MalformedConfig)));
                continue;
            }
            WorkToDo::ParsingArguments {
                driver: _,
                path: _,
                static_domain: _,
                recorder: _,
            } => {
                debt.fulfill(Box::new(Err(DandelionError::MalformedConfig)));
                continue;
            }
            WorkToDo::LoadingArguments {
                function: _,
                domain: _,
                ctx_size: _,
                recorder: _,
            } => {
                debt.fulfill(Box::new(Err(DandelionError::MalformedConfig)));
                continue;
            }
            WorkToDo::Shutdown() => {
                return debt;
            }
        }
    }
}

//TODO: VICTOR change so it can enqueue multiple things while having a maximum to wait on, and have it then schedule the ones it gets.
fn outer_engine(core_id: u8, queue: Box<dyn WorkQueue + Send>, fpgadata: FpgaData) {
    // set core affinity
    if !core_affinity::set_for_current(core_affinity::CoreId { id: core_id.into() }) {
        log::error!("core received core id that could not be set");
        return;
    }
    let runtime = Builder::new_multi_thread()
        .on_thread_start(move || {
            if !set_for_current(core_affinity::CoreId { id: core_id.into() }) {
                return;
            }
        })
        .worker_threads(1)
        .enable_all()
        .build()
        .or(Err(DandelionError::EngineError))
        .unwrap();
    let debt = runtime.block_on(engine_loop(queue, &fpgadata));
    drop(runtime);
    debt.fulfill(Box::new(Ok(WorkDone::Resources(vec![
        ComputeResource::CPU(core_id),
    ]))));
}

#[derive(Debug)]
pub struct FpgaData {
    cpu_slot: u8, //maybe redundant if we have a runtime
    runtime: Runtime,
    std_connection: std::net::SocketAddrV4, //ip/port for normal usage
    special_connection: std::net::SocketAddrV4, //different port for special control stuff
    //TODO: add debt set/hashmap that keeps track of all debts
    //TODO: debug stuff for invocations?
    tiles: [Tile; 4], //hard coded size for now
    unscheduled_invocations: Arc<Mutex<VecDeque<Box<Invocation>>>>,
    //other stuff as well? Like some state keeping
    invocation_counter: Mutex<u32>, //counter to have unique ids
}

pub struct FpgaDriver {}

impl Driver for FpgaDriver {
    fn start_engine(
        &self,
        resource: ComputeResource,
        queue: Box<dyn WorkQueue + Send>,
    ) -> DandelionResult<()> {
        println!("Starting FPGA engine");
        let core_id: u8 = match resource {
            ComputeResource::CPU(core_id) => core_id,
            _ => return Err(DandelionError::EngineResourceError),
        };
        // check that core is available
        let available_cores = match core_affinity::get_core_ids() {
            None => return Err(DandelionError::EngineResourceError),
            Some(cores) => cores,
        };
        if !available_cores.iter().any(|x| x.id == usize::from(core_id)) {
            return Err(DandelionError::EngineResourceError);
        }

        let runtime = Builder::new_multi_thread()
            .on_thread_start(move || {
                if !set_for_current(core_affinity::CoreId { id: core_id.into() }) {
                    return;
                }
            })
            .worker_threads(4)
            .enable_all()
            .build()
            .or(Err(DandelionError::EngineError))?;
        let tiles = std::array::from_fn(|_| Tile {
            unsent_invocations: Arc::new(Mutex::new(VecDeque::new())),
            sent_invocations: Arc::new(Mutex::new(VecDeque::new())),
        });
        let fpgadata = FpgaData {
            cpu_slot: core_id,
            runtime, //where do the configs for stuff go?...
            std_connection: SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 3456),
            special_connection: SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 4567),
            tiles,
            unscheduled_invocations: Arc::new(Mutex::new(VecDeque::new())),
            invocation_counter: Mutex::new(0),
        };

        //start_thread::<FpgaLoop>(cpu_slot, queue);
        //TODO: change fpga so that we run in a queue fashion
        std::thread::spawn(move || outer_engine(core_id, queue, fpgadata));
        return Ok(());
    }

    fn parse_function(
        &self,
        function_path: String,
        static_domain: &'static dyn crate::memory_domain::MemoryDomain,
    ) -> DandelionResult<Function> {
        let soft_max_tile_queue_length = 3;
        let config = match function_path.as_str() {
            "dummy_local_matrix" => {
                let dummyconfig: FpgaConfig = FpgaConfig {
                    dummy_func_num: 1,
                    bitstream_id: 1,
                    soft_max_tile_queue_length,
                };
                FunctionConfig::FpgaConfig(dummyconfig)
            }
            "dummy_input" => {
                let dummy_input_config = FpgaConfig {
                    dummy_func_num: 2,
                    bitstream_id: 1,
                    soft_max_tile_queue_length,
                };
                FunctionConfig::FpgaConfig(dummy_input_config)
            }
            _ => {
                eprintln!("WARNING, loading real configs NYI!");
                return Err(DandelionError::NotImplemented);
            }
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

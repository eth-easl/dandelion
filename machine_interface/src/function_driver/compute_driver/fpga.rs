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
use futures::{lock::MutexGuard, SinkExt};
use libc::IW_QUAL_QUAL_INVALID;
use libloading::{Library, Symbol};
use log;
use serde::{de::IntoDeserializer, Deserialize};
use std::{
    collections::VecDeque,
    fmt::format,
    mem::size_of,
    net::{Ipv4Addr, SocketAddrV4},
    panic,
    time::Instant,
};

use std::{os::unix::net::SocketAddr, str::FromStr, sync::Arc, sync::Mutex};
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt},
    sync::{
        mpsc::{self, Receiver, Sender},
        Notify,
    },
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
consider using parking lot crate for faster synchronous mutexes
*/

const HEADER_BIT: u8 = 0x80; //1000 0000
const DATA_BIT: u8 = 0x40; //0100 0000
const STOP_BIT: u8 = 0x08; //0000 1000
const ERR_BITS: u8 = 0x07; //0000 0111
const RESPONSE_BIT: u8 = 0x20; //0010 0000
const FOOTER_BIT: u8 = 0x10; //0001 0000

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
fn deserialize_response_message(buf: &[u8; PACKET_SIZE]) -> Result<Arc<ResponseMessage>, String> {
    let flag_byte = buf[0];
    let tile_id = buf[1];
    let is_err = flag_byte & ERR_BITS != 0;
    let is_response = flag_byte & RESPONSE_BIT != 0;
    let is_response_footer = flag_byte & FOOTER_BIT != 0;
    if is_err {
        Ok(Arc::new(ResponseMessage::Error(ResponseError {
            flag_byte,
            tile_id,
        })))
    } else if is_response_footer {
        let mut data: [u8; RESPONSE_FOOTER_BUF_SIZE] = [0; RESPONSE_FOOTER_BUF_SIZE];
        let data_size = u16::from_le_bytes([buf[2], buf[3]]);
        data.copy_from_slice(&buf[4..PACKET_SIZE]);
        Ok(Arc::new(ResponseMessage::Footer(ResponseFooter {
            flag_byte,
            tile_id,
            data_size,
            data,
        })))
    } else if is_response {
        let mut data: [u8; RESPONSE_DATA_BUF_SIZE] = [0; RESPONSE_DATA_BUF_SIZE];
        data.copy_from_slice(&buf[2..PACKET_SIZE]);
        Ok(Arc::new(ResponseMessage::Data(ResponseData {
            flag_byte,
            tile_id,
            data,
        })))
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
fn serialize_send_message(msg: &RequestMessage) -> [u8; PACKET_SIZE] {
    let mut result: [u8; PACKET_SIZE] = [0; PACKET_SIZE];
    match msg {
        RequestMessage::Header(header) => {
            result[0] = header.flag_byte;
            result[1] = header.tile_id;
            result[2..4].copy_from_slice(&header.bitstream_id.to_le_bytes());
            result[4..8].copy_from_slice(&header.data_size.to_le_bytes());
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
type BitstreamId = u16;

/*
a tile from now on should hold a queue of invocations which each hold their own info and states.
at any point in time only the frontmost invocation should have the state "running", and only one
invocation, the frontmost or second to front invocation should be "loading".
there should be a configurable maximum that defines how many functions should ever be in flight for a tile.


If our receivehandler function gets a packet for a tile, there should be a "running" invocation on that tile, that
we can assign our message to. else we should error out.
*/

#[derive(Debug)]
struct Invocation {
    id: InvocationId,                  //local invocation ID, for debugging purposes
    bitstream_id: BitstreamId,         //bitstream ID for which bitstream to load
    send_data: Arc<Mutex<Vec<u8>>>,    //just the data that needs to be sent
    receive_data: Arc<Mutex<Vec<u8>>>, //only the data parts of the messages, concatinated
    debt: Debt,       // Associated Invocation Debt that needs to be fulfilled at the end
    context: Context, // associated context, is read for sending and gets wiped and written to at the end.
}

#[derive(Debug, Clone)]
struct Tile {
    invocations: Arc<Mutex<VecDeque<Box<Invocation>>>>,
    /*
        simplified from before, we now just have a list of invocations.
        we only put invocations on there when we directly want to
        send them because we scheduled it.
        Meaning the last one in the queue may be not fully sent yet.

        This is nice, though, since the previous implementation had a bug where you couldn't
        start receiving a response to a partially sent invocation.

        push_back to add to the queue, and pop_front to remove from the queue.
    */
}

//should probably return a result
async fn send_msg(stream: &mut TcpStream, message: &RequestMessage) -> io::Result<()> {
    let send_buf = serialize_send_message(message);

    stream.write_all(&send_buf).await.expect("failed sending");
    stream.flush().await.expect("DIDNT FLUSH");
    return Ok(());
}
/**
 * This function gets a boxed invocation struct with empty queues.
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
fn schedule(fpgadata: &FpgaData, bitstream_id: BitstreamId) -> Result<usize, String> {
    //get all tile unsent invocation queues quickly
    //get the info we need and dont release the locks so we stay accurate
    let tiles = &fpgadata.tiles;
    let mut back_invocations: Vec<Option<(BitstreamId, usize)>> = vec![None; tiles.len()];
    //save both last bitstream id and length of the queue

    //copy bitstream_ids
    for i in 0..tiles.len() {
        //lock everybody and store the queue in unsent_queues
        let temp_queue_lock = tiles[i].invocations.lock().expect("locking failed badly");
        let temp_invocation_opt = temp_queue_lock.back();

        match temp_invocation_opt {
            Some(invocation) => {
                back_invocations[i] = Some((invocation.bitstream_id, temp_queue_lock.len()))
            }
            None => back_invocations[i] = None,
        }
        //drop temp_queue_lock
    }
    //this part has to run very quick
    let mut chosen_tile: usize = 0;
    let mut found = false;
    let mut shortest: usize = usize::MAX;

    //now decide, look only at the ones below threshold
    //unless empty tile, pick shortest within threshold that has the same bitstream
    //first runthrough
    for i in 0..back_invocations.len() {
        match back_invocations[i] {
            Some(invocation_data) => {
                if (invocation_data.1 < fpgadata.max_tile_queue_length)
                    && (invocation_data.0 == bitstream_id)
                {
                    if found {
                        if (shortest > invocation_data.1) {
                            //only choose if new shortest
                            shortest = invocation_data.1;
                            chosen_tile = i
                        }
                    } else {
                        found = true;
                        chosen_tile = i;
                        shortest = invocation_data.1;
                    }
                }
            }
            None => {
                // we want to fill empty queues first
                chosen_tile = i;
                found = true;
                shortest = 0;
                break; //we can end early
            }
        }
    }
    //if nothing was found yet, we want to pick the shortest queue
    if (!found) {
        for i in 0..back_invocations.len() {
            match back_invocations[i] {
                Some(invovocation_data) => {
                    if invovocation_data.1 < shortest {
                        shortest = invovocation_data.1;
                        //not setting found to true so we can print this out
                        chosen_tile = i;
                    }
                }
                None => {
                    //this is not a case that should be able to happen
                    eprintln!("what just happened here this is illegal");
                    return Err("died in scheduler".to_string());
                }
            }
        }
    }

    println!(
        "debug: scheduled invocation with shortest {:?}, matching bitstream or empty tile {:?}",
        shortest, found
    );
    return Ok(chosen_tile);
}

//note: current invocation counter needs to be increased before calling this.
async fn send(
    fpgadata: &FpgaData,
    invocation: Box<Invocation>,
    tile_id: usize,
) -> Result<String, String> {
    eprintln!("Connecting to {:?}", fpgadata.send_connection);
    //new connection for any sent invocation
    match TcpStream::connect(fpgadata.send_connection).await {
        Err(e) => Err(format!("Failed to connect: {e:?}").to_string()),
        Ok(mut stream) => {
            let send_data_arc = invocation.send_data.clone();

            let bitstream_id = invocation.bitstream_id;
            //first, put the invocation in the queue.
            {
                let mut queue = fpgadata.tiles[tile_id]
                    .invocations
                    .lock()
                    .expect("failed to lock queue");
                queue.push_back(invocation);
            }

            //now we can start sending

            //this is so we get none if the result is negative
            let (data_size, header_data, written_size) = {
                let send_data = send_data_arc.lock().expect("failed locking send_data");

                let data_size = send_data.len();
                let mut header_data = [0u8; REQUEST_HEADER_BUF_SIZE];
                let min_write_size = min(data_size, REQUEST_HEADER_BUF_SIZE);
                header_data[..min_write_size].copy_from_slice(&send_data[..min_write_size]);

                (data_size, header_data, min_write_size)
            }; //scoped so rust doesn't complaine about holding a lock through an await point.

            let mut current_sent_data = written_size;

            let tile_u8 = u8::try_from(tile_id).expect("converting tile id failed");
            let data_size_u32 = u32::try_from(data_size).expect("converting data size failed");
            println!("data size: {data_size:?}");
            let header_packet = RequestHeader {
                flag_byte: HEADER_BIT,
                tile_id: tile_u8,
                bitstream_id,
                data_size: data_size_u32,
                data: header_data,
            };

            let mut part_message = RequestMessage::Header(header_packet); //renamed so it is optimised for await and lives

            let mut waiter = send_msg(&mut stream, &part_message);

            //now if needed loop sending packet after packet

            //should create the message, then await the previous before sending the next

            if (current_sent_data < data_size) {
                let mut curr_data: [u8; REQUEST_DATA_BUF_SIZE] = [0; REQUEST_DATA_BUF_SIZE];

                while current_sent_data < data_size {
                    if current_sent_data + REQUEST_DATA_BUF_SIZE > data_size {
                        let rest = data_size - current_sent_data;
                        {
                            let send_data = send_data_arc.lock().expect("failed locking send_data");
                            curr_data[..rest]
                                .copy_from_slice(&send_data[current_sent_data..send_data.len()]);
                        }
                        curr_data[rest..].fill(0); //fill the empty region at the end so it doesn't have the data from last message
                    } else {
                        {
                            let send_data = send_data_arc.lock().expect("failed locking send_data");
                            curr_data.copy_from_slice(
                                &send_data
                                    [current_sent_data..current_sent_data + REQUEST_DATA_BUF_SIZE],
                            );
                        }
                    }
                    //this will panic instead of error out if somehting goes wrong..
                    let data_packet = RequestData {
                        flag_byte: DATA_BIT,
                        tile_id: tile_u8,
                        data: curr_data,
                    };

                    waiter
                        .await
                        .expect("failed sending message, in continued loop");

                    part_message = RequestMessage::Data(data_packet);
                    waiter = send_msg(&mut stream, &part_message);

                    current_sent_data += REQUEST_DATA_BUF_SIZE;
                }
            }

            waiter.await.expect("failed sending message");
            return Ok("all good".to_string());
        } //technically this means if the fpga doesn't need the whole input and finished its output before we send
          //everything, we would still be sending input after being done.
          //I'm not conscerned about that right now, as our current idea for the fpga waits until all the data is there
          //anyways.
    }
}

//starts tile workers and then continuously reads packets, forwarding them to the correct tile workers.
async fn receive_worker(fpgadata: Arc<FpgaData>) {
    //build listener
    let listener = TcpListener::bind(fpgadata.recv_connection)
        .await
        .expect("failed to bind recv_connection");

    //start tile workers here
    let mut senders: Vec<Sender<Box<[u8; PACKET_SIZE]>>> = Vec::new();
    for i in 0..fpgadata.tiles.len() {
        let (tx, mut rx) = mpsc::channel::<Box<[u8; PACKET_SIZE]>>(10);
        senders.push(tx);
        tokio::spawn(tile_worker(fpgadata.clone(), i, rx));
        //using the listener runtime because that's what this is running on already.
    }

    loop {
        let (mut socket, _) = listener
            .accept()
            .await
            .expect("failed to accept connection");
        loop {
            let mut buffer: Box<[u8; PACKET_SIZE]> = Box::new([0; PACKET_SIZE]);

            match socket.readable().await {
                Err(err) => {
                    eprintln!("failed to get readable: {err}.\n Re-establishing connection...");
                    break;
                }
                Ok(()) => {
                    let _n = match timeout(Duration::from_secs(2), socket.read(&mut *buffer)).await
                    {
                        Ok(Ok(PACKET_SIZE)) => PACKET_SIZE,
                        Ok(Ok(0)) => {
                            //sleep(Duration::from_millis(5)).await;
                            continue; //TODO: find a better way to do this, other than "busy" polling and sleeping
                        }

                        Ok(Ok(n)) => {
                            eprintln!("did not get packet size, got {n}");
                            panic!("something is wrong, I can feel it... read size");
                            //break;
                        }

                        Ok(Err(e)) => {
                            eprintln!("got error in read: {e}");
                            panic!("something is wrong, I can feel it... read err");
                            //break;
                        }

                        Err(_) => {
                            eprintln!("got timeout! reconnecting...");
                            break;
                        }
                    };
                    //read byte at posision 1 (that will be the tile id)
                    let packet_tile_id: u8 = buffer[1];
                    if usize::from(packet_tile_id) >= fpgadata.tiles.len() {
                        panic!("got nonsensical tile id {:?}", packet_tile_id);
                    }
                    //send to matching tile worker:
                    senders[usize::from(packet_tile_id)]
                        .send(buffer)
                        .await
                        .expect("failed to send to thread");
                }
            }
        }
    }

    //
}

/*
this should be a loop that:
receives a message, deserialize it,
find the first invocation in the queue for that tile and keep
it stored until it is fully finished.

when the final message comes for an invocation, decrement the counter,
and if low enough, wake the engine_loop with a notify_one.

*/
async fn tile_worker(
    fpgadata: Arc<FpgaData>,
    tile_id: usize,
    mut receiver: Receiver<Box<[u8; PACKET_SIZE]>>,
) {
    let arc_tile_invocations = fpgadata.tiles[tile_id].invocations.clone();
    loop {
        //first new received message
        if let Some(buffer_box) = receiver.recv().await {
            match *deserialize_response_message(&buffer_box).expect("got a trash response") {
                ResponseMessage::Data(response) => {
                    if (usize::from(response.tile_id) != tile_id) {
                        eprintln!("messed up tile id");
                        panic!();
                    }
                    //re-locking every time isn't nice. But with a synchronous mutex, you can't hold it
                    //across an await and i'm not going to start using tokio mutexes.

                    let tile_invocations = arc_tile_invocations
                        .lock()
                        .expect("couldn't lock tile invocaiton queue");

                    let invocation = tile_invocations
                        .front()
                        .expect("no invocation in queue found");
                    let arc_receive_data = invocation.receive_data.clone();
                    let mut receive_data =
                        arc_receive_data.lock().expect("couldn't lock receive_data");

                    drop(tile_invocations); //release lock quickly

                    //now that we own a mutable locked mutex to the receive data,
                    //we can add to the data and skip to the next

                    receive_data.extend_from_slice(&response.data[..]);
                }
                ResponseMessage::Footer(response) => {
                    if (usize::from(response.tile_id) != tile_id) {
                        eprintln!("messed up tile id");
                        panic!();
                    }

                    //we don't need to loop if we get a footer immediately

                    let mut tile_invocations = arc_tile_invocations
                        .lock()
                        .expect("couldn't lock tile invocation queue");
                    //since we reached the end we can pop the invocation immediately.
                    let invocation = tile_invocations.pop_front().expect("queue empty??");
                    drop(tile_invocations); //release lock on invocation queue

                    //copy the exact data needed

                    let Invocation {
                        receive_data,
                        debt,
                        mut context,
                        ..
                    } = *invocation;

                    let bytes_to_copy = response.data_size;

                    {
                        receive_data
                            .lock()
                            .expect("couldn't lock data")
                            .extend_from_slice(&response.data[..usize::from(bytes_to_copy)]);
                    }

                    //we are finished, so write to context and then fulfill the debt
                    write_to_context(receive_data, &mut context);

                    let results = Box::new(Ok(WorkDone::Context(context)));
                    debt.fulfill(results);
                    //lovely!
                    //now update the bookkeeping and if necessary, notify the main loop
                    let mut current_invocations = fpgadata
                        .current_invocations
                        .lock()
                        .expect("couldn't lock current invocations");
                    *current_invocations -= 1; //will and should panic if below 0
                    if (*current_invocations == fpgadata.below_start_threshold - 1) {
                        fpgadata.below_start_threshold_notify.notify_one();
                    }
                }
                ResponseMessage::Error(response_error) => {
                    if (usize::from(response_error.tile_id) != tile_id) {
                        eprintln!("messed up tile id, too??");
                    }
                    eprintln!("got an error response back!");
                    panic!();
                }
            }
        } else {
            eprintln!("Sender dropped without sending a buffer");
            panic!();
        }
    }
}

fn write_to_context(a_data: Arc<Mutex<Vec<u8>>>, context: &mut Context) {
    let data = a_data.lock().expect("couldln't lock data");
    let data_len = data.len();

    //"wipe" context for output
    context.clear_metadata();
    let output_offset = context
        .get_free_space_and_write_slice(&data)
        .expect("should be able to write to context");
    println!("output offset should be 0, got: {:?}", output_offset);
    context.content.push(Some(DataSet {
        ident: "functionOutputSet".to_string(),
        buffers: vec![DataItem {
            ident: "functionOutputItem".to_string(),
            data: Position {
                offset: output_offset as usize,
                size: data_len,
            },
            key: 0,
        }],
    }));
    context.state = ContextState::Run(0);
}
//

async fn engine_loop(queue: Box<dyn WorkQueue + Send>, fpgadata: &FpgaData) -> Debt {
    //
    log::debug!("FPGA engine init.");
    println!("FPGA engine init.");
    let notifier = fpgadata.below_start_threshold_notify.clone();
    loop {
        let mut current_invocations = 0;
        {
            let l_current_invocations = fpgadata
                .current_invocations
                .lock()
                .expect("couldn't lock current invocaitons");
            current_invocations = *l_current_invocations;
        }
        if current_invocations >= fpgadata.max_threshold {
            notifier.notified().await;
        } //sleep until there's new work available

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
                //trust receivers to correctly set max_hit and below_start_threshold flags

                //get invocation id
                let (invocation_id) = {
                    let mut invocation_counter = fpgadata
                        .invocation_counter
                        .lock()
                        .expect("sth wrong happened to the invocation counter");
                    *invocation_counter += 1;
                    let invocation_id: u32 = *invocation_counter;
                    invocation_id
                }; //so clippy doesn't complain we're holding a lock through an await

                log::debug!("fpga loop entered run, invocation_id: {:?}", invocation_id);
                if let FunctionConfig::FpgaConfig(fpgaconfig) = config {
                    match fpgaconfig.dummy_func_num {
                        0 => {
                            //This should build up the invocation from the context and then call send on it
                            //i need id, bitstream_id, send_data, receive_data, debt, context.
                            //bitstream_id is the first element in the function context, i've decided. This can be changed to something else later
                            print!("full functionality");
                            let (bitstream_id, send_data) = {
                                match &context.content[0] {
                                    None => {
                                        eprintln!("ERROR: no content in context received.");
                                        debt.fulfill(Box::new(Err(
                                            DandelionError::ContextMismatch,
                                        )));
                                        panic!();
                                    }
                                    Some(set) => {
                                        log::debug!("found set: {:?}", set);
                                        println!("found set: {:?}", set);
                                        if (set.buffers.len() != 2) {
                                            eprintln!(
                                                "set is built wrong!, {:?}",
                                                set.buffers.len()
                                            );
                                            debt.fulfill(Box::new(Err(
                                                DandelionError::ContextMismatch,
                                            )));
                                            panic!();
                                        }
                                        let bitstream_input_item = &set.buffers[0];
                                        //size here should be bitstream_id sized, so 2 bytes.
                                        if (bitstream_input_item.data.size
                                            != size_of::<BitstreamId>())
                                        {
                                            eprintln!("size mismatch");
                                            debt.fulfill(Box::new(Err(
                                                DandelionError::ContextMismatch,
                                            )));
                                            panic!();
                                        }
                                        let mut bitstream_input_vec: Vec<BitstreamId> = vec![0; 1];
                                        if let Err(e) = context.read(
                                            bitstream_input_item.data.offset,
                                            &mut bitstream_input_vec,
                                        ) {
                                            eprintln!(
                                                "ERROR: couldn't read bitstreamid from context"
                                            );
                                            debt.fulfill(Box::new(DandelionResult::Err(e)));
                                            panic!();
                                        }
                                        //now we should have the bitstream id
                                        //let's also read out to the send_data vec
                                        let send_data_input_item = &set.buffers[1];
                                        let send_data_size = send_data_input_item.data.size;
                                        let mut send_data = vec![0u8; send_data_size];

                                        if let Err(e) = context
                                            .read(send_data_input_item.data.offset, &mut send_data)
                                        {
                                            eprintln!("ERROR: couldn't read data from context");
                                            debt.fulfill(Box::new(DandelionResult::Err(e)));
                                            panic!();
                                        }

                                        (bitstream_input_vec[0], send_data)
                                    }
                                }
                            };
                            println!("got bitstreamid and send data from context");
                            //should be able to fill out the invocation now
                            let invocation = Box::new(Invocation {
                                id: invocation_id,
                                bitstream_id,
                                send_data: Arc::new(Mutex::new(send_data)),
                                receive_data: Arc::new(Mutex::new(Vec::new())),
                                debt,
                                context,
                            });
                            //schedule
                            let tile_id =
                                schedule(fpgadata, bitstream_id).expect("couldn't schedule? oof.");
                            //increase current invocations:
                            {
                                let mut l_current_invocations = fpgadata
                                    .current_invocations
                                    .lock()
                                    .expect("couldnt lock curr");
                                *l_current_invocations += 1;
                            }
                            //send it out:
                            if let Err(errstr) = send(fpgadata, invocation, tile_id).await {
                                eprintln!("error, couldn't send out, {:?}", errstr);
                                panic!();
                            }
                        }
                        1 => {
                            //test mode only one we implement rn
                            log::debug!("testing input through context, local only");

                            match &context.content[0] {
                                None => {
                                    eprintln!("ERROR: no content in context received.");
                                    debt.fulfill(Box::new(Err(DandelionError::ContextMismatch)));
                                }
                                Some(set) => {
                                    log::debug!("found set: {:?}", set);

                                    let input_item = &set.buffers[1];
                                    let input_size = input_item.data.size / 2;
                                    let mut input_vec: Vec<i16> = vec![0; input_size];

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
                                            3i16, 6i16, 6i16, 6i16, 6i16,
                                        ])
                                        .expect("should be able to write to context");
                                    println!("output offset should be 0, got: {:?}", output_offset);
                                    context.content.push(Some(DataSet {
                                        ident: "functionOutputSet".to_string(),
                                        buffers: vec![DataItem {
                                            ident: "functionOutputItem".to_string(),
                                            data: Position {
                                                offset: output_offset as usize,
                                                size: 10,
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
fn outer_engine(core_id: u8, queue: Box<dyn WorkQueue + Send>, fpgadata: Arc<FpgaData>) {
    // set core affinity
    if !core_affinity::set_for_current(core_affinity::CoreId { id: core_id.into() }) {
        log::error!("core received core id that could not be set");
        return;
    }

    fpgadata
        .listener_runtime
        .spawn(receive_worker(fpgadata.clone()));

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
    listener_runtime: Runtime,
    send_connection: std::net::SocketAddrV4, //ip/port for nsending messsages
    recv_connection: std::net::SocketAddrV4, //ip/port for recceiving messages
    special_connection: std::net::SocketAddrV4, //different port for special control stuff

    tiles: [Tile; 4],                  //hard coded number for now
    current_invocations: Mutex<usize>, //counter for in-flight invocations
    invocation_counter: Mutex<u32>,    //counter to have unique ids
    max_tile_queue_length: usize,      //soft per-tile maximum of in-flight invocations

    max_threshold: usize,         //max upper threshold for in-flight invocations
    below_start_threshold: usize, //threshold below which sending flow resumes
    below_start_threshold_notify: Arc<Notify>, //notifier to wake sending flow
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

        let listener_runtime = Builder::new_multi_thread()
            .on_thread_start(move || {
                if !set_for_current(core_affinity::CoreId { id: core_id.into() }) {
                    return;
                }
            })
            .worker_threads(5)
            .enable_all()
            .build()
            .or(Err(DandelionError::EngineError))?;

        let tiles = std::array::from_fn(|_| Tile {
            invocations: Arc::new(Mutex::new(VecDeque::new())),
        });

        let below_start_threshold_notify = Arc::new(Notify::new());
        let fpgadata = Arc::new(FpgaData {
            //cpu_slot: core_id,
            listener_runtime, //where do the configs for stuff go?...
            send_connection: SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 3456),
            recv_connection: SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 3457),
            special_connection: SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 3458),
            tiles,
            current_invocations: Mutex::new(0),
            invocation_counter: Mutex::new(0),
            max_tile_queue_length: 4,
            max_threshold: 4 * 4 + 2,
            below_start_threshold: 2 * 4 + 1, //komplett aus dem bauch heraus gewählt
            below_start_threshold_notify,
        });

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
        let config = match function_path.as_str() {
            "dummy_input" => {
                let dummy_input_config = FpgaConfig { dummy_func_num: 1 };
                FunctionConfig::FpgaConfig(dummy_input_config)
            }
            _ => {
                let fpga_config = FpgaConfig { dummy_func_num: 0 };
                FunctionConfig::FpgaConfig(fpga_config) //real case
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

/*
Thoughts about good scheduling.

If we are always scheduling the moment one spot frees up we are doing something wrong
I think a decent way would be to have multiple thresholds and other triggers for starting/stopping to schedule.

Stopping: when a set number of active/waiting invocations is reached, for example Tiles*3

Starting: when that total invocation number drops below Tiles*2 or at least one Tile is completely free.

There should be only one thread doing this and it probably should be the main loop.
I think polling is the only real option for this.
*/

/*
Thoughts about sending:
I want to simplify this.
For now: instead of sending multiple things in parallel, let's just schedule a thing and then
call the function that sends over the invocation.
This seems like a useful paradigm.
*/

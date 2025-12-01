use std::{collections::HashMap, hash::Hash};
use tokio::io::{AsyncBufRead, AsyncBufReadExt};
use log::{debug, info};
use std::collections::{HashSet};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Request {
    pub method: Method,
    pub path: String,
    pub headers: HashMap<String, String>,
    pub raw_data: Vec<u8>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum Method {
    Get,
    Post,
}

impl TryFrom<&str> for Method {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "GET" => Ok(Method::Get),
            "POST" => Ok(Method::Post),
            m => Err(anyhow::anyhow!("unsupported method: {m}")),
        }
    }
}

pub async fn parse_request(mut stream: impl AsyncBufRead + Unpin) -> anyhow::Result<Request> {
    let mut data: Vec<u8> = Vec::new();

    let mut line_buffer = String::new();
    stream.read_line(&mut line_buffer).await?;
    data.append(&mut line_buffer.as_bytes().to_vec());

    let mut parts = line_buffer.split_whitespace();

    let method: Method = parts
        .next()
        .ok_or(anyhow::anyhow!("missing method"))
        .and_then(TryInto::try_into)?;

    let path: String = parts
        .next()
        .ok_or(anyhow::anyhow!("missing path"))
        .map(Into::into)?;

    let mut headers = HashMap::new();

    loop {
        line_buffer.clear();
        stream.read_line(&mut line_buffer).await?;
        data.append(&mut line_buffer.as_bytes().to_vec());

        if line_buffer.is_empty() || line_buffer == "\n" || line_buffer == "\r\n" {
            break;
        }

        let mut comps = line_buffer.split(":");
        let key = comps.next().ok_or(anyhow::anyhow!("missing header name"))?;
        let value = comps
            .next()
            .ok_or(anyhow::anyhow!("missing header value"))?
            .trim();

        headers.insert(key.to_string(), value.to_string());
    }

    Ok(Request {
        method,
        path,
        headers,
        raw_data: data,
    })
}



/// Map HTTP/2 frame type byte to a human-readable name.
pub fn frame_type_name(t: u8) -> &'static str {
    match t {
        0x0 => "DATA",
        0x1 => "HEADERS",
        0x2 => "PRIORITY",
        0x3 => "RST_STREAM",
        0x4 => "SETTINGS",
        0x5 => "PUSH_PROMISE",
        0x6 => "PING",
        0x7 => "GOAWAY",
        0x8 => "WINDOW_UPDATE",
        0x9 => "CONTINUATION",
        _   => "UNKNOWN",
    }
}


/// Dump basic info for each HTTP/2 frame in `buf`.
// Very initial parsing
// To check if there is truncated frame, incomplete header and unended stream (incomplete req/resp)
// *** Return (the 1st variable): ***
// The num of complete req/resp(s) received
// *** Return (the 2nd variable): ***
// If there is truncated frame -> -1
// Else if there is incompleted Header (there are still following CONTINUATION frames not received) -> -2
// Else if there is unended stream (not receive the whole req or resp) -> -3
// Else (pass the intial parsing) -> 0
// ****** TO DO: Currently we don't limit the max num of req/resp(s) to receive ******
// ****** What might happen here we continuously receive incomplete req/resp (for example, only the header but not the data) *****
// ***** As a result, the peer might stops sending because of the http2 flow control, and the initla parsing is still not passed (dead lock) *******
pub fn http2_initial_parsing(mut buf: &[u8]) -> (usize, i32) {
    // Each HTTP/2 frame has a 9-byte header:
    //   0-2: Length (24 bits, big-endian)
    //   3:   Type (8 bits)
    //   4:   Flags (8 bits)
    //   5-8: R (1 bit, reserved) + Stream Identifier (31 bits)
    const HEADER_LEN: usize = 9;

    // Flags
    const FLAG_END_STREAM:  u8 = 0x1; // relevant for DATA and HEADERS
    const FLAG_END_HEADERS: u8 = 0x4; // relevant for HEADERS and CONTINUATION

    // Frame types we care about
    const FRAME_DATA:         u8 = 0x0;
    const FRAME_HEADERS:      u8 = 0x1;
    const FRAME_CONTINUATION: u8 = 0x9;


    let mut unended_streams_set = HashSet::new();
    let mut unended_headers_set  = HashSet::new();
    let mut num_of_currently_received_req_or_resp: usize = 0;

    while buf.len() >= HEADER_LEN {
        // Parse length (24-bit big-endian)
        let len = ((buf[0] as u32) << 16) | ((buf[1] as u32) << 8) | (buf[2] as u32);

        let frame_type = buf[3];
        let flags = buf[4];

        // Stream ID: 1 reserved bit + 31-bit ID
        let raw_stream_id =
            ((buf[5] as u32) << 24) |
            ((buf[6] as u32) << 16) |
            ((buf[7] as u32) << 8)  |
            (buf[8]  as u32);
        let stream_id = raw_stream_id & 0x7FFF_FFFF;

        // Check that the full frame (header + payload) is present
        let total_len = HEADER_LEN + (len as usize);
        if buf.len() < total_len {
            debug!(
                "Truncated frame: header says length = {}, \
                 but only {} bytes remain",
                len,
                buf.len()
            );
            // break;

            return (num_of_currently_received_req_or_resp, -1); // Truncated frame
        }

        // END_STREAM is only meaningful on DATA and HEADERS frames
        let end_stream = match frame_type {
            FRAME_DATA /* DATA */ | FRAME_HEADERS /* HEADERS */ => (flags & FLAG_END_STREAM) != 0,
            _ => false,
        };

        // END_STREAM is only meaningful on HEADERS and CONTINUATION frames
        let end_headers = match frame_type {
            FRAME_HEADERS | FRAME_CONTINUATION => (flags & FLAG_END_HEADERS) != 0,
            _ => false,
        };


        // DATA and HEADERS frames
        if frame_type == 0x0 || frame_type == 0x1 {
            if end_stream == false {
                unended_streams_set.insert(stream_id);
            }
            else {
                num_of_currently_received_req_or_resp = num_of_currently_received_req_or_resp + 1;
                if unended_streams_set.contains(&stream_id) {
                    unended_streams_set.remove(&stream_id);
                }
            }
        }

        // HEADRE and CONTINUATION frames
        match frame_type {
            FRAME_HEADERS => {
                if !end_headers {
                    // HEADERS without END_HEADERS -> header block continues
                    unended_headers_set.insert(stream_id);
                }
            }

            FRAME_CONTINUATION => {
                if end_headers {
                    // Final CONTINUATION for this header block
                    unended_headers_set.remove(&stream_id);
                }
            }

            _ => {}
        }



        debug!(
            "Frame: len={} type=0x{:02x} ({}) flags=0x{:02x} stream_id={} end_stream={} num_of_currently_received_req_or_resp={}",
            len,
            frame_type,
            frame_type_name(frame_type),
            flags,
            stream_id,
            end_stream,
            num_of_currently_received_req_or_resp,
        );

        // Advance to the next frame (skip payload)
        buf = &buf[total_len..];
    }

    if buf.len() > 0 && buf.len() < HEADER_LEN {
        debug!(
            "Leftover bytes ({}) smaller than a frame header; probably partial frame.",
            buf.len()
        );

        return (num_of_currently_received_req_or_resp, -1); // Truncated frame
    }

    if unended_headers_set.len() > 0 {
        return (num_of_currently_received_req_or_resp, -2); // No Truncated frame but has incomplete HEADER (there are following CONTINUATION)
    }    

    if unended_streams_set.len() > 0 {
        return (num_of_currently_received_req_or_resp, -3); // No Truncated frame but has unended stream
    }

    info!("Initial parsing finds {} req/resp", num_of_currently_received_req_or_resp);
    return (num_of_currently_received_req_or_resp, 0); // No truncated frame; No un_ended stream
}
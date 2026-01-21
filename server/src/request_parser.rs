use hpack::Decoder;
use log::{debug, info};
use std::cmp::PartialEq;
use std::collections::HashSet;
use std::{collections::HashMap, fmt, hash::Hash};
use tokio::io::{AsyncBufRead, AsyncBufReadExt};

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

#[repr(u8)]
#[derive(PartialEq)]
enum Http2FrameTypes {
    DATA = 0x0,
    HEADERS = 0x1,
    PRIORITY = 0x2,
    RST_STREAM = 0x3,
    SETTINGS = 0x4,
    PUSH_PROMISE = 0x5,
    PING = 0x6,
    GOAWAY = 0x7,
    WINDOW_UPDATE = 0x8,
    CONTINUATION = 0x9,
}

impl fmt::Display for Http2FrameTypes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Http2FrameTypes::DATA => "DATA",
            Http2FrameTypes::HEADERS => "HEADERS",
            Http2FrameTypes::PRIORITY => "PRIORITY",
            Http2FrameTypes::RST_STREAM => "RST_STREAM",
            Http2FrameTypes::SETTINGS => "SETTINGS",
            Http2FrameTypes::PUSH_PROMISE => "PUSH_PROMISE",
            Http2FrameTypes::PING => "PING",
            Http2FrameTypes::GOAWAY => "GOAWAY",
            Http2FrameTypes::WINDOW_UPDATE => "WINDOW_UPDATE",
            Http2FrameTypes::CONTINUATION => "CONTINUATION",
        };

        write!(f, "{}", s)
    }
}

impl TryFrom<u8> for Http2FrameTypes {
    type Error = &'static str;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x0 => Ok(Http2FrameTypes::DATA),
            0x1 => Ok(Http2FrameTypes::HEADERS),
            0x2 => Ok(Http2FrameTypes::PRIORITY),
            0x3 => Ok(Http2FrameTypes::RST_STREAM),
            0x4 => Ok(Http2FrameTypes::SETTINGS),
            0x5 => Ok(Http2FrameTypes::PUSH_PROMISE),
            0x6 => Ok(Http2FrameTypes::PING),
            0x7 => Ok(Http2FrameTypes::GOAWAY),
            0x8 => Ok(Http2FrameTypes::WINDOW_UPDATE),
            0x9 => Ok(Http2FrameTypes::CONTINUATION),
            _ => Err("Invalid HTTP/2 frame type"),
        }
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
    const FLAG_END_STREAM: u8 = 0x1; // relevant for DATA and HEADERS
    const FLAG_END_HEADERS: u8 = 0x4; // relevant for HEADERS and CONTINUATION

    let mut unended_streams_set = HashSet::new();
    let mut unended_headers_set = HashSet::new();
    let mut num_of_currently_received_req_or_resp: usize = 0;

    while buf.len() >= HEADER_LEN {
        // Parse length (24-bit big-endian)
        let len = ((buf[0] as u32) << 16) | ((buf[1] as u32) << 8) | (buf[2] as u32);

        let frame_type = Http2FrameTypes::try_from(buf[3]).unwrap();
        let flags = buf[4];

        // Stream ID: 1 reserved bit + 31-bit ID
        let raw_stream_id = ((buf[5] as u32) << 24)
            | ((buf[6] as u32) << 16)
            | ((buf[7] as u32) << 8)
            | (buf[8] as u32);
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
            Http2FrameTypes::DATA /* DATA */ | Http2FrameTypes::HEADERS /* HEADERS */ => {
                (flags & FLAG_END_STREAM) != 0
            }
            _ => false,
        };

        // END_STREAM is only meaningful on HEADERS and CONTINUATION frames
        let end_headers = match frame_type {
            Http2FrameTypes::HEADERS | Http2FrameTypes::CONTINUATION => {
                (flags & FLAG_END_HEADERS) != 0
            }
            _ => false,
        };

        // DATA and HEADERS frames
        if frame_type == Http2FrameTypes::DATA || frame_type == Http2FrameTypes::HEADERS {
            if end_stream == false {
                unended_streams_set.insert(stream_id);
            } else {
                num_of_currently_received_req_or_resp = num_of_currently_received_req_or_resp + 1;
                if unended_streams_set.contains(&stream_id) {
                    unended_streams_set.remove(&stream_id);
                }
            }
        }

        // HEADER and CONTINUATION frames
        match frame_type {
            Http2FrameTypes::HEADERS => {
                if !end_headers {
                    // HEADERS without END_HEADERS -> header block continues
                    unended_headers_set.insert(stream_id);
                }
            }

            Http2FrameTypes::CONTINUATION => {
                if end_headers {
                    // Final CONTINUATION for this header block
                    unended_headers_set.remove(&stream_id);
                }
            }

            _ => {}
        }

        debug!(
            "Frame: len={} type=({}) flags=0x{:02x} stream_id={} end_stream={} num_of_currently_received_req_or_resp={}",
            len,
            frame_type,
            flags,
            stream_id,
            end_stream,
            num_of_currently_received_req_or_resp,
        );

        debug!(
            "Frame payload: {}",
            decode_frame_payload(frame_type, &buf[9..total_len])
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

    info!(
        "Initial parsing finds {} req/resp",
        num_of_currently_received_req_or_resp
    );
    (num_of_currently_received_req_or_resp, 0) // No truncated frame; No un_ended stream
}

fn read_1(buf: &[u8], idx: usize) -> u8 {
    buf[idx]
}

fn read_2(buf: &[u8], idx: usize) -> u16 {
    buf[idx + 1] as u16 | ((buf[idx] as u16) << 8)
}

fn read_4(buf: &[u8], idx: usize) -> u32 {
    ((buf[idx + 0] as u32) << 24)
        | ((buf[idx + 1] as u32) << 16)
        | ((buf[idx + 2] as u32) << 8)
        | (buf[idx + 3] as u32)
}

fn decode_frame_payload(frame_type: Http2FrameTypes, buf: &[u8]) -> String {
    let mut result = String::from("\n");

    match frame_type {
        Http2FrameTypes::SETTINGS => {
            let mut idx = 0;

            loop {
                if idx + 6 >= buf.len() {
                    // for the case when no SETTINGS are provided
                    break;
                }

                let id: u16 = read_2(buf, idx);
                idx += 2;
                let value: u32 = read_4(buf, idx);
                idx += 4;

                let to_add = match id {
                    0x1 => format!("SETTINGS_HEADER_TABLE_SIZE (0x01) = {}", value),
                    0x2 => format!("SETTINGS_ENABLE_PUSH (0x02) = {}", value),
                    0x3 => format!("SETTINGS_MAX_CONCURRENT_STREAMS (0x03) = {}", value),
                    0x4 => format!("SETTINGS_INITIAL_WINDOW_SIZE (0x04) = {}", value),
                    0x5 => format!("SETTINGS_MAX_FRAME_SIZE (0x05) = {}", value),
                    0x6 => format!("SETTINGS_MAX_HEADER_LIST_SIZE (0x06) = {}", value),
                    _ => "Unsupported setting ID".parse().unwrap(),
                };

                result.push_str(&to_add);

                if idx == buf.len() {
                    break;
                } else {
                    result.push_str("\n");
                }
            }
        }

        Http2FrameTypes::WINDOW_UPDATE => {
            if 4 >= buf.len() {
                return result;
            }

            let val = read_4(buf, 0) & 0x7FFF_FFFF;
            result.push_str(&format!("Window Size Increment = {}", val));
        }

        Http2FrameTypes::HEADERS => {
            if cfg!(feature = "decode_hpack") {
                if buf.len() > 0 {
                    let mut decoder = Decoder::new();
                    let _ = decoder.decode_with_cb(buf, |name, value| {
                        result.push_str(&format!(
                            "{:?} {:?}\n",
                            str::from_utf8(name.iter().as_slice()),
                            str::from_utf8(value.iter().as_slice())
                        ));
                    });
                }
            }
        }

        Http2FrameTypes::DATA => {
            result.push_str(buf.iter().map(|x| *x as char).collect::<String>().as_str());
        }

        _ => {
            info!(
                "Cannot debug frame of type {}. Feature not yet implemented.",
                frame_type
            );
        }
    }

    result
}

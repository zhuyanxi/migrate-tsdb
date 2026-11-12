//! Shared mock Remote Write receiver used by the sink unit tests and the
//! engine-level end-to-end tests.
//!
//! The wire types are declared here instead of reusing the sink's own protobuf
//! structs on purpose: a wrong tag number or a dropped field in the sink must
//! not be masked by a shared definition.

#![allow(dead_code)]

use std::collections::VecDeque;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use migrate_core::batch::{Batch, FloatRow, SeriesRow};
use prost::Message;
use snap::raw::Decoder;

// ---------------------------------------------------------------------------
// Wire format, restated from the specification.
// ---------------------------------------------------------------------------

#[derive(Clone, PartialEq, Message)]
pub struct WireRequest {
    #[prost(message, repeated, tag = "1")]
    pub timeseries: Vec<WireSeries>,
}

#[derive(Clone, PartialEq, Message)]
pub struct WireSeries {
    #[prost(message, repeated, tag = "1")]
    pub labels: Vec<WireLabel>,
    #[prost(message, repeated, tag = "2")]
    pub samples: Vec<WireSample>,
}

#[derive(Clone, PartialEq, Message)]
pub struct WireLabel {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub value: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct WireSample {
    #[prost(double, tag = "1")]
    pub value: f64,
    #[prost(int64, tag = "2")]
    pub timestamp: i64,
}

impl WireSeries {
    pub fn labels(&self) -> Vec<(&str, &str)> {
        self.labels
            .iter()
            .map(|label| (label.name.as_str(), label.value.as_str()))
            .collect()
    }

    pub fn label(&self, name: &str) -> Option<&str> {
        self.labels
            .iter()
            .find(|label| label.name == name)
            .map(|label| label.value.as_str())
    }
}

// ---------------------------------------------------------------------------
// Mock receiver.
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct Reply {
    pub status: u16,
    pub body: &'static str,
    pub headers: Vec<(String, String)>,
    /// Close the connection in the middle of the response head, which is what a
    /// crashing or overloaded receiver looks like from the sender's side.
    pub partial_headers: bool,
}

impl Reply {
    pub fn status(status: u16) -> Self {
        Self {
            status,
            body: "",
            headers: Vec::new(),
            partial_headers: false,
        }
    }

    pub fn body(status: u16, body: &'static str) -> Self {
        Self {
            body,
            ..Self::status(status)
        }
    }

    pub fn header(mut self, name: &str, value: impl Into<String>) -> Self {
        self.headers.push((name.to_string(), value.into()));
        self
    }

    pub fn partial_headers(mut self) -> Self {
        self.partial_headers = true;
        self
    }
}

#[derive(Debug, Clone)]
pub struct Captured {
    pub method: String,
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Captured {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    pub fn decode(&self) -> WireRequest {
        let raw = Decoder::new()
            .decompress_vec(&self.body)
            .expect("request body must be snappy block format");
        WireRequest::decode(raw.as_slice()).expect("request body must be a WriteRequest")
    }
}

pub struct Receiver {
    addr: SocketAddr,
    captured: Arc<Mutex<Vec<Captured>>>,
    stop: Arc<AtomicBool>,
    acceptor: Option<thread::JoinHandle<()>>,
}

impl Receiver {
    pub fn start(replies: Vec<Reply>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock receiver");
        let addr = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();

        let captured = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let script = Arc::new(Mutex::new(VecDeque::from(replies)));

        let acceptor = {
            let captured = captured.clone();
            let stop = stop.clone();
            let script = script.clone();
            thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            let captured = captured.clone();
                            let script = script.clone();
                            thread::spawn(move || serve(stream, script, captured));
                        }
                        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(2));
                        }
                        Err(_) => break,
                    }
                }
            })
        };

        Self {
            addr,
            captured,
            stop,
            acceptor: Some(acceptor),
        }
    }

    pub fn url(&self, path: &str) -> String {
        format!("http://{}{}", self.addr, path)
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn requests(&self) -> Vec<Captured> {
        self.captured.lock().unwrap().clone()
    }

    pub fn request_count(&self) -> usize {
        self.captured.lock().unwrap().len()
    }

    /// Total number of samples across every captured request.
    pub fn total_samples(&self) -> usize {
        self.requests()
            .iter()
            .map(|request| {
                request
                    .decode()
                    .timeseries
                    .iter()
                    .map(|series| series.samples.len())
                    .sum::<usize>()
            })
            .sum()
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.acceptor.take() {
            let _ = handle.join();
        }
    }
}

fn serve(
    mut stream: TcpStream,
    script: Arc<Mutex<VecDeque<Reply>>>,
    captured: Arc<Mutex<Vec<Captured>>>,
) {
    let mut buffer = Vec::new();
    loop {
        let Some(request) = read_request(&mut stream, &mut buffer) else {
            return;
        };
        let reply = script
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| Reply::status(204));
        captured.lock().unwrap().push(request);
        if write_reply(&mut stream, &reply).is_err() {
            return;
        }
    }
}

fn read_request(stream: &mut TcpStream, buffer: &mut Vec<u8>) -> Option<Captured> {
    let header_end = loop {
        if let Some(position) = find(buffer, b"\r\n\r\n") {
            break position;
        }
        let mut chunk = [0u8; 8192];
        let read = stream.read(&mut chunk).ok()?;
        if read == 0 {
            return None;
        }
        buffer.extend_from_slice(&chunk[..read]);
    };

    let head = String::from_utf8_lossy(&buffer[..header_end]).into_owned();
    let mut lines = head.split("\r\n");
    let request_line = lines.next()?;
    let mut parts = request_line.split(' ');
    let method = parts.next()?.to_string();
    let path = parts.next()?.to_string();

    let mut headers = Vec::new();
    let mut content_length = 0usize;
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let name = name.trim().to_string();
        let value = value.trim().to_string();
        if name.eq_ignore_ascii_case("content-length") {
            content_length = value.parse().unwrap_or(0);
        }
        headers.push((name, value));
    }

    let body_start = header_end + 4;
    while buffer.len() < body_start + content_length {
        let mut chunk = [0u8; 8192];
        let read = stream.read(&mut chunk).ok()?;
        if read == 0 {
            return None;
        }
        buffer.extend_from_slice(&chunk[..read]);
    }
    let body = buffer[body_start..body_start + content_length].to_vec();
    buffer.drain(..body_start + content_length);

    Some(Captured {
        method,
        path,
        headers,
        body,
    })
}

fn write_reply(stream: &mut TcpStream, reply: &Reply) -> std::io::Result<()> {
    if reply.partial_headers {
        stream.write_all(
            format!(
                "HTTP/1.1 {} {}\r\nContent-",
                reply.status,
                reason(reply.status)
            )
            .as_bytes(),
        )?;
        stream.flush()?;
        return Err(std::io::Error::other("simulated truncation"));
    }

    let mut head = format!("HTTP/1.1 {} {}\r\n", reply.status, reason(reply.status));
    for (name, value) in &reply.headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str(&format!("Content-Length: {}\r\n", reply.body.len()));
    head.push_str("Connection: keep-alive\r\n\r\n");
    let mut payload = head.into_bytes();
    payload.extend_from_slice(reply.body.as_bytes());
    stream.write_all(&payload)
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        307 => "Temporary Redirect",
        400 => "Bad Request",
        401 => "Unauthorized",
        413 => "Payload Too Large",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "Unknown",
    }
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

// ---------------------------------------------------------------------------
// Batch helpers.
// ---------------------------------------------------------------------------

pub fn series(uid: u64, labels: &[(&str, &str)]) -> SeriesRow {
    SeriesRow {
        uid,
        series_ref: 0,
        labels: labels
            .iter()
            .map(|(name, value)| (Arc::<str>::from(*name), Arc::<str>::from(*value)))
            .collect(),
    }
}

pub fn float(uid: u64, ts: i64, value: f64) -> FloatRow {
    FloatRow { uid, ts, value }
}

pub fn one_series_batch() -> Batch {
    let mut batch = Batch::new();
    batch.push_series(series(1, &[("__name__", "test_metric"), ("job", "api")]));
    batch.push_float(float(1, 1_700_000_000_000, 1.5));
    batch
}

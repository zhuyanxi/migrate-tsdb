//! Prometheus Remote Write v1.0 sink.

use std::collections::HashMap;
use std::io::Read;
use std::sync::OnceLock;
use std::thread;
use std::time::Duration;

use bytes::Bytes;
use prost::Message;
use reqwest::blocking::{Client, Response};
use reqwest::header::{
    HeaderMap, HeaderName, HeaderValue, AUTHORIZATION, CONTENT_ENCODING, CONTENT_TYPE, RETRY_AFTER,
    USER_AGENT,
};
use reqwest::Url;
use snap::raw::Encoder;

use crate::batch::Batch;
use crate::error::{Error, Result};
use crate::sink::{BatchSink, SinkOutcome};

const BEARER_TOKEN_ENV: &str = "TSDB_MIGRATE_REMOTE_WRITE_BEARER_TOKEN";
const MAX_ATTEMPTS: usize = 5;
const INITIAL_BACKOFF: Duration = Duration::from_millis(200);
const MAX_BACKOFF: Duration = Duration::from_secs(10);
const MAX_RETRY_AFTER: Duration = Duration::from_secs(30);
/// Lower bound for a server-supplied `Retry-After`. Without it a receiver that
/// answers `Retry-After: 0` would be hit with the full retry budget in one go.
const MIN_RETRY_AFTER: Duration = Duration::from_millis(100);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Sinks are created per block and per worker, so a per-sink client would churn
/// a thread pool and a connection pool for every block. One process-wide client
/// keeps connections alive across blocks instead.
static CLIENT: OnceLock<Client> = OnceLock::new();

fn shared_client() -> Result<&'static Client> {
    if let Some(client) = CLIENT.get() {
        return Ok(client);
    }
    let built = Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|e| Error::sink(format!("build remote_write HTTP client: {e}")))?;
    Ok(CLIENT.get_or_init(|| built))
}

pub struct RemoteWriteSink {
    client: &'static Client,
    url: Url,
    headers: HeaderMap,
}

impl RemoteWriteSink {
    pub fn new(url: &str, tenant_header: &str, tenant: &str) -> Result<Self> {
        validate_config(url, tenant_header)?;
        let url = Url::parse(url.trim())
            .map_err(|e| Error::sink(format!("invalid remote_write URL: {e}")))?;

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_ENCODING, HeaderValue::from_static("snappy"));
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/x-protobuf"),
        );
        headers.insert(
            HeaderName::from_static("x-prometheus-remote-write-version"),
            HeaderValue::from_static("0.1.0"),
        );
        headers.insert(
            USER_AGENT,
            HeaderValue::from_static(concat!("tsdb-migrate/", env!("CARGO_PKG_VERSION"))),
        );

        let tenant_header = tenant_header.trim();
        if !tenant_header.is_empty() {
            let name = HeaderName::from_bytes(tenant_header.as_bytes())
                .map_err(|e| Error::config(format!("invalid remote_write tenant header: {e}")))?;
            if !tenant.is_empty() {
                let value = HeaderValue::from_str(tenant).map_err(|e| {
                    Error::config(format!("invalid tenant value for remote_write header: {e}"))
                })?;
                headers.insert(name, value);
            }
        }

        if let Ok(token) = std::env::var(BEARER_TOKEN_ENV) {
            let token = token.trim();
            if !token.is_empty() {
                let value = HeaderValue::from_str(&format!("Bearer {token}")).map_err(|e| {
                    Error::config(format!("invalid remote_write bearer token: {e}"))
                })?;
                headers.insert(AUTHORIZATION, value);
            }
        }

        Ok(Self {
            client: shared_client()?,
            url,
            headers,
        })
    }

    fn send(&self, body: Bytes) -> Result<()> {
        let mut backoff = INITIAL_BACKOFF;
        for attempt in 0..MAX_ATTEMPTS {
            let response = self
                .client
                .post(self.url.clone())
                .headers(self.headers.clone())
                .body(body.clone())
                .send();

            match response {
                Ok(response) if response.status().is_success() => return Ok(()),
                Ok(response)
                    if response.status().as_u16() == 429 || response.status().is_server_error() =>
                {
                    let status = response.status();
                    if attempt + 1 == MAX_ATTEMPTS {
                        return Err(Error::sink(format!(
                            "remote_write endpoint returned HTTP {status} after {MAX_ATTEMPTS} attempts"
                        )));
                    }
                    let delay = retry_after(&response)
                        .unwrap_or(backoff)
                        .max(MIN_RETRY_AFTER)
                        .min(MAX_RETRY_AFTER);
                    thread::sleep(delay);
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                }
                Ok(response) => return Err(response_error(response)),
                Err(error) => {
                    if attempt + 1 == MAX_ATTEMPTS {
                        return Err(Error::sink(format!(
                            "remote_write request failed after {MAX_ATTEMPTS} attempts: {}",
                            error.without_url()
                        )));
                    }
                    thread::sleep(backoff);
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                }
            }
        }
        unreachable!("remote_write retry loop always returns")
    }
}

pub fn validate_config(url: &str, tenant_header: &str) -> Result<()> {
    let url = Url::parse(url.trim())
        .map_err(|e| Error::config(format!("invalid remote_write URL: {e}")))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(Error::config(
            "remote_write URL must use the http or https scheme",
        ));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(Error::config(
            "remote_write URL must not contain embedded credentials",
        ));
    }

    let tenant_header = tenant_header.trim();
    if !tenant_header.is_empty() {
        let name = HeaderName::from_bytes(tenant_header.as_bytes())
            .map_err(|e| Error::config(format!("invalid remote_write tenant header: {e}")))?;
        if is_reserved_header(&name) {
            return Err(Error::config(format!(
                "remote_write tenant header {:?} conflicts with a protocol header",
                name.as_str()
            )));
        }
    }
    Ok(())
}

impl BatchSink for RemoteWriteSink {
    fn write(&mut self, _unit: u64, batch: &mut Batch) -> Result<()> {
        let request = build_write_request(batch)?;
        if request.timeseries.is_empty() {
            batch.clear();
            return Ok(());
        }
        let protobuf = request.encode_to_vec();
        let body = Bytes::from(
            Encoder::new()
                .compress_vec(&protobuf)
                .map_err(|e| Error::sink(format!("compress remote_write request: {e}")))?,
        );
        self.send(body)?;
        batch.clear();
        Ok(())
    }

    fn finish_unit(&mut self, _unit: u64) -> Result<()> {
        Ok(())
    }

    fn end_block(&mut self) -> Result<SinkOutcome> {
        Ok(SinkOutcome::default())
    }
}

fn is_reserved_header(name: &HeaderName) -> bool {
    // RFC 9110 hop-by-hop headers plus everything the protocol itself owns.
    if name.as_str().starts_with("x-prometheus-remote-write-") {
        return true;
    }
    matches!(
        name.as_str(),
        "content-encoding"
            | "content-type"
            | "content-length"
            | "user-agent"
            | "authorization"
            | "retry-after"
            | "host"
            | "connection"
            | "keep-alive"
            | "transfer-encoding"
            | "te"
            | "trailer"
            | "upgrade"
            | "expect"
            | "proxy-authorization"
            | "proxy-authenticate"
    )
}

fn retry_after(response: &Response) -> Option<Duration> {
    let raw = response.headers().get(RETRY_AFTER)?.to_str().ok()?;
    if let Ok(seconds) = raw.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    // RFC 9110 also allows an IMF-fixdate, e.g. "Wed, 21 Oct 2015 07:28:00 GMT".
    let at = chrono::DateTime::parse_from_rfc2822(raw).ok()?;
    let delta = at.with_timezone(&chrono::Utc) - chrono::Utc::now();
    Some(Duration::from_millis(delta.num_milliseconds().max(0) as u64))
}

fn response_error(response: Response) -> Error {
    let status = response.status();
    let mut body = Vec::new();
    let _ = response.take(2048).read_to_end(&mut body);
    let body = String::from_utf8_lossy(&body);
    let body = body.trim();
    if body.is_empty() {
        Error::sink(format!("remote_write endpoint returned HTTP {status}"))
    } else {
        Error::sink(format!(
            "remote_write endpoint returned HTTP {status}: {body}"
        ))
    }
}

fn build_write_request(batch: &Batch) -> Result<WriteRequest> {
    if !batch.hists.is_empty() {
        return Err(Error::sink(
            "Prometheus Remote Write v1.0 does not support native histogram samples",
        ));
    }

    let mut timeseries = Vec::with_capacity(batch.series.len());
    let mut indices = HashMap::with_capacity(batch.series.len());
    for row in &batch.series {
        let mut labels = row
            .labels
            .iter()
            .map(|(name, value)| {
                if !is_valid_label_name(name) {
                    return Err(Error::sink(format!(
                        "invalid Remote Write label name {name:?}"
                    )));
                }
                if value.is_empty() {
                    return Err(Error::sink(format!(
                        "Remote Write label {name:?} has an empty value"
                    )));
                }
                if name.as_ref() == "__name__" && !is_valid_metric_name(value) {
                    return Err(Error::sink(format!(
                        "invalid Remote Write metric name {value:?}"
                    )));
                }
                Ok(RemoteLabel {
                    name: name.to_string(),
                    value: value.to_string(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        labels.sort_by(|left, right| left.name.cmp(&right.name));
        if labels.windows(2).any(|pair| pair[0].name == pair[1].name) {
            return Err(Error::sink("duplicate Remote Write label name"));
        }

        let index = timeseries.len();
        if indices.insert(row.uid, index).is_some() {
            return Err(Error::sink("duplicate series UID in Remote Write batch"));
        }
        timeseries.push(RemoteTimeSeries {
            labels,
            samples: Vec::new(),
        });
    }

    for row in &batch.floats {
        let Some(index) = indices.get(&row.uid).copied() else {
            return Err(Error::sink(format!(
                "Remote Write sample has no series labels (uid={})",
                row.uid
            )));
        };
        timeseries[index].samples.push(RemoteSample {
            value: row.value,
            timestamp: row.ts,
        });
    }

    timeseries.retain(|series| !series.samples.is_empty());
    for series in &mut timeseries {
        series.samples.sort_by_key(|sample| sample.timestamp);
    }

    Ok(WriteRequest { timeseries })
}

fn is_valid_label_name(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn is_valid_metric_name(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_' || c == ':')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ':')
}

#[derive(Clone, PartialEq, Message)]
struct WriteRequest {
    #[prost(message, repeated, tag = "1")]
    timeseries: Vec<RemoteTimeSeries>,
}

#[derive(Clone, PartialEq, Message)]
struct RemoteTimeSeries {
    #[prost(message, repeated, tag = "1")]
    labels: Vec<RemoteLabel>,
    #[prost(message, repeated, tag = "2")]
    samples: Vec<RemoteSample>,
}

#[derive(Clone, PartialEq, Message)]
struct RemoteLabel {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(string, tag = "2")]
    value: String,
}

#[derive(Clone, PartialEq, Message)]
struct RemoteSample {
    #[prost(double, tag = "1")]
    value: f64,
    #[prost(int64, tag = "2")]
    timestamp: i64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::{FloatRow, HistRow, SeriesRow};
    use snap::raw::Decoder;
    use std::io::Write;
    use std::net::TcpListener;

    #[test]
    fn request_sorts_labels_and_samples_and_uses_snappy_block_encoding() {
        let mut batch = Batch::new();
        batch.push_series(SeriesRow {
            uid: 7,
            series_ref: 0,
            labels: vec![
                ("z".into(), "last".into()),
                ("__name__".into(), "demo_metric".into()),
                ("a".into(), "first".into()),
            ],
        });
        batch.push_float(FloatRow {
            uid: 7,
            ts: 20,
            value: 2.0,
        });
        batch.push_float(FloatRow {
            uid: 7,
            ts: 10,
            value: 1.0,
        });

        let request = build_write_request(&batch).unwrap();
        let protobuf = request.encode_to_vec();
        let compressed = Encoder::new().compress_vec(&protobuf).unwrap();
        let decoded = Decoder::new().decompress_vec(&compressed).unwrap();
        let decoded = WriteRequest::decode(decoded.as_slice()).unwrap();

        assert_eq!(
            decoded.timeseries[0]
                .labels
                .iter()
                .map(|label| label.name.as_str())
                .collect::<Vec<_>>(),
            vec!["__name__", "a", "z"]
        );
        assert_eq!(
            decoded.timeseries[0]
                .samples
                .iter()
                .map(|sample| sample.timestamp)
                .collect::<Vec<_>>(),
            vec![10, 20]
        );
    }

    #[test]
    fn rejects_native_histograms_in_remote_write_v1() {
        let mut batch = Batch::new();
        batch.push_hist(HistRow {
            uid: 7,
            ts: 1,
            st: 0,
            count: 1.0,
            zero_count: 0.0,
            sum: 1.0,
            schema: 0,
            zero_threshold: 0.0,
            pos_spans: Vec::new(),
            neg_spans: Vec::new(),
            pos_buckets: Vec::new(),
            neg_buckets: Vec::new(),
            custom_values: Vec::new(),
            reset_hint: 0,
        });

        let error = match build_write_request(&batch) {
            Err(error) => error,
            Ok(_) => panic!("native histogram was accepted by Remote Write v1.0"),
        };
        assert!(error
            .to_string()
            .contains("does not support native histogram"));
    }

    #[test]
    fn validates_endpoint_and_tenant_header() {
        assert!(validate_config("https://receiver.example/api/v1/write", "X-Scope-OrgID").is_ok());
        assert!(validate_config("ftp://receiver.example/write", "X-Scope-OrgID").is_err());
        assert!(validate_config("https://receiver.example/write", "Content-Type").is_err());
    }

    #[test]
    fn sends_remote_write_request_to_endpoint() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut request = Vec::new();
            let mut buffer = [0; 1024];
            loop {
                let read = stream.read(&mut buffer).unwrap();
                assert!(read > 0);
                request.extend_from_slice(&buffer[..read]);
                let Some(header_end) = request.windows(4).position(|w| w == b"\r\n\r\n") else {
                    continue;
                };
                let headers = String::from_utf8_lossy(&request[..header_end]);
                let content_length = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().ok())
                            .flatten()
                    })
                    .expect("Content-Length header");
                if request.len() >= header_end + 4 + content_length {
                    break;
                }
            }
            stream
                .write_all(
                    b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .unwrap();
            request
        });

        let mut sink = RemoteWriteSink::new(
            &format!("http://{address}/api/v1/write"),
            "X-Scope-OrgID",
            "tenant-a",
        )
        .unwrap();
        let mut batch = Batch::new();
        batch.push_series(SeriesRow {
            uid: 3,
            series_ref: 0,
            labels: vec![("__name__".into(), "test_metric".into())],
        });
        batch.push_float(FloatRow {
            uid: 3,
            ts: 123,
            value: 1.5,
        });
        sink.write(0, &mut batch).unwrap();

        let request = server.join().unwrap();
        let header_end = request.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
        let headers = String::from_utf8_lossy(&request[..header_end]).to_ascii_lowercase();
        assert!(headers.starts_with("post /api/v1/write http/1.1"));
        assert!(headers.contains("content-encoding: snappy"));
        assert!(headers.contains("content-type: application/x-protobuf"));
        assert!(headers.contains("x-prometheus-remote-write-version: 0.1.0"));
        assert!(headers.contains("x-scope-orgid: tenant-a"));

        let body = &request[header_end + 4..];
        let protobuf = Decoder::new().decompress_vec(body).unwrap();
        let decoded = WriteRequest::decode(protobuf.as_slice()).unwrap();
        assert_eq!(decoded.timeseries[0].samples[0].timestamp, 123);
        assert_eq!(decoded.timeseries[0].samples[0].value, 1.5);
        assert!(batch.is_empty());
    }
}

//! Black-box tests for the Prometheus Remote Write v1.0 sink.

#[path = "support/mod.rs"]
mod support;

use std::net::TcpListener;
use std::time::{Duration, Instant};

use migrate_core::batch::{Batch, HistRow};
use migrate_core::sink::remote_write::{validate_config, RemoteWriteSink};
use migrate_core::sink::BatchSink;
use support::{float, one_series_batch, series, Receiver, Reply};

// ---------------------------------------------------------------------------
// Happy paths.
// ---------------------------------------------------------------------------

#[test]
fn writes_a_spec_compliant_request() {
    let receiver = Receiver::start(vec![Reply::status(204)]);
    let mut sink =
        RemoteWriteSink::new(&receiver.url("/api/v1/write"), "X-Scope-OrgID", "tenant-a")
            .expect("sink");

    let mut batch = Batch::new();
    batch.push_series(series(
        1,
        &[("z", "last"), ("__name__", "demo_metric"), ("a", "first")],
    ));
    batch.push_float(float(1, 20, 2.0));
    batch.push_float(float(1, 10, 1.0));
    sink.write(0, &mut batch).unwrap();

    let requests = receiver.requests();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];

    assert_eq!(request.method, "POST");
    assert_eq!(request.path, "/api/v1/write");
    assert_eq!(request.header("content-encoding"), Some("snappy"));
    assert_eq!(
        request.header("content-type"),
        Some("application/x-protobuf")
    );
    assert_eq!(
        request.header("x-prometheus-remote-write-version"),
        Some("0.1.0")
    );
    let user_agent = request.header("user-agent").unwrap_or_default();
    assert!(
        user_agent.contains("tsdb-migrate"),
        "user-agent must identify the sender, got {user_agent:?}"
    );
    assert_eq!(request.header("x-scope-orgid"), Some("tenant-a"));

    // Block snappy starts with the uncompressed length; the framed format starts
    // with the stream identifier 0xff 0x06 0x00 0x00.
    assert_ne!(&request.body[..4], &[0xff, 0x06, 0x00, 0x00]);
    let decoded = request.decode();
    assert_eq!(decoded.timeseries.len(), 1);
    let labels: Vec<&str> = decoded.timeseries[0]
        .labels
        .iter()
        .map(|label| label.name.as_str())
        .collect();
    assert_eq!(labels, vec!["__name__", "a", "z"], "labels must be sorted");
    let timestamps: Vec<i64> = decoded.timeseries[0]
        .samples
        .iter()
        .map(|sample| sample.timestamp)
        .collect();
    assert_eq!(timestamps, vec![10, 20], "samples must be time ordered");
    assert!(batch.is_empty(), "batch must be released after a write");
}

#[test]
fn accepts_the_whole_2xx_family() {
    for status in [200u16, 201, 204, 299] {
        let receiver = Receiver::start(vec![Reply::status(status)]);
        let mut sink = RemoteWriteSink::new(&receiver.url("/write"), "", "").unwrap();
        sink.write(0, &mut one_series_batch())
            .unwrap_or_else(|e| panic!("status {status} must be treated as success: {e}"));
        assert_eq!(receiver.request_count(), 1);
    }
}

#[test]
fn omits_the_tenant_header_when_no_tenant_is_configured() {
    let receiver = Receiver::start(vec![Reply::status(204)]);
    let mut sink = RemoteWriteSink::new(&receiver.url("/write"), "X-Scope-OrgID", "").unwrap();
    sink.write(0, &mut one_series_batch()).unwrap();
    assert_eq!(receiver.requests()[0].header("x-scope-orgid"), None);
}

#[test]
fn sends_every_batch_in_order() {
    let receiver = Receiver::start(vec![Reply::status(204)]);
    let mut sink = RemoteWriteSink::new(&receiver.url("/write"), "", "").unwrap();

    for window in 0..3i64 {
        let mut batch = Batch::new();
        batch.push_series(series(1, &[("__name__", "counter_total")]));
        batch.push_float(float(1, window * 1000, window as f64));
        sink.write(window as u64, &mut batch).unwrap();
    }

    let requests = receiver.requests();
    assert_eq!(requests.len(), 3);
    let stamps: Vec<i64> = requests
        .iter()
        .map(|request| request.decode().timeseries[0].samples[0].timestamp)
        .collect();
    assert_eq!(stamps, vec![0, 1000, 2000]);
}

#[test]
fn handles_a_large_payload() {
    let receiver = Receiver::start(vec![Reply::status(204)]);
    let mut sink = RemoteWriteSink::new(&receiver.url("/write"), "", "").unwrap();

    let mut batch = Batch::new();
    for uid in 0..2_000u64 {
        batch.push_series(series(
            uid,
            &[
                ("__name__", "big_metric"),
                ("shard", "0"),
                ("instance", "host"),
            ],
        ));
        for point in 0..10i64 {
            batch.push_float(float(uid, point, point as f64));
        }
    }
    let inputs = batch.samples();
    sink.write(0, &mut batch).unwrap();

    let requests = receiver.requests();
    assert_eq!(requests.len(), 1);
    let decoded = requests[0].decode();
    assert_eq!(decoded.timeseries.len(), 2_000);
    let samples: usize = decoded.timeseries.iter().map(|ts| ts.samples.len()).sum();
    assert_eq!(samples, inputs);
    assert_eq!(samples, 20_000);
}

#[test]
fn skips_requests_that_carry_no_samples() {
    let receiver = Receiver::start(vec![Reply::status(204)]);
    let mut sink = RemoteWriteSink::new(&receiver.url("/write"), "", "").unwrap();

    let mut batch = Batch::new();
    batch.push_series(series(1, &[("__name__", "empty_metric")]));
    sink.write(0, &mut batch).unwrap();

    assert_eq!(receiver.request_count(), 0, "nothing to send");
    assert!(batch.is_empty(), "batch must still be released");
}

#[test]
fn round_trips_utf8_and_awkward_label_values() {
    let receiver = Receiver::start(vec![Reply::status(204)]);
    let mut sink = RemoteWriteSink::new(&receiver.url("/write"), "", "").unwrap();

    let mut batch = Batch::new();
    batch.push_series(series(
        1,
        &[
            ("__name__", "weird_total"),
            ("path", "/a b/c?d=e&f"),
            ("city", "北京·café"),
            ("quote", "a\"b'c"),
        ],
    ));
    batch.push_float(float(1, 1, 1.0));
    sink.write(0, &mut batch).unwrap();

    let decoded = receiver.requests()[0].decode();
    let labels = decoded.timeseries[0].labels();
    assert!(labels.contains(&("city", "北京·café")));
    assert!(labels.contains(&("quote", "a\"b'c")));
}

// ---------------------------------------------------------------------------
// Retry / status handling.
// ---------------------------------------------------------------------------

#[test]
fn retries_on_5xx_then_succeeds() {
    let receiver = Receiver::start(vec![Reply::status(503), Reply::status(204)]);
    let mut sink = RemoteWriteSink::new(&receiver.url("/write"), "", "").unwrap();

    let started = Instant::now();
    sink.write(0, &mut one_series_batch()).unwrap();
    let elapsed = started.elapsed();

    let requests = receiver.requests();
    assert_eq!(requests.len(), 2, "one retry expected");
    assert!(
        elapsed >= Duration::from_millis(150),
        "backoff missing: {elapsed:?}"
    );
    assert_eq!(
        requests[0].body, requests[1].body,
        "retry must replay the payload"
    );
}

#[test]
fn gives_up_after_max_attempts_on_persistent_5xx() {
    let receiver = Receiver::start(vec![Reply::status(500); 5]);
    let mut sink = RemoteWriteSink::new(&receiver.url("/write"), "", "").unwrap();

    let error = sink.write(0, &mut one_series_batch()).unwrap_err();
    let message = error.to_string();
    assert!(message.contains("500"), "unexpected message: {message}");
    assert!(message.contains('5'), "attempt count missing: {message}");
    assert_eq!(receiver.request_count(), 5);
}

#[test]
fn does_not_retry_on_client_errors() {
    let receiver = Receiver::start(vec![Reply::body(400, "out of order sample")]);
    let mut sink = RemoteWriteSink::new(&receiver.url("/write"), "", "").unwrap();

    let error = sink.write(0, &mut one_series_batch()).unwrap_err();
    let message = error.to_string();
    assert!(message.contains("400"), "unexpected message: {message}");
    assert!(
        message.contains("out of order sample"),
        "receiver message should be surfaced: {message}"
    );
    assert_eq!(receiver.request_count(), 1, "4xx must not be retried");
}

#[test]
fn retries_429_and_honours_retry_after() {
    let receiver = Receiver::start(vec![
        Reply::status(429).header("Retry-After", "1"),
        Reply::status(204),
    ]);
    let mut sink = RemoteWriteSink::new(&receiver.url("/write"), "", "").unwrap();

    let started = Instant::now();
    sink.write(0, &mut one_series_batch()).unwrap();
    let elapsed = started.elapsed();

    assert_eq!(receiver.request_count(), 2);
    assert!(
        elapsed >= Duration::from_millis(950),
        "Retry-After was ignored: {elapsed:?}"
    );
}

#[test]
fn treats_a_past_retry_after_date_as_retry_now() {
    // A date in the past means "you may retry immediately"; it must not be read
    // as a huge negative delay, and it still keeps the 100 ms floor.
    let receiver = Receiver::start(vec![
        Reply::status(503).header("Retry-After", "Wed, 21 Oct 2015 07:28:00 GMT"),
        Reply::status(204),
    ]);
    let mut sink = RemoteWriteSink::new(&receiver.url("/write"), "", "").unwrap();

    let started = Instant::now();
    sink.write(0, &mut one_series_batch()).unwrap();
    let elapsed = started.elapsed();

    assert_eq!(receiver.request_count(), 2);
    assert!(
        elapsed >= Duration::from_millis(90),
        "the retry floor was not applied: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(2),
        "a past date must not block: {elapsed:?}"
    );
}

#[test]
fn honours_a_future_retry_after_date() {
    // IMF-fixdate has one-second resolution, so aim far enough ahead that the
    // truncated value is still clearly in the future.
    let when = chrono::Utc::now() + chrono::Duration::milliseconds(2_600);
    let header = when.format("%a, %d %b %Y %H:%M:%S GMT").to_string();

    let receiver = Receiver::start(vec![
        Reply::status(503).header("Retry-After", header),
        Reply::status(204),
    ]);
    let mut sink = RemoteWriteSink::new(&receiver.url("/write"), "", "").unwrap();

    let started = Instant::now();
    sink.write(0, &mut one_series_batch()).unwrap();
    let elapsed = started.elapsed();

    assert_eq!(receiver.request_count(), 2);
    assert!(
        elapsed >= Duration::from_millis(1_500),
        "an HTTP-date Retry-After must be honoured: {elapsed:?}"
    );
}

#[test]
fn a_zero_retry_after_still_waits_before_retrying() {
    let receiver = Receiver::start(vec![
        Reply::status(429).header("Retry-After", "0"),
        Reply::status(204),
    ]);
    let mut sink = RemoteWriteSink::new(&receiver.url("/write"), "", "").unwrap();

    let started = Instant::now();
    sink.write(0, &mut one_series_batch()).unwrap();
    let elapsed = started.elapsed();

    assert_eq!(receiver.request_count(), 2);
    assert!(
        elapsed >= Duration::from_millis(90),
        "Retry-After: 0 must not become a hot loop: {elapsed:?}"
    );
}

#[test]
fn does_not_follow_redirects() {
    let receiver = Receiver::start(vec![Reply::status(307)]);
    let mut sink = RemoteWriteSink::new(&receiver.url("/write"), "", "").unwrap();

    let error = sink.write(0, &mut one_series_batch()).unwrap_err();
    assert!(error.to_string().contains("307"), "{error}");
    assert_eq!(receiver.request_count(), 1);
}

#[test]
fn retries_connection_failures_before_giving_up() {
    let address = {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap()
    };

    let mut sink = RemoteWriteSink::new(&format!("http://{address}/write"), "", "").unwrap();
    let started = Instant::now();
    let error = sink.write(0, &mut one_series_batch()).unwrap_err();
    assert!(
        error.to_string().contains("attempts"),
        "unexpected message: {error}"
    );
    assert!(
        started.elapsed() >= Duration::from_millis(1_500),
        "retries did not back off: {:?}",
        started.elapsed()
    );
}

#[test]
fn treats_a_broken_response_as_a_failure() {
    let receiver = Receiver::start(vec![
        Reply::status(200).partial_headers(),
        Reply::status(204),
        Reply::status(204),
        Reply::status(204),
        Reply::status(204),
    ]);
    let mut sink = RemoteWriteSink::new(&receiver.url("/write"), "", "").unwrap();

    sink.write(0, &mut one_series_batch())
        .expect("a response cut off mid-head must be retried, not accepted");
    assert_eq!(receiver.request_count(), 2);
}

#[test]
fn ignores_the_response_body() {
    // The spec reserves the response body for future use and requires clients to
    // ignore it, so a 2xx with a chatty body is still a success.
    let receiver = Receiver::start(vec![Reply::body(200, "{\"status\":\"ok\"}")]);
    let mut sink = RemoteWriteSink::new(&receiver.url("/write"), "", "").unwrap();
    sink.write(0, &mut one_series_batch()).unwrap();
    assert_eq!(receiver.request_count(), 1);
}

// ---------------------------------------------------------------------------
// Payload validation.
// ---------------------------------------------------------------------------

#[test]
fn rejects_native_histograms() {
    let receiver = Receiver::start(vec![Reply::status(204)]);
    let mut sink = RemoteWriteSink::new(&receiver.url("/write"), "", "").unwrap();

    let mut batch = Batch::new();
    batch.push_series(series(1, &[("__name__", "hist_metric")]));
    batch.push_hist(HistRow {
        uid: 1,
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

    let error = sink.write(0, &mut batch).unwrap_err();
    assert!(error.to_string().contains("histogram"), "{error}");
    assert_eq!(receiver.request_count(), 0);
}

#[test]
fn rejects_invalid_label_names() {
    let receiver = Receiver::start(vec![Reply::status(204)]);
    let mut sink = RemoteWriteSink::new(&receiver.url("/write"), "", "").unwrap();

    for name in ["bad-name", "1leading", "with space", ""] {
        let mut batch = Batch::new();
        batch.push_series(series(1, &[("__name__", "m"), (name, "v")]));
        batch.push_float(float(1, 1, 1.0));
        let error = sink.write(0, &mut batch).unwrap_err();
        assert!(
            error.to_string().contains("label"),
            "label {name:?} should be rejected: {error}"
        );
    }
    assert_eq!(receiver.request_count(), 0);
}

#[test]
fn rejects_invalid_metric_names() {
    let receiver = Receiver::start(vec![Reply::status(204)]);
    let mut sink = RemoteWriteSink::new(&receiver.url("/write"), "", "").unwrap();

    for name in ["1bad", "bad-name", "has space"] {
        let mut batch = Batch::new();
        batch.push_series(series(1, &[("__name__", name)]));
        batch.push_float(float(1, 1, 1.0));
        assert!(
            sink.write(0, &mut batch).is_err(),
            "metric {name:?} should be rejected"
        );
    }
    assert_eq!(receiver.request_count(), 0);
}

#[test]
fn rejects_empty_label_values_and_duplicate_names() {
    let receiver = Receiver::start(vec![Reply::status(204)]);
    let mut sink = RemoteWriteSink::new(&receiver.url("/write"), "", "").unwrap();

    let mut batch = Batch::new();
    batch.push_series(series(1, &[("__name__", "m"), ("job", "")]));
    batch.push_float(float(1, 1, 1.0));
    assert!(sink.write(0, &mut batch).is_err(), "empty value must fail");

    let mut batch = Batch::new();
    batch.push_series(series(1, &[("__name__", "m"), ("job", "a"), ("job", "b")]));
    batch.push_float(float(1, 1, 1.0));
    assert!(
        sink.write(0, &mut batch).is_err(),
        "duplicate label must fail"
    );

    assert_eq!(receiver.request_count(), 0);
}

#[test]
fn rejects_samples_without_series_labels() {
    let receiver = Receiver::start(vec![Reply::status(204)]);
    let mut sink = RemoteWriteSink::new(&receiver.url("/write"), "", "").unwrap();

    let mut batch = Batch::new();
    batch.push_float(float(9, 1, 1.0));
    assert!(sink.write(0, &mut batch).is_err());
    assert_eq!(receiver.request_count(), 0);
}

#[test]
fn shares_one_connection_across_batches() {
    let receiver = Receiver::start(vec![Reply::status(204)]);
    let mut sink = RemoteWriteSink::new(&receiver.url("/write"), "", "").unwrap();
    for _ in 0..5 {
        sink.write(0, &mut one_series_batch()).unwrap();
    }
    assert_eq!(receiver.request_count(), 5);
}

// ---------------------------------------------------------------------------
// Configuration validation.
// ---------------------------------------------------------------------------

#[test]
fn validates_endpoint_and_tenant_header() {
    assert!(validate_config("https://receiver.example/api/v1/write", "X-Scope-OrgID").is_ok());
    assert!(validate_config("http://127.0.0.1:9009/write", "").is_ok());
    assert!(validate_config("ftp://receiver.example/write", "").is_err());
    assert!(validate_config("receiver.example/write", "").is_err());
    assert!(validate_config("https://user:pass@receiver.example/write", "").is_err());
    assert!(validate_config("https://receiver.example/write", "Content-Type").is_err());
    assert!(validate_config("https://receiver.example/write", "bad header").is_err());
}

#[test]
fn rejects_endpoint_errors_before_sending() {
    assert!(RemoteWriteSink::new("ftp://receiver.example/write", "", "").is_err());
    assert!(RemoteWriteSink::new("https://user:pass@receiver.example/write", "", "").is_err());
    assert!(
        RemoteWriteSink::new("https://receiver.example/write", "Content-Encoding", "").is_err()
    );
}

#[test]
fn rejects_protocol_and_hop_by_hop_headers_as_tenant_header() {
    for header in [
        "X-Prometheus-Remote-Write-Version",
        "x-prometheus-remote-write-timeout",
        "Connection",
        "Transfer-Encoding",
        "Expect",
    ] {
        assert!(
            validate_config("https://receiver.example/write", header).is_err(),
            "{header} must not be usable as the tenant header"
        );
    }
}

#[test]
fn tenant_value_is_forwarded_verbatim() {
    let receiver = Receiver::start(vec![Reply::status(204)]);
    let mut sink =
        RemoteWriteSink::new(&receiver.url("/write"), "X-Scope-OrgID", "tenant/a-1").unwrap();
    sink.write(0, &mut one_series_batch()).unwrap();
    assert_eq!(
        receiver.requests()[0].header("x-scope-orgid"),
        Some("tenant/a-1")
    );
}

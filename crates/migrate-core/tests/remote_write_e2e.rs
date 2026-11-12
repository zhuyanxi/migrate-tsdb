//! End-to-end tests: real TSDB block -> pipeline -> Remote Write HTTP endpoint.
//!
//! These tests are skipped when `fixtures/gen/data/block` has not been built
//! (`./scripts/dev-fixtures.sh` produces it).

#[path = "support/mod.rs"]
mod support;

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use migrate_core::config::{
    EngineOptions, FilterConfig, JobConfig, LabelMatcher, SinkConfig, SinkKind,
};
use migrate_core::engine::{Engine, EventSink, LogLevel};
use migrate_core::{Control, JobState};
use support::Receiver;

#[derive(Default)]
struct RecordingEvents {
    lines: Mutex<Vec<(LogLevel, String)>>,
}

impl EventSink for RecordingEvents {
    fn log(&self, level: LogLevel, msg: &str) {
        self.lines.lock().unwrap().push((level, msg.to_string()));
    }
}

impl RecordingEvents {
    fn has(&self, needle: &str) -> bool {
        self.lines
            .lock()
            .unwrap()
            .iter()
            .any(|(_, line)| line.contains(needle))
    }
}

fn fixture_block() -> Option<PathBuf> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/gen/data/block");
    path.is_dir().then_some(path)
}

fn scratch_dir(tag: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "tsdb-rw-e2e-{tag}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&path).unwrap();
    path
}

fn config(block: &Path, out: &Path, url: &str, matcher: Option<&str>) -> JobConfig {
    JobConfig {
        name: "rw-e2e".to_string(),
        data_dir: block.to_path_buf(),
        output_dir: out.to_path_buf(),
        filter: FilterConfig {
            matchers: matcher
                .map(|metric| {
                    vec![LabelMatcher {
                        name: "__name__".to_string(),
                        value: metric.to_string(),
                    }]
                })
                .unwrap_or_default(),
            ..Default::default()
        },
        sink: SinkConfig {
            kind: SinkKind::RemoteWrite,
            remote_write_url: url.to_string(),
            remote_write_tenant_header: "X-Scope-OrgID".to_string(),
            ..Default::default()
        },
        options: EngineOptions {
            block_parallel: 4,
            decode_workers: 3,
            unit_series: 2,
            batch_samples: 1024,
            ..Default::default()
        },
        ..Default::default()
    }
}

#[test]
fn pushes_a_real_block_to_a_remote_write_endpoint() {
    let Some(block) = fixture_block() else {
        eprintln!("skipping: fixtures/gen/data/block is not built");
        return;
    };

    let receiver = Receiver::start(vec![]);
    let out = scratch_dir("push");
    let cfg = config(
        &block,
        &out,
        &receiver.url("/api/v1/write"),
        Some("fixture_metric_00"),
    );
    let events = Arc::new(RecordingEvents::default());

    let engine = Engine::new(cfg, Arc::new(Control::new()), events.clone()).expect("engine");
    assert_eq!(
        engine.cfg.options.block_parallel, 1,
        "remote write must serialise blocks to keep per-series order"
    );
    assert!(events.has("serializes blocks"));

    let report = engine.run().expect("run");
    assert_eq!(report.state, JobState::Completed);
    assert_eq!(report.failed_blocks, 0);
    assert_eq!(report.totals.samples, 121);

    let requests = receiver.requests();
    assert_eq!(requests.len(), 1, "121 samples fit in a single request");

    let decoded = requests[0].decode();
    assert_eq!(decoded.timeseries.len(), 1);
    let series = &decoded.timeseries[0];
    assert_eq!(series.label("__name__"), Some("fixture_metric_00"));
    assert_eq!(series.label("job"), Some("fixture"));
    assert_eq!(series.label("instance"), Some("host-00"));
    assert_eq!(series.label("shard"), Some("0"));
    assert_eq!(series.samples.len(), 121);

    let timestamps: Vec<i64> = series.samples.iter().map(|s| s.timestamp).collect();
    assert!(
        timestamps.windows(2).all(|w| w[0] < w[1]),
        "samples must be strictly increasing in time"
    );
    assert_eq!(timestamps[0], 1_700_000_000_000);
    assert_eq!(timestamps[120], 1_700_000_000_000 + 120 * 15_000);

    // Remote write leaves no Parquet behind. The block directory only carries
    // the completion marker used by resume.
    let entries: Vec<String> = std::fs::read_dir(out.join("blocks"))
        .unwrap()
        .flatten()
        .flat_map(|block| std::fs::read_dir(block.path()).unwrap().flatten())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(
        entries,
        vec!["_complete.json".to_string()],
        "remote write must not create part files"
    );
    assert!(out.join("manifest.json").exists());

    let _ = std::fs::remove_dir_all(&out);
}

#[test]
fn reports_native_histograms_as_a_block_failure() {
    let Some(block) = fixture_block() else {
        eprintln!("skipping: fixtures/gen/data/block is not built");
        return;
    };

    let receiver = Receiver::start(vec![]);
    let out = scratch_dir("hist");
    let cfg = config(&block, &out, &receiver.url("/api/v1/write"), None);
    let events = Arc::new(RecordingEvents::default());

    let engine = Engine::new(cfg, Arc::new(Control::new()), events.clone()).expect("engine");
    let report = engine.run().expect("run returns a report");

    assert_eq!(report.state, JobState::Failed);
    assert_eq!(report.failed_blocks, 1);
    assert!(
        events.has("histogram"),
        "the failure must name the unsupported sample type"
    );
    // Series of one unit are dealt round-robin to the decode workers, so workers
    // that only hold float series may already have pushed their share. The
    // failure therefore has to be loud: it can leave a partially written remote.
    assert!(
        receiver.total_samples() < report.totals.samples as usize,
        "the run must not deliver every sample"
    );

    let _ = std::fs::remove_dir_all(&out);
}

#[test]
fn rejects_overlapping_blocks_before_starting() {
    let Some(source) = fixture_block() else {
        eprintln!("skipping: fixtures/gen/data/block is not built");
        return;
    };

    // Two identical copies with distinct ULIDs but the same time range.
    let staging = scratch_dir("overlap-src");
    let original = std::fs::read_dir(&source)
        .unwrap()
        .flatten()
        .find(|entry| entry.file_type().map(|t| t.is_dir()).unwrap_or(false))
        .expect("block directory")
        .path();
    for ulid in ["01AAAAAAAAAAAAAAAAAAAAAAA1", "01AAAAAAAAAAAAAAAAAAAAAAA2"] {
        let target = staging.join(ulid);
        std::fs::create_dir_all(&target).unwrap();
        copy_dir(&original, &target);
        let meta_path = target.join("meta.json");
        let mut meta: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&meta_path).unwrap()).unwrap();
        meta["ulid"] = serde_json::Value::String(ulid.to_string());
        std::fs::write(&meta_path, serde_json::to_vec(&meta).unwrap()).unwrap();
    }

    let receiver = Receiver::start(vec![]);
    let out = scratch_dir("overlap");
    let mut cfg = config(&staging, &out, &receiver.url("/api/v1/write"), None);
    cfg.filter = FilterConfig::default();

    let engine = Engine::new(
        cfg,
        Arc::new(Control::new()),
        Arc::new(RecordingEvents::default()),
    );
    let error = match engine {
        Ok(_) => panic!("overlapping blocks must be rejected before the job starts"),
        Err(error) => error.to_string(),
    };
    assert!(error.contains("overlap"), "unexpected error: {error}");
    assert_eq!(receiver.request_count(), 0);

    let _ = std::fs::remove_dir_all(&out);
    let _ = std::fs::remove_dir_all(&staging);
}

#[test]
fn reports_the_sink_kind_in_snapshots() {
    let Some(block) = fixture_block() else {
        eprintln!("skipping: fixtures/gen/data/block is not built");
        return;
    };
    let out = scratch_dir("snapshot");
    let cfg = config(
        &block,
        &out,
        "http://127.0.0.1:9/write",
        Some("fixture_metric_00"),
    );
    let engine = Engine::new(
        cfg,
        Arc::new(Control::new()),
        Arc::new(RecordingEvents::default()),
    )
    .expect("engine");
    let snapshot = engine.stats.snapshot(JobState::Created, None);
    assert_eq!(snapshot.sink_kind, "remote_write");
    let _ = std::fs::remove_dir_all(&out);
}

fn copy_dir(from: &Path, to: &Path) {
    for entry in std::fs::read_dir(from).unwrap().flatten() {
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            std::fs::create_dir_all(&target).unwrap();
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).unwrap();
        }
    }
}

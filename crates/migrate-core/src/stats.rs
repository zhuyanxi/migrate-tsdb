//! Progress accounting shared by the CLI and HTTP API.
//!
//! All hot-path counters are atomics owned by a block; job totals are summed
//! lazily when a snapshot is requested (a few thousand atomics is cheap).

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};

use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};

use crate::config::{EngineOptions, JobConfig, SinkKind};
use crate::control::JobState;
use crate::planner::PlannedBlock;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BlockStatus {
    Pending,
    Running,
    Paused,
    Completed,
    Failed,
    Skipped,
}

#[derive(Debug)]
struct BlockRuntime {
    status: BlockStatus,
    error: Option<String>,
    started_ms: Option<i64>,
    finished_ms: Option<i64>,
}

#[derive(Debug)]
pub struct BlockStats {
    /// Internal identity: `tenant/ulid` for Mimir, `ulid` for local.
    pub key: String,
    pub ulid: String,
    pub tenant: Option<String>,
    pub path: String,
    pub min_time: i64,
    pub max_time: i64,
    pub series_total: u64,
    pub chunks_total: u64,
    pub samples_expected: u64,
    pub units_total: u64,

    pub series_done: AtomicU64,
    pub chunks_done: AtomicU64,
    pub units_done: AtomicU64,
    pub floats_done: AtomicU64,
    pub histograms_done: AtomicU64,
    pub skipped_tombstone: AtomicU64,
    pub skipped_range: AtomicU64,
    pub series_skipped_filter: AtomicU64,
    pub bytes_read: AtomicU64,
    pub bytes_written: AtomicU64,
    pub crc_errors: AtomicU64,
    pub decode_errors: AtomicU64,
    pub unknown_encodings: AtomicU64,
    pub current_unit: AtomicU64,
    pub next_offset: AtomicU64,

    runtime: Mutex<BlockRuntime>,
}

impl BlockStats {
    pub fn new(p: &PlannedBlock, unit_series: usize) -> Self {
        let units = if p.stats.series == 0 {
            0
        } else {
            (p.stats.series + unit_series as u64 - 1) / unit_series as u64
        };
        Self {
            key: p.info.key(),
            ulid: p.info.meta.ulid.clone(),
            tenant: p.info.tenant_opt().map(str::to_string),
            path: p.info.path.display().to_string(),
            min_time: p.info.meta.min_time,
            max_time: p.info.meta.max_time,
            series_total: p.stats.series,
            chunks_total: p.stats.chunks,
            samples_expected: p.samples_expected,
            units_total: units,
            series_done: AtomicU64::new(0),
            chunks_done: AtomicU64::new(0),
            units_done: AtomicU64::new(0),
            floats_done: AtomicU64::new(0),
            histograms_done: AtomicU64::new(0),
            skipped_tombstone: AtomicU64::new(0),
            skipped_range: AtomicU64::new(0),
            series_skipped_filter: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            crc_errors: AtomicU64::new(0),
            decode_errors: AtomicU64::new(0),
            unknown_encodings: AtomicU64::new(0),
            current_unit: AtomicU64::new(0),
            next_offset: AtomicU64::new(0),
            runtime: Mutex::new(BlockRuntime {
                status: BlockStatus::Pending,
                error: None,
                started_ms: None,
                finished_ms: None,
            }),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BlockRuntime> {
        self.runtime.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn status(&self) -> BlockStatus {
        self.lock().status
    }

    pub fn set_status(&self, status: BlockStatus) {
        let mut rt = self.lock();
        rt.status = status;
        if status == BlockStatus::Running && rt.started_ms.is_none() {
            rt.started_ms = Some(Utc::now().timestamp_millis());
        }
        if matches!(
            status,
            BlockStatus::Completed | BlockStatus::Failed | BlockStatus::Skipped
        ) && rt.finished_ms.is_none()
        {
            rt.finished_ms = Some(Utc::now().timestamp_millis());
        }
    }

    pub fn set_finished_ms(&self, ms: i64) {
        self.lock().finished_ms = Some(ms);
    }

    pub fn set_error(&self, err: impl Into<String>) {
        self.lock().error = Some(err.into());
    }

    pub fn error(&self) -> Option<String> {
        self.lock().error.clone()
    }

    pub fn restore(
        &self,
        series_done: u64,
        chunks_done: u64,
        units_done: u64,
        floats: u64,
        histograms: u64,
        skipped_tombstone: u64,
        skipped_range: u64,
        bytes_read: u64,
        next_offset: u64,
    ) {
        self.series_done.store(series_done, Ordering::Relaxed);
        self.chunks_done.store(chunks_done, Ordering::Relaxed);
        self.units_done.store(units_done, Ordering::Relaxed);
        self.floats_done.store(floats, Ordering::Relaxed);
        self.histograms_done.store(histograms, Ordering::Relaxed);
        self.skipped_tombstone
            .store(skipped_tombstone, Ordering::Relaxed);
        self.skipped_range.store(skipped_range, Ordering::Relaxed);
        self.bytes_read.store(bytes_read, Ordering::Relaxed);
        self.next_offset.store(next_offset, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> BlockSnapshot {
        let rt = self.lock();
        let started = rt.started_ms;
        let finished = rt.finished_ms;
        let duration_ms = match (started, finished) {
            (Some(a), Some(b)) => Some((b - a).max(0) as u64),
            (Some(a), None) => Some((Utc::now().timestamp_millis() - a).max(0) as u64),
            _ => None,
        };
        BlockSnapshot {
            key: self.key.clone(),
            ulid: self.ulid.clone(),
            tenant: self.tenant.clone(),
            path: self.path.clone(),
            min_time: self.min_time,
            max_time: self.max_time,
            status: rt.status,
            error: rt.error.clone(),
            series_total: self.series_total,
            series_done: self.series_done.load(Ordering::Relaxed),
            chunks_total: self.chunks_total,
            chunks_done: self.chunks_done.load(Ordering::Relaxed),
            samples_expected: self.samples_expected,
            floats: self.floats_done.load(Ordering::Relaxed),
            histograms: self.histograms_done.load(Ordering::Relaxed),
            samples_skipped_tombstone: self.skipped_tombstone.load(Ordering::Relaxed),
            samples_skipped_range: self.skipped_range.load(Ordering::Relaxed),
            series_skipped_filter: self.series_skipped_filter.load(Ordering::Relaxed),
            units_total: self.units_total,
            units_done: self.units_done.load(Ordering::Relaxed),
            current_unit: self.current_unit.load(Ordering::Relaxed),
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            crc_errors: self.crc_errors.load(Ordering::Relaxed),
            decode_errors: self.decode_errors.load(Ordering::Relaxed),
            unknown_encodings: self.unknown_encodings.load(Ordering::Relaxed),
            started_at: started.map(ms_to_rfc3339),
            finished_at: finished.map(ms_to_rfc3339),
            duration_ms,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Totals {
    /// Number of distinct Mimir tenants (0 for a local directory source).
    #[serde(default)]
    pub tenants: u64,
    pub series_total: u64,
    pub series_done: u64,
    pub chunks_total: u64,
    pub chunks_done: u64,
    pub samples_expected: u64,
    pub floats: u64,
    pub histograms: u64,
    pub samples: u64,
    pub samples_skipped_tombstone: u64,
    pub samples_skipped_range: u64,
    pub series_skipped_filter: u64,
    pub units_total: u64,
    pub units_done: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub crc_errors: u64,
    pub decode_errors: u64,
    pub unknown_encodings: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RateSnapshot {
    pub samples_per_sec: f64,
    pub bytes_per_sec: f64,
    pub chunks_per_sec: f64,
    pub series_per_sec: f64,
    pub window_secs: f64,
}

#[derive(Debug, Clone, Copy)]
struct RatePoint {
    at_ms: i64,
    samples: u64,
    bytes: u64,
    chunks: u64,
    series: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockSnapshot {
    #[serde(default)]
    pub key: String,
    pub ulid: String,
    #[serde(default)]
    pub tenant: Option<String>,
    pub path: String,
    pub min_time: i64,
    pub max_time: i64,
    pub status: BlockStatus,
    pub error: Option<String>,
    pub series_total: u64,
    pub series_done: u64,
    pub chunks_total: u64,
    pub chunks_done: u64,
    pub samples_expected: u64,
    pub floats: u64,
    pub histograms: u64,
    pub samples_skipped_tombstone: u64,
    pub samples_skipped_range: u64,
    pub series_skipped_filter: u64,
    pub units_total: u64,
    pub units_done: u64,
    pub current_unit: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub crc_errors: u64,
    pub decode_errors: u64,
    pub unknown_encodings: u64,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub duration_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobSnapshot {
    pub job_id: String,
    pub name: String,
    pub state: JobState,
    pub error: Option<String>,
    pub data_dir: String,
    pub output_dir: String,
    pub started_at: String,
    pub elapsed_ms: u64,
    pub totals: Totals,
    pub blocks_total: u64,
    pub blocks_completed: u64,
    pub blocks_failed: u64,
    pub blocks_skipped: u64,
    pub blocks_running: u64,
    pub blocks_pending: u64,
    pub rate: RateSnapshot,
    pub eta_seconds: Option<u64>,
    pub options: EngineOptions,
    pub sink_kind: String,
}

pub struct JobStats {
    pub job_id: String,
    pub config: JobConfig,
    pub started_ms: i64,
    finished_ms: AtomicI64,
    pub blocks: Vec<std::sync::Arc<BlockStats>>,
    pub rate: RwLock<RateSnapshot>,
    rate_points: Mutex<Vec<RatePoint>>,
    pub errors: AtomicU64,
}

impl JobStats {
    pub fn new(
        job_id: String,
        config: JobConfig,
        plan_blocks: &[PlannedBlock],
        started_ms: Option<i64>,
    ) -> Self {
        let blocks = plan_blocks
            .iter()
            .map(|b| std::sync::Arc::new(BlockStats::new(b, config.options.unit_series)))
            .collect();
        Self {
            job_id,
            config,
            started_ms: started_ms.unwrap_or_else(|| Utc::now().timestamp_millis()),
            finished_ms: AtomicI64::new(0),
            blocks,
            rate: RwLock::new(RateSnapshot::default()),
            rate_points: Mutex::new(Vec::new()),
            errors: AtomicU64::new(0),
        }
    }

    pub fn totals(&self) -> Totals {
        let mut t = Totals::default();
        let mut tenants = std::collections::BTreeSet::new();
        for b in &self.blocks {
            if let Some(tenant) = &b.tenant {
                tenants.insert(tenant.clone());
            }
            t.series_total += b.series_total;
            t.series_done += b.series_done.load(Ordering::Relaxed);
            t.chunks_total += b.chunks_total;
            t.chunks_done += b.chunks_done.load(Ordering::Relaxed);
            t.samples_expected += b.samples_expected;
            t.floats += b.floats_done.load(Ordering::Relaxed);
            t.histograms += b.histograms_done.load(Ordering::Relaxed);
            t.samples_skipped_tombstone += b.skipped_tombstone.load(Ordering::Relaxed);
            t.samples_skipped_range += b.skipped_range.load(Ordering::Relaxed);
            t.series_skipped_filter += b.series_skipped_filter.load(Ordering::Relaxed);
            t.units_total += b.units_total;
            t.units_done += b.units_done.load(Ordering::Relaxed);
            t.bytes_read += b.bytes_read.load(Ordering::Relaxed);
            t.bytes_written += b.bytes_written.load(Ordering::Relaxed);
            t.crc_errors += b.crc_errors.load(Ordering::Relaxed);
            t.decode_errors += b.decode_errors.load(Ordering::Relaxed);
            t.unknown_encodings += b.unknown_encodings.load(Ordering::Relaxed);
        }
        t.samples = t.floats + t.histograms;
        t.tenants = tenants.len() as u64;
        t
    }

    /// Samples the counters and refreshes the rolling rate window. Called once
    /// per second by the engine monitor.
    pub fn tick_rates(&self) {
        let t = self.totals();
        let now = Utc::now().timestamp_millis();
        let mut points = self.rate_points.lock().unwrap_or_else(|e| e.into_inner());
        points.push(RatePoint {
            at_ms: now,
            samples: t.samples,
            bytes: t.bytes_read,
            chunks: t.chunks_done,
            series: t.series_done,
        });
        // Keep ~30 seconds, but at least two points.
        while points.len() > 30 {
            points.remove(0);
        }
        let first = points.first().copied();
        let last = points.last().copied();
        if let (Some(first), Some(last)) = (first, last) {
            let dt = (last.at_ms - first.at_ms) as f64 / 1000.0;
            if dt >= 1.0 {
                let rate = RateSnapshot {
                    samples_per_sec: (last.samples.saturating_sub(first.samples)) as f64 / dt,
                    bytes_per_sec: (last.bytes.saturating_sub(first.bytes)) as f64 / dt,
                    chunks_per_sec: (last.chunks.saturating_sub(first.chunks)) as f64 / dt,
                    series_per_sec: (last.series.saturating_sub(first.series)) as f64 / dt,
                    window_secs: dt,
                };
                *self.rate.write().unwrap_or_else(|e| e.into_inner()) = rate;
            }
        }
    }

    pub fn clear_finished(&self) {
        self.finished_ms.store(0, Ordering::Relaxed);
    }

    pub fn mark_finished(&self) {
        self.finished_ms
            .store(Utc::now().timestamp_millis(), Ordering::Relaxed);
    }

    pub fn set_finished_ms(&self, ms: i64) {
        self.finished_ms.store(ms, Ordering::Relaxed);
    }

    pub fn refresh_rate(&self) {
        self.tick_rates();
    }

    pub fn snapshot(&self, state: JobState, error: Option<String>) -> JobSnapshot {
        let t = self.totals();
        let mut completed = 0;
        let mut failed = 0;
        let mut skipped = 0;
        let mut running = 0;
        let mut pending = 0;
        for b in &self.blocks {
            match b.status() {
                BlockStatus::Completed => completed += 1,
                BlockStatus::Failed => failed += 1,
                BlockStatus::Skipped => skipped += 1,
                BlockStatus::Running | BlockStatus::Paused => running += 1,
                BlockStatus::Pending => pending += 1,
            }
        }
        let rate = self.rate.read().unwrap_or_else(|e| e.into_inner()).clone();
        let eta = estimate_eta(&t, &rate);
        JobSnapshot {
            job_id: self.job_id.clone(),
            name: self.config.name.clone(),
            state,
            error,
            data_dir: self.config.data_dir.display().to_string(),
            output_dir: self.config.output_dir.display().to_string(),
            started_at: ms_to_rfc3339(self.started_ms),
            elapsed_ms: {
                let end = match self.finished_ms.load(Ordering::Relaxed) {
                    0 => Utc::now().timestamp_millis(),
                    v => v,
                };
                (end - self.started_ms).max(0) as u64
            },
            totals: t,
            blocks_total: self.blocks.len() as u64,
            blocks_completed: completed,
            blocks_failed: failed,
            blocks_skipped: skipped,
            blocks_running: running,
            blocks_pending: pending,
            rate,
            eta_seconds: eta,
            options: self.config.options.clone(),
            sink_kind: match self.config.sink.kind {
                SinkKind::RemoteWrite => "remote_write".to_string(),
                kind => format!("{kind:?}").to_lowercase(),
            },
        }
    }

    pub fn block_snapshot(&self, key: &str) -> Option<BlockSnapshot> {
        self.blocks
            .iter()
            .find(|b| b.key == key || b.ulid == key)
            .map(|b| b.snapshot())
    }
}

pub fn estimate_eta(t: &Totals, rate: &RateSnapshot) -> Option<u64> {
    // Prefer chunk progress: exact and independent of tombstones.
    if t.chunks_total > 0 && rate.chunks_per_sec > 0.01 {
        let remaining = t.chunks_total.saturating_sub(t.chunks_done);
        return Some((remaining as f64 / rate.chunks_per_sec).ceil() as u64);
    }
    if t.series_total > 0 && rate.series_per_sec > 0.01 {
        let remaining = t.series_total.saturating_sub(t.series_done);
        return Some((remaining as f64 / rate.series_per_sec).ceil() as u64);
    }
    if t.samples_expected > 0 && rate.samples_per_sec > 0.01 {
        let remaining = t.samples_expected.saturating_sub(t.samples);
        return Some((remaining as f64 / rate.samples_per_sec).ceil() as u64);
    }
    None
}

pub fn block_status_str(s: BlockStatus) -> &'static str {
    match s {
        BlockStatus::Pending => "pending",
        BlockStatus::Running => "running",
        BlockStatus::Paused => "paused",
        BlockStatus::Completed => "completed",
        BlockStatus::Failed => "failed",
        BlockStatus::Skipped => "skipped",
    }
}

pub fn ms_to_rfc3339(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let nanos = (ms.rem_euclid(1000) * 1_000_000) as u32;
    Utc.timestamp_opt(secs, nanos)
        .single()
        .unwrap_or_else(Utc::now)
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

pub fn parse_rfc3339_ms(s: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|d| d.timestamp_millis())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eta_prefers_chunks() {
        let t = Totals {
            chunks_total: 100,
            chunks_done: 40,
            ..Default::default()
        };
        let r = RateSnapshot {
            chunks_per_sec: 2.0,
            ..Default::default()
        };
        assert_eq!(estimate_eta(&t, &r), Some(30));
    }

    #[test]
    fn timestamp_format() {
        assert_eq!(ms_to_rfc3339(0), "1970-01-01T00:00:00.000Z");
    }
}

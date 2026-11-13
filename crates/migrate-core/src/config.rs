//! Configuration types shared by the CLI and the HTTP server.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

fn default_block_parallel() -> usize {
    2
}
fn default_decode_workers() -> usize {
    4
}
fn default_batch_samples() -> usize {
    65_536
}
fn default_batch_bytes() -> usize {
    32 * 1024 * 1024
}
fn default_unit_series() -> usize {
    50_000
}
fn default_channel_capacity() -> usize {
    2048
}
fn default_row_group_size() -> usize {
    65_536
}
fn default_name() -> String {
    "migration".to_string()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SinkKind {
    Parquet,
    Ndjson,
    Duckdb,
    #[serde(rename = "remote_write")]
    RemoteWrite,
}

impl Default for SinkKind {
    fn default() -> Self {
        SinkKind::Parquet
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Compression {
    Zstd,
    Snappy,
    None,
}

impl Default for Compression {
    fn default() -> Self {
        Compression::Zstd
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SinkConfig {
    pub kind: SinkKind,
    pub compression: Compression,
    pub row_group_size: usize,
    /// DuckDB output path. For a Prometheus source this is a `.duckdb` file
    /// (default `<out>/migration.duckdb`). For a Mimir source it is a directory
    /// (default `<out>/duckdb/`) containing one `<tenant>.duckdb` per tenant.
    pub duckdb_path: String,
    /// Execute the generated DuckDB load SQL with the `duckdb` CLI.
    pub duckdb_exec: bool,
    /// Full Prometheus Remote Write v1.0 endpoint URL.
    pub remote_write_url: String,
    /// Header used to forward Mimir tenant IDs; empty disables it.
    #[serde(default = "default_remote_write_tenant_header")]
    pub remote_write_tenant_header: String,
}

impl Default for SinkConfig {
    fn default() -> Self {
        Self {
            kind: SinkKind::Parquet,
            compression: Compression::Zstd,
            row_group_size: default_row_group_size(),
            duckdb_path: String::new(),
            duckdb_exec: true,
            remote_write_url: String::new(),
            remote_write_tenant_header: default_remote_write_tenant_header(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LabelMatcher {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct FilterConfig {
    /// Inclusive lower bound in milliseconds.
    pub min_time: Option<i64>,
    /// Inclusive upper bound in milliseconds.
    pub max_time: Option<i64>,
    /// Exact-match label matchers, ANDed together.
    pub matchers: Vec<LabelMatcher>,
}

impl FilterConfig {
    pub fn is_empty(&self) -> bool {
        self.min_time.is_none() && self.max_time.is_none() && self.matchers.is_empty()
    }

    pub fn matches_labels(&self, labels: &[(std::sync::Arc<str>, std::sync::Arc<str>)]) -> bool {
        self.matchers.iter().all(|m| {
            labels
                .iter()
                .any(|(n, v)| n.as_ref() == m.name && v.as_ref() == m.value)
        })
    }

    #[inline]
    pub fn accepts_ts(&self, ts: i64) -> bool {
        self.min_time.map_or(true, |min| ts >= min) && self.max_time.map_or(true, |max| ts <= max)
    }

    /// Whether a chunk can be skipped entirely based on its time range.
    #[inline]
    pub fn chunk_relevant(&self, min_time: i64, max_time: i64) -> bool {
        self.min_time.map_or(true, |min| max_time >= min)
            && self.max_time.map_or(true, |max| min_time <= max)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MimirBackend {
    /// AWS S3 (or an S3-compatible endpoint such as MinIO).
    S3,
    /// A local/NFS directory that mirrors the bucket layout (testing, air-gapped).
    Local,
}

impl Default for MimirBackend {
    fn default() -> Self {
        MimirBackend::S3
    }
}

fn default_region() -> String {
    "us-east-1".to_string()
}
fn default_remote_write_tenant_header() -> String {
    "X-Scope-OrgID".to_string()
}
fn default_downloads() -> usize {
    8
}
fn default_part_mb() -> u64 {
    256
}
fn default_download_retries() -> usize {
    20
}

/// Grafana Mimir block storage on object storage.
///
/// Layout: `<prefix>/<tenant>/<ULID>/{meta.json,index,tombstones,chunks/...}`.
/// Credentials are read from the standard AWS environment variables; they are
/// deliberately not part of this structure so they never reach job state files.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MimirConfig {
    pub backend: MimirBackend,
    /// S3 bucket name, or the bucket root path for `backend=local`.
    pub bucket: String,
    /// Storage prefix inside the bucket (Mimir `-blocks-storage.storage-prefix`).
    pub prefix: String,
    #[serde(default = "default_region")]
    pub region: String,
    /// Custom S3 endpoint (MinIO, Ceph, LocalStack). Empty = AWS default.
    pub endpoint: String,
    /// Exact tenant IDs to migrate. Empty = discover and migrate all tenants.
    pub tenants: Vec<String>,
    /// Local cache for downloaded blocks. Empty = `<output_dir>/.cache`.
    pub cache_dir: String,
    /// Keep downloaded blocks after successful migration (debug / repeat runs).
    pub keep_cache: bool,
    #[serde(default = "default_downloads")]
    pub max_concurrent_downloads: usize,
    /// Ranged-GET part size in MiB. Objects are fetched in parts of this size,
    /// so an interrupted transfer resumes from the last completed byte instead
    /// of restarting the whole object (a 20 GiB `index` needs this).
    #[serde(default = "default_part_mb")]
    pub download_part_mb: u64,
    /// Deadline for a single ranged GET in seconds. `0` disables the request
    /// deadline entirely: object_store defaults to 30 s, which aborts the body
    /// stream of any object that takes longer to transfer.
    pub download_timeout_secs: u64,
    /// Abort a stalled body read after this many seconds without data.
    /// `0` disables the stall timeout (a dead connection then hangs forever).
    pub download_read_timeout_secs: u64,
    /// Consecutive failed attempts **without progress** before an object is
    /// given up on. Any byte of progress resets the counter, so a slow link
    /// that keeps resuming never trips it.
    #[serde(default = "default_download_retries")]
    pub download_retries: usize,
    /// Allow plain HTTP S3 endpoints (MinIO/LocalStack).
    pub allow_http: bool,
    /// Use path-style addressing (`endpoint/bucket/key`) instead of virtual host.
    pub force_path_style: bool,
}

impl Default for MimirConfig {
    fn default() -> Self {
        Self {
            backend: MimirBackend::S3,
            bucket: String::new(),
            prefix: String::new(),
            region: default_region(),
            endpoint: String::new(),
            tenants: Vec::new(),
            cache_dir: String::new(),
            keep_cache: false,
            max_concurrent_downloads: default_downloads(),
            download_part_mb: default_part_mb(),
            download_timeout_secs: 0,
            download_read_timeout_secs: 120,
            download_retries: default_download_retries(),
            allow_http: false,
            force_path_style: false,
        }
    }
}

impl MimirConfig {
    pub fn validate(&self) -> Result<()> {
        if self.bucket.trim().is_empty() {
            return Err(Error::config("mimir.bucket is required"));
        }
        if self.backend == MimirBackend::S3 && self.bucket.contains('/') {
            return Err(Error::config(
                "mimir.bucket must be a bucket name (not a path) for S3",
            ));
        }
        if self.max_concurrent_downloads == 0 || self.max_concurrent_downloads > 64 {
            return Err(Error::config(
                "mimir.max_concurrent_downloads must be in 1..=64",
            ));
        }
        if self.download_part_mb == 0 || self.download_part_mb > 4096 {
            return Err(Error::config("mimir.download_part_mb must be in 1..=4096"));
        }
        if self.download_retries == 0 || self.download_retries > 1000 {
            return Err(Error::config("mimir.download_retries must be in 1..=1000"));
        }
        if self.download_timeout_secs > 7 * 24 * 3600 {
            return Err(Error::config(
                "mimir.download_timeout_secs must be <= 604800 (7 days)",
            ));
        }
        if self.download_read_timeout_secs > 3600 {
            return Err(Error::config(
                "mimir.download_read_timeout_secs must be <= 3600",
            ));
        }
        for t in &self.tenants {
            if !is_valid_tenant(t) {
                return Err(Error::config(format!(
                    "invalid tenant ID {t:?}; allowed: [A-Za-z0-9._-]{{1,128}}"
                )));
            }
        }
        if !self.prefix.is_empty() && self.prefix.contains("..") {
            return Err(Error::config("mimir.prefix must not contain '..'"));
        }
        Ok(())
    }

    /// Normalized storage prefix with exactly one trailing '/', or "".
    pub fn normalized_prefix(&self) -> String {
        let p = self.prefix.trim_matches('/');
        if p.is_empty() {
            String::new()
        } else {
            format!("{p}/")
        }
    }

    /// Ranged-GET part size in bytes (clamped to a sane range).
    pub fn download_part_bytes(&self) -> u64 {
        self.download_part_mb.clamp(1, 4096) * 1024 * 1024
    }

    pub fn cache_root(&self, output_dir: &std::path::Path) -> PathBuf {
        if self.cache_dir.trim().is_empty() {
            output_dir.join(".cache")
        } else {
            let p = PathBuf::from(self.cache_dir.trim());
            if p.is_absolute() {
                p
            } else {
                output_dir.join(p)
            }
        }
    }
}

pub fn is_valid_tenant(t: &str) -> bool {
    !t.is_empty()
        && t.len() <= 128
        && t.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
        && t != "."
        && t != ".."
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct EngineOptions {
    /// Number of blocks processed concurrently.
    pub block_parallel: usize,
    /// Decoder threads per block.
    pub decode_workers: usize,
    /// Flush a batch when it holds at least this many samples.
    pub batch_samples: usize,
    /// Flush a batch when its estimated size exceeds this many bytes.
    pub batch_bytes: usize,
    /// Checkpoint granularity: series per unit.
    pub unit_series: usize,
    /// Bounded series channel capacity per block.
    pub channel_capacity: usize,
    /// Verify CRC32C checksums.
    pub verify_crc: bool,
    /// Apply madvise(SEQUENTIAL) to chunk mappings.
    pub mmap_advise: bool,
    /// Continue on chunk decode errors instead of failing the block.
    pub skip_bad_chunks: bool,
    /// Skip blocks that already have a completion marker.
    pub resume: bool,
    /// Re-migrate blocks even if a completion marker exists.
    pub force: bool,
    /// Continue with remaining blocks when one block fails.
    pub continue_on_error: bool,
}

impl Default for EngineOptions {
    fn default() -> Self {
        Self {
            block_parallel: default_block_parallel(),
            decode_workers: default_decode_workers(),
            batch_samples: default_batch_samples(),
            batch_bytes: default_batch_bytes(),
            unit_series: default_unit_series(),
            channel_capacity: default_channel_capacity(),
            verify_crc: true,
            mmap_advise: true,
            skip_bad_chunks: false,
            resume: true,
            force: false,
            continue_on_error: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct JobConfig {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub data_dir: PathBuf,
    pub output_dir: PathBuf,
    /// Mimir / S3 data source. When set, `data_dir` is ignored.
    #[serde(default)]
    pub mimir: Option<MimirConfig>,
    /// Optional block ULID allow-list.
    #[serde(default)]
    pub blocks: Vec<String>,
    #[serde(default)]
    pub filter: FilterConfig,
    #[serde(default)]
    pub sink: SinkConfig,
    #[serde(default)]
    pub options: EngineOptions,
}

impl Default for JobConfig {
    fn default() -> Self {
        Self {
            name: default_name(),
            data_dir: PathBuf::new(),
            output_dir: PathBuf::new(),
            mimir: None,
            blocks: Vec::new(),
            filter: FilterConfig::default(),
            sink: SinkConfig::default(),
            options: EngineOptions::default(),
        }
    }
}

impl JobConfig {
    pub fn validate(&self) -> Result<()> {
        if self.output_dir.as_os_str().is_empty() {
            return Err(Error::config("output_dir is required"));
        }
        match &self.mimir {
            Some(m) => m.validate()?,
            None => {
                if self.data_dir.as_os_str().is_empty() {
                    return Err(Error::config("data_dir or mimir source is required"));
                }
            }
        }
        let o = &self.options;
        if o.block_parallel == 0 {
            return Err(Error::config("block_parallel must be >= 1"));
        }
        if o.decode_workers == 0 {
            return Err(Error::config("decode_workers must be >= 1"));
        }
        if o.batch_samples < 1024 {
            return Err(Error::config("batch_samples must be >= 1024"));
        }
        if o.batch_bytes < 1024 * 1024 {
            return Err(Error::config("batch_bytes must be >= 1 MiB"));
        }
        if o.unit_series == 0 {
            return Err(Error::config("unit_series must be >= 1"));
        }
        if o.channel_capacity == 0 {
            return Err(Error::config("channel_capacity must be >= 1"));
        }
        if self.sink.row_group_size == 0 {
            return Err(Error::config("row_group_size must be >= 1"));
        }
        if self.sink.kind == SinkKind::RemoteWrite && self.sink.remote_write_url.trim().is_empty() {
            return Err(Error::config(
                "sink.remote_write_url is required for remote_write",
            ));
        }
        if let (Some(min), Some(max)) = (self.filter.min_time, self.filter.max_time) {
            if min > max {
                return Err(Error::config("filter.min_time > filter.max_time"));
            }
        }
        if self.sink.kind == SinkKind::Duckdb && self.sink.duckdb_path.is_empty() {
            // Allowed: default path is derived from output_dir.
        }
        Ok(())
    }

    /// Directory holding per-source-block output.
    pub fn blocks_dir(&self) -> PathBuf {
        self.output_dir.join("blocks")
    }

    pub fn state_dir(&self) -> PathBuf {
        self.output_dir.join(".state")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid_except_paths() {
        let mut c = JobConfig::default();
        assert!(c.validate().is_err());
        c.data_dir = "/tmp/x".into();
        c.output_dir = "/tmp/y".into();
        c.validate().unwrap();
    }

    #[test]
    fn time_filter_inclusive() {
        let f = FilterConfig {
            min_time: Some(10),
            max_time: Some(20),
            matchers: vec![],
        };
        assert!(!f.accepts_ts(9));
        assert!(f.accepts_ts(10));
        assert!(f.accepts_ts(20));
        assert!(!f.accepts_ts(21));
        assert!(!f.chunk_relevant(0, 9));
        assert!(f.chunk_relevant(0, 10));
    }

    #[test]
    fn remote_write_requires_an_endpoint() {
        let mut c = JobConfig::default();
        c.data_dir = "/tmp/input".into();
        c.output_dir = "/tmp/output".into();
        c.sink.kind = SinkKind::RemoteWrite;
        assert!(c.validate().is_err());

        c.sink.remote_write_url = "https://receiver.example/api/v1/write".to_string();
        c.validate().unwrap();
    }
}

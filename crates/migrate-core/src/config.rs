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
    /// Path of the DuckDB database (only for `kind = duckdb`). If empty, a
    /// `migration.duckdb` file is created inside the output directory.
    pub duckdb_path: String,
    /// Execute the generated DuckDB load SQL with the `duckdb` CLI.
    pub duckdb_exec: bool,
}

impl Default for SinkConfig {
    fn default() -> Self {
        Self {
            kind: SinkKind::Parquet,
            compression: Compression::Zstd,
            row_group_size: default_row_group_size(),
            duckdb_path: String::new(),
            duckdb_exec: true,
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
    pub data_dir: PathBuf,
    pub output_dir: PathBuf,
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
            blocks: Vec::new(),
            filter: FilterConfig::default(),
            sink: SinkConfig::default(),
            options: EngineOptions::default(),
        }
    }
}

impl JobConfig {
    pub fn validate(&self) -> Result<()> {
        if self.data_dir.as_os_str().is_empty() {
            return Err(Error::config("data_dir is required"));
        }
        if self.output_dir.as_os_str().is_empty() {
            return Err(Error::config("output_dir is required"));
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
}

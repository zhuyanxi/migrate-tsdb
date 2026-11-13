//! Block discovery and `meta.json` parsing.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
#[cfg(feature = "meta")]
use std::path::Path;

#[cfg(feature = "meta")]
use crate::error::{Error, Result};

fn default_version() -> i32 {
    1
}

/// Sample, series and chunk counters copied from `meta.json`.
///
/// Prometheus writes these at block creation time, so they describe the block
/// *before* tombstones are applied. Blocks written by older versions or by
/// custom tooling may carry zeros; use [`crate::IndexReader::scan_stats`] when
/// an exact count matters.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BlockStats {
    /// Total samples (float plus histogram).
    #[serde(rename = "numSamples", default)]
    pub num_samples: u64,
    /// Float samples.
    #[serde(rename = "numFloatSamples", default)]
    pub num_float_samples: u64,
    /// Native histogram samples.
    #[serde(rename = "numHistogramSamples", default)]
    pub num_histogram_samples: u64,
    /// Series in the block.
    #[serde(rename = "numSeries", default)]
    pub num_series: u64,
    /// Chunks in the block.
    #[serde(rename = "numChunks", default)]
    pub num_chunks: u64,
    /// Number of tombstone intervals recorded at creation time.
    #[serde(rename = "numTombstones", default)]
    pub num_tombstones: u64,
}

/// Compaction lineage recorded in `meta.json`.
///
/// Useful to tell a freshly ingested block from a compacted one, and to know
/// whether Prometheus considers a block deletable.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BlockMetaCompaction {
    /// Compaction level; `0` means a block straight from the head.
    #[serde(default)]
    pub level: i32,
    /// Blocks this one was built from, as ULIDs.
    #[serde(default)]
    pub sources: Vec<String>,
    /// Time ranges that were merged into this block.
    #[serde(default)]
    pub parents: Vec<ParentMeta>,
    /// Prometheus marked the block as safe to delete.
    #[serde(default)]
    pub deletable: bool,
    /// The last compaction attempt failed.
    #[serde(default)]
    pub failed: bool,
    /// Free-form hints written by the compactor.
    #[serde(default)]
    pub hints: Vec<String>,
}

/// One time range that a compacted block was built from.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ParentMeta {
    /// ULID of the source block.
    #[serde(default)]
    pub ulid: String,
    /// Inclusive lower bound in milliseconds since the Unix epoch.
    #[serde(default)]
    pub min_time: i64,
    /// Exclusive upper bound in milliseconds since the Unix epoch.
    #[serde(default)]
    pub max_time: i64,
}

/// The decoded contents of a block's `meta.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockMeta {
    /// Block ULID. [`crate::BlockReader`] fills this from the directory name
    /// when the field is missing.
    #[serde(default)]
    pub ulid: String,
    /// Inclusive lower bound of the block in milliseconds since the Unix epoch.
    #[serde(rename = "minTime", default)]
    pub min_time: i64,
    /// Exclusive upper bound of the block in milliseconds since the Unix epoch.
    #[serde(rename = "maxTime", default)]
    pub max_time: i64,
    /// Counters captured when the block was written.
    #[serde(default)]
    pub stats: BlockStats,
    /// Compaction lineage.
    #[serde(default)]
    pub compaction: BlockMetaCompaction,
    /// Index format version the block was written with.
    #[serde(default = "default_version")]
    pub version: i32,
}

impl BlockMeta {
    /// Samples present in the block *before* tombstones are applied.
    pub fn samples_hint(&self) -> u64 {
        if self.stats.num_samples > 0 {
            self.stats.num_samples
        } else {
            self.stats.num_float_samples + self.stats.num_histogram_samples
        }
    }
}

/// A discovered block: where it lives, what it claims, and how big it is.
#[derive(Debug, Clone)]
pub struct BlockInfo {
    /// Local block directory (for remote sources this is the download cache).
    pub path: PathBuf,
    /// Mimir tenant; empty for a plain local data directory.
    pub tenant: String,
    /// True when the block came from an object store and was materialised locally.
    pub remote: bool,
    /// Decoded `meta.json`, or a caller-supplied equivalent.
    pub meta: BlockMeta,
    /// Size of the `index` file in bytes.
    pub index_bytes: u64,
    /// Size of all `chunks/` segment files in bytes.
    pub chunks_bytes: u64,
    /// Size of the `tombstones` file in bytes, `0` when absent.
    pub tombstones_bytes: u64,
    /// Non-fatal observations, e.g. overlapping time ranges.
    pub warnings: Vec<String>,
}

impl BlockInfo {
    /// Block ULID.
    pub fn ulid(&self) -> &str {
        &self.meta.ulid
    }

    /// Stable internal key: `tenant/ulid` for Mimir, `ulid` for local blocks.
    pub fn key(&self) -> String {
        if self.tenant.is_empty() {
            self.meta.ulid.clone()
        } else {
            format!("{}/{}", self.tenant, self.meta.ulid)
        }
    }

    /// Tenant ID, or `None` for a plain local directory.
    pub fn tenant_opt(&self) -> Option<&str> {
        if self.tenant.is_empty() {
            None
        } else {
            Some(&self.tenant)
        }
    }

    /// Sum of the index, chunk and tombstone sizes.
    pub fn size_bytes(&self) -> u64 {
        self.index_bytes + self.chunks_bytes + self.tombstones_bytes
    }
}

/// Parses a `meta.json` file.
#[cfg(feature = "meta")]
pub fn read_meta(path: &Path) -> Result<BlockMeta> {
    let data = std::fs::read(path)?;
    serde_json::from_slice(&data)
        .map_err(|e| Error::format(format!("parse {}: {e}", path.display())))
}

#[cfg(feature = "meta")]
fn dir_size(path: &Path) -> u64 {
    let Ok(rd) = std::fs::read_dir(path) else {
        return 0;
    };
    let mut total = 0;
    for entry in rd.flatten() {
        let p = entry.path();
        match entry.metadata() {
            Ok(md) if md.is_file() => total += md.len(),
            Ok(md) if md.is_dir() => total += dir_size(&p),
            _ => {}
        }
    }
    total
}

#[cfg(feature = "meta")]
fn looks_like_block(dir: &Path) -> bool {
    dir.join("meta.json").is_file() && dir.join("index").is_file() && dir.join("chunks").is_dir()
}

/// Discovers blocks under `root`.
///
/// `root` may be a Prometheus data directory (containing ULID block dirs plus
/// `wal/` and `chunks_head/`) or a single block directory.
#[cfg(feature = "meta")]
pub fn discover_blocks(root: &Path) -> Result<Vec<BlockInfo>> {
    if !root.exists() {
        return Err(Error::format(format!(
            "data directory does not exist: {}",
            root.display()
        )));
    }

    let mut dirs = Vec::new();
    if looks_like_block(root) {
        dirs.push(root.to_path_buf());
    } else {
        let rd = std::fs::read_dir(root)?;
        for entry in rd.flatten() {
            let p = entry.path();
            if p.is_dir() && looks_like_block(&p) {
                dirs.push(p);
            }
        }
    }

    let mut blocks = Vec::with_capacity(dirs.len());
    for dir in dirs {
        let meta = read_meta(&dir.join("meta.json"))?;
        let index_bytes = std::fs::metadata(dir.join("index"))
            .map(|m| m.len())
            .unwrap_or(0);
        let chunks_bytes = dir_size(&dir.join("chunks"));
        let tombstones_bytes = std::fs::metadata(dir.join("tombstones"))
            .map(|m| m.len())
            .unwrap_or(0);
        let mut warnings = Vec::new();
        if meta.stats.num_series == 0 && meta.stats.num_samples == 0 {
            warnings.push(
                "meta.json has no stats; series/chunk totals require an index prepass".into(),
            );
        }
        if meta.version > 3 {
            warnings.push(format!("unknown index version {}", meta.version));
        }
        blocks.push(BlockInfo {
            path: dir,
            tenant: String::new(),
            remote: false,
            meta,
            index_bytes,
            chunks_bytes,
            tombstones_bytes,
            warnings,
        });
    }

    blocks.sort_by(|a, b| {
        a.meta
            .min_time
            .cmp(&b.meta.min_time)
            .then_with(|| a.meta.ulid.cmp(&b.meta.ulid))
    });

    // Prometheus allows overlapping blocks after a crash; surface it instead of
    // silently exporting duplicate samples.
    for i in 1..blocks.len() {
        let (prev_max, cur_min) = (blocks[i - 1].meta.max_time, blocks[i].meta.min_time);
        if prev_max > cur_min {
            let msg = format!(
                "overlaps with block {} (prev maxTime {} > minTime {})",
                blocks[i - 1].meta.ulid,
                prev_max,
                cur_min
            );
            blocks[i].warnings.push(msg.clone());
            let msg2 = format!(
                "overlaps with block {} (maxTime {} > next minTime {})",
                blocks[i].meta.ulid, prev_max, cur_min
            );
            blocks[i - 1].warnings.push(msg2);
        }
    }

    Ok(blocks)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stats_hint_falls_back_to_parts() {
        let m = BlockMeta {
            ulid: "x".into(),
            min_time: 0,
            max_time: 1,
            stats: BlockStats {
                num_float_samples: 3,
                num_histogram_samples: 4,
                ..Default::default()
            },
            compaction: Default::default(),
            version: 2,
        };
        assert_eq!(m.samples_hint(), 7);
    }
}

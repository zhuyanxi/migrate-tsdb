//! Block discovery and `meta.json` parsing.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

fn default_version() -> i32 {
    1
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BlockStats {
    #[serde(rename = "numSamples", default)]
    pub num_samples: u64,
    #[serde(rename = "numFloatSamples", default)]
    pub num_float_samples: u64,
    #[serde(rename = "numHistogramSamples", default)]
    pub num_histogram_samples: u64,
    #[serde(rename = "numSeries", default)]
    pub num_series: u64,
    #[serde(rename = "numChunks", default)]
    pub num_chunks: u64,
    #[serde(rename = "numTombstones", default)]
    pub num_tombstones: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BlockMetaCompaction {
    #[serde(default)]
    pub level: i32,
    #[serde(default)]
    pub sources: Vec<String>,
    #[serde(default)]
    pub parents: Vec<ParentMeta>,
    #[serde(default)]
    pub deletable: bool,
    #[serde(default)]
    pub failed: bool,
    #[serde(default)]
    pub hints: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ParentMeta {
    #[serde(default)]
    pub ulid: String,
    #[serde(default)]
    pub min_time: i64,
    #[serde(default)]
    pub max_time: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockMeta {
    #[serde(default)]
    pub ulid: String,
    #[serde(rename = "minTime", default)]
    pub min_time: i64,
    #[serde(rename = "maxTime", default)]
    pub max_time: i64,
    #[serde(default)]
    pub stats: BlockStats,
    #[serde(default)]
    pub compaction: BlockMetaCompaction,
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

#[derive(Debug, Clone)]
pub struct BlockInfo {
    pub path: PathBuf,
    pub meta: BlockMeta,
    pub index_bytes: u64,
    pub chunks_bytes: u64,
    pub tombstones_bytes: u64,
    /// Non-fatal observations, e.g. overlapping time ranges.
    pub warnings: Vec<String>,
}

impl BlockInfo {
    pub fn ulid(&self) -> &str {
        &self.meta.ulid
    }

    pub fn size_bytes(&self) -> u64 {
        self.index_bytes + self.chunks_bytes + self.tombstones_bytes
    }
}

pub fn read_meta(path: &Path) -> Result<BlockMeta> {
    let data = std::fs::read(path)?;
    serde_json::from_slice(&data)
        .map_err(|e| Error::format(format!("parse {}: {e}", path.display())))
}

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

fn looks_like_block(dir: &Path) -> bool {
    dir.join("meta.json").is_file() && dir.join("index").is_file() && dir.join("chunks").is_dir()
}

/// Discovers blocks under `root`.
///
/// `root` may be a Prometheus data directory (containing ULID block dirs plus
/// `wal/` and `chunks_head/`) or a single block directory.
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

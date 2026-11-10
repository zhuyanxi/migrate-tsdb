//! Turns a data directory + filters into a concrete block plan.

use std::path::Path;

use crate::config::JobConfig;
use crate::error::{Error, Result};
use tsdb_reader::{discover_blocks, BlockInfo, BlockReader, IndexStats, ReaderOptions};

#[derive(Debug, Clone)]
pub struct PlannedBlock {
    pub info: BlockInfo,
    pub stats: IndexStats,
    pub samples_expected: u64,
}

#[derive(Debug, Clone, Default)]
pub struct Plan {
    pub blocks: Vec<PlannedBlock>,
    pub skipped_by_time: usize,
    pub warnings: Vec<String>,
}

impl Plan {
    pub fn series_total(&self) -> u64 {
        self.blocks.iter().map(|b| b.stats.series).sum()
    }
    pub fn chunks_total(&self) -> u64 {
        self.blocks.iter().map(|b| b.stats.chunks).sum()
    }
    pub fn samples_expected(&self) -> u64 {
        self.blocks.iter().map(|b| b.samples_expected).sum()
    }
}

pub fn plan(cfg: &JobConfig) -> Result<Plan> {
    cfg.validate()?;
    let data_dir = Path::new(&cfg.data_dir);
    let discovered = discover_blocks(data_dir)?;
    if discovered.is_empty() {
        return Err(Error::config(format!(
            "no Prometheus TSDB blocks found under {}",
            data_dir.display()
        )));
    }

    let mut warnings = Vec::new();
    for b in &discovered {
        for w in &b.warnings {
            warnings.push(format!("block {}: {w}", b.meta.ulid));
        }
    }

    let mut selected: Vec<BlockInfo> = if cfg.blocks.is_empty() {
        discovered
    } else {
        let wanted: std::collections::HashSet<&str> =
            cfg.blocks.iter().map(String::as_str).collect();
        let found: std::collections::HashSet<String> = discovered
            .iter()
            .filter(|b| {
                wanted.contains(b.meta.ulid.as_str())
                    || b.path
                        .file_name()
                        .and_then(|s| s.to_str())
                        .map(|n| wanted.contains(n))
                        .unwrap_or(false)
            })
            .map(|b| b.meta.ulid.clone())
            .collect();
        for w in &cfg.blocks {
            if !found.contains(w) {
                warnings.push(format!("requested block {w} not found; ignored"));
            }
        }
        discovered
            .into_iter()
            .filter(|b| {
                wanted.contains(b.meta.ulid.as_str())
                    || b.path
                        .file_name()
                        .and_then(|s| s.to_str())
                        .map(|n| wanted.contains(n))
                        .unwrap_or(false)
            })
            .collect()
    };

    // Time overlap uses Prometheus block ranges [minTime, maxTime).
    let mut skipped_by_time = 0;
    if cfg.filter.min_time.is_some() || cfg.filter.max_time.is_some() {
        selected.retain(|b| {
            let min_ok = cfg
                .filter
                .min_time
                .map(|min| b.meta.max_time > min)
                .unwrap_or(true);
            let max_ok = cfg
                .filter
                .max_time
                .map(|max| b.meta.min_time <= max)
                .unwrap_or(true);
            if min_ok && max_ok {
                true
            } else {
                skipped_by_time += 1;
                false
            }
        });
    }

    if selected.is_empty() {
        return Err(Error::config(
            "no blocks matched the requested block list / time range",
        ));
    }

    let mut blocks = Vec::with_capacity(selected.len());
    for info in selected {
        let meta_stats = &info.meta.stats;
        let stats = if meta_stats.num_series > 0 && meta_stats.num_chunks > 0 {
            IndexStats {
                series: meta_stats.num_series,
                chunks: meta_stats.num_chunks,
                min_series_offset: 0,
                end_series_offset: 0,
            }
        } else {
            // Old blocks may have no stats; a structural index scan is cheap
            // (no samples, no symbol lookups) and gives exact progress totals.
            let reader = BlockReader::open(
                &info.path,
                ReaderOptions {
                    verify_crc: false,
                    mmap_advise: false,
                },
            )?;
            let s = reader.scan_stats()?;
            warnings.push(format!(
                "block {}: meta.json had no stats, scanned index for totals",
                info.meta.ulid
            ));
            s
        };
        let samples_expected = info.meta.samples_hint();
        blocks.push(PlannedBlock {
            info,
            stats,
            samples_expected,
        });
    }

    blocks.sort_by(|a, b| {
        a.info
            .meta
            .min_time
            .cmp(&b.info.meta.min_time)
            .then_with(|| a.info.meta.ulid.cmp(&b.info.meta.ulid))
    });

    Ok(Plan {
        blocks,
        skipped_by_time,
        warnings,
    })
}

/// Estimates output bytes for a pre-flight disk space check.
pub fn estimated_output_bytes(plan: &Plan) -> u64 {
    // Parquet with zstd typically lands around 0.5-0.9x the compressed chunk
    // size; use 1.1x plus 2 MiB per block of metadata as a safety margin.
    plan.blocks
        .iter()
        .map(|b| (b.info.chunks_bytes as f64 * 1.1) as u64 + 2 * 1024 * 1024)
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_plan_totals() {
        assert_eq!(Plan::default().series_total(), 0);
    }
}

//! Manifest written next to the output data.

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::config::EngineOptions;
use crate::control::JobState;
use crate::error::Result;
use crate::stats::{BlockSnapshot, JobStats, Totals};

pub const MANIFEST_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub version: u32,
    pub job_id: String,
    pub name: String,
    pub data_dir: String,
    pub output_dir: String,
    pub started_at: String,
    pub finished_at: String,
    pub state: JobState,
    pub options: EngineOptions,
    pub totals: Totals,
    pub blocks: Vec<BlockSnapshot>,
    pub warnings: Vec<String>,
}

pub fn write(
    out_dir: &Path,
    stats: &JobStats,
    state: JobState,
    warnings: Vec<String>,
) -> Result<()> {
    let manifest = Manifest {
        version: MANIFEST_VERSION,
        job_id: stats.job_id.clone(),
        name: stats.config.name.clone(),
        data_dir: stats.config.data_dir.display().to_string(),
        output_dir: stats.config.output_dir.display().to_string(),
        started_at: crate::stats::ms_to_rfc3339(stats.started_ms),
        finished_at: crate::stats::ms_to_rfc3339(chrono::Utc::now().timestamp_millis()),
        state,
        options: stats.config.options.clone(),
        totals: stats.totals(),
        blocks: stats.blocks.iter().map(|b| b.snapshot()).collect(),
        warnings,
    };
    let data = serde_json::to_vec_pretty(&manifest)
        .map_err(|e| crate::Error::state(format!("serialize manifest: {e}")))?;
    std::fs::create_dir_all(out_dir)?;
    let path = out_dir.join("manifest.json");
    let tmp = out_dir.join("manifest.json.tmp");
    {
        use std::io::Write;
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(&data)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, &path)?;
    Ok(())
}

pub fn read(out_dir: &Path) -> Result<Manifest> {
    let path = out_dir.join("manifest.json");
    let bytes = std::fs::read(&path)?;
    serde_json::from_slice(&bytes)
        .map_err(|e| crate::Error::state(format!("parse {}: {e}", path.display())))
}

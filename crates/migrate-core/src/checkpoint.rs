//! Unit-level checkpoints and exactly-once resume.
//!
//! A unit is committed only after every worker has finalized its part files,
//! so the checkpoint can simply record `completed_unit` and the index offset of
//! the next unit. On resume, part files belonging to later units are deleted.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::stats::ms_to_rfc3339;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockCheckpoint {
    #[serde(default)]
    pub tenant: String,
    pub block: String,
    /// Highest fully committed unit; -1 means nothing committed yet.
    pub completed_unit: i64,
    /// Byte offset into the index where the next unit starts.
    pub next_offset: u64,
    pub series_done: u64,
    pub chunks_done: u64,
    pub units_done: u64,
    pub floats: u64,
    pub histograms: u64,
    pub skipped_tombstone: u64,
    pub skipped_range: u64,
    pub bytes_read: u64,
    pub updated_at: String,
}

impl BlockCheckpoint {
    pub fn empty(tenant: &str, block: &str, start_offset: u64) -> Self {
        Self {
            tenant: tenant.to_string(),
            block: block.to_string(),
            completed_unit: -1,
            next_offset: start_offset,
            series_done: 0,
            chunks_done: 0,
            units_done: 0,
            floats: 0,
            histograms: 0,
            skipped_tombstone: 0,
            skipped_range: 0,
            bytes_read: 0,
            updated_at: ms_to_rfc3339(chrono::Utc::now().timestamp_millis()),
        }
    }

    pub fn start_unit(&self) -> u64 {
        (self.completed_unit + 1).max(0) as u64
    }
}

pub fn checkpoint_path(state_dir: &Path, tenant: &str, block: &str) -> PathBuf {
    let base = state_dir.join("blocks");
    if tenant.is_empty() {
        base.join(format!("{block}.json"))
    } else {
        base.join(tenant).join(format!("{block}.json"))
    }
}

pub fn load(state_dir: &Path, tenant: &str, block: &str) -> Result<Option<BlockCheckpoint>> {
    let path = checkpoint_path(state_dir, tenant, block);
    match std::fs::read(&path) {
        Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes).map_err(|e| {
            crate::Error::state(format!("parse checkpoint {}: {e}", path.display()))
        })?)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}
pub fn save(state_dir: &Path, ckpt: &BlockCheckpoint) -> Result<()> {
    let path = checkpoint_path(state_dir, &ckpt.tenant, &ckpt.block);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let data = serde_json::to_vec_pretty(ckpt).map_err(|e| crate::Error::state(e.to_string()))?;
    let tmp = path.with_extension("json.tmp");
    {
        use std::io::Write;
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(&data)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, &path)?;
    Ok(())
}

pub fn remove(state_dir: &Path, tenant: &str, block: &str) -> Result<()> {
    let path = checkpoint_path(state_dir, tenant, block);
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}

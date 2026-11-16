//! Sink abstraction and Remote Write implementation.

pub mod remote_write;

use std::path::{Path, PathBuf};

use crate::batch::Batch;
use crate::config::JobConfig;
use crate::error::Result;

/// Per-worker sink. Each worker owns one instance so that no locking is needed
/// on the hot path.
pub trait BatchSink: Send {
    fn write(&mut self, unit: u64, batch: &mut Batch) -> Result<()>;
    /// Marks the end of a unit after its requests have completed.
    fn finish_unit(&mut self, unit: u64) -> Result<()>;
    /// Finalizes this block's sink state.
    fn end_block(&mut self) -> Result<()>;
}

pub fn block_output_dir(cfg: &JobConfig, tenant: &str, block_id: &str) -> PathBuf {
    let base = cfg.blocks_dir();
    if tenant.is_empty() {
        base.join(format!("block={block_id}"))
    } else {
        base.join(format!("tenant={tenant}"))
            .join(format!("block={block_id}"))
    }
}

pub fn create_sink(
    cfg: &JobConfig,
    tenant: &str,
    _block_id: &str,
    _worker: usize,
) -> Result<Box<dyn BatchSink>> {
    Ok(Box::new(remote_write::RemoteWriteSink::new(
        &cfg.sink.remote_write_url,
        &cfg.sink.remote_write_tenant_header,
        tenant,
    )?))
}

pub fn remove_block_output(cfg: &JobConfig, tenant: &str, block_id: &str) -> Result<()> {
    let dir = block_output_dir(cfg, tenant, block_id);
    if dir.exists() {
        std::fs::remove_dir_all(&dir)?;
    }
    Ok(())
}

pub fn check_writable(dir: &Path) -> Result<()> {
    std::fs::create_dir_all(dir)?;
    let probe = dir.join(".tsdb-migrate-write-test");
    std::fs::write(&probe, b"ok")?;
    std::fs::remove_file(&probe)?;
    Ok(())
}

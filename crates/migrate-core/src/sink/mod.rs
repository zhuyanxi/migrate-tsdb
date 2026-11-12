//! Sink abstraction and implementations.

pub mod ndjson;
pub mod parquet;
pub mod remote_write;

use std::path::{Path, PathBuf};

use crate::batch::Batch;
use crate::config::{JobConfig, SinkKind};
use crate::error::{Error, Result};

#[derive(Debug, Default)]
pub struct SinkOutcome {
    pub files: Vec<PathBuf>,
    pub bytes: u64,
}

/// Per-worker sink. Each worker owns one instance so that no locking is needed
/// on the hot path; part files are disjoint by worker id.
pub trait BatchSink: Send {
    fn write(&mut self, unit: u64, batch: &mut Batch) -> Result<()>;
    /// Finalizes and fsyncs all part files of `unit`.
    fn finish_unit(&mut self, unit: u64) -> Result<()>;
    /// Finalizes the block and reports written files/bytes.
    fn end_block(&mut self) -> Result<SinkOutcome>;
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

/// Output directory for a block, used by verify/UI-independent tools.
pub fn output_dir_for(out_dir: &Path, tenant: Option<&str>, block_id: &str) -> PathBuf {
    let base = out_dir.join("blocks");
    match tenant {
        Some(t) if !t.is_empty() => base
            .join(format!("tenant={t}"))
            .join(format!("block={block_id}")),
        _ => base.join(format!("block={block_id}")),
    }
}

pub fn create_sink(
    cfg: &JobConfig,
    tenant: &str,
    block_id: &str,
    worker: usize,
) -> Result<Box<dyn BatchSink>> {
    let dir = block_output_dir(cfg, tenant, block_id);
    match cfg.sink.kind {
        SinkKind::Parquet | SinkKind::Duckdb => Ok(Box::new(parquet::ParquetSink::new(
            dir,
            worker,
            cfg.sink.compression,
            cfg.sink.row_group_size,
        )?)),
        SinkKind::Ndjson => Ok(Box::new(ndjson::NdjsonSink::new(dir, worker)?)),
        SinkKind::RemoteWrite => Ok(Box::new(remote_write::RemoteWriteSink::new(
            &cfg.sink.remote_write_url,
            &cfg.sink.remote_write_tenant_header,
            tenant,
        )?)),
    }
}

pub fn dir_size(dir: &Path) -> u64 {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return 0;
    };
    let mut total = 0;
    for e in rd.flatten() {
        match e.metadata() {
            Ok(m) if m.is_file() => total += m.len(),
            Ok(m) if m.is_dir() => total += dir_size(&e.path()),
            _ => {}
        }
    }
    total
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

pub(crate) fn part_path(dir: &Path, dataset: &str, unit: u64, worker: usize, ext: &str) -> PathBuf {
    dir.join(dataset)
        .join(format!("part-u{:06}-w{:02}.{ext}", unit, worker))
}

pub(crate) fn ensure_parent(path: &Path) -> Result<()> {
    if let Some(p) = path.parent() {
        std::fs::create_dir_all(p)?;
    } else {
        return Err(Error::sink(format!(
            "path has no parent directory: {}",
            path.display()
        )));
    }
    Ok(())
}

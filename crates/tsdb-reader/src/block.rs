//! A single TSDB block: index + chunk segments + tombstones.

use std::path::{Path, PathBuf};

use crate::chunks::{ChunkReader, ChunkSlice};
use crate::error::{Error, Result};
use crate::index::{ChunkMeta, IndexReader, IndexStats, SeriesIter};
use crate::meta::{read_meta, BlockInfo, BlockMeta};
use crate::tombstones::Tombstones;

#[derive(Debug, Clone, Copy)]
pub struct ReaderOptions {
    /// Validate CRC32C checksums for index TOC, series records, chunks and
    /// tombstones. Disabling improves throughput but hides corruption.
    pub verify_crc: bool,
    /// Apply `madvise(SEQUENTIAL)` to chunk segment mappings.
    pub mmap_advise: bool,
}

impl Default for ReaderOptions {
    fn default() -> Self {
        Self {
            verify_crc: true,
            mmap_advise: true,
        }
    }
}

pub struct BlockReader {
    info: BlockInfo,
    index: IndexReader,
    chunks: ChunkReader,
    tombstones: Tombstones,
}

impl BlockReader {
    pub fn open(path: &Path, opts: ReaderOptions) -> Result<Self> {
        let meta_path = path.join("meta.json");
        if !meta_path.is_file() {
            return Err(Error::format(format!(
                "not a block directory (missing meta.json): {}",
                path.display()
            )));
        }
        let mut meta = read_meta(&meta_path)?;
        if meta.ulid.is_empty() {
            meta.ulid = path
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_string();
        }
        let index = IndexReader::open(&path.join("index"), opts.verify_crc)?;
        let chunks = ChunkReader::open(&path.join("chunks"), opts.verify_crc, opts.mmap_advise)?;
        let tombstones = Tombstones::open(path, opts.verify_crc)?;

        let index_bytes = std::fs::metadata(path.join("index"))
            .map(|m| m.len())
            .unwrap_or(0);
        let tombstones_bytes = std::fs::metadata(path.join("tombstones"))
            .map(|m| m.len())
            .unwrap_or(0);
        let info = BlockInfo {
            path: path.to_path_buf(),
            meta,
            index_bytes,
            chunks_bytes: chunks.bytes(),
            tombstones_bytes,
            warnings: Vec::new(),
        };
        Ok(Self {
            info,
            index,
            chunks,
            tombstones,
        })
    }

    pub fn open_info(info: BlockInfo, opts: ReaderOptions) -> Result<Self> {
        let mut reader = Self::open(&info.path, opts)?;
        reader.info.warnings = info.warnings;
        Ok(reader)
    }

    pub fn path(&self) -> &Path {
        &self.info.path
    }

    pub fn info(&self) -> &BlockInfo {
        &self.info
    }

    pub fn meta(&self) -> &BlockMeta {
        &self.info.meta
    }

    pub fn block_id(&self) -> &str {
        &self.info.meta.ulid
    }

    pub fn index(&self) -> &IndexReader {
        &self.index
    }

    pub fn tombstones(&self) -> &Tombstones {
        &self.tombstones
    }

    pub fn series(&self, start_offset: u64) -> SeriesIter<'_> {
        self.index.series(start_offset)
    }

    pub fn chunk(&self, meta: &ChunkMeta) -> Result<ChunkSlice<'_>> {
        self.chunks.chunk(meta.reference)
    }

    /// Shared chunk reader for decode workers (immutable, thread-safe).
    pub fn chunk_reader(&self) -> &ChunkReader {
        &self.chunks
    }

    pub fn chunk_bytes(&self) -> u64 {
        self.chunks.bytes()
    }

    /// Series/chunk totals derived from `meta.json` (cheap, may be zero for
    /// blocks written by old versions or custom tooling).
    pub fn stats_from_meta(&self) -> IndexStats {
        let s = &self.meta().stats;
        IndexStats {
            series: s.num_series,
            chunks: s.num_chunks,
            min_series_offset: self.index.series_start(),
            end_series_offset: self.index.series_end(),
        }
    }

    /// Exact totals by scanning the index series section (no label resolution).
    pub fn scan_stats(&self) -> Result<IndexStats> {
        self.index.scan_stats()
    }

    /// Splits the reader so that the index can be moved into a single reader
    /// thread while chunk segments and tombstones are shared with workers.
    pub fn into_parts(self) -> (IndexReader, ChunkReader, Tombstones, BlockInfo) {
        (self.index, self.chunks, self.tombstones, self.info)
    }

    /// Ensures the tombstone refs look plausible for this block. Prometheus
    /// block tombstones use the same series refs as the index.
    pub fn validate_tombstones(&self) -> Result<()> {
        if self.tombstones.series_count() == 0 {
            return Ok(());
        }
        // A quick structural check: refs must be < 2^32 for v2/v3 (series id
        // is offset/16 and index files stay below 64 GiB).
        for (r, _) in self.tombstones.iter_raw() {
            if *r > u32::MAX as u64 {
                return Err(Error::format(format!(
                    "tombstone series ref {r} out of range"
                )));
            }
        }
        Ok(())
    }
}

impl std::fmt::Debug for BlockReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockReader")
            .field("path", &self.info.path)
            .field("ulid", &self.info.meta.ulid)
            .finish()
    }
}

/// Convenience for callers that only have a directory name.
pub fn block_dir(root: &Path, ulid: &str) -> PathBuf {
    root.join(ulid)
}

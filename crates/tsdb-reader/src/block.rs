//! A single TSDB block: index + chunk segments + tombstones.

use std::path::{Path, PathBuf};

use crate::chunks::{ChunkReader, ChunkSlice};
use crate::error::{Error, Result};
use crate::index::{ChunkMeta, IndexReader, IndexStats, SeriesIter};
#[cfg(feature = "meta")]
use crate::meta::read_meta;
use crate::meta::{BlockInfo, BlockMeta};
use crate::tombstones::Tombstones;

/// Knobs applied when opening a block.
///
/// The defaults are what you want for a normal export: checksums on, sequential
/// read-ahead on.
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

/// A block opened for reading: index, chunk segments and tombstones.
///
/// The reader owns the underlying mappings or buffers. On unix with the `mmap`
/// feature the files stay mapped for the lifetime of the reader, which means
/// they must not be truncated by another process while it is in use.
///
/// `BlockReader` is `Send` but not `Sync`: the symbol cache is a `RefCell`, so
/// index traversal belongs to one thread. Chunk segments are shared freely via
/// [`BlockReader::chunk_reader`] or [`BlockReader::into_parts`].
pub struct BlockReader {
    info: BlockInfo,
    index: IndexReader,
    chunks: ChunkReader,
    tombstones: Tombstones,
}

impl BlockReader {
    /// Opens a block directory: parses `meta.json`, then the index, chunk
    /// segments and tombstones.
    ///
    /// Available with the `meta` feature. Without it, use
    /// [`BlockReader::open_with_meta`] or [`BlockReader::open_info`].
    #[cfg(feature = "meta")]
    pub fn open(path: &Path, opts: ReaderOptions) -> Result<Self> {
        let meta_path = path.join("meta.json");
        if !meta_path.is_file() {
            return Err(Error::format(format!(
                "not a block directory (missing meta.json): {}",
                path.display()
            )));
        }
        let meta = read_meta(&meta_path)?;
        Self::open_with_meta(path, meta, opts)
    }

    /// Opens a block whose `meta.json` the caller already parsed.
    ///
    /// This is the entry point for builds without the `meta` feature and for
    /// callers that keep block metadata in their own store.
    pub fn open_with_meta(path: &Path, mut meta: BlockMeta, opts: ReaderOptions) -> Result<Self> {
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
            tenant: String::new(),
            remote: false,
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

    /// Opens a block reusing metadata that was already discovered.
    ///
    /// The passed [`BlockInfo`] stays authoritative: `tenant`, `remote` and
    /// `warnings` cannot be derived from the block directory, so they are never
    /// overwritten. Only the byte counts measured while opening are refreshed,
    /// and `meta` is filled from disk when the caller left its ULID empty.
    pub fn open_info(info: BlockInfo, opts: ReaderOptions) -> Result<Self> {
        let mut reader = Self::open_with_meta(&info.path, info.meta.clone(), opts)?;
        let mut merged = info;
        merged.index_bytes = reader.info.index_bytes;
        merged.chunks_bytes = reader.info.chunks_bytes;
        merged.tombstones_bytes = reader.info.tombstones_bytes;
        if merged.meta.ulid.is_empty() {
            merged.meta = reader.info.meta;
        }
        merged.warnings.extend(reader.info.warnings);
        reader.info = merged;
        Ok(reader)
    }

    /// Directory this block was opened from.
    pub fn path(&self) -> &Path {
        &self.info.path
    }

    /// Metadata plus the sizes measured when the block was opened.
    pub fn info(&self) -> &BlockInfo {
        &self.info
    }

    /// Parsed `meta.json`.
    pub fn meta(&self) -> &BlockMeta {
        &self.info.meta
    }

    /// Block ULID, taken from `meta.json` or the directory name.
    pub fn block_id(&self) -> &str {
        &self.info.meta.ulid
    }

    /// The index reader backing this block.
    pub fn index(&self) -> &IndexReader {
        &self.index
    }

    /// Deletion intervals loaded for this block.
    pub fn tombstones(&self) -> &Tombstones {
        &self.tombstones
    }

    /// Iterates series in lexicographic label order.
    ///
    /// `start_offset` is a byte offset previously reported by
    /// [`crate::SeriesIter::position`]; pass `0` to start at the beginning.
    pub fn series(&self, start_offset: u64) -> SeriesIter<'_> {
        self.index.series(start_offset)
    }

    /// Reads the chunk described by `meta`, verifying its checksum when the
    /// reader was opened with `verify_crc`.
    pub fn chunk(&self, meta: &ChunkMeta) -> Result<ChunkSlice<'_>> {
        self.chunks.chunk(meta.reference)
    }

    /// Shared chunk reader for decode workers (immutable, thread-safe).
    /// Shared chunk reader, safe to hand to several decode threads.
    pub fn chunk_reader(&self) -> &ChunkReader {
        &self.chunks
    }

    /// Total size of the chunk segments in bytes.
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

#[cfg(all(test, feature = "meta"))]
mod tests {
    use super::*;
    use crate::meta::{BlockMeta, BlockMetaCompaction, BlockStats};

    fn fixture_block_dir() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/data/block/01M70C8VV61N5H5VWXB17HQQ1R")
    }

    fn info_with(tenant: &str, remote: bool, ulid: &str) -> BlockInfo {
        BlockInfo {
            path: fixture_block_dir(),
            tenant: tenant.to_string(),
            remote,
            meta: BlockMeta {
                ulid: ulid.to_string(),
                min_time: 0,
                max_time: 0,
                stats: BlockStats::default(),
                compaction: BlockMetaCompaction::default(),
                version: 2,
            },
            index_bytes: 0,
            chunks_bytes: 0,
            tombstones_bytes: 0,
            warnings: vec!["discovery note".to_string()],
        }
    }

    #[test]
    fn open_info_keeps_caller_metadata() {
        let info = info_with("tenant-a", true, "01M70C8VV61N5H5VWXB17HQQ1R");
        let reader = BlockReader::open_info(info, ReaderOptions::default()).unwrap();

        assert_eq!(reader.info().tenant, "tenant-a");
        assert!(reader.info().remote);
        assert_eq!(reader.info().warnings, vec!["discovery note"]);
        assert_eq!(reader.block_id(), "01M70C8VV61N5H5VWXB17HQQ1R");
        // Sizes must come from disk, not from the caller's zeros.
        assert!(reader.info().index_bytes > 0);
        assert!(reader.info().chunks_bytes > 0);
    }

    #[test]
    fn open_info_falls_back_to_the_directory_name() {
        let info = info_with("", false, "");
        let reader = BlockReader::open_info(info, ReaderOptions::default()).unwrap();

        assert_eq!(
            reader.block_id(),
            "01M70C8VV61N5H5VWXB17HQQ1R",
            "an empty ULID must be taken from the directory name"
        );
        // The rest of the caller's meta is authoritative, even when meta.json
        // on disk says something else.
        assert_eq!(reader.meta().version, 2);
        assert_eq!(reader.meta().min_time, 0);
    }

    #[test]
    fn open_info_matches_open_for_a_local_block() {
        let dir = fixture_block_dir();
        let direct = BlockReader::open(&dir, ReaderOptions::default()).unwrap();
        let via_info = BlockReader::open_info(
            BlockInfo {
                path: dir.clone(),
                tenant: String::new(),
                remote: false,
                meta: direct.meta().clone(),
                index_bytes: 0,
                chunks_bytes: 0,
                tombstones_bytes: 0,
                warnings: Vec::new(),
            },
            ReaderOptions::default(),
        )
        .unwrap();

        assert_eq!(direct.info().index_bytes, via_info.info().index_bytes);
        assert_eq!(direct.info().chunks_bytes, via_info.info().chunks_bytes);
        assert_eq!(
            direct.info().tombstones_bytes,
            via_info.info().tombstones_bytes
        );
        assert_eq!(direct.block_id(), via_info.block_id());
        assert_eq!(
            direct.scan_stats().unwrap().series,
            via_info.scan_stats().unwrap().series
        );
    }

    #[test]
    fn open_rejects_a_non_block_directory() {
        let error = BlockReader::open(&std::env::temp_dir(), ReaderOptions::default())
            .expect_err("temp dir is not a block");
        assert!(error.to_string().contains("meta.json"), "{error}");
    }

    #[test]
    fn block_dir_joins_the_ulid() {
        assert_eq!(
            block_dir(Path::new("/data"), "01ABC"),
            PathBuf::from("/data/01ABC")
        );
    }
}

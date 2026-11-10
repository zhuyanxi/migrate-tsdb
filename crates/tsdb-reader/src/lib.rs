//! Zero-copy reader for Prometheus TSDB blocks.
//!
//! The crate intentionally implements only what a full sequential export
//! needs: index TOC/symbols/series traversal, chunk segment access with
//! CRC32C validation, all chunk encodings, and tombstones filtering.

pub mod block;
pub mod chunkenc;
pub mod chunks;
pub mod error;
pub mod index;
pub mod meta;
pub mod tombstones;
pub mod varint;

pub use block::{BlockReader, ReaderOptions};
pub use chunkenc::{HistogramSample, SampleSink, Span};
pub use chunks::ChunkSlice;
pub use error::{Error, Result};
pub use index::{ChunkMeta, IndexReader, IndexStats, SeriesIter, SeriesRef};
pub use meta::{discover_blocks, BlockInfo, BlockMeta, BlockStats};
pub use tombstones::Tombstones;

#![doc = include_str!("../README.md")]

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

#![doc = include_str!("../README.md")]
#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

pub mod block;
mod checksum;
pub mod chunkenc;
pub mod chunks;
pub mod error;
pub mod index;
pub mod meta;
mod storage;
pub mod tombstones;
pub mod varint;

pub use block::{BlockReader, ReaderOptions};
pub use chunkenc::{HistogramSample, SampleSink, Span};
pub use chunks::ChunkReader;
pub use chunks::ChunkSlice;
pub use error::{Error, Result};
pub use index::{ChunkMeta, IndexReader, IndexStats, SeriesIter, SeriesRef};
#[cfg(feature = "meta")]
pub use meta::discover_blocks;
pub use meta::{BlockInfo, BlockMeta, BlockStats};
pub use tombstones::Tombstones;

//! Backing storage for index and chunk data.
//!
//! With the `mmap` feature the bytes are a read-only memory map, which keeps
//! large segments out of the process RSS. Without it the same bytes are owned
//! buffers, so the crate builds on targets where `memmap2` is unavailable and
//! callers can hand over data they already hold in memory.

use std::ops::Deref;
use std::path::Path;

use crate::error::Result;

/// Either a memory map or an owned buffer. Both deref to the raw bytes so the
/// parsers stay identical in both configurations.
pub(crate) enum Segment {
    #[cfg(feature = "mmap")]
    Mapped(memmap2::Mmap),
    Owned(Vec<u8>),
}

impl Deref for Segment {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        match self {
            #[cfg(feature = "mmap")]
            Segment::Mapped(map) => map,
            Segment::Owned(bytes) => bytes,
        }
    }
}

impl std::fmt::Debug for Segment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Segment").field("len", &self.len()).finish()
    }
}

/// Loads `path` as a segment, mapping it when the `mmap` feature is on.
pub(crate) fn open(path: &Path) -> Result<Segment> {
    #[cfg(feature = "mmap")]
    {
        let file = std::fs::File::open(path)?;
        // SAFETY: the file is opened read-only and the mapping outlives every
        // reference derived from it. As with any `mmap`, truncating the file
        // while it is mapped makes affected reads fault; see the note on
        // [`crate::BlockReader`].
        Ok(Segment::Mapped(unsafe {
            memmap2::MmapOptions::new().map(&file)?
        }))
    }
    #[cfg(not(feature = "mmap"))]
    {
        Ok(Segment::Owned(std::fs::read(path)?))
    }
}

/// Wraps an in-memory buffer as a segment.
pub(crate) fn owned(bytes: Vec<u8>) -> Segment {
    Segment::Owned(bytes)
}

/// Hints the kernel to read `segment` sequentially. No-op without `mmap`.
pub(crate) fn advise_sequential(segment: &Segment) {
    #[cfg(all(feature = "mmap", unix))]
    if let Segment::Mapped(map) = segment {
        let _ = map.advise(memmap2::Advice::Sequential);
    }
    #[cfg(not(all(feature = "mmap", unix)))]
    let _ = segment;
}

/// Drops the read-ahead hint. Only exists where the hint was applied.
#[cfg(all(feature = "mmap", unix))]
pub(crate) fn advise_normal(segment: &Segment) {
    if let Segment::Mapped(map) = segment {
        let _ = map.advise(memmap2::Advice::Normal);
    }
}

/// Wraps a memory map as a segment.
#[cfg(feature = "mmap")]
pub(crate) fn mapped(map: memmap2::Mmap) -> Segment {
    Segment::Mapped(map)
}

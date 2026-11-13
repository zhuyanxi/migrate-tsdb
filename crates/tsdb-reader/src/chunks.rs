//! Chunk segment access.
//!
//! Layout per segment (see `tsdb/docs/format/chunks.md`):
//! magic(4) version(1) padding(3), then repeated records of
//! `len(uvarint) encoding(1) data(len) crc32c(4)`.
//!
//! `len` counts the chunk body only: it excludes the encoding byte and the
//! trailing CRC. The CRC covers the encoding byte followed by the body, i.e.
//! `crc32c(encoding || data)`.

use std::path::Path;

use crate::checksum;
use crate::error::{Error, Result};
use crate::storage::{self, Segment};
use crate::varint;

const MAGIC_CHUNKS: u32 = 0x85BD_40DD;
/// Size of a chunk segment header: magic(4) + version(1) + padding(3).
pub const SEGMENT_HEADER_SIZE: usize = 8;

/// A zero-copy view of one chunk record inside a segment.
///
/// `data` borrows straight out of the mapped segment, so the slice is only
/// valid while the owning [`ChunkReader`] is alive.
#[derive(Debug)]
pub struct ChunkSlice<'a> {
    /// Chunk encoding, i.e. one of the `ENC_*` constants in [`crate::chunkenc`].
    pub encoding: u8,
    /// Compressed chunk body, with the encoding byte already stripped.
    pub data: &'a [u8],
    /// Segment the record lives in (the high 32 bits of a chunk reference).
    pub segment: u32,
    /// Byte offset of the record inside that segment.
    pub offset: u32,
    /// Length of the chunk body as stored on disk. Excludes the encoding byte
    /// and the trailing CRC.
    pub encoded_len: u32,
}

/// A block's `chunks/` directory, one segment per numeric file.
///
/// With the `mmap` feature the segments are mapped lazily by the OS; without
/// it they are held in memory. Either way [`ChunkReader::chunk`] does not
/// allocate and returns a slice into the segment.
pub struct ChunkReader {
    segments: Vec<Segment>,
    verify_crc: bool,
    bytes: u64,
}

impl std::fmt::Debug for ChunkReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChunkReader")
            .field("segments", &self.segments.len())
            .field("bytes", &self.bytes)
            .finish()
    }
}

impl ChunkReader {
    /// Opens a block's `chunks/` directory.
    ///
    /// Segment files are ordered by their numeric name. `verify_crc` is only
    /// honoured when the crate is built with the `crc` feature; `advise` asks
    /// the kernel to read sequentially on unix targets.
    pub fn open(dir: &Path, verify_crc: bool, advise: bool) -> Result<Self> {
        let mut files: Vec<(u64, std::path::PathBuf)> = Vec::new();
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if let Ok(seq) = name.parse::<u64>() {
                let md = entry.metadata()?;
                if md.is_file() {
                    files.push((seq, entry.path()));
                }
            }
        }
        files.sort_by_key(|(seq, _)| *seq);

        let mut segments = Vec::with_capacity(files.len());
        let mut bytes = 0u64;
        for (seq, path) in &files {
            let segment = storage::open(path)?;
            Self::push_segment(&mut segments, &mut bytes, *seq, segment, advise)?;
        }
        if segments.is_empty() {
            return Err(Error::format(format!(
                "no chunk segments in {}",
                dir.display()
            )));
        }
        Ok(Self {
            segments,
            verify_crc,
            bytes,
        })
    }

    /// Builds a reader from segments that are already in memory.
    ///
    /// `segments` must be ordered by sequence number, exactly like the numeric
    /// files inside a block's `chunks/` directory. Chunk references index into
    /// this slice, so the order is part of the format.
    pub fn from_segments(segments: Vec<Vec<u8>>, verify_crc: bool) -> Result<Self> {
        if segments.is_empty() {
            return Err(Error::format("no chunk segments supplied"));
        }
        let mut out = Vec::with_capacity(segments.len());
        let mut bytes = 0u64;
        for (seq, buffer) in segments.into_iter().enumerate() {
            Self::push_segment(
                &mut out,
                &mut bytes,
                seq as u64,
                storage::owned(buffer),
                false,
            )?;
        }
        Ok(Self {
            segments: out,
            verify_crc,
            bytes,
        })
    }

    fn push_segment(
        segments: &mut Vec<Segment>,
        bytes: &mut u64,
        seq: u64,
        segment: Segment,
        advise: bool,
    ) -> Result<()> {
        if segment.len() < SEGMENT_HEADER_SIZE {
            return Err(Error::format(format!(
                "chunk segment {seq} too small: {} bytes",
                segment.len()
            )));
        }
        let magic = u32::from_be_bytes([segment[0], segment[1], segment[2], segment[3]]);
        if magic != MAGIC_CHUNKS {
            return Err(Error::format(format!(
                "invalid chunk segment magic 0x{magic:08x} in segment {seq}"
            )));
        }
        if segment[4] != 1 {
            return Err(Error::Unsupported(format!(
                "chunk segment format version {}",
                segment[4]
            )));
        }
        if advise {
            storage::advise_sequential(&segment);
        }
        *bytes += segment.len() as u64;
        segments.push(segment);
        Ok(())
    }

    /// Total size of all segments in bytes.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Number of segment files backing this block.
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    /// Returns a zero-copy view of the chunk referenced by `reference`.
    pub fn chunk(&self, reference: u64) -> Result<ChunkSlice<'_>> {
        let segment = (reference >> 32) as u32;
        let offset = reference as u32;
        let seg = self.segments.get(segment as usize).ok_or_else(|| {
            Error::format(format!(
                "chunk reference {reference:#x}: segment {segment} out of range ({})",
                self.segments.len()
            ))
        })?;
        let start = offset as usize;
        if start + 5 > seg.len() {
            return Err(Error::format(format!(
                "chunk reference {reference:#x}: offset {start} out of range"
            )));
        }
        let mut p = start;
        let len = varint::uvarint(seg, &mut p)? as usize;
        if len == 0 {
            return Err(Error::format("chunk length is zero"));
        }
        let enc_start = p;
        // `len` counts the chunk body only (excluding the encoding byte).
        let data_end = enc_start
            .checked_add(1 + len)
            .ok_or_else(|| Error::format("chunk length overflow"))?;
        let crc_end = data_end
            .checked_add(4)
            .ok_or_else(|| Error::format("chunk length overflow"))?;
        if crc_end > seg.len() {
            return Err(Error::format(format!(
                "chunk reference {reference:#x}: truncated chunk"
            )));
        }
        let encoding = seg[enc_start];
        if self.verify_crc {
            let want = u32::from_be_bytes([
                seg[data_end],
                seg[data_end + 1],
                seg[data_end + 2],
                seg[data_end + 3],
            ]);
            checksum::verify(true, want, &seg[enc_start..data_end], "chunk")?;
        }
        Ok(ChunkSlice {
            encoding,
            data: &seg[enc_start + 1..data_end],
            segment,
            offset,
            encoded_len: len as u32,
        })
    }
}

#[cfg(all(feature = "mmap", unix))]
impl Drop for ChunkReader {
    fn drop(&mut self) {
        for segment in &self.segments {
            storage::advise_normal(segment);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "crc")]
    use crate::chunkenc::ENC_XOR2;
    use crate::chunkenc::{ENC_NONE, ENC_XOR};
    use std::io::Write;

    /// A chunk reference is `(segment << 32) | offset`; the first record of
    /// segment 0 starts right after the 8-byte segment header.
    const FIRST: u64 = SEGMENT_HEADER_SIZE as u64;

    /// Builds one segment holding a single chunk record.
    fn segment_with(encoding: u8, data: &[u8]) -> Vec<u8> {
        let mut body = Vec::new();
        body.push(encoding);
        body.extend_from_slice(data);

        let mut seg = Vec::new();
        seg.extend_from_slice(&MAGIC_CHUNKS.to_be_bytes());
        seg.push(1);
        seg.extend_from_slice(&[0, 0, 0]);
        // `len` covers the body only, never the encoding byte.
        let mut len = [0u8; 10];
        let n = {
            let mut value = data.len() as u64;
            let mut i = 0;
            loop {
                let mut byte = (value & 0x7f) as u8;
                value >>= 7;
                if value != 0 {
                    byte |= 0x80;
                }
                len[i] = byte;
                i += 1;
                if value == 0 {
                    break i;
                }
            }
        };
        seg.extend_from_slice(&len[..n]);
        seg.extend_from_slice(&body);
        seg.extend_from_slice(&crc32c::crc32c(&body).to_be_bytes());
        seg
    }

    fn reader_with(tag: &str, seg: Vec<u8>) -> (ChunkReader, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "tsdb-reader-chunks-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let mut file = std::fs::File::create(dir.join("000001")).unwrap();
        file.write_all(&seg).unwrap();
        let reader = ChunkReader::open(&dir, true, false).unwrap();
        (reader, dir)
    }

    #[test]
    fn len_excludes_the_encoding_byte() {
        let data = [1u8, 2, 3, 4];
        let (reader, dir) = reader_with("len", segment_with(ENC_XOR, &data));
        let slice = reader.chunk(FIRST).unwrap();

        assert_eq!(slice.encoding, ENC_XOR);
        assert_eq!(slice.data, &data);
        // The documented contract: len == body length, encoding NOT included.
        assert_eq!(slice.encoded_len, data.len() as u32);
        assert_eq!(slice.segment, 0);
        assert_eq!(slice.offset, SEGMENT_HEADER_SIZE as u32);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(feature = "crc")]
    #[test]
    fn crc_covers_encoding_and_data() {
        let mut seg = segment_with(ENC_XOR2, &[9, 8, 7]);
        // Flip a bit in the encoding byte: the CRC must notice.
        let header = SEGMENT_HEADER_SIZE;
        assert_ne!(seg[header], 0);
        seg[header] ^= 0x01;

        let dir = std::env::temp_dir().join(format!(
            "tsdb-reader-chunks-crc-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("000001"), &seg).unwrap();
        let reader = ChunkReader::open(&dir, true, false).unwrap();
        assert!(matches!(reader.chunk(FIRST), Err(Error::Checksum("chunk"))));
        // With verification off the corruption is surfaced as data instead.
        let lax = ChunkReader::open(&dir, false, false).unwrap();
        assert!(lax.chunk(FIRST).is_ok());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn single_byte_body_at_the_segment_tail_is_accepted() {
        // The CRC of this record ends exactly at the end of the segment.
        let (reader, dir) = reader_with("tail", segment_with(ENC_NONE, &[0x42]));
        let slice = reader.chunk(FIRST).unwrap();
        assert_eq!(slice.encoding, ENC_NONE);
        assert_eq!(slice.encoded_len, 1);
        assert_eq!(slice.data, &[0x42]);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn zero_length_chunk_is_rejected() {
        let (reader, dir) = reader_with("zero", segment_with(ENC_NONE, &[]));
        assert!(matches!(reader.chunk(FIRST), Err(Error::Format(_))));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn truncated_chunk_is_rejected() {
        let mut seg = segment_with(ENC_XOR, &[1, 2, 3, 4]);
        seg.truncate(seg.len() - 2); // cut into the CRC

        let dir = std::env::temp_dir().join(format!(
            "tsdb-reader-chunks-trunc-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("000001"), &seg).unwrap();
        let reader = ChunkReader::open(&dir, true, false).unwrap();
        assert!(matches!(reader.chunk(FIRST), Err(Error::Format(_))));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn out_of_range_references_are_rejected() {
        let (reader, dir) = reader_with("range", segment_with(ENC_XOR, &[1]));
        assert!(reader.chunk(1 << 32).is_err(), "unknown segment");
        assert!(reader.chunk(4096).is_err(), "offset past the segment");
        assert!(reader.chunk(FIRST).is_ok(), "the first record is readable");
        assert_eq!(reader.segment_count(), 1);
        assert!(reader.bytes() >= SEGMENT_HEADER_SIZE as u64);
        std::fs::remove_dir_all(dir).unwrap();
    }
}

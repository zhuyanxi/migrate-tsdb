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

#[cfg(unix)]
use memmap2::Advice;
use memmap2::{Mmap, MmapOptions};

use crate::error::{Error, Result};
use crate::varint;

const MAGIC_CHUNKS: u32 = 0x85BD_40DD;
pub const SEGMENT_HEADER_SIZE: usize = 8;

#[derive(Debug)]
pub struct ChunkSlice<'a> {
    pub encoding: u8,
    pub data: &'a [u8],
    pub segment: u32,
    pub offset: u32,
    pub encoded_len: u32,
}

pub struct ChunkReader {
    segments: Vec<Mmap>,
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
            let file = std::fs::File::open(path)?;
            let md = file.metadata()?;
            if md.len() < SEGMENT_HEADER_SIZE as u64 {
                return Err(Error::format(format!(
                    "chunk segment {} too small: {}",
                    path.display(),
                    md.len()
                )));
            }
            let mmap = unsafe { MmapOptions::new().map(&file)? };
            let magic = u32::from_be_bytes([mmap[0], mmap[1], mmap[2], mmap[3]]);
            if magic != MAGIC_CHUNKS {
                return Err(Error::format(format!(
                    "invalid chunk segment magic 0x{magic:08x} in {}",
                    path.display()
                )));
            }
            if mmap[4] != 1 {
                return Err(Error::Unsupported(format!(
                    "chunk segment format version {}",
                    mmap[4]
                )));
            }
            if advise {
                #[cfg(unix)]
                let _ = mmap.advise(Advice::Sequential);
            }
            bytes += mmap.len() as u64;
            segments.push(mmap);
            let _ = seq;
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

    pub fn bytes(&self) -> u64 {
        self.bytes
    }

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
            let got = crc32c::crc32c(&seg[enc_start..data_end]);
            if want != got {
                return Err(Error::Checksum("chunk"));
            }
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

impl Drop for ChunkReader {
    fn drop(&mut self) {
        for seg in &self.segments {
            #[cfg(unix)]
            let _ = seg.advise(Advice::Normal);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunkenc::{ENC_NONE, ENC_XOR, ENC_XOR2};
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

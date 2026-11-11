//! Chunk segment access.
//!
//! Layout per segment (see `tsdb/docs/format/chunks.md`):
//! magic(4) version(1) padding(3), then repeated:
//! `len(uvarint) encoding(1) data(len-1) crc32c(4)`.
//! `len` includes the encoding byte. CRC covers encoding+data.

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

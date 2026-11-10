//! Prometheus TSDB index reader (format v1/v2/v3).
//!
//! Only the symbols and series sections are used: a full export does not need
//! the postings engine, so we deliberately avoid building any label index in
//! memory. Series are visited sequentially in lexicographic label order, which
//! is also the physical order of their chunks in the segment files.

use std::cell::RefCell;
use std::path::Path;
use std::sync::Arc;

use memmap2::{Mmap, MmapOptions};

use crate::error::{Error, Result};
use crate::varint;

const MAGIC_INDEX: u32 = 0xBAAA_D700;
const HEADER_LEN: usize = 5;
const TOC_LEN: usize = 6 * 8 + 4;
const SERIES_ALIGN: usize = 16;
const SYMBOL_FACTOR: usize = 32;
const SYMBOL_CACHE_BITS: usize = 14;

#[derive(Debug, Clone, Copy, Default)]
pub struct Toc {
    pub symbols: u64,
    pub series: u64,
    pub label_indices: u64,
    pub label_indices_table: u64,
    pub postings: u64,
    pub postings_table: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkMeta {
    /// Packed chunk reference: high 32 bits segment index, low 32 bits offset.
    pub reference: u64,
    pub min_time: i64,
    pub max_time: i64,
}

impl ChunkMeta {
    pub fn segment(&self) -> u32 {
        (self.reference >> 32) as u32
    }

    pub fn offset(&self) -> u32 {
        self.reference as u32
    }
}

#[derive(Debug, Clone)]
pub struct SeriesRef {
    /// Reference as stored in the index (v2/v3: byte offset / 16).
    pub id: u64,
    /// Byte offset of the series record in the index file.
    pub offset: u64,
    pub labels: Vec<(Arc<str>, Arc<str>)>,
    pub chunks: Vec<ChunkMeta>,
}

impl SeriesRef {
    pub fn label(&self, name: &str) -> Option<&str> {
        self.labels
            .iter()
            .find(|(n, _)| n.as_ref() == name)
            .map(|(_, v)| v.as_ref())
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct IndexStats {
    pub series: u64,
    pub chunks: u64,
    pub min_series_offset: u64,
    pub end_series_offset: u64,
}

struct Symbols {
    /// Absolute file offset of the first symbol entry.
    base: usize,
    /// Absolute file offset of the end of the symbol section (CRC excluded).
    end: usize,
    count: usize,
    /// Absolute offsets of every 32nd symbol (same scheme as Prometheus).
    offsets: Vec<usize>,
    version: u8,
    cache: RefCell<Vec<Option<(u32, Arc<str>)>>>,
}

impl Symbols {
    fn empty() -> Self {
        Self {
            base: 0,
            end: 0,
            count: 0,
            offsets: Vec::new(),
            version: 1,
            cache: RefCell::new(vec![None; 1 << SYMBOL_CACHE_BITS]),
        }
    }

    fn new(mmap: &[u8], off: usize, version: u8, verify: bool) -> Result<Self> {
        if off == 0 || off + 8 > mmap.len() {
            return Err(Error::format("invalid symbol table offset"));
        }
        let len = be32(mmap, off)? as usize;
        let count = be32(mmap, off + 4)? as usize;
        let payload_start = off + 4;
        let payload_end = payload_start
            .checked_add(len)
            .ok_or_else(|| Error::format("symbol table length overflow"))?;
        if payload_end + 4 > mmap.len() {
            return Err(Error::format("symbol table exceeds file size"));
        }
        if verify {
            let want = be32(mmap, payload_end)?;
            let got = crc32c::crc32c(&mmap[payload_start..payload_end]);
            if want != got {
                return Err(Error::Checksum("index symbols"));
            }
        }

        let mut offsets = Vec::with_capacity(1 + count / SYMBOL_FACTOR);
        let mut pos = off + 8; // skip len + count
        let mut seen = 0usize;
        while pos < payload_end {
            if seen % SYMBOL_FACTOR == 0 {
                offsets.push(pos);
            }
            let slen = varint::uvarint(mmap, &mut pos)? as usize;
            pos = pos
                .checked_add(slen)
                .ok_or_else(|| Error::format("symbol length overflow"))?;
            if pos > payload_end {
                return Err(Error::format("symbol exceeds symbol table"));
            }
            seen += 1;
        }
        if seen != count {
            return Err(Error::format(format!(
                "symbol count mismatch: header {count}, scanned {seen}"
            )));
        }

        Ok(Self {
            base: off + 8,
            end: payload_end,
            count,
            offsets,
            version,
            cache: RefCell::new(vec![None; 1 << SYMBOL_CACHE_BITS]),
        })
    }

    #[inline]
    fn lookup(&self, mmap: &[u8], r: u32) -> Result<Arc<str>> {
        let slot = ((r as usize).wrapping_mul(0x9E37_79B1) >> (32 - SYMBOL_CACHE_BITS))
            & ((1 << SYMBOL_CACHE_BITS) - 1);
        if let Some((cached_ref, s)) = &self.cache.borrow()[slot] {
            if *cached_ref == r {
                return Ok(s.clone());
            }
        }

        let mut pos = if self.version == 1 {
            r as usize
        } else {
            let idx = r as usize;
            if idx >= self.count {
                return Err(Error::format(format!(
                    "symbol reference {idx} out of range (count {})",
                    self.count
                )));
            }
            let mut p = *self
                .offsets
                .get(idx / SYMBOL_FACTOR)
                .ok_or_else(|| Error::format("symbol offset group missing"))?;
            for _ in 0..(idx % SYMBOL_FACTOR) {
                let slen = varint::uvarint(mmap, &mut p)? as usize;
                p += slen;
            }
            p
        };

        if pos < self.base || pos >= self.end {
            return Err(Error::format(format!("symbol offset {pos} out of range")));
        }
        let len = varint::uvarint(mmap, &mut pos)? as usize;
        let end = pos
            .checked_add(len)
            .ok_or_else(|| Error::format("symbol length overflow"))?;
        if end > self.end {
            return Err(Error::format("symbol exceeds symbol table"));
        }
        let s: Arc<str> = std::str::from_utf8(&mmap[pos..end])
            .map_err(|e| Error::format(format!("invalid utf-8 symbol: {e}")))?
            .into();
        self.cache.borrow_mut()[slot] = Some((r, s.clone()));
        Ok(s)
    }
}

pub struct IndexReader {
    mmap: Mmap,
    version: u8,
    toc: Toc,
    symbols: Symbols,
    series_end: usize,
    verify_crc: bool,
}

impl std::fmt::Debug for IndexReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexReader")
            .field("version", &self.version)
            .field("toc", &self.toc)
            .field("series_end", &self.series_end)
            .finish()
    }
}

impl IndexReader {
    pub fn open(path: &Path, verify_crc: bool) -> Result<Self> {
        let file = std::fs::File::open(path)?;
        let mmap = unsafe { MmapOptions::new().map(&file)? };
        Self::from_mmap(mmap, verify_crc)
    }

    pub fn from_mmap(mmap: Mmap, verify_crc: bool) -> Result<Self> {
        let len = mmap.len();
        if len < HEADER_LEN + TOC_LEN {
            return Err(Error::format("index file too small"));
        }
        let magic = be32(&mmap, 0)?;
        if magic != MAGIC_INDEX {
            return Err(Error::format(format!("invalid index magic 0x{magic:08x}")));
        }
        let version = mmap[4];
        if !(1..=3).contains(&version) {
            return Err(Error::Unsupported(format!(
                "index format version {version}"
            )));
        }

        let toc_off = len - TOC_LEN;
        let want = be32(&mmap, toc_off + 48)?;
        if verify_crc {
            let got = crc32c::crc32c(&mmap[toc_off..toc_off + 48]);
            if want != got {
                return Err(Error::Checksum("index toc"));
            }
        }
        let toc = Toc {
            symbols: be64(&mmap, toc_off)?,
            series: be64(&mmap, toc_off + 8)?,
            label_indices: be64(&mmap, toc_off + 16)?,
            label_indices_table: be64(&mmap, toc_off + 24)?,
            postings: be64(&mmap, toc_off + 32)?,
            postings_table: be64(&mmap, toc_off + 40)?,
        };
        let series_start = toc.series as usize;
        if toc.series != 0 && (series_start >= len || series_start < HEADER_LEN) {
            return Err(Error::format("series section offset out of range"));
        }

        // The series section ends where the next section starts. `label_indices`
        // is the canonical boundary for v2/v3; fall back defensively if a writer
        // left it unset.
        let mut series_end = len - TOC_LEN;
        for cand in [toc.label_indices, toc.postings, toc.postings_table] {
            let c = cand as usize;
            if cand != 0 && c > series_start && c <= len - TOC_LEN {
                series_end = series_end.min(c);
            }
        }
        if toc.series == 0 || series_start >= series_end {
            series_end = series_start;
        }

        let symbols = if toc.symbols == 0 || toc.symbols as usize >= len {
            Symbols::empty()
        } else {
            Symbols::new(&mmap, toc.symbols as usize, version, verify_crc)?
        };

        Ok(Self {
            mmap,
            version,
            toc,
            symbols,
            series_end,
            verify_crc,
        })
    }

    pub fn version(&self) -> u8 {
        self.version
    }

    pub fn toc(&self) -> Toc {
        self.toc
    }

    pub fn series_start(&self) -> u64 {
        align_up(self.toc.series as usize, self.align()) as u64
    }

    pub fn series_end(&self) -> u64 {
        self.series_end as u64
    }

    pub fn bytes(&self) -> &[u8] {
        &self.mmap
    }

    #[inline]
    fn align(&self) -> usize {
        if self.version == 1 {
            1
        } else {
            SERIES_ALIGN
        }
    }

    /// Iterates series beginning at the given byte offset (0 means "from the
    /// start of the series section").
    pub fn series(&self, start_offset: u64) -> SeriesIter<'_> {
        let start = if start_offset == 0 {
            self.series_start() as usize
        } else {
            start_offset as usize
        };
        SeriesIter {
            index: self,
            pos: start,
            done: false,
        }
    }

    /// Scans the series section without resolving symbols, collecting exact
    /// series/chunk counts. Used when `meta.json` has no stats.
    pub fn scan_stats(&self) -> Result<IndexStats> {
        let mut pos = self.series_start() as usize;
        let mut stats = IndexStats {
            min_series_offset: pos as u64,
            end_series_offset: pos as u64,
            ..Default::default()
        };
        while pos < self.series_end {
            if self.mmap.get(pos).copied().unwrap_or(0) == 0 {
                break;
            }
            let start = pos;
            let (payload_len, mut p) = varint::peek_uvarint(&self.mmap, pos)?;
            let payload_end = p
                .checked_add(payload_len as usize)
                .ok_or_else(|| Error::format("series length overflow"))?;
            if payload_end + 4 > self.series_end {
                return Err(Error::format("series record exceeds series section"));
            }
            let labels = varint::uvarint(&self.mmap, &mut p)?;
            for _ in 0..labels {
                varint::uvarint(&self.mmap, &mut p)?;
                varint::uvarint(&self.mmap, &mut p)?;
            }
            let chunks = varint::uvarint(&self.mmap, &mut p)?;
            if chunks > 0 {
                varint::varint(&self.mmap, &mut p)?; // mint
                varint::uvarint(&self.mmap, &mut p)?; // maxt - mint
                varint::uvarint(&self.mmap, &mut p)?; // ref
                for _ in 1..chunks {
                    varint::uvarint(&self.mmap, &mut p)?;
                    varint::uvarint(&self.mmap, &mut p)?;
                    varint::varint(&self.mmap, &mut p)?;
                }
            }
            if p > payload_end {
                return Err(Error::format("series payload truncated"));
            }
            stats.series += 1;
            stats.chunks += chunks;

            let next = align_up(payload_end + 4, self.align());
            pos = next;
            stats.end_series_offset = next as u64;
            if next <= start {
                return Err(Error::format("series parser made no progress"));
            }
        }
        Ok(stats)
    }

    fn parse_series(&self, pos: usize) -> Result<(SeriesRef, usize)> {
        let start = pos;
        let (payload_len, payload_start) = varint::peek_uvarint(&self.mmap, start)?;
        let payload_len = payload_len as usize;
        let payload_end = payload_start
            .checked_add(payload_len)
            .ok_or_else(|| Error::format("series length overflow"))?;
        if payload_end + 4 > self.mmap.len() {
            return Err(Error::format("series record exceeds index file"));
        }
        if self.verify_crc {
            let want = be32(&self.mmap, payload_end)?;
            let got = crc32c::crc32c(&self.mmap[payload_start..payload_end]);
            if want != got {
                return Err(Error::Checksum("index series"));
            }
        }

        let mut p = payload_start;
        let num_labels = varint::uvarint(&self.mmap, &mut p)?;
        let mut labels = Vec::with_capacity(num_labels.min(64) as usize);
        for _ in 0..num_labels {
            let name = varint::uvarint32(&self.mmap, &mut p)?;
            let value = varint::uvarint32(&self.mmap, &mut p)?;
            labels.push((
                self.symbols.lookup(&self.mmap, name)?,
                self.symbols.lookup(&self.mmap, value)?,
            ));
        }

        let num_chunks = varint::uvarint(&self.mmap, &mut p)?;
        let mut chunks = Vec::with_capacity(num_chunks.min(1024) as usize);
        if num_chunks > 0 {
            let mint = varint::varint(&self.mmap, &mut p)?;
            let maxt = mint
                .checked_add(varint::uvarint(&self.mmap, &mut p)? as i64)
                .ok_or_else(|| Error::format("chunk maxt overflow"))?;
            let reference = varint::uvarint(&self.mmap, &mut p)?;
            let mut prev_maxt = maxt;
            let mut prev_ref = reference as i64;
            chunks.push(ChunkMeta {
                reference,
                min_time: mint,
                max_time: maxt,
            });
            for _ in 1..num_chunks {
                let mint = prev_maxt
                    .checked_add(varint::uvarint(&self.mmap, &mut p)? as i64)
                    .ok_or_else(|| Error::format("chunk mint overflow"))?;
                let maxt = mint
                    .checked_add(varint::uvarint(&self.mmap, &mut p)? as i64)
                    .ok_or_else(|| Error::format("chunk maxt overflow"))?;
                let delta = varint::varint(&self.mmap, &mut p)?;
                let reference = prev_ref
                    .checked_add(delta)
                    .ok_or_else(|| Error::format("chunk ref overflow"))?
                    as u64;
                prev_maxt = maxt;
                prev_ref = reference as i64;
                chunks.push(ChunkMeta {
                    reference,
                    min_time: mint,
                    max_time: maxt,
                });
            }
        }
        if p > payload_end {
            return Err(Error::format("series payload truncated"));
        }

        let next = align_up(payload_end + 4, self.align());
        let id = if self.version == 1 {
            start as u64
        } else {
            (start / SERIES_ALIGN) as u64
        };
        Ok((
            SeriesRef {
                id,
                offset: start as u64,
                labels,
                chunks,
            },
            next,
        ))
    }
}

pub struct SeriesIter<'a> {
    index: &'a IndexReader,
    pos: usize,
    done: bool,
}

impl SeriesIter<'_> {
    /// Current byte offset in the index; the start of the next series.
    pub fn position(&self) -> u64 {
        self.pos as u64
    }

    pub fn is_done(&self) -> bool {
        self.done || self.pos >= self.index.series_end
    }
}

impl Iterator for SeriesIter<'_> {
    type Item = Result<SeriesRef>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        if self.pos >= self.index.series_end {
            self.done = true;
            return None;
        }
        // Series sections are zero padded before the next section starts.
        if self.index.mmap.get(self.pos).copied().unwrap_or(0) == 0 {
            self.done = true;
            return None;
        }
        match self.index.parse_series(self.pos) {
            Ok((series, next)) => {
                self.pos = next;
                Some(Ok(series))
            }
            Err(e) => {
                self.done = true;
                Some(Err(e))
            }
        }
    }
}

#[inline]
fn align_up(v: usize, align: usize) -> usize {
    if align <= 1 {
        v
    } else {
        (v + align - 1) & !(align - 1)
    }
}

#[inline]
fn be32(buf: &[u8], off: usize) -> Result<u32> {
    let b = buf.get(off..off + 4).ok_or(Error::Eof(off))?;
    Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
}

#[inline]
fn be64(buf: &[u8], off: usize) -> Result<u64> {
    let b = buf.get(off..off + 8).ok_or(Error::Eof(off))?;
    Ok(u64::from_be_bytes([
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alignment() {
        assert_eq!(align_up(0, 16), 0);
        assert_eq!(align_up(1, 16), 16);
        assert_eq!(align_up(16, 16), 16);
        assert_eq!(align_up(17, 16), 32);
        assert_eq!(align_up(17, 1), 17);
    }

    #[test]
    fn series_id_uses_offset_div_16() {
        assert_eq!((160usize / SERIES_ALIGN) as u64, 10);
    }
}

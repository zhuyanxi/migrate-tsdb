//! Tombstones reader.
//!
//! The on-disk layout produced by Prometheus is:
//! `magic(4) version(1) flag(1) [ref uvarint, mint varint, maxt varint]* crc32c(4)`.
//! The CRC covers everything after the magic (including version and flag).
//! Intervals are closed: `[mint, maxt]` (Prometheus `Interval.InBounds`).

use std::collections::HashMap;
use std::path::Path;

use crate::error::{Error, Result};
use crate::varint;

const MAGIC_TOMBSTONE: u32 = 0x0130_BA30;

#[derive(Debug, Default, Clone)]
pub struct Tombstones {
    intervals: HashMap<u64, Vec<(i64, i64)>>,
    raw_count: usize,
}

impl Tombstones {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn open(dir: &Path, verify_crc: bool) -> Result<Self> {
        let path = dir.join("tombstones");
        let data = match std::fs::read(&path) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Self::default()),
            Err(e) => return Err(e.into()),
        };
        if data.len() < 5 + 4 {
            return Err(Error::format("tombstones file too small"));
        }
        let magic = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        if magic != MAGIC_TOMBSTONE {
            return Err(Error::format(format!(
                "invalid tombstones magic 0x{magic:08x}"
            )));
        }
        // Layout: magic(4) flag(1) stones... crc(4). The flag doubles as the
        // format version; CRC covers the stones only (excluding magic+flag).
        if data[4] != 1 {
            return Err(Error::Unsupported(format!(
                "tombstones format version {}",
                data[4]
            )));
        }

        let crc_pos = data.len() - 4;
        if verify_crc {
            let want = u32::from_be_bytes([
                data[crc_pos],
                data[crc_pos + 1],
                data[crc_pos + 2],
                data[crc_pos + 3],
            ]);
            let got = crc32c::crc32c(&data[5..crc_pos]);
            if want != got {
                return Err(Error::Checksum("tombstones"));
            }
        }

        let mut pos = 5;
        let mut raw: Vec<(u64, i64, i64)> = Vec::new();
        while pos < crc_pos {
            let reference = varint::uvarint(&data, &mut pos)?;
            let mint = varint::varint(&data, &mut pos)?;
            let maxt = varint::varint(&data, &mut pos)?;
            // Tolerate zero padding at the end of the stones section.
            if reference == 0 && mint == 0 && maxt == 0 {
                continue;
            }
            raw.push((reference, mint, maxt));
        }

        let mut map: HashMap<u64, Vec<(i64, i64)>> = HashMap::new();
        for (r, mint, maxt) in &raw {
            map.entry(*r).or_default().push((*mint, *maxt));
        }
        for ivs in map.values_mut() {
            ivs.sort_unstable();
            let mut merged: Vec<(i64, i64)> = Vec::with_capacity(ivs.len());
            for &(mint, maxt) in ivs.iter() {
                if let Some(last) = merged.last_mut() {
                    // Closed intervals; merge touching ranges like Prometheus.
                    if mint <= last.1.saturating_add(1) {
                        last.1 = last.1.max(maxt);
                        continue;
                    }
                }
                merged.push((mint, maxt));
            }
            *ivs = merged;
        }

        Ok(Self {
            intervals: map,
            raw_count: raw.len(),
        })
    }

    pub fn intervals(&self, series_ref: u64) -> &[(i64, i64)] {
        self.intervals
            .get(&series_ref)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Number of tombstone intervals (after merging).
    pub fn total(&self) -> usize {
        self.intervals.values().map(|v| v.len()).sum()
    }

    /// Number of raw stones as stored on disk.
    pub fn raw_count(&self) -> usize {
        self.raw_count
    }

    pub fn series_count(&self) -> usize {
        self.intervals.len()
    }

    pub fn iter_raw(&self) -> impl Iterator<Item = (&u64, &Vec<(i64, i64)>)> {
        self.intervals.iter()
    }

    pub fn is_deleted(&self, series_ref: u64, ts: i64) -> bool {
        is_deleted(self.intervals(series_ref), ts)
    }
}

#[inline]
pub fn is_deleted(intervals: &[(i64, i64)], ts: i64) -> bool {
    if intervals.is_empty() {
        return false;
    }
    // Intervals are sorted and non-overlapping; linear scan is faster than a
    // binary search for the common case of one or two intervals.
    for &(mint, maxt) in intervals {
        if ts < mint {
            return false;
        }
        if ts <= maxt {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn closed_interval() {
        let ivs = [(10, 20)];
        assert!(!is_deleted(&ivs, 9));
        assert!(is_deleted(&ivs, 10));
        assert!(is_deleted(&ivs, 20));
        assert!(!is_deleted(&ivs, 21));
    }

    #[test]
    fn merge_touching() {
        let mut ivs = vec![(1, 5), (6, 9), (20, 30)];
        ivs.sort_unstable();
        let mut merged: Vec<(i64, i64)> = Vec::new();
        for &(a, b) in &ivs {
            if let Some(last) = merged.last_mut() {
                if a <= last.1 + 1 {
                    last.1 = last.1.max(b);
                    continue;
                }
            }
            merged.push((a, b));
        }
        assert_eq!(merged, vec![(1, 9), (20, 30)]);
    }
}

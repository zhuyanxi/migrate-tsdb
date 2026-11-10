//! Go-compatible varint decoding (binary.PutUvarint / PutVarint).
//!
//! Prometheus uses Go's unsigned LEB128 and zig-zag signed varints in both the
//! index format and chunk bit streams. Error behaviour follows Go's
//! `binary.Uvarint`: overflow and truncation are reported separately.

use crate::error::{Error, Result};

pub const MAX_VARINT_LEN: usize = 10;

/// Reads an unsigned LEB128 varint, advancing `pos`.
pub fn uvarint(buf: &[u8], pos: &mut usize) -> Result<u64> {
    let start = *pos;
    let mut x: u64 = 0;
    let mut s: u32 = 0;
    for i in 0..MAX_VARINT_LEN {
        let b = *buf.get(*pos).ok_or(Error::Eof(start))?;
        *pos += 1;
        if b < 0x80 {
            if i == 9 && b > 1 {
                return Err(Error::format("uvarint overflows 64 bits"));
            }
            return Ok(x | ((b as u64) << s));
        }
        x |= ((b & 0x7f) as u64) << s;
        s += 7;
    }
    Err(Error::format("uvarint too long"))
}

/// Reads a zig-zag encoded signed varint, advancing `pos`.
pub fn varint(buf: &[u8], pos: &mut usize) -> Result<i64> {
    let u = uvarint(buf, pos)?;
    Ok(((u >> 1) as i64) ^ -((u & 1) as i64))
}

/// Reads a varint without advancing `pos`, returning (value, next_pos).
pub fn peek_uvarint(buf: &[u8], pos: usize) -> Result<(u64, usize)> {
    let mut p = pos;
    let v = uvarint(buf, &mut p)?;
    Ok((v, p))
}

/// Upper bound for a 32-bit length field (used by index sections).
pub fn uvarint32(buf: &[u8], pos: &mut usize) -> Result<u32> {
    let v = uvarint(buf, pos)?;
    u32::try_from(v).map_err(|_| Error::format(format!("uvarint {v} does not fit in u32")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_examples() {
        let cases: &[(&[u8], u64)] = &[
            (&[0x00], 0),
            (&[0x01], 1),
            (&[0x7f], 127),
            (&[0x80, 0x01], 128),
            (&[0xff, 0x01], 255),
            (&[0xac, 0x02], 300),
            (&[0x80, 0x80, 0x80, 0x80, 0x10], 1 << 32),
        ];
        for (bytes, want) in cases {
            let mut pos = 0;
            assert_eq!(uvarint(bytes, &mut pos).unwrap(), *want);
            assert_eq!(pos, bytes.len());
        }
    }

    #[test]
    fn zigzag() {
        for (bytes, want) in [
            (&[0x00u8][..], 0i64),
            (&[0x01][..], -1),
            (&[0x02][..], 1),
            (&[0x03][..], -2),
            (&[0x04][..], 2),
        ] {
            let mut pos = 0;
            assert_eq!(varint(bytes, &mut pos).unwrap(), want);
        }
    }

    #[test]
    fn truncated_is_error() {
        let mut pos = 0;
        assert!(matches!(uvarint(&[0x80], &mut pos), Err(Error::Eof(0))));
    }
}

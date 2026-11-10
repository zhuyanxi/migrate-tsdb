//! XOR chunk decoder, ported from `tsdb/chunkenc/xor.go`.

use crate::error::{Error, Result};

use super::bstream::BitReader;
use super::SampleSink;

const CHUNK_HEADER_SIZE: usize = 2;

#[inline]
pub(crate) fn xor_read(
    br: &mut BitReader<'_>,
    value: &mut f64,
    leading: &mut u8,
    trailing: &mut u8,
) -> Result<()> {
    if !br.read_bit()? {
        return Ok(());
    }
    if !br.read_bit()? {
        // Reuse the previous leading/trailing zero window.
        let mbits = 64 - *leading - *trailing;
        let bits = br.read_bits(mbits)?;
        let v = value.to_bits() ^ (bits << *trailing);
        *value = f64::from_bits(v);
        return Ok(());
    }
    let new_leading = br.read_bits(5)? as u8;
    let mut mbits = br.read_bits(6)? as u8;
    if mbits == 0 {
        mbits = 64;
    }
    let new_trailing = 64 - new_leading - mbits;
    *leading = new_leading;
    *trailing = new_trailing;
    let bits = br.read_bits(mbits)?;
    let v = value.to_bits() ^ (bits << new_trailing);
    *value = f64::from_bits(v);
    Ok(())
}

/// Decodes all samples of an XOR chunk into `sink`.
pub fn decode<S: SampleSink>(data: &[u8], sink: &mut S) -> Result<u64> {
    if data.len() < CHUNK_HEADER_SIZE {
        return Err(Error::decode("XOR chunk shorter than header"));
    }
    let total = u16::from_be_bytes([data[0], data[1]]) as u64;
    if total == 0 {
        return Ok(0);
    }
    let mut br = BitReader::new(&data[CHUNK_HEADER_SIZE..]);

    let mut t: i64 = 0;
    let mut value: f64 = 0.0;
    let mut leading: u8 = 0;
    let mut trailing: u8 = 0;
    let mut t_delta: u64 = 0;

    for i in 0..total {
        if i == 0 {
            t = br.read_varint()?;
            let bits = br.read_bits(64)?;
            value = f64::from_bits(bits);
        } else if i == 1 {
            t_delta = br.read_uvarint()?;
            t = t.wrapping_add(t_delta as i64);
            xor_read(&mut br, &mut value, &mut leading, &mut trailing)?;
        } else {
            // Delta-of-delta prefix: 0, 10, 110, 1110, 1111.
            let mut d: u8 = 0;
            for _ in 0..4 {
                d <<= 1;
                if !br.read_bit()? {
                    break;
                }
                d |= 1;
            }
            let mut dod: i64 = 0;
            let sz: u8 = match d {
                0b0 => 0,
                0b10 => 14,
                0b110 => 17,
                0b1110 => 20,
                0b1111 => {
                    dod = br.read_bits(64)? as i64;
                    0
                }
                _ => return Err(Error::decode("invalid XOR dod prefix")),
            };
            if sz != 0 {
                let mut bits = br.read_bits(sz)?;
                if bits > (1u64 << (sz - 1)) {
                    bits = bits.wrapping_sub(1u64 << sz);
                }
                dod = bits as i64;
            }
            t_delta = (t_delta as i64).wrapping_add(dod) as u64;
            t = t.wrapping_add(t_delta as i64);
            xor_read(&mut br, &mut value, &mut leading, &mut trailing)?;
        }
        sink.push_float(t, value);
    }
    Ok(total)
}

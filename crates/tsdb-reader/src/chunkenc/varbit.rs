//! Prometheus varbit integer encodings (`chunkenc/varbit.go`).
//!
//! The sign handling intentionally mirrors the upstream implementation,
//! including the `>` (not `>=`) threshold check.

use crate::error::{Error, Result};

use super::bstream::BitReader;

pub fn read_varbit_int(br: &mut BitReader<'_>) -> Result<i64> {
    let mut d: u8 = 0;
    for _ in 0..8 {
        d <<= 1;
        if !br.read_bit()? {
            break;
        }
        d |= 1;
    }

    let mut val: i64 = 0;
    let mut sz: u8 = 0;
    match d {
        0b0 => {}
        0b10 => sz = 3,
        0b110 => sz = 6,
        0b1110 => sz = 9,
        0b11110 => sz = 12,
        0b111110 => sz = 18,
        0b1111110 => sz = 25,
        0b11111110 => sz = 56,
        0b11111111 => {
            let bits = br.read_bits(64)?;
            val = bits as i64;
        }
        _ => return Err(Error::decode(format!("invalid varbit pattern {d:b}"))),
    }

    if sz != 0 {
        let mut bits = br.read_bits(sz)?;
        if bits > (1u64 << (sz - 1)) {
            bits = bits.wrapping_sub(1u64 << sz);
        }
        val = bits as i64;
    }
    Ok(val)
}

pub fn read_varbit_uint(br: &mut BitReader<'_>) -> Result<u64> {
    let mut d: u8 = 0;
    for _ in 0..8 {
        d <<= 1;
        if !br.read_bit()? {
            break;
        }
        d |= 1;
    }

    let mut bits: u64 = 0;
    let mut sz: u8 = 0;
    match d {
        0b0 => {}
        0b10 => sz = 3,
        0b110 => sz = 6,
        0b1110 => sz = 9,
        0b11110 => sz = 12,
        0b111110 => sz = 18,
        0b1111110 => sz = 25,
        0b11111110 => sz = 56,
        0b11111111 => bits = br.read_bits(64)?,
        _ => return Err(Error::decode(format!("invalid varbit pattern {d:b}"))),
    }
    if sz != 0 {
        bits = br.read_bits(sz)?;
    }
    Ok(bits)
}

#[cfg(test)]
mod tests {
    use super::*;

    // These encodings are produced by Go's putVarbitInt. A round-trip test
    // against real fixtures lives in tests/chunkenc_truth.rs; here we only
    // check the bucket boundaries and sign handling on hand-built bits.

    fn bits_bytes(bits: &str) -> Vec<u8> {
        let mut out = Vec::new();
        let mut cur = 0u8;
        let mut n = 0;
        for ch in bits.chars() {
            cur = (cur << 1) | if ch == '1' { 1 } else { 0 };
            n += 1;
            if n == 8 {
                out.push(cur);
                cur = 0;
                n = 0;
            }
        }
        if n > 0 {
            out.push(cur << (8 - n));
        }
        out
    }

    #[test]
    fn zero_is_single_bit() {
        let data = bits_bytes("0");
        let mut br = BitReader::new(&data);
        assert_eq!(read_varbit_int(&mut br).unwrap(), 0);
    }

    #[test]
    fn small_positive_and_negative() {
        // 0b10 + 3 bits: val=1 -> 001 ; val=-1 -> 111
        let data = bits_bytes("10001");
        let mut br = BitReader::new(&data);
        assert_eq!(read_varbit_int(&mut br).unwrap(), 1);

        let data = bits_bytes("10111");
        let mut br = BitReader::new(&data);
        assert_eq!(read_varbit_int(&mut br).unwrap(), -1);
    }
}

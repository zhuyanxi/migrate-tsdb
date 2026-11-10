//! Bit stream reader compatible with Prometheus' `chunkenc.bstreamReader`.
//!
//! Prometheus writes bits MSB-first and refills its internal buffer with
//! big-endian bytes; `read_bits(n)` returns the next `n` bits as the low bits
//! of a `u64`. This implementation keeps the same semantics so the decoders can
//! be a direct port.

use crate::error::{Error, Result};

pub struct BitReader<'a> {
    data: &'a [u8],
    pos: usize,
    buffer: u64,
    valid: u8,
}

impl<'a> BitReader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            pos: 0,
            buffer: 0,
            valid: 0,
        }
    }

    #[inline]
    pub fn bits_left_hint(&self) -> usize {
        (self.data.len() - self.pos) * 8 + self.valid as usize
    }

    #[inline]
    fn load_next(&mut self, nbits: u8) -> bool {
        if self.pos >= self.data.len() {
            return false;
        }
        // Fast path: at least 9 bytes remain, so a full 8-byte word cannot
        // cross the end of the buffer.
        if self.pos + 8 < self.data.len() {
            let b = &self.data[self.pos..self.pos + 8];
            self.buffer = u64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]);
            self.pos += 8;
            self.valid = 64;
            return true;
        }
        let mut nbytes = (nbits as usize / 8) + 1;
        if self.pos + nbytes > self.data.len() {
            nbytes = self.data.len() - self.pos;
        }
        let mut buf = 0u64;
        for i in 0..nbytes {
            buf |= (self.data[self.pos + i] as u64) << (8 * (nbytes - i - 1));
        }
        self.buffer = buf;
        self.pos += nbytes;
        self.valid = (nbytes * 8) as u8;
        true
    }

    #[inline]
    pub fn read_bit(&mut self) -> Result<bool> {
        if self.valid == 0 && !self.load_next(1) {
            return Err(Error::Eof(self.pos));
        }
        self.valid -= 1;
        Ok((self.buffer >> self.valid) & 1 == 1)
    }

    #[inline]
    pub fn read_bits(&mut self, nbits: u8) -> Result<u64> {
        if nbits == 0 {
            return Ok(0);
        }
        debug_assert!(nbits <= 64);
        if self.valid == 0 && !self.load_next(nbits) {
            return Err(Error::Eof(self.pos));
        }
        if nbits <= self.valid {
            self.valid -= nbits;
            return Ok((self.buffer >> self.valid) & mask(nbits));
        }
        let remaining = self.valid;
        let mut v = (self.buffer & mask(remaining)) << (nbits - remaining);
        let need = nbits - remaining;
        self.valid = 0;
        if !self.load_next(need) {
            return Err(Error::Eof(self.pos));
        }
        v |= (self.buffer >> (self.valid - need)) & mask(need);
        self.valid -= need;
        Ok(v)
    }

    /// Reads a big-endian byte from the bit stream (Go's `ReadByte`).
    #[inline]
    pub fn read_byte(&mut self) -> Result<u8> {
        Ok(self.read_bits(8)? as u8)
    }

    /// Go's `binary.ReadUvarint` over the bit stream.
    pub fn read_uvarint(&mut self) -> Result<u64> {
        let mut x = 0u64;
        let mut s = 0u32;
        for _ in 0..10 {
            let b = self.read_byte()?;
            if b < 0x80 {
                return Ok(x | ((b as u64) << s));
            }
            x |= ((b & 0x7f) as u64) << s;
            s += 7;
        }
        Err(Error::decode("uvarint too long"))
    }

    /// Go's `binary.ReadVarint` (zig-zag).
    pub fn read_varint(&mut self) -> Result<i64> {
        let u = self.read_uvarint()?;
        Ok(((u >> 1) as i64) ^ -((u & 1) as i64))
    }
}

#[inline]
pub(crate) fn mask(n: u8) -> u64 {
    if n >= 64 {
        u64::MAX
    } else {
        (1u64 << n) - 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn msb_first() {
        // 0b1011_0010
        let mut br = BitReader::new(&[0b1011_0010]);
        assert!(br.read_bit().unwrap());
        assert!(!br.read_bit().unwrap());
        assert!(br.read_bit().unwrap());
        assert!(br.read_bit().unwrap());
        assert_eq!(br.read_bits(4).unwrap(), 0b0010);
    }

    #[test]
    fn cross_byte_reads() {
        let data = [0x12, 0x34, 0x56, 0x78];
        let mut br = BitReader::new(&data);
        assert_eq!(br.read_bits(16).unwrap(), 0x1234);
        assert_eq!(br.read_bits(16).unwrap(), 0x5678);
    }

    #[test]
    fn eof() {
        let mut br = BitReader::new(&[0xff]);
        assert_eq!(br.read_bits(8).unwrap(), 0xff);
        assert!(br.read_bit().is_err());
    }
}

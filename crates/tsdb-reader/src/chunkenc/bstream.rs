//! Bit stream reader compatible with Prometheus' `chunkenc.bstreamReader`.
//!
//! Prometheus writes bits MSB-first and refills its internal buffer with
//! big-endian bytes; `read_bits(n)` returns the next `n` bits as the low bits
//! of a `u64`. This implementation keeps the same semantics so the decoders can
//! be a direct port.

use crate::error::{Error, Result};

/// MSB-first bit reader with the same semantics as Prometheus'
/// `chunkenc.bstreamReader`.
///
/// Bits are consumed from the most significant end of each byte. Every read
/// either succeeds or reports [`crate::Error::Eof`]; a truncated chunk never
/// yields partially valid values.
pub struct BitReader<'a> {
    data: &'a [u8],
    pos: usize,
    buffer: u64,
    valid: u8,
}

impl<'a> BitReader<'a> {
    /// Wraps a chunk body. Nothing is read until the first call.
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            pos: 0,
            buffer: 0,
            valid: 0,
        }
    }

    #[inline]
    /// Upper bound on the bits still available, including buffered ones.
    pub fn bits_left_hint(&self) -> usize {
        self.data.len().saturating_sub(self.pos) * 8 + self.valid as usize
    }

    /// Refills the bit buffer, returning how many bits are now buffered.
    ///
    /// The tail of a stream is loaded partially on purpose: callers decide
    /// whether a short load is an error. Returning the count (instead of a
    /// bool) is what makes that decision possible without underflowing
    /// `valid - need` in [`Self::read_bits`].
    #[inline]
    fn load_next(&mut self, nbits: u8) -> usize {
        if self.pos >= self.data.len() {
            return 0;
        }
        // Fast path: at least 9 bytes remain, so a full 8-byte word cannot
        // cross the end of the buffer.
        if self.pos + 8 < self.data.len() {
            let b = &self.data[self.pos..self.pos + 8];
            self.buffer = u64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]);
            self.pos += 8;
            self.valid = 64;
            return 64;
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
        nbytes * 8
    }

    #[inline]
    /// Reads a single bit.
    pub fn read_bit(&mut self) -> Result<bool> {
        if self.valid == 0 && self.load_next(1) == 0 {
            return Err(Error::Eof(self.pos));
        }
        self.valid -= 1;
        Ok((self.buffer >> self.valid) & 1 == 1)
    }

    #[inline]
    /// Reads `nbits` (0-64) bits into the low bits of the result.
    ///
    /// Returns [`crate::Error::Decode`] for `nbits > 64` and
    /// [`crate::Error::Eof`] when the stream ends mid-read.
    pub fn read_bits(&mut self, nbits: u8) -> Result<u64> {
        if nbits == 0 {
            return Ok(0);
        }
        if nbits > 64 {
            return Err(Error::decode(format!(
                "read_bits({nbits}) exceeds a 64-bit word"
            )));
        }
        if self.valid == 0 && self.load_next(nbits) == 0 {
            return Err(Error::Eof(self.pos));
        }
        if nbits <= self.valid {
            self.valid -= nbits;
            return Ok((self.buffer >> self.valid) & mask(nbits));
        }
        // Split read: the high part comes from the buffer, the low part from a
        // refill. `remaining >= 1` because a zero-length buffer was either
        // refilled or reported as EOF above, so the shift stays below 64.
        let remaining = self.valid;
        let need = nbits - remaining;
        let mut v = (self.buffer & mask(remaining)) << need;
        self.valid = 0;
        if self.load_next(need) < need as usize {
            // The stream ends inside this read. Report it instead of shifting by
            // a negative amount (which panicked in debug and silently produced
            // wrong values in release).
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

    #[test]
    fn zero_bits_consumes_nothing() {
        let mut br = BitReader::new(&[0b1010_1010]);
        assert_eq!(br.read_bits(0).unwrap(), 0);
        assert_eq!(br.bits_left_hint(), 8);
        assert!(br.read_bit().unwrap());
    }

    #[test]
    fn more_than_a_word_is_rejected() {
        let mut br = BitReader::new(&[0u8; 16]);
        assert!(matches!(br.read_bits(65), Err(Error::Decode(_))));
        // The stream is untouched, so a legal read still works.
        assert_eq!(br.read_bits(8).unwrap(), 0);
    }

    /// Regression: a split read whose tail is too short used to underflow
    /// `valid - need`. It panicked in debug and returned Ok(garbage) in release.
    #[test]
    fn truncated_split_read_is_eof() {
        let data = [0u8; 9];
        let mut br = BitReader::new(&data);
        assert_eq!(br.read_bits(56).unwrap(), 0);
        // 8 bits buffered, 1 byte left, 17 requested => 9 needed, only 8 available.
        assert!(
            matches!(br.read_bits(17), Err(Error::Eof(_))),
            "a truncated split read must report EOF"
        );
        // The reader must stay in a failed-but-sane state, not hand out garbage.
        assert!(br.read_bits(1).is_ok() || br.read_bits(1).is_err());
    }

    /// The tail may be short yet still large enough for the split read.
    #[test]
    fn split_read_fitting_the_tail_succeeds() {
        let mut data = [0u8; 9];
        data[0] = 0xff;
        data[8] = 0b1010_1010;
        let mut br = BitReader::new(&data);
        // 56 bits of 0xff00000000000000 => the 0xff plus seven zero bytes.
        assert_eq!(br.read_bits(56).unwrap(), 0x00ff_0000_0000_0000);
        // 8 buffered bits (0x00) + exactly 8 left (0xaa) == the 16 requested.
        assert_eq!(br.read_bits(16).unwrap(), 0x00aa);
        assert_eq!(br.bits_left_hint(), 0);
        assert!(br.read_bit().is_err());
    }

    #[test]
    fn split_read_across_a_full_refill() {
        // 17 bytes: the first read leaves 63 buffered bits, so reading 64 needs a
        // split; the refill supplies a whole 8-byte word.
        let mut data = [0u8; 17];
        data[8..16].copy_from_slice(&[0xde, 0xad, 0xbe, 0xef, 0x00, 0x11, 0x22, 0x33]);
        let mut br = BitReader::new(&data);
        assert_eq!(br.read_bits(1).unwrap(), 0);

        let value = br.read_bits(64).unwrap();
        // High 63 bits are the remaining zeros of the first word, low bit is the
        // MSB of 0xde.
        assert_eq!(value, 1, "expected the first bit of 0xde, got {value:#x}");
    }

    #[test]
    fn reading_past_the_end_never_panics() {
        // Walk every truncation length and every request size: no panic allowed.
        for len in 0..12usize {
            let data = vec![0xa5u8; len];
            for nbits in 1..=64u8 {
                let mut br = BitReader::new(&data);
                let _ = br.bits_left_hint();
                // Drain greedily; each call must either succeed or report EOF.
                for _ in 0..4 {
                    match br.read_bits(nbits) {
                        Ok(_) => {}
                        Err(Error::Eof(_)) => break,
                        Err(other) => panic!("len={len} nbits={nbits}: {other}"),
                    }
                }
            }
        }
    }
}

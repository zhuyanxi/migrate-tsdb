//! XOR2 chunk decoder, ported from `tsdb/chunkenc/xor2.go`.
//!
//! XOR2 adds a joint timestamp/value control prefix (sample >= 2) and an
//! optional start timestamp (ST) field. ST is decoded for bit-exactness but is
//! not exported for float samples (Prometheus does not persist float ST).

use crate::error::{Error, Result};

use super::bstream::BitReader;
use super::varbit::read_varbit_int;
use super::{SampleSink, STALE_NAN_BITS};

const CHUNK_HEADER_SIZE: usize = 2;
const ST_HEADER_SIZE: usize = 1;

struct State {
    leading: u8,
    trailing: u8,
    t: i64,
    t_delta: u64,
    value: f64,
    baseline: f64,
    st: i64,
    st_diff: i64,
    first_st_known: bool,
    first_st_change_on: u8,
}

impl State {
    fn new(st_header: u8) -> Self {
        Self {
            leading: 0,
            trailing: 0,
            t: 0,
            t_delta: 0,
            value: 0.0,
            baseline: 0.0,
            st: 0,
            st_diff: 0,
            first_st_known: st_header & 0x80 != 0,
            first_st_change_on: st_header & 0x7f,
        }
    }

    fn read_dod(&mut self, br: &mut BitReader<'_>, width: u8) -> Result<()> {
        let mut b = br.read_bits(width)?;
        if width < 64 && b >= (1u64 << (width - 1)) {
            b = b.wrapping_sub(1u64 << width);
        }
        self.t_delta = (self.t_delta as i64).wrapping_add(b as i64) as u64;
        self.t = self.t.wrapping_add(self.t_delta as i64);
        Ok(())
    }

    fn decode_new_leading_trailing(&mut self, br: &mut BitReader<'_>) -> Result<()> {
        let new_leading = br.read_bits(5)? as u8;
        let mut sigbits = br.read_bits(6)? as u8;
        if sigbits == 0 {
            sigbits = 64;
        }
        let trailing = 64 - new_leading - sigbits;
        self.leading = new_leading;
        self.trailing = trailing;
        let bits = br.read_bits(sigbits)?;
        self.value = f64::from_bits(self.baseline.to_bits() ^ (bits << trailing));
        self.baseline = self.value;
        Ok(())
    }

    fn reuse_window(&mut self, br: &mut BitReader<'_>) -> Result<()> {
        let sz = 64 - self.leading - self.trailing;
        let bits = br.read_bits(sz)?;
        self.value = f64::from_bits(self.baseline.to_bits() ^ (bits << self.trailing));
        self.baseline = self.value;
        Ok(())
    }

    /// Value encoding for dod != 0: `0`, `10`, `110`, `111`.
    fn decode_value(&mut self, br: &mut BitReader<'_>) -> Result<()> {
        if !br.read_bit()? {
            self.value = self.baseline;
            return Ok(());
        }
        if !br.read_bit()? {
            return self.reuse_window(br);
        }
        if !br.read_bit()? {
            return self.decode_new_leading_trailing(br);
        }
        self.value = f64::from_bits(STALE_NAN_BITS);
        Ok(())
    }

    /// Value encoding after `10` (dod == 0, value known changed, non-stale).
    fn decode_value_known_non_zero(&mut self, br: &mut BitReader<'_>) -> Result<()> {
        if !br.read_bit()? {
            self.reuse_window(br)
        } else {
            self.decode_new_leading_trailing(br)
        }
    }

    fn read_control(&mut self, br: &mut BitReader<'_>) -> Result<u8> {
        if !br.read_bit()? {
            return Ok(0);
        }
        if !br.read_bit()? {
            return Ok(1);
        }
        if !br.read_bit()? {
            return Ok(2);
        }
        if !br.read_bit()? {
            return Ok(3);
        }
        if !br.read_bit()? {
            return Ok(4);
        }
        Ok(5)
    }

    fn decode_st(&mut self, br: &mut BitReader<'_>, num_read: u64, prev_t: i64) -> Result<()> {
        if num_read == 1 {
            if self.first_st_known {
                let diff = br.read_varint()?;
                self.st_diff = diff;
                self.st = self.t - diff;
            }
            return Ok(());
        }
        if num_read == 2 {
            if self.first_st_change_on == 1 {
                let sdod = read_varbit_int(br)?;
                self.st_diff = sdod;
                self.st = prev_t - sdod;
            }
            return Ok(());
        }
        if self.first_st_change_on > 0 && num_read - 1 >= self.first_st_change_on as u64 {
            let sdod = read_varbit_int(br)?;
            if num_read - 1 == self.first_st_change_on as u64 {
                self.st_diff = sdod;
            } else {
                self.st_diff = self.st_diff.wrapping_add(sdod);
            }
            self.st = prev_t.wrapping_sub(self.st_diff);
        }
        Ok(())
    }
}

pub fn decode<S: SampleSink>(data: &[u8], sink: &mut S) -> Result<u64> {
    if data.len() < CHUNK_HEADER_SIZE + ST_HEADER_SIZE {
        return Err(Error::decode("XOR2 chunk shorter than header"));
    }
    let total = u16::from_be_bytes([data[0], data[1]]) as u64;
    let mut st = State::new(data[2]);
    if total == 0 {
        return Ok(0);
    }
    let mut br = BitReader::new(&data[3..]);

    // Sample 0.
    st.t = br.read_varint()?;
    st.value = f64::from_bits(br.read_bits(64)?);
    if st.value.to_bits() != STALE_NAN_BITS {
        st.baseline = st.value;
    }
    if st.first_st_known {
        let diff = br.read_varint()?;
        st.st_diff = diff;
        st.st = st.t - diff;
    }
    sink.push_float(st.t, st.value);
    if total == 1 {
        return Ok(1);
    }

    // Sample 1.
    let prev_t = st.t;
    let delta = br.read_uvarint()?;
    st.t_delta = delta;
    st.t = st.t.wrapping_add(delta as i64);
    st.decode_value(&mut br)?;
    if st.first_st_change_on == 1 {
        let sdod = read_varbit_int(&mut br)?;
        st.st_diff = sdod;
        st.st = prev_t - sdod;
    }
    sink.push_float(st.t, st.value);

    // Samples 2..n. `num_read` is the number of samples already emitted.
    let mut num_read = 2u64;
    while num_read < total {
        let prev_t = st.t;
        let ctrl = st.read_control(&mut br)?;
        match ctrl {
            0 => {
                st.t = st.t.wrapping_add(st.t_delta as i64);
                st.value = st.baseline;
            }
            1 => {
                st.t = st.t.wrapping_add(st.t_delta as i64);
                st.decode_value_known_non_zero(&mut br)?;
            }
            2 => {
                st.read_dod(&mut br, 13)?;
                st.decode_value(&mut br)?;
            }
            3 => {
                st.read_dod(&mut br, 20)?;
                st.decode_value(&mut br)?;
            }
            4 => {
                st.read_dod(&mut br, 64)?;
                st.decode_value(&mut br)?;
            }
            _ => {
                st.t = st.t.wrapping_add(st.t_delta as i64);
                st.value = f64::from_bits(STALE_NAN_BITS);
            }
        }
        st.decode_st(&mut br, num_read, prev_t)?;
        sink.push_float(st.t, st.value);
        num_read += 1;
    }

    Ok(total)
}

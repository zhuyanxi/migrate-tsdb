//! Histogram / float histogram decoders, ported from
//! `tsdb/chunkenc/histogram.go`, `float_histogram.go`, `histogram_st.go`
//! and `histogram_meta.go`.

use crate::error::{Error, Result};

use super::bstream::BitReader;
use super::varbit::{read_varbit_int, read_varbit_uint};
use super::xor::xor_read;
use super::{HistogramSample, SampleSink, Span, STALE_NAN_BITS};

const CHUNK_HEADER_SIZE: usize = 3;
const CUSTOM_BUCKETS_SCHEMA: i32 = -53;

struct Layout {
    schema: i32,
    zero_threshold: f64,
    pos_spans: Vec<Span>,
    neg_spans: Vec<Span>,
    custom_values: Vec<f64>,
}

fn is_known_schema(schema: i32) -> bool {
    (-4..=8).contains(&schema) || schema == CUSTOM_BUCKETS_SCHEMA
}

fn read_zero_threshold(br: &mut BitReader<'_>) -> Result<f64> {
    let b = br.read_byte()?;
    match b {
        0 => Ok(0.0),
        255 => Ok(f64::from_bits(br.read_bits(64)?)),
        other => Ok(0.5 * 2f64.powi(other as i32 - 243)),
    }
}

fn read_spans(br: &mut BitReader<'_>) -> Result<Vec<Span>> {
    let num = read_varbit_uint(br)?;
    if num > 1_000_000 {
        return Err(Error::decode(format!("unreasonable span count {num}")));
    }
    let mut spans = Vec::with_capacity(num as usize);
    for _ in 0..num {
        let length = read_varbit_uint(br)?;
        let offset = read_varbit_int(br)?;
        spans.push(Span {
            offset: offset as i32,
            length: length as u32,
        });
    }
    Ok(spans)
}

fn read_custom_values(br: &mut BitReader<'_>) -> Result<Vec<f64>> {
    let num = read_varbit_uint(br)?;
    if num > 1_000_000 {
        return Err(Error::decode(format!(
            "unreasonable custom value count {num}"
        )));
    }
    let mut out = Vec::with_capacity(num as usize);
    for _ in 0..num {
        let v = read_varbit_uint(br)?;
        if v == 0 {
            out.push(f64::from_bits(br.read_bits(64)?));
        } else {
            out.push((v - 1) as f64 / 1000.0);
        }
    }
    Ok(out)
}

fn read_layout(br: &mut BitReader<'_>) -> Result<Layout> {
    let zero_threshold = read_zero_threshold(br)?;
    let schema = read_varbit_int(br)? as i32;
    if !is_known_schema(schema) {
        return Err(Error::decode(format!("unknown histogram schema {schema}")));
    }
    let pos_spans = read_spans(br)?;
    let neg_spans = read_spans(br)?;
    let custom_values = if schema == CUSTOM_BUCKETS_SCHEMA {
        read_custom_values(br)?
    } else {
        Vec::new()
    };
    Ok(Layout {
        schema,
        zero_threshold,
        pos_spans,
        neg_spans,
        custom_values,
    })
}

fn count_buckets(spans: &[Span]) -> usize {
    spans.iter().map(|s| s.length as usize).sum()
}

struct Header {
    total: u64,
    reset_hint: u8,
    st_known: bool,
    st_change_on: u8,
}

fn header(data: &[u8], st_encoding: bool) -> Result<Header> {
    if data.len() < CHUNK_HEADER_SIZE {
        return Err(Error::decode("histogram chunk shorter than header"));
    }
    let raw_count = u16::from_be_bytes([data[0], data[1]]);
    let (total, reset_hint, st_known, st_change_on) = if st_encoding {
        (
            (raw_count & 0x3fff) as u64,
            (data[0] & 0xc0) >> 6,
            data[2] & 0x80 != 0,
            data[2] & 0x7f,
        )
    } else {
        (raw_count as u64, (data[2] & 0xc0) >> 6, false, 0)
    };
    Ok(Header {
        total,
        reset_hint,
        st_known,
        st_change_on,
    })
}

/// Decodes ST data appended after a histogram sample.
///
/// `num_read` is 1-based and already includes the current sample, matching
/// Go's `stDecoder.decode(br, it.numRead, ...)`.
fn decode_st(
    br: &mut BitReader<'_>,
    st_known: bool,
    change_on: u8,
    num_read: u64,
    cur_t: i64,
    prev_t: i64,
    st: &mut i64,
    st_diff: &mut i64,
) -> Result<()> {
    if !st_known && change_on == 0 {
        return Ok(());
    }
    match num_read {
        1 => {
            if st_known {
                let diff = br.read_varint()?;
                *st_diff = diff;
                *st = cur_t - diff;
            }
        }
        2 => {
            if change_on == 1 {
                let sdod = read_varbit_int(br)?;
                *st_diff = sdod;
                *st = prev_t - sdod;
            }
        }
        n => {
            if change_on > 0 && n - 1 >= change_on as u64 {
                let sdod = read_varbit_int(br)?;
                if n - 1 == change_on as u64 {
                    *st_diff = sdod;
                } else {
                    *st_diff = st_diff.wrapping_add(sdod);
                }
                *st = prev_t.wrapping_sub(*st_diff);
            }
        }
    }
    Ok(())
}

struct IntState {
    t: i64,
    t_delta: i64,
    count: u64,
    count_delta: i64,
    zero_count: u64,
    zero_delta: i64,
    sum: f64,
    leading: u8,
    trailing: u8,
    st: i64,
    st_diff: i64,
    /// First-order bucket deltas (Prometheus `pBuckets`).
    pos_delta: Vec<i64>,
    neg_delta: Vec<i64>,
    /// Second-order deltas (Prometheus `pBucketsDelta`).
    pos_dod: Vec<i64>,
    neg_dod: Vec<i64>,
    pos_abs: Vec<f64>,
    neg_abs: Vec<f64>,
}

/// Decodes an integer histogram chunk (optionally with ST).
pub fn decode_int<S: SampleSink>(data: &[u8], st_encoding: bool, sink: &mut S) -> Result<u64> {
    let hdr = header(data, st_encoding)?;
    if hdr.total == 0 {
        return Ok(0);
    }
    let mut br = BitReader::new(&data[CHUNK_HEADER_SIZE..]);

    let layout = read_layout(&mut br)?;
    let np = count_buckets(&layout.pos_spans);
    let nn = count_buckets(&layout.neg_spans);
    if np + nn > 16_000_000 {
        return Err(Error::decode("histogram has too many buckets"));
    }

    let mut st = IntState {
        t: read_varbit_int(&mut br)?,
        t_delta: 0,
        count: read_varbit_uint(&mut br)?,
        count_delta: 0,
        zero_count: read_varbit_uint(&mut br)?,
        zero_delta: 0,
        sum: f64::from_bits(br.read_bits(64)?),
        leading: 0,
        trailing: 0,
        st: 0,
        st_diff: 0,
        pos_delta: vec![0; np],
        neg_delta: vec![0; nn],
        pos_dod: vec![0; np],
        neg_dod: vec![0; nn],
        pos_abs: vec![0.0; np],
        neg_abs: vec![0.0; nn],
    };

    let mut current: i64 = 0;
    for i in 0..np {
        let v = read_varbit_int(&mut br)?;
        st.pos_delta[i] = v;
        current += v;
        st.pos_abs[i] = current as f64;
    }
    current = 0;
    for i in 0..nn {
        let v = read_varbit_int(&mut br)?;
        st.neg_delta[i] = v;
        current += v;
        st.neg_abs[i] = current as f64;
    }

    let mut num_read = 1u64;
    decode_st(
        &mut br,
        hdr.st_known,
        hdr.st_change_on,
        num_read,
        st.t,
        st.t,
        &mut st.st,
        &mut st.st_diff,
    )?;
    push_int(sink, &layout, &st, hdr.reset_hint);

    while num_read < hdr.total {
        let prev_t = st.t;
        let t_dod = read_varbit_int(&mut br)?;
        st.t_delta += t_dod;
        st.t = st.t.wrapping_add(st.t_delta);

        let count_dod = read_varbit_int(&mut br)?;
        st.count_delta += count_dod;
        st.count = (st.count as i64).wrapping_add(st.count_delta) as u64;

        let zero_dod = read_varbit_int(&mut br)?;
        st.zero_delta += zero_dod;
        st.zero_count = (st.zero_count as i64).wrapping_add(st.zero_delta) as u64;

        xor_read(&mut br, &mut st.sum, &mut st.leading, &mut st.trailing)?;
        let stale = st.sum.to_bits() == STALE_NAN_BITS;

        if !stale {
            let mut current: i64 = 0;
            for i in 0..np {
                let dod = read_varbit_int(&mut br)?;
                st.pos_dod[i] += dod;
                st.pos_delta[i] += st.pos_dod[i];
                current += st.pos_delta[i];
                st.pos_abs[i] = current as f64;
            }
            current = 0;
            for i in 0..nn {
                let dod = read_varbit_int(&mut br)?;
                st.neg_dod[i] += dod;
                st.neg_delta[i] += st.neg_dod[i];
                current += st.neg_delta[i];
                st.neg_abs[i] = current as f64;
            }
        }

        num_read += 1;
        decode_st(
            &mut br,
            hdr.st_known,
            hdr.st_change_on,
            num_read,
            st.t,
            prev_t,
            &mut st.st,
            &mut st.st_diff,
        )?;

        if stale {
            push_stale(sink, st.t, st.st, st.sum, hdr.reset_hint);
        } else {
            push_int(sink, &layout, &st, hdr.reset_hint);
        }
    }

    Ok(hdr.total)
}

fn push_int<S: SampleSink>(sink: &mut S, layout: &Layout, st: &IntState, reset_hint: u8) {
    let sample = HistogramSample {
        ts: st.t,
        st: st.st,
        count: st.count as f64,
        zero_count: st.zero_count as f64,
        sum: st.sum,
        schema: layout.schema,
        zero_threshold: layout.zero_threshold,
        pos_spans: &layout.pos_spans,
        neg_spans: &layout.neg_spans,
        pos_buckets: &st.pos_abs,
        neg_buckets: &st.neg_abs,
        custom_values: &layout.custom_values,
        reset_hint,
    };
    sink.push_histogram(&sample);
}

fn push_stale<S: SampleSink>(sink: &mut S, ts: i64, st: i64, sum: f64, reset_hint: u8) {
    let empty: [f64; 0] = [];
    let sample = HistogramSample {
        ts,
        st,
        count: 0.0,
        zero_count: 0.0,
        sum,
        schema: 0,
        zero_threshold: 0.0,
        pos_spans: &[],
        neg_spans: &[],
        pos_buckets: &empty,
        neg_buckets: &empty,
        custom_values: &[],
        reset_hint,
    };
    sink.push_histogram(&sample);
}

struct FloatState {
    t: i64,
    t_delta: i64,
    count: f64,
    count_leading: u8,
    count_trailing: u8,
    zero_count: f64,
    zero_leading: u8,
    zero_trailing: u8,
    sum: f64,
    leading: u8,
    trailing: u8,
    st: i64,
    st_diff: i64,
    pos_buckets: Vec<f64>,
    neg_buckets: Vec<f64>,
    pos_leading: Vec<u8>,
    pos_trailing: Vec<u8>,
    neg_leading: Vec<u8>,
    neg_trailing: Vec<u8>,
}

/// Decodes a float histogram chunk (optionally with ST).
pub fn decode_float<S: SampleSink>(data: &[u8], st_encoding: bool, sink: &mut S) -> Result<u64> {
    let hdr = header(data, st_encoding)?;
    if hdr.total == 0 {
        return Ok(0);
    }
    let mut br = BitReader::new(&data[CHUNK_HEADER_SIZE..]);

    let layout = read_layout(&mut br)?;
    let np = count_buckets(&layout.pos_spans);
    let nn = count_buckets(&layout.neg_spans);
    if np + nn > 16_000_000 {
        return Err(Error::decode("histogram has too many buckets"));
    }

    let mut st = FloatState {
        t: read_varbit_int(&mut br)?,
        t_delta: 0,
        count: f64::from_bits(br.read_bits(64)?),
        count_leading: 0,
        count_trailing: 0,
        zero_count: f64::from_bits(br.read_bits(64)?),
        zero_leading: 0,
        zero_trailing: 0,
        sum: f64::from_bits(br.read_bits(64)?),
        leading: 0,
        trailing: 0,
        st: 0,
        st_diff: 0,
        pos_buckets: vec![0.0; np],
        neg_buckets: vec![0.0; nn],
        pos_leading: vec![0; np],
        pos_trailing: vec![0; np],
        neg_leading: vec![0; nn],
        neg_trailing: vec![0; nn],
    };

    for i in 0..np {
        st.pos_buckets[i] = f64::from_bits(br.read_bits(64)?);
    }
    for i in 0..nn {
        st.neg_buckets[i] = f64::from_bits(br.read_bits(64)?);
    }

    let mut num_read = 1u64;
    decode_st(
        &mut br,
        hdr.st_known,
        hdr.st_change_on,
        num_read,
        st.t,
        st.t,
        &mut st.st,
        &mut st.st_diff,
    )?;
    push_float(sink, &layout, &st, hdr.reset_hint);

    while num_read < hdr.total {
        let prev_t = st.t;
        let t_dod = read_varbit_int(&mut br)?;
        st.t_delta += t_dod;
        st.t = st.t.wrapping_add(st.t_delta);

        xor_read(
            &mut br,
            &mut st.count,
            &mut st.count_leading,
            &mut st.count_trailing,
        )?;
        xor_read(
            &mut br,
            &mut st.zero_count,
            &mut st.zero_leading,
            &mut st.zero_trailing,
        )?;
        xor_read(&mut br, &mut st.sum, &mut st.leading, &mut st.trailing)?;
        let stale = st.sum.to_bits() == STALE_NAN_BITS;

        if !stale {
            for i in 0..np {
                xor_read(
                    &mut br,
                    &mut st.pos_buckets[i],
                    &mut st.pos_leading[i],
                    &mut st.pos_trailing[i],
                )?;
            }
            for i in 0..nn {
                xor_read(
                    &mut br,
                    &mut st.neg_buckets[i],
                    &mut st.neg_leading[i],
                    &mut st.neg_trailing[i],
                )?;
            }
        }

        num_read += 1;
        decode_st(
            &mut br,
            hdr.st_known,
            hdr.st_change_on,
            num_read,
            st.t,
            prev_t,
            &mut st.st,
            &mut st.st_diff,
        )?;

        if stale {
            push_stale(sink, st.t, st.st, st.sum, hdr.reset_hint);
        } else {
            push_float(sink, &layout, &st, hdr.reset_hint);
        }
    }

    Ok(hdr.total)
}

fn push_float<S: SampleSink>(sink: &mut S, layout: &Layout, st: &FloatState, reset_hint: u8) {
    let sample = HistogramSample {
        ts: st.t,
        st: st.st,
        count: st.count,
        zero_count: st.zero_count,
        sum: st.sum,
        schema: layout.schema,
        zero_threshold: layout.zero_threshold,
        pos_spans: &layout.pos_spans,
        neg_spans: &layout.neg_spans,
        pos_buckets: &st.pos_buckets,
        neg_buckets: &st.neg_buckets,
        custom_values: &layout.custom_values,
        reset_hint,
    };
    sink.push_histogram(&sample);
}

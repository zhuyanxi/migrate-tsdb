//! Chunk encoding dispatch and shared sample types.

pub mod bstream;
pub mod histogram;
pub mod varbit;
pub mod xor;
pub mod xor2;

use crate::error::{Error, Result};

/// `math.Float64frombits(value.StaleNaN)` used by Prometheus.
pub const STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub offset: i32,
    pub length: u32,
}

#[derive(Debug, Clone, Copy)]
pub struct HistogramSample<'a> {
    pub ts: i64,
    /// Start timestamp (native histograms with ST encoding only).
    pub st: i64,
    pub count: f64,
    pub zero_count: f64,
    pub sum: f64,
    pub schema: i32,
    pub zero_threshold: f64,
    pub pos_spans: &'a [Span],
    pub neg_spans: &'a [Span],
    /// Absolute bucket counts (deltas already accumulated).
    pub pos_buckets: &'a [f64],
    pub neg_buckets: &'a [f64],
    pub custom_values: &'a [f64],
    /// Raw 2-bit header: 0 unknown, 1 not-reset, 2 reset, 3 gauge.
    pub reset_hint: u8,
}

/// Receives decoded samples in time order.
pub trait SampleSink {
    fn push_float(&mut self, ts: i64, value: f64);
    fn push_histogram(&mut self, h: &HistogramSample<'_>);
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DecodeOutcome {
    pub floats: u64,
    pub histograms: u64,
}

impl DecodeOutcome {
    pub fn samples(&self) -> u64 {
        self.floats + self.histograms
    }
}

pub const ENC_NONE: u8 = 0;
pub const ENC_XOR: u8 = 1;
pub const ENC_HISTOGRAM: u8 = 2;
pub const ENC_FLOAT_HISTOGRAM: u8 = 3;
pub const ENC_XOR2: u8 = 4;
pub const ENC_HISTOGRAM_ST: u8 = 5;
pub const ENC_FLOAT_HISTOGRAM_ST: u8 = 6;

pub fn encoding_name(enc: u8) -> &'static str {
    match enc {
        ENC_NONE => "none",
        ENC_XOR => "XOR",
        ENC_HISTOGRAM => "histogram",
        ENC_FLOAT_HISTOGRAM => "floathistogram",
        ENC_XOR2 => "XOR2",
        ENC_HISTOGRAM_ST => "histogramST",
        ENC_FLOAT_HISTOGRAM_ST => "floathistogramST",
        _ => "unknown",
    }
}

/// Decodes a chunk body (encoding byte already stripped).
pub fn decode<S: SampleSink>(encoding: u8, data: &[u8], sink: &mut S) -> Result<DecodeOutcome> {
    match encoding {
        ENC_NONE => Ok(DecodeOutcome::default()),
        ENC_XOR => Ok(DecodeOutcome {
            floats: xor::decode(data, sink)?,
            histograms: 0,
        }),
        ENC_XOR2 => Ok(DecodeOutcome {
            floats: xor2::decode(data, sink)?,
            histograms: 0,
        }),
        ENC_HISTOGRAM => Ok(DecodeOutcome {
            floats: 0,
            histograms: histogram::decode_int(data, false, sink)?,
        }),
        ENC_HISTOGRAM_ST => Ok(DecodeOutcome {
            floats: 0,
            histograms: histogram::decode_int(data, true, sink)?,
        }),
        ENC_FLOAT_HISTOGRAM => Ok(DecodeOutcome {
            floats: 0,
            histograms: histogram::decode_float(data, false, sink)?,
        }),
        ENC_FLOAT_HISTOGRAM_ST => Ok(DecodeOutcome {
            floats: 0,
            histograms: histogram::decode_float(data, true, sink)?,
        }),
        other => Err(Error::Unsupported(format!(
            "chunk encoding {other} ({})",
            encoding_name(other)
        ))),
    }
}

//! Chunk encoding dispatch and shared sample types.

pub mod bstream;
pub mod histogram;
pub mod varbit;
pub mod xor;
pub mod xor2;

use crate::error::{Error, Result};

/// `math.Float64frombits(value.StaleNaN)` used by Prometheus.
pub const STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;

/// One run of consecutive populated buckets in a native histogram.
///
/// Prometheus stores spans as deltas to keep histograms with a few populated
/// buckets compact; this is the decoded form.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    /// Offset of the first bucket in the run, relative to the schema's zero
    /// bucket.
    pub offset: i32,
    /// Number of buckets in the run.
    pub length: u32,
}

/// One decoded native histogram sample.
///
/// All bucket slices borrow from the decoder's scratch buffers, so the sample
/// is only valid for the duration of [`SampleSink::push_histogram`]. Copy the
/// values out if you need to keep them.
#[derive(Debug, Clone, Copy)]
pub struct HistogramSample<'a> {
    /// Sample timestamp, in milliseconds since the Unix epoch.
    pub ts: i64,
    /// Start timestamp (native histograms with ST encoding only), `0` otherwise.
    pub st: i64,
    /// Total number of observations.
    pub count: f64,
    /// Observations in the zero bucket.
    pub zero_count: f64,
    /// Sum of all observed values.
    pub sum: f64,
    /// Bucket schema; the resolution of the exponential buckets.
    pub schema: i32,
    /// Width of the zero bucket (`[-zero_threshold, +zero_threshold]`).
    pub zero_threshold: f64,
    /// Runs of populated positive buckets.
    pub pos_spans: &'a [Span],
    /// Runs of populated negative buckets.
    pub neg_spans: &'a [Span],
    /// Absolute bucket counts (deltas already accumulated).
    pub pos_buckets: &'a [f64],
    /// Absolute negative bucket counts (deltas already accumulated).
    pub neg_buckets: &'a [f64],
    /// Custom bucket bounds when the histogram uses them instead of a schema.
    pub custom_values: &'a [f64],
    /// Raw 2-bit header: 0 unknown, 1 not-reset, 2 reset, 3 gauge.
    pub reset_hint: u8,
}

/// Receives decoded samples in time order.
///
/// `chunkenc::decode` calls these once per sample, without allocating; the
/// sink is where an export writes rows or forwards them to another system.
pub trait SampleSink {
    /// A float sample. NaNs, including Prometheus stale markers, are passed
    /// through unchanged.
    fn push_float(&mut self, ts: i64, value: f64);
    /// A native histogram sample. The borrow ends when the call returns.
    fn push_histogram(&mut self, h: &HistogramSample<'_>);
}

/// How many samples a chunk produced, split by kind.
#[derive(Debug, Clone, Copy, Default)]
pub struct DecodeOutcome {
    /// Float samples decoded.
    pub floats: u64,
    /// Native histogram samples decoded.
    pub histograms: u64,
}

impl DecodeOutcome {
    /// Total samples, float plus histogram.
    pub fn samples(&self) -> u64 {
        self.floats + self.histograms
    }
}

/// Not a real chunk: an empty placeholder.
pub const ENC_NONE: u8 = 0;
/// Gorilla-style XOR encoding for float samples (index v1 era).
pub const ENC_XOR: u8 = 1;
/// Integer native histogram, without a start timestamp.
pub const ENC_HISTOGRAM: u8 = 2;
/// Float native histogram, without a start timestamp.
pub const ENC_FLOAT_HISTOGRAM: u8 = 3;
/// XOR encoding with improved timestamp compression (index v2+).
pub const ENC_XOR2: u8 = 4;
/// Integer native histogram carrying a start timestamp.
pub const ENC_HISTOGRAM_ST: u8 = 5;
/// Float native histogram carrying a start timestamp.
pub const ENC_FLOAT_HISTOGRAM_ST: u8 = 6;

/// Human-readable name of an encoding byte, for logs and error messages.
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

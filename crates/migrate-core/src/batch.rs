//! Bounded in-memory batch handed to a sink.

use std::sync::Arc;

use tsdb_reader::Span;

#[derive(Debug, Clone)]
pub struct SeriesRow {
    pub uid: u64,
    pub series_ref: u64,
    pub labels: Vec<(Arc<str>, Arc<str>)>,
}

#[derive(Debug, Clone, Copy)]
pub struct FloatRow {
    pub uid: u64,
    pub ts: i64,
    pub value: f64,
}

#[derive(Debug, Clone)]
pub struct HistRow {
    pub uid: u64,
    pub ts: i64,
    pub st: i64,
    pub count: f64,
    pub zero_count: f64,
    pub sum: f64,
    pub schema: i32,
    pub zero_threshold: f64,
    pub pos_spans: Vec<Span>,
    pub neg_spans: Vec<Span>,
    pub pos_buckets: Vec<f64>,
    pub neg_buckets: Vec<f64>,
    pub custom_values: Vec<f64>,
    pub reset_hint: u8,
}

#[derive(Debug, Clone, Copy)]
pub struct BatchLimits {
    pub max_samples: usize,
    pub max_bytes: usize,
}

#[derive(Debug, Default)]
pub struct Batch {
    pub series: Vec<SeriesRow>,
    pub floats: Vec<FloatRow>,
    pub hists: Vec<HistRow>,
    bytes: usize,
}

impl Batch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn clear(&mut self) {
        self.series.clear();
        self.floats.clear();
        self.hists.clear();
        self.bytes = 0;
    }

    pub fn is_empty(&self) -> bool {
        self.floats.is_empty() && self.hists.is_empty() && self.series.is_empty()
    }

    pub fn samples(&self) -> usize {
        self.floats.len() + self.hists.len()
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }

    pub fn push_series(&mut self, row: SeriesRow) {
        self.bytes += 40
            + row
                .labels
                .iter()
                .map(|(k, v)| k.len() + v.len() + 8)
                .sum::<usize>();
        self.series.push(row);
    }

    pub fn push_float(&mut self, row: FloatRow) {
        self.bytes += 32;
        self.floats.push(row);
    }

    pub fn push_hist(&mut self, row: HistRow) {
        self.bytes += 160
            + (row.pos_buckets.len() + row.neg_buckets.len()) * 8
            + (row.pos_spans.len() + row.neg_spans.len()) * 8
            + row.custom_values.len() * 8;
        self.hists.push(row);
    }

    pub fn is_full(&self, limits: BatchLimits) -> bool {
        self.samples() >= limits.max_samples || self.bytes >= limits.max_bytes
    }

    /// Approximate heap usage, used by tests and metrics.
    pub fn estimated_bytes(&self) -> usize {
        self.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limits() {
        let mut b = Batch::new();
        let limits = BatchLimits {
            max_samples: 2,
            max_bytes: usize::MAX,
        };
        assert!(!b.is_full(limits));
        b.push_float(FloatRow {
            uid: 1,
            ts: 0,
            value: 1.0,
        });
        assert!(!b.is_full(limits));
        b.push_float(FloatRow {
            uid: 1,
            ts: 1,
            value: 2.0,
        });
        assert!(b.is_full(limits));
        b.clear();
        assert!(b.is_empty());
    }
}

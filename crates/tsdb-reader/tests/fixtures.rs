//! Cross-checks the independent reader against blocks and chunk fixtures
//! produced by the upstream Prometheus implementation (fixtures/gen).

use std::collections::HashMap;
use std::path::PathBuf;

use serde::Deserialize;
use tsdb_reader::chunkenc::{self, HistogramSample, SampleSink, Span};
use tsdb_reader::{BlockReader, ReaderOptions};

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/gen/data")
}

#[derive(Deserialize, Debug, Clone)]
struct FloatGT {
    t: i64,
    bits: u64,
    #[allow(dead_code)]
    v: String,
}

#[derive(Deserialize, Debug, Clone)]
struct SpanGT {
    offset: i32,
    length: u32,
}

#[derive(Deserialize, Debug, Clone)]
struct HistGT {
    t: i64,
    st: i64,
    count: f64,
    zero_count: f64,
    sum: f64,
    schema: i32,
    zero_threshold: f64,
    pos_spans: Vec<SpanGT>,
    neg_spans: Vec<SpanGT>,
    pos_buckets: Vec<f64>,
    neg_buckets: Vec<f64>,
    reset_hint: u8,
}

#[derive(Deserialize, Debug)]
struct SeriesGT {
    key: String,
    labels: HashMap<String, String>,
    #[serde(default)]
    floats: Vec<FloatGT>,
    #[serde(default)]
    hists: Vec<HistGT>,
}

#[derive(Deserialize, Debug)]
struct BlockGT {
    ulid: String,
    min_time: i64,
    max_time: i64,
    series: Vec<SeriesGT>,
    #[serde(default)]
    tombstones: HashMap<String, Vec<[i64; 2]>>,
}

#[derive(Default)]
struct Collector {
    floats: Vec<(i64, u64)>,
    hists: Vec<OwnedHist>,
}

#[derive(Debug, Clone)]
struct OwnedHist {
    t: i64,
    st: i64,
    count: f64,
    zero_count: f64,
    sum: f64,
    schema: i32,
    zero_threshold: f64,
    pos_spans: Vec<Span>,
    neg_spans: Vec<Span>,
    pos_buckets: Vec<f64>,
    neg_buckets: Vec<f64>,
    custom_values: Vec<f64>,
    reset_hint: u8,
}

impl SampleSink for Collector {
    fn push_float(&mut self, ts: i64, value: f64) {
        self.floats.push((ts, value.to_bits()));
    }

    fn push_histogram(&mut self, h: &HistogramSample<'_>) {
        self.hists.push(OwnedHist {
            t: h.ts,
            st: h.st,
            count: h.count,
            zero_count: h.zero_count,
            sum: h.sum,
            schema: h.schema,
            zero_threshold: h.zero_threshold,
            pos_spans: h.pos_spans.to_vec(),
            neg_spans: h.neg_spans.to_vec(),
            pos_buckets: h.pos_buckets.to_vec(),
            neg_buckets: h.neg_buckets.to_vec(),
            custom_values: h.custom_values.to_vec(),
            reset_hint: h.reset_hint,
        });
    }
}

fn canonical(labels: &[(std::sync::Arc<str>, std::sync::Arc<str>)]) -> String {
    let mut s = String::new();
    for (n, v) in labels {
        s.push_str(n);
        s.push('\0');
        s.push_str(v);
        s.push('\0');
    }
    s
}

#[test]
fn block_matches_prometheus_ground_truth() {
    let dir = fixture_dir();
    let gt_raw = std::fs::read_to_string(dir.join("block/ground_truth.json")).unwrap();
    let gt: BlockGT = serde_json::from_str(&gt_raw).unwrap();

    let block_dir = dir.join("block").join(&gt.ulid);
    let block = BlockReader::open(&block_dir, ReaderOptions::default()).unwrap();

    assert_eq!(block.block_id(), gt.ulid);
    assert_eq!(block.meta().min_time, gt.min_time);
    assert_eq!(block.meta().max_time, gt.max_time);

    let stats = block.scan_stats().unwrap();
    assert_eq!(stats.series, gt.series.len() as u64, "series count");
    assert!(
        block.meta().stats.num_chunks > 0,
        "fixture should carry stats"
    );
    assert_eq!(
        stats.chunks,
        block.meta().stats.num_chunks,
        "chunk count from scan vs meta"
    );

    let gt_by_key: HashMap<&str, &SeriesGT> =
        gt.series.iter().map(|s| (s.key.as_str(), s)).collect();

    let mut seen = 0usize;
    let mut total_floats = 0u64;
    let mut total_hists = 0u64;
    let mut tombstoned_hits = 0usize;

    for series in block.series(0) {
        let series = series.unwrap();
        let key = canonical(&series.labels);
        let expect = gt_by_key
            .get(key.as_str())
            .unwrap_or_else(|| panic!("unexpected series {key:?}"));
        seen += 1;

        // Labels must match exactly.
        assert_eq!(series.labels.len(), expect.labels.len());
        for (n, v) in &series.labels {
            assert_eq!(
                expect.labels.get(n.as_ref()).map(String::as_str),
                Some(v.as_ref())
            );
        }

        let mut sink = Collector::default();
        for chunk in &series.chunks {
            let slice = block.chunk(chunk).unwrap();
            let outcome = chunkenc::decode(slice.encoding, slice.data, &mut sink).unwrap();
            assert!(outcome.samples() > 0, "empty chunk in fixture");
            total_floats += outcome.floats;
            total_hists += outcome.histograms;
        }

        let got_floats: Vec<(i64, u64)> = sink.floats.clone();
        let want_floats: Vec<(i64, u64)> = expect.floats.iter().map(|f| (f.t, f.bits)).collect();
        assert_eq!(got_floats, want_floats, "float samples for {key:?}");

        assert_eq!(sink.hists.len(), expect.hists.len(), "hist count {key:?}");
        for (got, want) in sink.hists.iter().zip(&expect.hists) {
            assert_eq!(got.t, want.t, "hist ts {key:?}");
            assert_eq!(got.st, want.st, "hist st {key:?}");
            assert_eq!(got.count, want.count, "hist count {key:?}");
            assert_eq!(got.zero_count, want.zero_count, "hist zero_count {key:?}");
            assert_eq!(got.sum.to_bits(), want.sum.to_bits(), "hist sum {key:?}");
            assert_eq!(got.schema, want.schema, "hist schema {key:?}");
            assert_eq!(
                got.zero_threshold.to_bits(),
                want.zero_threshold.to_bits(),
                "hist zero_threshold {key:?}"
            );
            let spans = |v: &[SpanGT]| {
                v.iter()
                    .map(|s| Span {
                        offset: s.offset,
                        length: s.length,
                    })
                    .collect::<Vec<_>>()
            };
            assert_eq!(got.pos_spans, spans(&want.pos_spans), "pos spans {key:?}");
            assert_eq!(got.neg_spans, spans(&want.neg_spans), "neg spans {key:?}");
            assert_eq!(got.pos_buckets, want.pos_buckets, "pos buckets {key:?}");
            assert_eq!(got.neg_buckets, want.neg_buckets, "neg buckets {key:?}");
        }

        // Tombstones are keyed by the index series id.
        let deleted = block.tombstones().intervals(series.id);
        if let Some(want) = gt.tombstones.get(&key) {
            assert_eq!(deleted.len(), want.len(), "tombstone interval count");
            for (got, want) in deleted.iter().zip(want) {
                assert_eq!((got.0, got.1), (want[0], want[1]));
            }
            for f in &expect.floats {
                if block.tombstones().is_deleted(series.id, f.t) {
                    tombstoned_hits += 1;
                }
            }
        } else {
            assert!(deleted.is_empty(), "unexpected tombstones for {key:?}");
        }
    }

    assert_eq!(seen, gt.series.len(), "visited series");
    // 42 float series + 2 histogram series.
    assert_eq!(
        total_floats,
        block.meta().stats.num_float_samples,
        "float sample total"
    );
    assert_eq!(
        total_hists,
        block.meta().stats.num_histogram_samples,
        "histogram sample total"
    );
    assert!(tombstoned_hits > 0, "fixture must exercise tombstones");
}

#[derive(Deserialize, Debug)]
struct ChunkFixture {
    name: String,
    encoding: String,
    bytes_hex: String,
    #[serde(default)]
    floats: Vec<FloatGT>,
    #[serde(default)]
    hists: Vec<HistGT>,
}

#[test]
fn chunk_encodings_match_prometheus_ground_truth() {
    let path = fixture_dir().join("chunks.json");
    let raw = std::fs::read_to_string(&path).unwrap();
    let fixtures: Vec<ChunkFixture> = serde_json::from_str(&raw).unwrap();
    assert_eq!(fixtures.len(), 6, "expected all six chunk encodings");

    for fx in fixtures {
        let bytes = hex_decode(&fx.bytes_hex);
        let enc = match fx.encoding.as_str() {
            "XOR" => 1u8,
            "histogram" => 2,
            "floathistogram" => 3,
            "XOR2" => 4,
            "histogramST" => 5,
            "floathistogramST" => 6,
            other => panic!("unknown fixture encoding {other}"),
        };
        let mut sink = Collector::default();
        let outcome = chunkenc::decode(enc, &bytes, &mut sink)
            .unwrap_or_else(|e| panic!("decode {}: {e}", fx.name));

        assert_eq!(
            sink.floats.len() as u64,
            outcome.floats,
            "float count {}",
            fx.name
        );
        let want_floats: Vec<(i64, u64)> = fx.floats.iter().map(|f| (f.t, f.bits)).collect();
        assert_eq!(sink.floats, want_floats, "floats in {}", fx.name);

        assert_eq!(sink.hists.len(), fx.hists.len(), "hists in {}", fx.name);
        for (idx, (got, want)) in sink.hists.iter().zip(&fx.hists).enumerate() {
            assert_eq!(got.t, want.t, "ts in {} idx {}", fx.name, idx);
            assert_eq!(got.st, want.st, "st in {}", fx.name);
            assert_eq!(got.count, want.count, "count in {}", fx.name);
            assert_eq!(got.zero_count, want.zero_count, "zero in {}", fx.name);
            assert_eq!(got.sum.to_bits(), want.sum.to_bits(), "sum in {}", fx.name);
            assert_eq!(got.schema, want.schema, "schema in {}", fx.name);
            assert_eq!(
                got.zero_threshold.to_bits(),
                want.zero_threshold.to_bits(),
                "threshold in {}",
                fx.name
            );
            assert_eq!(
                got.pos_buckets, want.pos_buckets,
                "pos buckets in {} idx {} (got t={} want t={})",
                fx.name, idx, got.t, want.t
            );
            assert_eq!(
                got.neg_buckets, want.neg_buckets,
                "neg buckets in {}",
                fx.name
            );
            let spans = |v: &[SpanGT]| {
                v.iter()
                    .map(|s| Span {
                        offset: s.offset,
                        length: s.length,
                    })
                    .collect::<Vec<_>>()
            };
            assert_eq!(
                got.pos_spans,
                spans(&want.pos_spans),
                "pos spans in {}",
                fx.name
            );
            assert_eq!(
                got.neg_spans,
                spans(&want.neg_spans),
                "neg spans in {}",
                fx.name
            );
        }
    }
}

fn hex_decode(s: &str) -> Vec<u8> {
    assert!(s.len() % 2 == 0);
    (0..s.len() / 2)
        .map(|i| u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).unwrap())
        .collect()
}

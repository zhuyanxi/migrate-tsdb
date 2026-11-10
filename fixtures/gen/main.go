// Command tsdbfixture generates real Prometheus TSDB blocks and chunk fixtures
// using the upstream Prometheus implementation in ../../prometheus.
//
// It is a development/verification tool. The generated data is used by Rust
// tests to cross-check the independent reader implementation.
package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/tombstones"
)

type floatGT struct {
	T    int64  `json:"t"`
	Bits uint64 `json:"bits"`
	V    string `json:"v"`
}

func fgt(t int64, v float64) floatGT {
	desc := strconv.FormatFloat(v, 'g', -1, 64)
	if math.IsNaN(v) {
		if math.Float64bits(v) == value.StaleNaN {
			desc = "StaleNaN"
		} else {
			desc = "NaN"
		}
	}
	return floatGT{T: t, Bits: math.Float64bits(v), V: desc}
}

type spanGT struct {
	Offset int32  `json:"offset"`
	Length uint32 `json:"length"`
}

type histGT struct {
	T             int64     `json:"t"`
	ST            int64     `json:"st"`
	Count         float64   `json:"count"`
	ZeroCount     float64   `json:"zero_count"`
	Sum           float64   `json:"sum"`
	Schema        int32     `json:"schema"`
	ZeroThreshold float64   `json:"zero_threshold"`
	PosSpans      []spanGT  `json:"pos_spans"`
	NegSpans      []spanGT  `json:"neg_spans"`
	PosBuckets    []float64 `json:"pos_buckets"`
	NegBuckets    []float64 `json:"neg_buckets"`
	ResetHint     uint8     `json:"reset_hint"`
}

type seriesGT struct {
	Key    string            `json:"key"`
	Labels map[string]string `json:"labels"`
	Floats []floatGT         `json:"floats,omitempty"`
	Hists  []histGT          `json:"hists,omitempty"`
}

type blockGT struct {
	ULID       string                `json:"ulid"`
	MinTime    int64                 `json:"min_time"`
	MaxTime    int64                 `json:"max_time"`
	Series     []seriesGT            `json:"series"`
	Tombstones map[string][][2]int64 `json:"tombstones,omitempty"`
}

type chunkFixture struct {
	Name     string    `json:"name"`
	Encoding string    `json:"encoding"`
	BytesHex string    `json:"bytes_hex"`
	Floats   []floatGT `json:"floats,omitempty"`
	Hists    []histGT  `json:"hists,omitempty"`
}

func main() {
	out := flag.String("out", "data", "output directory")
	big := flag.Bool("big", false, "additionally generate a larger block for benchmarks")
	bench := flag.Int("bench-series", 0, "generate a benchmark block with N series and no ground truth")
	decode := flag.String("decode", "", "decode a chunks.json fixture with the upstream reader and print samples")
	only := flag.String("only", "", "only decode this fixture name")
	flag.Parse()
	if *decode != "" {
		if err := verifyChunkFile(*decode, *only); err != nil {
			fatal(err)
		}
		return
	}
	if err := os.MkdirAll(*out, 0o755); err != nil {
		fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	if err := genBlock(logger, filepath.Join(*out, "block"), 40, 121, true); err != nil {
		fatal(err)
	}
	if *big {
		if err := genBlock(logger, filepath.Join(*out, "block-big"), 400, 1441, true); err != nil {
			fatal(err)
		}
	}
	if *bench > 0 {
		if err := genBlock(logger, filepath.Join(*out, "bench"), *bench, 1441, false); err != nil {
			fatal(err)
		}
	}
	if err := genChunks(filepath.Join(*out, "chunks.json")); err != nil {
		fatal(err)
	}
	fmt.Println("fixtures written to", *out)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}

// genBlock writes a real block through Prometheus' BlockWriter, then adds
// tombstones to it. Ground truth is emitted next to the block.
func genBlock(logger *slog.Logger, dir string, nSeries int, nSamples int, writeTruth bool) error {
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	ctx := context.Background()
	bw, err := tsdb.NewBlockWriter(logger, dir, 4*time.Hour.Milliseconds())
	if err != nil {
		return err
	}
	defer bw.Close()

	base := int64(1_700_000_000_000) // fixed epoch, ms
	step := int64(15_000)
	app := bw.Appender(ctx)

	gt := blockGT{Tombstones: map[string][][2]int64{}}
	seriesList := make([]seriesGT, 0, nSeries)
	for i := 0; i < nSeries; i++ {
		name := fmt.Sprintf("fixture_metric_%02d", i)
		lset := labels.FromStrings(
			"__name__", name,
			"job", "fixture",
			"instance", fmt.Sprintf("host-%02d", i%7),
			"shard", fmt.Sprintf("%d", i%3),
		)
		s := seriesGT{Key: canonical(lset), Labels: map[string]string{}}
		lset.Range(func(l labels.Label) { s.Labels[l.Name] = l.Value })

		// A handful of series carry stale markers and irregular gaps.
		for j := 0; j < nSamples; j++ {
			t := base + int64(j)*step
			if i%13 == 5 && j > 30 && j < 34 {
				continue // gap
			}
			v := float64(i)*100 + float64(j)*0.5
			if i%17 == 3 && j == 50 {
				v = math.Float64frombits(value.StaleNaN)
			}
			if _, err := app.Append(0, lset, t, v); err != nil {
				return fmt.Errorf("append %s@%d: %w", name, t, err)
			}
			s.Floats = append(s.Floats, fgt(t, v))
		}
		seriesList = append(seriesList, s)
	}

	// Two native histogram series (integer and float), base encodings.
	for i := 0; i < 2; i++ {
		name := fmt.Sprintf("fixture_hist_%02d", i)
		lset := labels.FromStrings("__name__", name, "job", "fixture")
		s := seriesGT{Key: canonical(lset), Labels: map[string]string{}}
		lset.Range(func(l labels.Label) { s.Labels[l.Name] = l.Value })
		histBase := base + 6*time.Hour.Milliseconds()
		for j := 0; j < 60; j++ {
			t := histBase + int64(j)*step
			if i == 0 {
				h := &histogram.Histogram{
					Schema:          1,
					ZeroThreshold:   0.001,
					ZeroCount:       uint64(j % 3),
					Count:           uint64(10 + j),
					Sum:             123.5 + float64(j),
					PositiveSpans:   []histogram.Span{{Offset: 0, Length: 2}, {Offset: 1, Length: 2}},
					PositiveBuckets: []int64{2, 3, 1, 2 + int64(j%4)},
					NegativeSpans:   []histogram.Span{{Offset: 0, Length: 1}},
					NegativeBuckets: []int64{1},
				}
				fixIntHist(h)
				if _, err := app.AppendHistogram(0, lset, t, h, nil); err != nil {
					return fmt.Errorf("append hist %s@%d: %w", name, t, err)
				}
				s.Hists = append(s.Hists, histFromInt(h, t, 0))
			} else {
				fh := &histogram.FloatHistogram{
					Schema:          2,
					ZeroThreshold:   0.01,
					ZeroCount:       float64(j % 4),
					Count:           20.5 + float64(j),
					Sum:             456.25 + float64(j)*2,
					PositiveSpans:   []histogram.Span{{Offset: 1, Length: 3}},
					PositiveBuckets: []float64{1.5, 2.5, 3.5 + float64(j%3)},
					NegativeSpans:   []histogram.Span{{Offset: 2, Length: 2}},
					NegativeBuckets: []float64{0.5, 100.25 + float64(j)},
				}
				fixFloatHist(fh)
				if _, err := app.AppendHistogram(0, lset, t, nil, fh); err != nil {
					return fmt.Errorf("append fhist %s@%d: %w", name, t, err)
				}
				s.Hists = append(s.Hists, histFromFloat(fh, t, 0))
			}
		}
		seriesList = append(seriesList, s)
	}

	if err := app.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	ulid, err := bw.Flush(ctx)
	if err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	blockDir := filepath.Join(dir, ulid.String())

	// Tombstones: delete a 10 minute window from one known series. The
	// tombstone series ref must match the ref in the block index (offset/16),
	// not the append order, so resolve it through Prometheus' index reader.
	deletedKey := canonical(labels.FromStrings(
		"__name__", "fixture_metric_03", "job", "fixture", "instance", "host-03", "shard", "0",
	))
	ref, err := findSeriesRef(logger, blockDir, deletedKey)
	if err != nil {
		return err
	}
	tomb := tombstones.NewMemTombstones()
	iv := tombstones.Interval{Mint: base + 10*60_000, Maxt: base + 20*60_000}
	tomb.AddInterval(ref, iv)
	if _, err := tombstones.WriteFile(logger, blockDir, tomb); err != nil {
		return fmt.Errorf("write tombstones: %w", err)
	}
	fmt.Printf("tombstone: series %q ref=%d interval=[%d,%d]\n", deletedKey, ref, iv.Mint, iv.Maxt)
	gt.Tombstones[deletedKey] = [][2]int64{{iv.Mint, iv.Maxt}}

	// Read block meta for authoritative time range.
	meta, err := readMeta(filepath.Join(blockDir, "meta.json"))
	if err != nil {
		return err
	}
	gt.ULID = ulid.String()
	gt.MinTime = meta.MinTime
	gt.MaxTime = meta.MaxTime
	gt.Series = seriesList
	sort.Slice(gt.Series, func(i, j int) bool { return gt.Series[i].Key < gt.Series[j].Key })

	if writeTruth {
		b, err := json.MarshalIndent(gt, "", "  ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(dir, "ground_truth.json"), b, 0o644); err != nil {
			return err
		}
	}
	fmt.Printf("block %s: %d series, w=%d, dir=%s\n", ulid, len(seriesList), nSamples, blockDir)
	return nil
}

func findSeriesRef(logger *slog.Logger, blockDir, key string) (storage.SeriesRef, error) {
	b, err := tsdb.OpenBlock(logger, blockDir, chunkenc.NewPool(), nil)
	if err != nil {
		return 0, err
	}
	defer b.Close()
	idx, err := b.Index()
	if err != nil {
		return 0, err
	}
	defer idx.Close()
	p := idx.PostingsForLabelMatching(context.Background(), "__name__", func(string) bool { return true })
	var builder labels.ScratchBuilder
	var chks []chunks.Meta
	for p.Next() {
		ref := p.At()
		if err := idx.Series(ref, &builder, &chks); err != nil {
			return 0, err
		}
		if canonical(builder.Labels()) == key {
			return ref, nil
		}
	}
	return 0, fmt.Errorf("series %q not found in block index", key)
}

type metaJSON struct {
	ULID    string `json:"ulid"`
	MinTime int64  `json:"minTime"`
	MaxTime int64  `json:"maxTime"`
}

func readMeta(path string) (metaJSON, error) {
	var m metaJSON
	b, err := os.ReadFile(path)
	if err != nil {
		return m, err
	}
	return m, json.Unmarshal(b, &m)
}

func canonical(l labels.Labels) string {
	s := ""
	l.Range(func(x labels.Label) { s += x.Name + "\x00" + x.Value + "\x00" })
	return s
}

func spansGT(spans []histogram.Span) []spanGT {
	out := make([]spanGT, 0, len(spans))
	for _, s := range spans {
		out = append(out, spanGT{Offset: s.Offset, Length: s.Length})
	}
	return out
}

// absolute converts Prometheus delta-encoded integer buckets to absolute counts.
func absolute(b []int64) []float64 {
	out := make([]float64, 0, len(b))
	var cur int64
	for _, v := range b {
		cur += v
		out = append(out, float64(cur))
	}
	return out
}

func histFromInt(h *histogram.Histogram, t, st int64) histGT {
	return histGT{
		T: t, ST: st,
		Count: float64(h.Count), ZeroCount: float64(h.ZeroCount), Sum: h.Sum,
		Schema: h.Schema, ZeroThreshold: h.ZeroThreshold,
		PosSpans: spansGT(h.PositiveSpans), NegSpans: spansGT(h.NegativeSpans),
		PosBuckets: absolute(h.PositiveBuckets), NegBuckets: absolute(h.NegativeBuckets),
		ResetHint: uint8(h.CounterResetHint),
	}
}

func copyFloats(in []float64) []float64 {
	out := make([]float64, len(in))
	copy(out, in)
	return out
}

func histFromFloat(h *histogram.FloatHistogram, t, st int64) histGT {
	return histGT{
		T: t, ST: st,
		Count: h.Count, ZeroCount: h.ZeroCount, Sum: h.Sum,
		Schema: h.Schema, ZeroThreshold: h.ZeroThreshold,
		PosSpans: spansGT(h.PositiveSpans), NegSpans: spansGT(h.NegativeSpans),
		PosBuckets: copyFloats(h.PositiveBuckets),
		NegBuckets: copyFloats(h.NegativeBuckets),
		ResetHint:  uint8(h.CounterResetHint),
	}
}

func fixIntHist(h *histogram.Histogram) {
	var sum uint64
	var cur int64
	for _, d := range h.PositiveBuckets {
		cur += d
		sum += uint64(cur)
	}
	cur = 0
	for _, d := range h.NegativeBuckets {
		cur += d
		sum += uint64(cur)
	}
	h.Count = sum + h.ZeroCount
}

func fixFloatHist(h *histogram.FloatHistogram) {
	var sum float64
	for _, v := range h.PositiveBuckets {
		sum += v
	}
	for _, v := range h.NegativeBuckets {
		sum += v
	}
	h.Count = sum + h.ZeroCount
}

func genChunks(path string) error {
	var fixtures []chunkFixture

	// --- XOR ---
	{
		chk := chunkenc.NewXORChunk()
		app, _ := chk.Appender()
		var want []floatGT
		base := int64(1_700_000_000_000)
		for i := 0; i < 100; i++ {
			t := base + int64(i)*15_000 + int64(i%7)*137
			v := math.Sin(float64(i)/7)*100 + float64(i%5)
			if i == 42 {
				v = math.NaN()
			}
			app.Append(0, t, v)
			want = append(want, fgt(t, v))
		}
		fixtures = append(fixtures, chunkFixture{Name: "xor", Encoding: "XOR", BytesHex: hex.EncodeToString(chk.Bytes()), Floats: want})
	}

	// --- XOR2 ---
	{
		chk := chunkenc.NewXOR2Chunk()
		app, _ := chk.Appender()
		var want []floatGT
		base := int64(1_700_000_000_000)
		for i := 0; i < 120; i++ {
			t := base + int64(i)*15_000 + int64(i%11)*3
			var v float64
			switch i % 9 {
			case 0:
				v = 1.5
			case 1:
				v = 1.5 // unchanged value
			case 2:
				v = math.Float64frombits(value.StaleNaN)
			default:
				v = float64(i)*1.25 + 0.125
			}
			app.Append(0, t, v)
			want = append(want, fgt(t, v))
		}
		fixtures = append(fixtures, chunkFixture{Name: "xor2", Encoding: "XOR2", BytesHex: hex.EncodeToString(chk.Bytes()), Floats: want})
	}

	// --- Histogram (integer) ---
	{
		var chk chunkenc.Chunk = chunkenc.NewHistogramChunk()
		app, _ := chk.Appender()
		var want []histGT
		base := int64(1_700_000_000_000)
		cur := app
		for i := 0; i < 8; i++ {
			t := base + int64(i)*15_000
			h := &histogram.Histogram{
				Schema:          1,
				ZeroThreshold:   0.001,
				ZeroCount:       uint64(i),
				Count:           uint64(100 + i*3),
				Sum:             1000.5 + float64(i)*7.25,
				PositiveSpans:   []histogram.Span{{Offset: 0, Length: 2}, {Offset: 2, Length: 2}},
				PositiveBuckets: []int64{int64(1 + i), int64(2 + i), int64(3 + i), int64(4 + i)},
				NegativeSpans:   []histogram.Span{{Offset: 1, Length: 2}},
				NegativeBuckets: []int64{int64(2 + i), int64(3 + i)},
			}
			fixIntHist(h)
			newChk, _, newApp, err := cur.AppendHistogram(nil, 0, t, h, false)
			if err != nil {
				return err
			}
			if newChk != nil {
				chk = newChk
				cur = newApp
			}
			want = append(want, histFromInt(h, t, 0))
		}
		fixtures = append(fixtures, chunkFixture{Name: "histogram", Encoding: "histogram", BytesHex: hex.EncodeToString(chk.Bytes()), Hists: want})
	}

	// --- Float histogram ---
	{
		var chk chunkenc.Chunk = chunkenc.NewFloatHistogramChunk()
		app, _ := chk.Appender()
		var want []histGT
		base := int64(1_700_000_000_000)
		cur := app
		for i := 0; i < 8; i++ {
			t := base + int64(i)*15_000
			h := &histogram.FloatHistogram{
				Schema:          2,
				ZeroThreshold:   0.01,
				ZeroCount:       float64(i) + 0.5,
				Count:           200.5 + float64(i),
				Sum:             2000.25 + float64(i)*3.5,
				PositiveSpans:   []histogram.Span{{Offset: 1, Length: 3}},
				PositiveBuckets: []float64{1.5 + float64(i), 2.25 + float64(i), 3.75 + float64(i)},
				NegativeSpans:   []histogram.Span{{Offset: 0, Length: 1}},
				NegativeBuckets: []float64{4.5 + float64(i)},
			}
			fixFloatHist(h)
			newChk, _, newApp, err := cur.AppendFloatHistogram(nil, 0, t, h, false)
			if err != nil {
				return err
			}
			if newChk != nil {
				chk = newChk
				cur = newApp
			}
			want = append(want, histFromFloat(h, t, 0))
		}
		fixtures = append(fixtures, chunkFixture{Name: "floathistogram", Encoding: "floathistogram", BytesHex: hex.EncodeToString(chk.Bytes()), Hists: want})
	}

	// --- Histogram with ST ---
	{
		var chk chunkenc.Chunk = chunkenc.NewHistogramSTChunk()
		app, _ := chk.Appender()
		var want []histGT
		base := int64(1_700_000_000_000)
		cur := app
		for i := 0; i < 8; i++ {
			t := base + int64(i)*15_000
			st := t - int64(30_000)
			if i >= 20 {
				st = t - int64(45_000)
			}
			h := &histogram.Histogram{
				Schema:          1,
				ZeroThreshold:   0.001,
				ZeroCount:       uint64(i),
				Count:           uint64(50 + i),
				Sum:             500.5 + float64(i),
				PositiveSpans:   []histogram.Span{{Offset: 0, Length: 2}},
				PositiveBuckets: []int64{int64(1 + i), int64(2 + i)},
			}
			fixIntHist(h)
			newChk, _, newApp, err := cur.AppendHistogram(nil, st, t, h, false)
			if err != nil {
				return err
			}
			if newChk != nil {
				chk = newChk
				cur = newApp
			}
			want = append(want, histFromInt(h, t, st))
		}
		fixtures = append(fixtures, chunkFixture{Name: "histogram_st", Encoding: "histogramST", BytesHex: hex.EncodeToString(chk.Bytes()), Hists: want})
	}

	// --- Float histogram with ST ---
	{
		var chk chunkenc.Chunk = chunkenc.NewFloatHistogramSTChunk()
		app, _ := chk.Appender()
		var want []histGT
		base := int64(1_700_000_000_000)
		cur := app
		for i := 0; i < 8; i++ {
			t := base + int64(i)*15_000
			st := t - int64(20_000)
			if i >= 10 && i < 25 {
				st = t - int64(35_000)
			}
			h := &histogram.FloatHistogram{
				Schema:          2,
				ZeroThreshold:   0.01,
				ZeroCount:       float64(i),
				Count:           70.5 + float64(i),
				Sum:             700.25 + float64(i)*2,
				PositiveSpans:   []histogram.Span{{Offset: 0, Length: 2}},
				PositiveBuckets: []float64{1.25 + float64(i), 2.5 + float64(i)},
			}
			fixFloatHist(h)
			newChk, _, newApp, err := cur.AppendFloatHistogram(nil, st, t, h, false)
			if err != nil {
				return err
			}
			if newChk != nil {
				chk = newChk
				cur = newApp
			}
			want = append(want, histFromFloat(h, t, st))
		}
		fixtures = append(fixtures, chunkFixture{Name: "floathistogram_st", Encoding: "floathistogramST", BytesHex: hex.EncodeToString(chk.Bytes()), Hists: want})
	}

	b, err := json.MarshalIndent(fixtures, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

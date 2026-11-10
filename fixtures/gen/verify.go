package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// verifyChunkFile decodes chunk fixtures with the upstream implementation and
// prints the decoded samples. Used to debug reader mismatches.
func verifyChunkFile(path, only string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var fixtures []chunkFixture
	if err := json.Unmarshal(b, &fixtures); err != nil {
		return err
	}
	for _, f := range fixtures {
		if only != "" && f.Name != only {
			continue
		}
		raw, err := hex.DecodeString(f.BytesHex)
		if err != nil {
			return err
		}
		enc := chunkEnc(f.Encoding)
		chk, err := chunkenc.FromData(enc, raw)
		if err != nil {
			return err
		}
		fmt.Printf("== %s enc=%s samples=%d bytes=%d\n", f.Name, f.Encoding, chk.NumSamples(), len(raw))
		it := chk.Iterator(nil)
		i := 0
		for {
			vt := it.Next()
			if vt == chunkenc.ValNone {
				break
			}
			switch vt {
			case chunkenc.ValHistogram:
				_, h := it.AtHistogram(nil)
				fmt.Printf("  %d t=%d count=%d zero=%d sum=%v pos=%v neg=%v\n", i, it.AtT(), h.Count, h.ZeroCount, h.Sum, h.PositiveBuckets, h.NegativeBuckets)
			case chunkenc.ValFloatHistogram:
				_, h := it.AtFloatHistogram(nil)
				fmt.Printf("  %d t=%d count=%v zero=%v sum=%v pos=%v neg=%v\n", i, it.AtT(), h.Count, h.ZeroCount, h.Sum, h.PositiveBuckets, h.NegativeBuckets)
			default:
				t, val := it.At()
				fmt.Printf("  %d t=%d v=%v\n", i, t, val)
			}
			i++
			if i > 15 {
				break
			}
		}
	}
	return nil
}

func chunkEnc(name string) chunkenc.Encoding {
	switch name {
	case "XOR":
		return chunkenc.EncXOR
	case "histogram":
		return chunkenc.EncHistogram
	case "floathistogram":
		return chunkenc.EncFloatHistogram
	case "XOR2":
		return chunkenc.EncXOR2
	case "histogramST":
		return chunkenc.EncHistogramST
	case "floathistogramST":
		return chunkenc.EncFloatHistogramST
	}
	return chunkenc.EncNone
}

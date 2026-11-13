# tsdb-reader

Zero-copy, read-only parser for [Prometheus TSDB](https://github.com/prometheus/prometheus/tree/main/tsdb) blocks.

The crate implements exactly what a full sequential export needs — index TOC and
symbols, series traversal, chunk segment access with CRC32C validation, all chunk
encodings and tombstones filtering — and nothing more. It is the format layer of
[`tsdb-migrate`](https://github.com/zhuyanxi/migrate-tsdb), extracted so other
tools can read blocks without reimplementing the on-disk formats.

## What it supports

| Area | Coverage |
|------|----------|
| Index format | v1, v2, v3 (Prometheus 2.x–3.x) |
| Chunk encodings | XOR, XOR2, histogram, float histogram, and their `ST` variants |
| Tombstones | v1, closed intervals, merged on load |
| Layout | Prometheus data directory or a single block directory |

## What it does **not** support

- `chunks_head/` and the WAL — only persisted blocks.
- The postings / label-index sections, so there is no matcher API; series are
  visited sequentially in lexicographic label order.
- Writing blocks.
- `no_std` or `wasm32-unknown-unknown`: the crate maps files with `memmap2` and
  relies on `std::fs`.

## Cargo features

All three are enabled by default. `default-features = false` leaves only
`serde` and `thiserror`, which the type definitions need.

| Feature | Default | Pulls in | Turn it off to |
|---------|---------|----------|----------------|
| `mmap` | on | `memmap2` | drop the memory-mapping dependency and read segments into owned buffers |
| `crc` | on | `crc32c` | skip checksum verification entirely and drop the CRC dependency |
| `meta` | on | `serde_json` | drop JSON parsing and supply `BlockMeta` from your own store |

The features are independent: any combination compiles and is tested in CI.

What changes without them:

- **`mmap` off** — `IndexReader::open` and `ChunkReader::open` read files into
  memory instead of mapping them. `IndexReader::from_bytes` and
  `ChunkReader::from_segments` accept buffers either way, so object-store and
  in-memory pipelines are unaffected. `IndexReader::from_mmap` only exists with
  the feature on.
- **`crc` off** — `ReaderOptions::verify_crc` and the `verify_crc` arguments
  become no-ops; `Error::Checksum` is never produced. Corrupt data then surfaces
  as a parse error, or not at all.
- **`meta` off** — `read_meta`, `discover_blocks` and `BlockReader::open` are
  not compiled. Use `BlockReader::open_with_meta(path, meta, opts)` or
  `BlockReader::open_info(info, opts)` instead.

## Example

```rust,no_run
# fn main() -> Result<(), Box<dyn std::error::Error>> {
# #[cfg(feature = "meta")]
# {
use std::path::Path;
use tsdb_reader::{BlockReader, ReaderOptions, discover_blocks};

let blocks = discover_blocks(Path::new("/var/lib/prometheus/data"))?;

for info in &blocks {
    let block = BlockReader::open(&info.path, ReaderOptions::default())?;
    println!(
        "{}: {} series, {} chunks, {}..{}",
        block.block_id(),
        block.meta().stats.num_series,
        block.meta().stats.num_chunks,
        block.meta().min_time,
        block.meta().max_time,
    );
}
# }
# Ok(())
# }
```

Decoding samples is a two-step process: walk the series, then decode each chunk
into a sink of your own.

```rust,no_run
# fn main() -> Result<(), Box<dyn std::error::Error>> {
# #[cfg(feature = "meta")]
# {
use std::path::Path;
use tsdb_reader::chunkenc::{self, HistogramSample, SampleSink};
use tsdb_reader::{BlockReader, ReaderOptions};

#[derive(Default)]
struct PrintSink;
impl SampleSink for PrintSink {
    fn push_float(&mut self, ts: i64, value: f64) {
        println!("{ts} {value}");
    }
    fn push_histogram(&mut self, _h: &HistogramSample<'_>) {}
}

let block = BlockReader::open(
    Path::new("/data/01ARZ3NDEKTSV4RRFFQ69G5FAV"),
    ReaderOptions::default(),
)?;
let mut sink = PrintSink;

for series in block.series(0) {
    let series = series?;
    for chunk in &series.chunks {
        let slice = block.chunk(chunk)?;
        chunkenc::decode(slice.encoding, slice.data, &mut sink)?;
    }
}
# }
# Ok(())
# }
```

## Threading model

`ChunkReader` is `Send + Sync`: chunk segments can be decoded from several
threads at once (`BlockReader::chunk_reader()` or `into_parts()`).
`IndexReader` and therefore `BlockReader` are `Send` but **not `Sync`** — the
symbol cache is a `RefCell`, so index traversal must stay on one thread.

## Verification

Every byte-level detail is cross-checked against the upstream Go implementation:
`fixtures/gen` writes real blocks with the vendored Prometheus code and dumps
ground truth, and `cargo test` compares the reader's output sample by sample.
The fixture ships inside the crate (`tests/data`) so the tests also run from a
`cargo package` tarball.

## License

MIT — see [LICENSE](LICENSE).

The chunk decoders in `src/chunkenc/` are a Rust port of Prometheus' TSDB
encoders and stay under the Apache License 2.0 (see `LICENSE-APACHE` and
`NOTICE` for the exact file list and attribution).

#!/usr/bin/env bash
# End-to-end smoke test: fixture -> Parquet -> verify -> DuckDB query.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
OUT="$(mktemp -d /tmp/tsdb-migrate-e2e.XXXXXX)"
trap 'rm -rf "$OUT"' EXIT

echo "== build =="
cargo build --release -p tsdb-migrate-cli >/dev/null

echo "== inspect / plan =="
./target/release/tsdb-migrate inspect --data-dir fixtures/gen/data/block
./target/release/tsdb-migrate plan --data-dir fixtures/gen/data/block --out "$OUT"

echo "== migrate =="
./target/release/tsdb-migrate migrate \
  --data-dir fixtures/gen/data/block --out "$OUT" \
  --block-parallel 1 --decode-workers 2 --unit-series 10 --progress-interval 60

echo "== verify =="
./target/release/tsdb-migrate verify --out "$OUT"

echo "== manifests =="
python3 - "$OUT" <<'PY'
import json, sys
m = json.load(open(sys.argv[1] + "/manifest.json"))
t = m["totals"]
print({k: t[k] for k in ("series_total", "chunks_total", "samples", "floats", "histograms",
                          "samples_skipped_tombstone", "crc_errors", "decode_errors")})
print("state:", m["state"], "blocks:", len(m["blocks"]))
PY

if python3 -c "import duckdb" >/dev/null 2>&1; then
  echo "== duckdb =="
  python3 - "$OUT" <<'PY'
import duckdb, sys
root = sys.argv[1]
c = duckdb.connect()
c.execute(f"CREATE TABLE s AS SELECT * FROM read_parquet('{root}/blocks/*/samples/*.parquet', hive_partitioning=true)")
c.execute(f"CREATE TABLE x AS SELECT * FROM read_parquet('{root}/blocks/*/series/*.parquet', hive_partitioning=true)")
print("rows:", c.execute("SELECT count(*) FROM s").fetchone()[0])
print(c.execute("SELECT x.labels['__name__'], count(*) FROM s JOIN x USING(series_uid) WHERE s.block=x.block GROUP BY 1 ORDER BY 2 DESC LIMIT 3").fetchall())
PY
fi

echo "OK: $OUT"

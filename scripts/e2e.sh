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
  echo "== duckdb metric tables =="
  python3 - "$OUT" <<'PY'
import duckdb, sys
root=sys.argv[1]
sql=open(root + "/load_duckdb.sql").read()
clean="\n".join(l for l in sql.splitlines() if not l.strip().startswith("--"))
c=duckdb.connect()
for st in [x.strip() for x in clean.split(";") if x.strip()]:
    c.execute(st)
print("metric tables:", len(c.execute("show tables").fetchall()))
print("fixture_metric_00 rows:", c.execute('SELECT count(*) FROM "fixture_metric_00"').fetchone()[0])
print("fixture_metric_00 cols:", [r[0] for r in c.execute('DESCRIBE "fixture_metric_00"').fetchall()])
print("fixture_hist_00 rows:", c.execute('SELECT count(*) FROM "fixture_hist_00"').fetchone()[0])
PY
fi

echo "OK: $OUT"

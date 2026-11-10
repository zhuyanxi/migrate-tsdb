#!/usr/bin/env bash
# Mimir layout end-to-end test using a local directory as the S3 bucket.
# Creates two tenants with the SAME block ULID and verifies tenant isolation.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
WORK="$(mktemp -d /tmp/tsdb-mimir-e2e.XXXXXX)"
trap 'rm -rf "$WORK"' EXIT

cargo build --release -p tsdb-migrate-cli >/dev/null
BIN=./target/release/tsdb-migrate
ULID="$(python3 -c "import json;print(json.load(open('fixtures/gen/data/block/ground_truth.json'))['ulid'])")"
SRC="fixtures/gen/data/block/$ULID"

for tenant in tenant-a tenant-b; do
  mkdir -p "$WORK/bucket/$tenant"
  cp -r "$SRC" "$WORK/bucket/$tenant/"
done

echo "== tenants =="
"$BIN" inspect --mimir-local-root "$WORK/bucket"

echo "== migrate =="
"$BIN" migrate --mimir-local-root "$WORK/bucket" --out "$WORK/out" \
  --block-parallel 2 --decode-workers 2 --unit-series 10 --progress-interval 60

echo "== verify =="
"$BIN" verify --out "$WORK/out"

echo "== output layout =="
find "$WORK/out/blocks" -maxdepth 2 -type d | sed "s#$WORK/out/##" | sort

if python3 -c "import duckdb" >/dev/null 2>&1; then
  echo "== duckdb =="
  python3 - "$WORK/out" <<'PY'
import duckdb, sys
root=sys.argv[1]
c=duckdb.connect()
print(c.execute(f"SELECT tenant, count(*) FROM read_parquet('{root}/blocks/**/samples/*.parquet', hive_partitioning=true) GROUP BY 1 ORDER BY 1").fetchall())
PY
fi
echo "OK: $WORK"

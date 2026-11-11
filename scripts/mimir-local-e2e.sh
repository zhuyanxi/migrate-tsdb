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
  echo "== duckdb per-tenant metric tables =="
  python3 - "$WORK/out" <<'PY'
import duckdb, glob, sys
root=sys.argv[1]
for path in sorted(glob.glob(root + "/load_duckdb_*.sql")):
    tenant=path.rsplit("load_duckdb_",1)[1][:-4]
    sql=open(path).read()
    clean="\n".join(l for l in sql.splitlines() if not l.strip().startswith("--"))
    c=duckdb.connect()
    for st in [x.strip() for x in clean.split(";") if x.strip()]:
        c.execute(st)
    print(tenant, "metric tables:", len(c.execute("show tables").fetchall()),
          "fixture_metric_00:", c.execute('SELECT count(*) FROM "fixture_metric_00"').fetchone()[0])
PY
fi
echo "OK: $WORK"

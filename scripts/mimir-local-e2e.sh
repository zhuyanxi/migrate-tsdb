#!/usr/bin/env bash
# Mimir source smoke test using a local directory as the S3 bucket.
# Remote Write delivery is covered by scripts/e2e.sh.
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

echo "== plan tenant isolation =="
"$BIN" plan --mimir-local-root "$WORK/bucket" --out "$WORK/state" --json > "$WORK/plan.json"
python3 - "$WORK/plan.json" <<'PY'
import json, sys
plan = json.load(open(sys.argv[1]))
blocks = plan["blocks"]
assert len(blocks) == 2, blocks
assert {block["tenant"] for block in blocks} == {"tenant-a", "tenant-b"}, blocks
print("planned blocks:", [(block["tenant"], block["ulid"]) for block in blocks])
PY

echo "OK: Mimir tenant layout"

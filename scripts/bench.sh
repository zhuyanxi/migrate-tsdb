#!/usr/bin/env bash
# Regenerates the benchmark block and measures release throughput/RSS.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
if [ "${1:-}" = "gen" ]; then
  (cd fixtures/gen && go run . -out data -bench-series "${2:-20000}")
  exit 0
fi
REMOTE_WRITE_URL="${TSDB_MIGRATE_BENCH_REMOTE_WRITE_URL:-}"
if [ -z "$REMOTE_WRITE_URL" ]; then
  echo "Set TSDB_MIGRATE_BENCH_REMOTE_WRITE_URL to a test Remote Write receiver" >&2
  exit 2
fi
cargo build --release -p tsdb-migrate-cli >/dev/null
for w in 1 2 4; do
  out="$(mktemp -d /tmp/tsdb-bench-$w.XXXXXX)"
  echo "== decode_workers=$w =="
  /usr/bin/time -l ./target/release/tsdb-migrate migrate \
    --data-dir fixtures/gen/data/bench --out "$out" \
    --remote-write-url "$REMOTE_WRITE_URL" \
    --block-parallel 1 --decode-workers "$w" --unit-series 5000 --progress-interval 60 \
    2>&1 | grep -E "完成：|real|maximum resident"
  rm -rf "$out"
done

#!/usr/bin/env bash
# End-to-end smoke test: fixture planning and Remote Write delivery to a local receiver.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
OUT="$(mktemp -d /tmp/tsdb-migrate-e2e.XXXXXX)"
trap 'rm -rf "$OUT"' EXIT

echo "== build =="
cargo build --release -p tsdb-migrate-cli >/dev/null
BIN=./target/release/tsdb-migrate

echo "== inspect / plan =="
"$BIN" inspect --data-dir fixtures/gen/data/block
"$BIN" plan --data-dir fixtures/gen/data/block --out "$OUT"

echo "== Remote Write end-to-end =="
cargo test -p migrate-core --test remote_write_e2e

echo "OK: Remote Write fixture flow"

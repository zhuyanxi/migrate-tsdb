#!/usr/bin/env bash
# Copies the small fixture set into the tsdb-reader crate so that
# `cargo test` works from a `cargo package` tarball (where fixtures/ is absent).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC="$ROOT/fixtures/gen/data"
DST="$ROOT/crates/tsdb-reader/tests/data"

if [[ ! -d "$SRC/block" ]]; then
  echo "missing $SRC/block; run ./scripts/dev-fixtures.sh first" >&2
  exit 1
fi

rm -rf "$DST/block"
mkdir -p "$DST"
cp -R "$SRC/block" "$DST/"
cp "$SRC/chunks.json" "$DST/"
echo "synced fixtures -> crates/tsdb-reader/tests/data"
du -sh "$DST"

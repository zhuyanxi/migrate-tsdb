#!/usr/bin/env bash
# Regenerates the Prometheus test blocks and chunk fixtures using the vendored
# upstream Go implementation. Requires Go 1.26+.
set -euo pipefail
cd "$(dirname "$0")/../fixtures/gen"
go run . -out data "$@"
echo "fixtures: $(pwd)/data"

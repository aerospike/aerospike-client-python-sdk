#!/usr/bin/env bash
# Build the Aerospike Python SDK distribution artifacts (sdist + wheel).
#
# Used by:
#   - .github/workflows/release.yml          (manual release)
#   - .github/workflows/build-and-deploy.yml (planned: §0.6 of the new
#     release pipeline; called as `build-script-path` by the shared
#     reusable workflow)
#
# Inputs (environment variables, all optional):
#   OUTDIR    — output directory for built artifacts (default: dist)
#   PYTHON    — python executable to use      (default: python3)
#
# Outputs:
#   $OUTDIR/aerospike_sdk-*.whl
#   $OUTDIR/aerospike_sdk-*.tar.gz
#
# This script is intentionally minimal: it assumes ANTLR generation
# (`make generate-ael`) has already been run by the caller. The CI
# workflows that call this script run that step explicitly.

set -euo pipefail

OUTDIR="${OUTDIR:-dist}"
PYTHON="${PYTHON:-python3}"

echo "==> build-artifacts: PYTHON=$PYTHON OUTDIR=$OUTDIR"

"$PYTHON" -m pip install --quiet --upgrade build
"$PYTHON" -m build --sdist --wheel --outdir "$OUTDIR"

echo "==> build-artifacts: produced —"
ls -lh "$OUTDIR"

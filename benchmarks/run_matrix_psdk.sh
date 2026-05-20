#!/usr/bin/env bash
# PSDK-only matrix runner — subset of run_matrix.sh.
#
# Runs the PSDK cells only: sync (fast-path + builder × 32t/1t × FT/non-FT),
# async single-loop, AsyncPool, and (optionally) the three batch sweeps.
# Skips PAC, Rust core, and legacy cells.
#
# Typical wall time:
#   - core 16 PSDK cells (sync + async + pool): ~5 min
#   - + 17 batch-sweep cells (SKIP_BATCH=0): ~10 min total
#
# Output: per-cell text files under $OUT (default /tmp/matrix-runs),
# plus a short summary line on stdout per cell.
#
# Usage:
#   bash benchmarks/run_matrix_psdk.sh
#   SKIP_BATCH=1 bash benchmarks/run_matrix_psdk.sh      # skip batch sweeps
#   AEROSPIKE_HOST=127.0.0.1:3000 bash benchmarks/run_matrix_psdk.sh
#   OUT=/tmp/my-runs bash benchmarks/run_matrix_psdk.sh
set -u

# --- Zombie guard ----------------------------------------------------------
# Stale bench processes from a previous run (SSH-dropped, killed-locally-only,
# etc.) can steal CPU on the bench-client and silently lower reported TPS below
# the true client / server capability. Refuse to start until they're cleared.
#
# Match only real bench executables (python invoking benchmarks.benchmark, or
# the rust-core binary) — NOT shell wrappers / pgrep commands that happen to
# carry the pattern as an argv string.
zombies=$(ps -eo pid,comm,args | awk '
    $2 == "python" || $2 ~ /^python3?\.?[0-9t]*$/ {
        if ($0 ~ /-m[[:space:]]+benchmarks\.benchmark/) print $1;
    }
    $2 == "rust-core" || ($2 == "main" && $0 ~ /rust-core\/target/) { print $1 }
' || true)
if [[ -n "$zombies" ]]; then
  echo "ERROR: existing bench processes detected — kill them before re-running:" >&2
  echo "  $zombies" | tr ' ' '\n' >&2
  echo "" >&2
  echo "  pkill -9 -f 'python.*benchmarks\\.benchmark|rust-core/target'" >&2
  exit 1
fi

# --- Python interpreter ----------------------------------------------------
if ! command -v python >/dev/null 2>&1; then
  if [[ -n "${VENV_PATH:-}" && -f "$VENV_PATH/bin/activate" ]]; then
    # shellcheck disable=SC1091
    source "$VENV_PATH/bin/activate"
  elif [[ -f "$HOME/venv-ft/bin/activate" ]]; then
    # shellcheck disable=SC1091
    source "$HOME/venv-ft/bin/activate"
  else
    echo "ERROR: no python on PATH and no venv to activate" >&2
    echo "  set VENV_PATH=/path/to/venv or activate one before running" >&2
    exit 1
  fi
fi

# --- Config ----------------------------------------------------------------
OUT="${OUT:-/tmp/matrix-runs}"
HOST="${AEROSPIKE_HOST:-10.138.0.3:3100}"
NS="${NAMESPACE:-test}"
SET="${SET:-test}"
KEYS="${KEYS:-100000}"
DURATION="${DURATION:-15}"
WARMUP="${WARMUP:-3}"
SKIP_BATCH="${SKIP_BATCH:-0}"

mkdir -p "$OUT"
rm -f "$OUT"/*.txt

ARGS=(-H "$HOST" -n "$NS" -s "$SET" -k "$KEYS" -o I8
      -w RU,50 -d "$DURATION"
      --services-alternate --no-tracemalloc)

run() {
  local tag="$1"; shift
  echo "[run] $tag"
  "$@" >"$OUT/$tag.txt" 2>&1
  tail -6 "$OUT/$tag.txt" \
    | grep -E "Total TPS|Latency p50|Errors:" \
    | sed "s/^/    /"
}

export AEROSPIKE_HOST="$HOST"
export AEROSPIKE_USE_SERVICES_ALTERNATE=true

# --- PSDK sync (fast-path + builder × 32t/1t × FT/non-FT) ------------------
for gil in 0 1; do
  for threads in 32 1; do
    for fp in --fast-path --no-fast-path; do
      sfx=${fp//-/_}
      tag="psdk_sync${sfx}_t${threads}_gil${gil}"
      PYTHON_GIL=$gil ALLOW_GIL_ON=1 \
        run "$tag" python -m benchmarks.benchmark "${ARGS[@]}" \
          --mode sync --threads $threads $fp
    done
  done
done

# --- PSDK async single-loop ------------------------------------------------
for gil in 0 1; do
  for fp in --fast-path --no-fast-path; do
    sfx=${fp//-/_}
    tag="psdk_async${sfx}_z32_gil${gil}"
    PYTHON_GIL=$gil ALLOW_GIL_ON=1 \
      run "$tag" python -m benchmarks.benchmark "${ARGS[@]}" \
        --mode async -z 32 $fp
  done
done

# --- PSDK AsyncPool (4 loops × 64 tasks) -----------------------------------
for gil in 0 1; do
  for fp in --fast-path --no-fast-path; do
    sfx=${fp//-/_}
    tag="psdk_pool${sfx}_4x64_gil${gil}"
    PYTHON_GIL=$gil ALLOW_GIL_ON=1 \
      run "$tag" python -m benchmarks.benchmark "${ARGS[@]}" \
        --mode async --pool-loops 4 -z 64 $fp
  done
done

if [[ "$SKIP_BATCH" == "1" ]]; then
  echo "[done] $(ls -1 "$OUT" | wc -l) cells in $OUT (batch sweeps skipped)"
  exit 0
fi

# --- PSDK async batch sweep (single-loop builder, 32 tasks, FT) ------------
for bsz in 1 4 16 32 64 128; do
  tag="psdk_async_builder_batch${bsz}_z32_gil0"
  PYTHON_GIL=0 \
    run "$tag" python -m benchmarks.benchmark "${ARGS[@]}" \
      --mode async -z 32 --no-fast-path --batch-size $bsz
done

# --- PSDK AsyncPool batch sweep (4×64, FT) ---------------------------------
for bsz in 1 4 16 32 64; do
  tag="psdk_pool_builder_batch${bsz}_4x64_gil0"
  PYTHON_GIL=0 \
    run "$tag" python -m benchmarks.benchmark "${ARGS[@]}" \
      --mode async --pool-loops 4 -z 64 --no-fast-path --batch-size $bsz
done

# --- PSDK sync builder batch sweep (32 threads, FT) ------------------------
for bsz in 1 4 16 32 64 128; do
  tag="psdk_sync_builder_batch${bsz}_t32_gil0"
  PYTHON_GIL=0 \
    run "$tag" python -m benchmarks.benchmark "${ARGS[@]}" \
      --mode sync --threads 32 --no-fast-path --batch-size $bsz
done

echo "[done] $(ls -1 "$OUT" | wc -l) cells in $OUT"

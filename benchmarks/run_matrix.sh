#!/usr/bin/env bash
# Cross-client TPS+latency matrix runner.
#
# Runs the canonical 30-cell matrix (PSDK / PAC / Rust core / legacy
# across {sync, async, AsyncPool} × {32t/1t} × {FT, non-FT}) plus a
# batch-size sweep for the async builder paths.
#
# Output: per-cell text files under $OUT (default /tmp/matrix-runs),
# plus a short summary line on stdout per cell.
#
# Required env: a reachable Aerospike server. Defaults to bench-asd
# (10.138.0.3:3100) but override via AEROSPIKE_HOST.
#
# Usage:
#   bash benchmarks/run_matrix.sh
#   AEROSPIKE_HOST=127.0.0.1:3000 bash benchmarks/run_matrix.sh
#   OUT=/tmp/my-runs bash benchmarks/run_matrix.sh
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
# Honor an active venv ($VIRTUAL_ENV / `python` on PATH) when present; fall
# back to $VENV_PATH/bin/activate if set, then ~/venv-ft for bench-client.
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

mkdir -p "$OUT"
rm -f "$OUT"/*.txt


# Common bench args (-d / --services-alternate / no-tracemalloc are fixed).
# --with-telemetry enables 1-in-100 latency sampling so the summary block
# includes "Latency p50/p99/p99.9" — The framework's per-op sampling cost is
# ~1-2% TPS hit, so disabled by default.

ARGS=(-H "$HOST" -n "$NS" -s "$SET" -k "$KEYS" -o I8
      -w RU,50 -d "$DURATION"
      --services-alternate --no-tracemalloc)
      #--services-alternate --no-tracemalloc --with-telemetry)

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

# --- PAC sync direct (pac-blocking) ----------------------------------------
for gil in 0 1; do
  for threads in 32 1; do
    tag="pac_sync_t${threads}_gil${gil}"
    PYTHON_GIL=$gil ALLOW_GIL_ON=1 \
      run "$tag" python -m benchmarks.benchmark "${ARGS[@]}" \
        --mode pac-blocking --threads $threads
  done
done

# --- PAC async direct (pac-async) ------------------------------------------
for gil in 0 1; do
  for tasks in 32 1; do
    tag="pac_async_z${tasks}_gil${gil}"
    PYTHON_GIL=$gil ALLOW_GIL_ON=1 \
      run "$tag" python -m benchmarks.benchmark "${ARGS[@]}" \
        --mode pac-async -z $tasks
  done
done

# --- Legacy aerospike C client (single-threaded; importing it forces GIL on)
for gil in 0 1; do
  tag="legacy_sync_t1_gil${gil}"
  PYTHON_GIL=$gil ALLOW_GIL_ON=1 \
    run "$tag" python -m benchmarks.benchmark "${ARGS[@]}" \
      --mode legacy-sync --threads 1
done

# --- Rust core binary (no Python, no GIL) ----------------------------------
if [[ -x benchmarks/rust-core/target/release/rust-core ]]; then
  for mode in async sync; do
    for tasks in 32 1; do
      tag="rust_${mode}_t${tasks}"
      echo "[run] $tag"
      MODE=$mode TASKS=$tasks DURATION="$DURATION" WARMUP="$WARMUP" \
        KEYS="$KEYS" READ_PCT=50 \
        AEROSPIKE_HOST="$HOST" NAMESPACE="$NS" SET="$SET" \
        benchmarks/rust-core/target/release/rust-core \
        >"$OUT/$tag.txt" 2>&1
      tail -3 "$OUT/$tag.txt" | sed "s/^/    /"
    done
  done
else
  echo "[skip] rust-core binary not built (cargo build --release first)"
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

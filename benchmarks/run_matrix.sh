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


# Common bench args. --no-services-alternate is REQUIRED on bench-asd: the
# cluster publishes its real addresses via the standard services list, so
# enabling services-alternate routes through unreachable alternate addresses
# and fast-fails ~2/3 of ops (only one of three nodes stays reachable). Those
# errors are excluded from ok-TPS but throttle the run to one node's traffic,
# so the whole result is invalid. The error gate below catches it if it recurs.
# --with-telemetry enables 1-in-100 latency sampling so the summary block
# includes "Latency p50/p99/p99.9" — ~1-2% TPS hit, off elsewhere by default.

ARGS=(-H "$HOST" -n "$NS" -s "$SET" -k "$KEYS" -o I8
      -w RU,50 -d "$DURATION"
      --no-services-alternate --no-tracemalloc --with-telemetry)

# Fail the matrix if any cell's real-error rate exceeds this (percent of
# attempts). Routing / services-alternate poisoning shows up as ~67%; healthy
# runs are ~0%. Tune via MAX_ERR_PCT=<n> if a populated-keyspace caveat applies.
MAX_ERR_PCT="${MAX_ERR_PCT:-1.0}"
FAILURES=()

# Pull the cell's error percentage (PSDK prints "(X% of ops)", rust-core prints
# "(X% of attempts)") and record a failure if it exceeds MAX_ERR_PCT.
assess_errors() {
  local tag="$1" file="$2" pct
  pct=$(grep -oE '\([0-9]+\.[0-9]+% of (ops|attempts)\)' "$file" \
        | head -1 | grep -oE '[0-9]+\.[0-9]+' || true)
  [[ -z "$pct" ]] && return 0
  if awk -v p="$pct" -v m="$MAX_ERR_PCT" 'BEGIN { exit !(p + 0 > m + 0) }'; then
    echo "    [FAIL] $tag error_rate=${pct}% > ${MAX_ERR_PCT}% (routing/services-alternate?)" >&2
    FAILURES+=("$tag (${pct}%)")
  fi
}

run() {
  local tag="$1"; shift
  echo "[run] $tag"
  "$@" >"$OUT/$tag.txt" 2>&1
  tail -6 "$OUT/$tag.txt" \
    | grep -E "Total TPS|Latency p50|Errors:" \
    | sed "s/^/    /"
  assess_errors "$tag" "$OUT/$tag.txt"
}

export AEROSPIKE_HOST="$HOST"
export AEROSPIKE_USE_SERVICES_ALTERNATE=false

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

# --- PSDK sync EXPERIMENTAL current_thread_runtime -------------------------
# Per-thread Tokio current_thread runtime + per-thread PAC _LocalClient.
# Dormant by default — only fires when the bench passes the explicit flag.
# Kept in-matrix so each lever lands with both default + ct_runtime numbers.
for gil in 0 1; do
  for threads in 32 1; do
    for fp in --fast-path --no-fast-path; do
      sfx=${fp//-/_}
      tag="psdk_sync${sfx}_t${threads}_ctrt_gil${gil}"
      PYTHON_GIL=$gil ALLOW_GIL_ON=1 \
        run "$tag" python -m benchmarks.benchmark "${ARGS[@]}" \
          --mode sync --threads $threads $fp --current-thread-runtime
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

# --- PAC sync EXPERIMENTAL current_thread_runtime --------------------------
# Per-thread Tokio current_thread runtime + per-thread PAC _LocalClient.
# Dormant by default — only fires when the bench passes the explicit flag.
# Apples-to-apples PAC-direct ct_runtime measurement (no PSDK layer);
# compare against psdk_sync_*_ctrt_gil* cells to isolate PSDK overhead
# under ct_runtime.
for gil in 0 1; do
  for threads in 32 1; do
    tag="pac_sync_t${threads}_ctrt_gil${gil}"
    PYTHON_GIL=$gil ALLOW_GIL_ON=1 \
      run "$tag" python -m benchmarks.benchmark "${ARGS[@]}" \
        --mode pac-blocking --threads $threads --current-thread-runtime
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
        AEROSPIKE_USE_SERVICES_ALTERNATE=false \
        benchmarks/rust-core/target/release/rust-core \
        >"$OUT/$tag.txt" 2>&1
      tail -3 "$OUT/$tag.txt" | sed "s/^/    /"
      assess_errors "$tag" "$OUT/$tag.txt"
    done
  done
else
  echo "[skip] rust-core binary not built (cargo build --release first)"
fi

# --- Equal-concurrency async sweep -----------------------------------------
# Compare the three async clients at MATCHED concurrency (z=32 is already in
# the base sections above). Avoids the apples-to-oranges trap of pitting a
# 32-task Rust run against a 512-task Python pool, and shows where each client
# stops scaling. FT only to keep the cell count down.
ZSWEEP="${ZSWEEP:-64 128 256 512}"
for z in $ZSWEEP; do
  PYTHON_GIL=0 \
    run "psdk_async_fast_path_z${z}_gil0" \
      python -m benchmarks.benchmark "${ARGS[@]}" --mode async -z "$z" --fast-path
  PYTHON_GIL=0 \
    run "pac_async_z${z}_gil0" \
      python -m benchmarks.benchmark "${ARGS[@]}" --mode pac-async -z "$z"
done

if [[ -x benchmarks/rust-core/target/release/rust-core ]]; then
  for z in $ZSWEEP; do
    tag="rust_async_t${z}"
    echo "[run] $tag"
    MODE=async TASKS=$z DURATION="$DURATION" WARMUP="$WARMUP" \
      KEYS="$KEYS" READ_PCT=50 \
      AEROSPIKE_HOST="$HOST" NAMESPACE="$NS" SET="$SET" \
      AEROSPIKE_USE_SERVICES_ALTERNATE=false \
      benchmarks/rust-core/target/release/rust-core \
      >"$OUT/$tag.txt" 2>&1
    tail -3 "$OUT/$tag.txt" | sed "s/^/    /"
    assess_errors "$tag" "$OUT/$tag.txt"
  done
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

if (( ${#FAILURES[@]} > 0 )); then
  echo "" >&2
  echo "[FAIL] ${#FAILURES[@]} cell(s) exceeded ${MAX_ERR_PCT}% errors — results are NOT trustworthy:" >&2
  printf '         %s\n' "${FAILURES[@]}" >&2
  echo "  Confirm the seed host and --services-alternate match the cluster's advertised addresses." >&2
  exit 1
fi

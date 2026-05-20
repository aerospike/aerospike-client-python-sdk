# Aerospike Python SDK benchmarks

This directory contains benchmark tools for measuring single-record and batch
throughput and latency against a live Aerospike cluster.

## Requirements

- Repository root on ``PYTHONPATH`` (run from the repo root).
- Installed ``aerospike-sdk`` with its async client dependency.
- A reachable cluster seed (defaults to ``AEROSPIKE_HOST`` from ``aerospike.env``).

## Running

```bash
python -m benchmarks.benchmark --help
```

Common examples:

```bash
# Async read/update 50/50 on key space 100k, 32 concurrent tasks, 10 seconds
python -m benchmarks.benchmark -k 100000 -z 32 -w RU,50 -d 10

# Populate keys 1..N with insert workload then benchmark reads
python -m benchmarks.benchmark -w I -k 100000 -c 100000 -z 32 --truncate
python -m benchmarks.benchmark -w RU,50 -k 100000 -z 32 -d 10

# Multi-bin read/write mix (80% reads; 60% read-all; 30% write-all)
python -m benchmarks.benchmark -o I1,S128 -w RU,80,60,30 -d 10

# PSDK sync client (delegates to PAC `_blocking` per op, no per-op event loop)
python -m benchmarks.benchmark --mode sync -w RU,50 --threads 4 -d 10

# Batch commands (each operation touches 20 keys)
python -m benchmarks.benchmark --batch-size 20 -w RU,50 -d 10

# TLS cluster with authentication
python -m benchmarks.benchmark -H host:tls_name:port --tls-ca-file ca.pem -U admin -P admin
```

## Flags

| Flag | Meaning |
|------|---------|
| ``-H`` / ``--hosts`` | Seed hosts (from ``aerospike.env`` by default). |
| ``-n`` | Namespace |
| ``-s`` | Set name |
| ``-k`` | Random key space size |
| ``-o`` | Bin spec: ``I1``, ``S128``, ``B1024``, combined with commas |
| ``-w`` | Workload: ``I``, ``RU,50``, ``RU,80,60,30``, ``RR,20``, ``RMU``, ``RMI``, ``RMD`` |
| ``-z`` / ``--async-tasks`` | Number of concurrent async tasks (default: 32) |
| ``--threads`` | Number of OS threads for sync mode (falls back to ``-z``) |
| ``-d`` | Duration in seconds |
| ``-c`` | Stop after this many successful operations |
| ``--batch-size`` | Keys per batch command (``0`` or ``1`` for single-record) |
| ``--latency`` | ``COLUMNS,SHIFT`` histogram shape |
| ``--mode`` | ``async`` (default, PSDK), ``sync`` (PSDK SyncClient), ``pac-blocking`` (PAC sync direct), ``pac-async`` (PAC async direct), or ``legacy-sync`` (legacy ``aerospike`` C client) |
| ``--warmup`` / ``--cooldown`` | Full-second intervals dropped from the summary |
| ``--truncate`` | Truncate the set before running |
| ``--truncate-after`` | Truncate the set after running |
| ``--tls-ca-file`` | CA certificate for TLS connections |
| ``--tls-cert-file`` / ``--tls-key-file`` | Client cert/key for mutual TLS |
| ``-U`` / ``-P`` | Username / password for authentication |
| ``--auth-mode`` | ``INTERNAL``, ``EXTERNAL``, or ``PKI`` |

Note: Python reserves ``-h`` for help; use ``-H`` for hosts.

## Makefile targets

- ``make bench`` -- populate 100k keys then run a 10s ``RU,50`` async benchmark.
- ``make bench-quick`` -- short smoke run (still requires a live cluster).
- ``make bench-compare`` -- full comparison: PAC, PSDK async, PSDK sync, and legacy client.

## Comparison tool

``compare.py`` runs identical workloads across multiple clients and prints a
side-by-side table with TPS, latency percentiles, and memory usage.

```bash
python -m benchmarks.compare --help

# Full comparison (PAC, PSDK async, sync, legacy)
python -m benchmarks.compare -k 100000 -z 32 --threads 4 -d 15

# PSDK-only (skip legacy)
python -m benchmarks.compare -k 100000 -z 32 -d 15 --skip-legacy

# Export to CSV
python -m benchmarks.compare -k 100000 -d 30 --runs 3 --csv results.csv
```

### Clients compared

| Client | Description | Concurrency |
|--------|-------------|-------------|
| **PAC** | Raw Python Async Client (no SDK layer) | ``-z`` async tasks |
| **PSDK async** | Full SDK with builder chain | ``-z`` async tasks |
| **PSDK sync** | SDK SyncClient via PAC `_blocking` (per-thread loop runner, no per-op event loop) | ``--threads`` OS threads |
| **Legacy** | C-extension client (``get``/``operate``, same ops as PSDK) | Single-threaded (hardcoded) |

### Configuration

Client paths are resolved from:
1. CLI flags (``--pac-path``, ``--legacy-path``)
2. Environment variables (``PAC_PATH``, ``LEGACY_CLIENT_PATH``)
3. Each repo's ``.python-version`` file (for pyenv interpreter resolution)
4. Fallback defaults (``~/tmp/aerospike-client-python-async``, ``~/code/aerospike-client-python``)

Set ``PAC_PATH`` and ``LEGACY_CLIENT_PATH`` in your ``aerospike.env`` for
persistent configuration.

## Output

Per-second lines include read/write/total TPS, timeout and error counts, and
cumulative latency columns. After the run, a summary reports average and median
TPS (with warmup/cooldown intervals removed), latency percentiles (p50, p90,
p99, p99.9, max), peak RSS, optional ``tracemalloc`` peak, and optional
``psutil`` process CPU if installed.

## Legacy benchmark

``legacy_benchmark.py`` is a standalone benchmark for the legacy Aerospike
Python client (C extension). Unlike the original ``kvs.py`` tool, it uses
``client.get()`` for reads and ``client.operate()`` for writes -- the same
operations as the PSDK -- so TPS and latency numbers are directly comparable.
The compare tool invokes it automatically when a legacy client path is available.

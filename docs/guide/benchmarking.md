# Benchmarking Guide

This guide documents the architecture, setup, and measured TPS / latency for the Aerospike Python SDK (`PSDK`) and the Aerospike Python Async Client (`PAC`). The reference setup uses two isolated VMs on Google Cloud Platform; the same methodology works on any other cloud provider (AWS EC2, Azure VMs, etc.) or on dedicated on-prem hardware — only the VM provisioning steps would change.

## Architecture

```
┌─────────────────────────┐                                     ┌─────────────────────────┐
│      bench-client       │◄──────────── TCP :3100 ────────────►│   bench-asd × 3 nodes   │
│   c3-standard-8 (8 vCPU)│                                     │   c3-standard-8 (8 vCPU)│
│   32 GB RAM, 30 GB disk │                                     │   32 GB RAM, 30 GB disk │
│   Ubuntu 24.04 LTS      │                                     │   Ubuntu 24.04 LTS      │
│                         │                                     │                         │
│   Python 3.14t (free-   │                                     │   Aerospike Enterprise  │
│     threaded, no GIL)   │                                     │   8.1.1.1               │
│   Rust 1.95.0           │                                     │   in-memory storage     │
│   PAC, PSDK from source │                                     │   (4 GB, namespace test)│
└─────────────────────────┘                                     └─────────────────────────┘
```

`bench-asd` is a 3-node Aerospike cluster (each node a separate `c3-standard-8` VM). All four VMs (1 client + 3 server nodes) run within the same VPC/subnet, giving sub-millisecond network RTT. They use `c3-standard-8` machine types (Intel Sapphire Rapids, 8 vCPUs, 32 GB RAM each) to provide dedicated, non-shared compute.

### Why dedicated, isolated VMs?

Local benchmarking on macOS via Podman / Docker Desktop hits several bottlenecks that distort results:

- **Userspace TCP proxy** (Docker Desktop's `gvproxy`) — adds 2-5 ms per hop, capping TPS at ~15K regardless of client capability.
- **CPU contention** — co-locating `asd` and the Python client on a shared VM creates resource competition that masks true scaling behavior.
- **uvloop + free-threading** — multiple uvloop instances on separate OS threads under free-threaded Python can cause silent freezes. PSDK's `AsyncPool` explicitly uses `asyncio.SelectorEventLoop` for worker threads to avoid this.

Dedicated VMs on isolated CPU cores with direct, low-latency networking between client and server eliminate all of these issues. GCP `c3-standard-8` (8 dedicated vCPUs each) on the same VPC is the reference setup. Equivalent isolation on AWS (`c7i.2xlarge` / dedicated tenancy / placement groups), Azure (`Fsv2-series`), or on-prem (two adjacent physical hosts on a quiet switch) reproduces the numbers within run-to-run noise.

## Environment

| Component | Version |
|-----------|---------|
| GCP machine type | `c3-standard-8` (8 vCPU, 32 GB) |
| OS | Ubuntu 24.04 LTS, kernel 6.17.0-gcp |
| Python | 3.14.5 free-threaded build (e.g. 3.14t) |
| Rust | 1.95.0 |
| PAC | `aerospike-async` 0.5.0a1 (built from source with `mimalloc` global allocator) |
| PSDK | `aerospike-sdk` 0.9.0a2 (built from source) |
| Legacy Python client | `aerospike` 19.2.1 (single-threaded, sync, C client; published PyPI wheel) |
| Aerospike server | Enterprise 8.1.1.1, 3-node cluster, in-memory, 4 GB per node, RF=1 |

## Workload

All measurements use the same workload across every client:

- **100,000 keys** seeded into `test.test` set with single-bin records
- **50/50 read/write mix** (`RU,50`)
- **Single-bin payload**: `{"b0": <int>}` — the int is the key id (no per-op rng for bin values)
- **Shared client** across all worker threads / tasks
- **15 seconds measured** + 3 seconds warmup (no separate cooldown)
- **Sampled latency**: 1-in-100 ops timed → p50 / p99 / p99.9 reported

**Bench RNG / key construction**: as of 2026-05-25, the harness uses PAC's
`FastRng` (xoshiro256++) per worker instead of CPython's `random.Random`
(Mersenne Twister) — matches the JSDK `RandomShift` / Rust core `SmallRng`
methodology and removes a ~5 µs/op Python-stdlib RNG handicap that
otherwise inflated the bench-harness overhead. Keys are constructed per op
via PAC's `Key.from_int_user_key(ns, set, kid)` fast-path, which skips
Python `str()` conversion + `PythonValue` enum dispatch (~2 µs/op).
Net: the bench's per-op overhead matches JSDK/Rust core methodology
within a few hundred nanoseconds, so reported TPS reflects client
capability rather than Python stdlib cost.

Free-threaded (FT) runs use `PYTHON_GIL=0`. Non-FT runs use `PYTHON_GIL=1 ALLOW_GIL_ON=1` on the same free-threaded binary — same wheel, same imports, GIL state flipped.

## Running the benchmarks

The framework bench (`python -m benchmarks.benchmark`) carries all the modes for the cells in this document. Each invocation prints per-second TPS / error / timeout lines plus a final summary block.

```bash
# PSDK sync — fast-path (session.get / session.put) by default
PYTHON_GIL=0 python -m benchmarks.benchmark \
  -H <bench-asd>:3100 --services-alternate \
  -n test -s test -k 100000 -o I8 -w RU,50 \
  -d 15 --warmup 3 --cooldown 0 \
  --mode sync --threads 32 --fast-path

# Same harness, builder API (session.query / upsert chained)
PYTHON_GIL=0 python -m benchmarks.benchmark \
  -H <bench-asd>:3100 --services-alternate \
  -n test -s test -k 100000 -o I8 -w RU,50 \
  -d 15 --warmup 3 --cooldown 0 \
  --mode sync --threads 32 --no-fast-path

# PSDK async — single client, N concurrent tasks
PYTHON_GIL=0 python -m benchmarks.benchmark \
  -H <bench-asd>:3100 --services-alternate \
  -n test -s test -k 100000 -o I8 -w RU,50 \
  -d 15 --warmup 3 --cooldown 0 \
  --mode async -z 32 --fast-path

# PSDK async — AsyncPool (N loops × M tasks per loop), free-threaded only
PYTHON_GIL=0 python -m benchmarks.benchmark \
  -H <bench-asd>:3100 --services-alternate \
  -n test -s test -k 100000 -o I8 -w RU,50 \
  -d 15 --warmup 3 --cooldown 0 \
  --mode async --pool-loops 4 -z 64 --fast-path

# PAC sync direct — bypasses PSDK, calls PAC `_blocking` entries
PYTHON_GIL=0 python -m benchmarks.benchmark \
  -H <bench-asd>:3100 --services-alternate \
  -n test -s test -k 100000 -o I8 -w RU,50 \
  -d 15 --warmup 3 --cooldown 0 \
  --mode pac-blocking --threads 32

# PAC async direct — bypasses PSDK, calls PAC async entries
PYTHON_GIL=0 python -m benchmarks.benchmark \
  -H <bench-asd>:3100 --services-alternate \
  -n test -s test -k 100000 -o I8 -w RU,50 \
  -d 15 --warmup 3 --cooldown 0 \
  --mode pac-async -z 32

# Legacy `aerospike` C client (single-threaded — that client doesn't support
# multi-threaded fan-out; importing it on a free-threaded build also auto-
# re-enables the GIL because the C extension hasn't declared FT-safety).
python -m benchmarks.benchmark \
  -H <bench-asd>:3100 --services-alternate \
  -n test -s test -k 100000 -o I8 -w RU,50 \
  -d 15 --warmup 3 --cooldown 0 \
  --mode legacy-sync --threads 1

# Non-FT comparison: same binary, GIL forced on
PYTHON_GIL=1 ALLOW_GIL_ON=1 python -m benchmarks.benchmark ... (same args)
```

The Rust core (no Python) is benched via a standalone Rust binary that talks to `aerospike-core` directly — no PyO3, no Python interpreter at all. This gives the language-floor TPS for the same workload:

```bash
cargo build --release --manifest-path benchmarks/rust-core/Cargo.toml
MODE=async TASKS=32 DURATION=15 WARMUP=3 \
  AEROSPIKE_HOST=<bench-asd>:3100 \
  benchmarks/rust-core/target/release/rust-core
```

Every cell in the matrix below was produced by `python -m benchmarks.benchmark --mode ...` against bench-asd (`<bench-asd>:3100`), except the Rust-core rows, which use the dedicated Rust binary at `benchmarks/rust-core/`.

## Cross-client TPS — single-key (batch size 1)

50/50 RW, 100K keys, 32 threads / tasks (or 4×64 for AsyncPool), 15 s measured. Free-threaded runs use `PYTHON_GIL=0`; non-FT runs use `PYTHON_GIL=1 ALLOW_GIL_ON=1`. The Rust core has no GIL — one number applies, shown in the FT column.

| Client / Mode | Threads / Tasks | FT TPS | non-FT TPS |
|---|---|---|---|
| **PSDK sync, fast-path** (`session.get` / `session.put`) | 32 | **214,093** | 53,203 |
| PSDK sync, builder (chained API) | 32 | 153,461 | 31,960 |
| **PSDK async AsyncPool, fast-path** | 4×64 | **172,885** | 56,000 |
| **PSDK async AsyncPool, fast-path** | 6×64 | **177,685** | (FT only) |
| **PSDK async AsyncPool, fast-path** | 8×64 | **181,851** | (FT only) |
| **PSDK async AsyncPool, fast-path** | 12×64 | **180,325** | (FT only) |
| PSDK async AsyncPool, builder | 4×64 | 146,638 | 38,208 |
| PSDK async single-loop, fast-path | 32 tasks | 112,596 | 75,830 |
| PSDK async single-loop, builder | 32 tasks | 63,082 | 50,048 |
| PSDK sync, fast-path | 1 | 12,954 | 14,088 |
| PSDK sync, builder | 1 | 12,452 | 12,536 |
| **PAC sync direct** (`pac-blocking`) | 32 | **220,217** | 48,899 |
| **PAC async direct** (`pac-async`) | 32 tasks | **118,665** | 68,284 |
| PAC sync | 1 | 12,848 | 13,466 |
| PAC async | 1 task | 9,394 | 4,036 |
| **Rust core, async** (Tokio tasks, no Python) | 32 tasks | **289,885** | n/a (no GIL) |
| Rust core, sync (OS threads + `Handle::block_on`) | 32 | 246,038 | n/a (no GIL) |
| Rust core, async | 1 task | 16,765 | n/a (no GIL) |
| Rust core, sync | 1 | 14,167 | n/a (no GIL) |
| Python legacy (sync, C client) | 1 | 14,724 | 15,759 |

## Cross-client latency

p50 / p99 / p99.9 in microseconds, sampled 1-in-100 ops during measurement. Framework rows are rounded to 100 µs precision (the per-second histogram bucket size); Rust-core rows are exact.

| Client / Mode | Threads / Tasks | FT (µs) | non-FT (µs) |
|---|---|---|---|
| PSDK sync, fast-path | 32 | **100 / 300 / 400** | 700 / 2,600 / 3,900 |
| PSDK sync, builder | 32 | 200 / 400 / 700 | 1,500 / 5,200 / 5,800 |
| PSDK sync, fast-path | 1 | 100 / 100 / 100 | 100 / 100 / 400 |
| PSDK sync, builder | 1 | 100 / 100 / 100 | 100 / 100 / 200 |
| PSDK async single-loop, fast-path | 32 tasks | 300 / 400 / 1,000 | 500 / 600 / 900 |
| PSDK async single-loop, builder | 32 tasks | 500 / 600 / 900 | 1,000 / 1,200 / 1,300 |
| PSDK async AsyncPool, fast-path | 4×64 | 1,400 / 4,700 / 7,400 | 5,100 / 14,100 / 14,200 |
| PSDK async AsyncPool, fast-path | 6×64 | 2,000 / 6,500 / 9,900 | (FT only) |
| PSDK async AsyncPool, fast-path | 8×64 | 2,500 / 8,800 / 13,300 | (FT only) |
| PSDK async AsyncPool, fast-path | 12×64 | 3,600 / 15,800 / 27,300 | (FT only) |
| PSDK async AsyncPool, builder | 4×64 | 1,600 / 4,000 / 5,000 | 8,700 / 26,900 / 27,000 |
| PAC sync | 32 | 100 / 300 / 400 | 600 / 2,800 / 3,800 |
| PAC sync | 1 | 100 / 100 / 100 | 100 / 100 / 300 |
| PAC async | 32 tasks | 300 / 400 / 800 | 500 / 600 / 600 |
| PAC async | 1 task | 100 / 100 / 200 | 200 / 300 / 300 |
| **Rust core, async** | 32 tasks | **106 / 184 / 223** | n/a (no GIL) |
| Rust core, sync | 32 | 127 / 184 / 273 | n/a (no GIL) |
| Rust core, async | 1 task | 59 / 75 / 119 | n/a (no GIL) |
| Rust core, sync | 1 | 69 / 86 / 116 | n/a (no GIL) |
| Python legacy (sync) | 1 | 100 / 100 / 100 | 100 / 100 / 100 |

(batch-sweeps)=
## Batch sweeps

The single-key cells above measure one record per `execute()`. Real applications often batch multiple keys per call to amortize network and per-op overhead. The sweeps below hold concurrency constant (32 threads / tasks) and vary `--batch-size`. Free-threaded only.

### PSDK sync builder

`session.query([keys]).execute()` and `session.batch().upsert(k).put(b).execute()`. Routes through PAC's `batch_read_blocking` / `batch_operate_blocking` directly — no asyncio loop in the path.

| Batch size | Total TPS | × b=1 |
|---|---|---|
| 1 | 155,758 | 1.00× |
| 4 | 226,188 | 1.45× |
| 16 | 425,984 | 2.73× |
| 32 | 488,432 | 3.14× |
| 64 | 536,416 | 3.44× |
| **128** | **562,752** | **3.61×** |

### PSDK async single-loop builder

`await session.query([keys]).execute()` and friends — one event loop, 32 concurrent tasks.

| Batch size | Total TPS | × b=1 |
|---|---|---|
| 1 | 62,842 | 1.00× |
| 4 | 70,368 | 1.12× |
| 16 | 156,000 | 2.48× |
| 32 | 166,080 | 2.64× |
| 64 | 203,552 | 3.24× |
| **128** | **230,144** | **3.66×** |

### PSDK async AsyncPool builder

Four event loops × 64 tasks per loop. Free-threaded only.

| Batch size | Total TPS | × b=1 (pool) |
|---|---|---|
| 1 | 143,700 | 1.00× |
| 4 | 156,138 | 1.09× |
| 16 | 267,808 | 1.86× |
| 32 | 309,744 | 2.16× |
| **64** | **335,328** | **2.33×** |

**Headline**: the **PSDK sync builder scales monotonically through batch=128 to 563K TPS** — the highest number in the entire matrix and **94% above Rust-core async direct (290K)**. Sync batch routes via PAC's `batch_*_blocking` entries with one PyO3 boundary per batch, so doubling the batch size keeps amortizing the per-call Python cost without ceiling out. The b=128 peak is 3.6× the single-key sync builder (which itself moved from 114K → 156K with the bench-RNG / key-construction cleanups landed in 2026-05-25).

The async single-loop sweep tops out around 230K (batch=128) — the asyncio ↔ Tokio bridge cost per `execute()` doesn't go away just because each call moves more data. AsyncPool recovers most of that by running 4 loops in parallel, hitting 335K at batch=64.

## Stack cost analysis

Layering the headline single-key TPS numbers across clients shows where every transition costs:

| Layer | TPS | Note |
|---|---|---|
| **Rust core async direct** | **289,885** | `aerospike-core` via Tokio tasks — single-key language floor, no Python |
| Rust core sync (`block_on`) | 246,038 | `aerospike-core` via OS threads + `block_on` |
| **PAC sync direct** | **220,217** | PyO3 wrapper over `aerospike-core` blocking, no SDK |
| **PSDK sync, fast-path** | **214,093** | SDK `session.get` / `session.put` → PAC blocking |
| **PSDK async AsyncPool, fast-path (8×64)** | **181,851** | 8 event loops × 64 tasks (FT only, with per-Client runtime) |
| **PSDK async AsyncPool, fast-path (4×64)** | **172,885** | 4 event loops × 64 tasks (FT only) |
| PSDK sync, builder | 153,461 | SDK chained builder → execute → stream |
| PSDK async AsyncPool, builder (4×64) | 146,638 | 4 loops, full builder path |
| PAC async direct, 32 tasks | 118,665 | PyO3 wrapper, asyncio ↔ Tokio bridge |
| PSDK async single-loop, fast-path | 112,596 | One event loop, `session.get` / `session.put` |
| PSDK async single-loop, builder | 63,082 | One event loop, full builder path |
| Python legacy (sync) | 14,724 | Single-thread C client baseline |

### Sync stack — boundary cost is small

| Transition | TPS | Δ |
|---|---|---|
| Rust core async (reference) | 289,885 | — |
| → Rust core sync (`block_on`) | 246,038 | **−15%** (Rust `block_on` overhead) |
| → PAC sync direct (PyO3 wrap) | 220,217 | **−11%** (PyO3 + Python boundary cost) |
| → PSDK sync, fast-path | 214,093 | **−3%** (PSDK SDK layer dispatch) |
| → PSDK sync, builder | 153,461 | **−28%** (chained builder + stream wrap in Python) |

**Sync key insight**: PSDK sync fast-path is within 3% of PAC sync direct — the SDK layer is essentially free. The 28% builder tax on single-key calls is the cost of Python interpreter time on a chained-allocation pattern; the fast-path avoids it. With batching (see [Batch sweeps](#batch-sweeps)), the same builder hits 488K TPS at batch=32 and 563K at batch=128 — *higher* than the Rust async single-record ceiling.

### Async stack — boundary cost is much higher

| Transition | TPS | Δ |
|---|---|---|
| Rust core async (reference) | 289,885 | — |
| → PAC async direct, 32 tasks | 118,665 | **−59%** (asyncio ↔ Tokio bridge: every op crosses twice) |
| → PSDK async single-loop, fast-path | 112,596 | **−5%** (PSDK SDK layer) |
| → PSDK async AsyncPool, fast-path (4×64) | 172,885 | **+53%** vs single-loop (multi-loop + per-Client runtime, FT only) |
| → PSDK async AsyncPool, fast-path (8×64) | 181,851 | **+62%** vs single-loop |
| → PSDK async AsyncPool, fast-path (12×64) | 180,325 | **+60%** vs single-loop (TPS ceiling on 8-core hw) |

**Async key insight**: the per-loop ceiling around ~113K is the fundamental cost of the async bridge pattern — every op crosses Tokio ↔ asyncio twice (submit, then complete). `AsyncPool` recovers most of that by running N loops on N OS threads in parallel, each with its own dedicated PAC Tokio runtime (per-Client runtime isolation, auto-enabled at `loop_count >= 4`). TPS scales monotonically through 4–12 loops to ~180K — a 1.5× lift over single-loop, closing most of the gap to the sync path. Only useful under free-threaded Python; under regular CPython the GIL serializes the loops and the pool is slower than a single client (see [AsyncPool note](#asyncpool-is-a-free-threading-feature)).

### Practical takeaway

- **Sync clients pay only the PyO3 boundary cost** (~11%). The SDK layer adds ~3%.
- **Async clients pay PyO3 + asyncio event-loop scheduling + Tokio worker bounce** — much more expensive per op (~59% drop vs Rust async). `AsyncPool` is the way to scale async across cores, but only on free-threaded Python.
- **The chained-builder API pays an additional Python-interpreter cost** on single-key calls (~28% on sync, more on async). On batch calls, that cost amortizes across keys; at batch=128 the sync builder *exceeds* the single-record Rust async ceiling by 94%.
- **For maximum throughput**: use the sync API on free-threaded Python with batches when the workload tolerates batching. Use the fast-path (`session.get` / `session.put`) for single-key reads/writes when you don't need filters / error handlers / TTL hooks. Reserve the async API for genuinely async workloads (web servers, etc.).

## Fast-path vs builder

PSDK exposes two API shapes for single-key reads and writes:

- **Builder** (chained): `session.query(key).execute()` and `session.upsert(key).put(bins).execute()`. Returns a `RecordStream` of wrapped `RecordResult`s. Supports filter expressions, error handlers, TTL overrides, generation checks, batch operations, and secondary-index queries.
- **Fast-path** (direct): `session.get(key)` and `session.put(key, bins)`. Bypasses the builder + stream wrap and calls PAC's native `_blocking` / async entry points directly with the session-cached policy. Single-key only; no filter / error-handler / TTL hooks. Errors raise directly (cache misses raise `RecordNotFound`).

Speedup of fast-path over builder on **single-key** dispatch at 32 threads / 4×64 tasks, FT:

| Config | Builder TPS | Fast-path TPS | Speedup |
|---|---|---|---|
| PSDK async, single client | 63,082 | 112,596 | **1.78×** |
| PSDK async, AsyncPool 4×64 | 146,638 | 172,885 | **1.18×** |
| PSDK sync | 153,461 | 214,093 | **1.40×** |

These speedups are for single-key dispatch. With batching, the builder amortizes its per-op overhead across many keys per call — at batch=32 the sync builder reaches 488K TPS (vs 214K for sync fast-path). The fast-path stays single-key only; for any workload that can batch, the builder eventually wins.

The builder has irreducible Python overhead per op (builder object allocation, `_OperationSpec` finalization, `RecordResult` wrapping, generator-based stream iteration). The fast-path skips all of it.

See [`performance.md`](performance.md) for the user-facing decision guide.

(asyncpool-is-a-free-threading-feature)=
## AsyncPool is a free-threading feature

`AsyncPool` runs N event loops on N OS threads with one PAC client each. Its value is **multi-thread parallelism across CPU cores** — which only materializes under free-threaded Python (`PYTHON_GIL=0`).

Under non-FT Python the GIL still serializes all Python execution. AsyncPool ends up with 256 outstanding tasks across 4 threads competing for one interpreter, plus the per-loop orchestration overhead — net slower than a single-client async setup:

| Config | non-FT TPS | vs single-loop non-FT |
|---|---|---|
| async single-loop, fast-path, 32 tasks | 75,830 | baseline |
| async AsyncPool 4×64, fast-path | 56,000 | **−26%** |
| async single-loop, builder, 32 tasks | 50,048 | baseline |
| async AsyncPool 4×64, builder | 38,208 | **−24%** |

**On regular Python or with `PYTHON_GIL=1`, use a single `Client` + `asyncio.gather`. Reserve `AsyncPool` for free-threaded runs only.**

## Error classification

The framework treats `RecordNotFound` (cache miss on a point read) as a successful read with no record — not an error. This matches the semantics used by other Aerospike SDKs. Real errors (timeouts, connection failures, server-side errors, etc.) are counted separately as either `Errors:` or `Timeouts:` in the per-second ticker and the summary block.

To verify error accounting on a fresh dataset, pass `--truncate` to the bench command; with the fix in place all modes report `Errors: 0` even when half the early reads cache-miss.

# Benchmarking Guide

This guide documents the architecture, setup, and measured TPS / latency for the Aerospike Python SDK (`PSDK`) and the Aerospike Python Async Client (`PAC`). The reference setup uses two isolated VMs on Google Cloud Platform; the same methodology works on any other cloud provider (AWS EC2, Azure VMs, etc.) or on dedicated on-prem hardware — only the VM provisioning steps would change.

## Architecture

```
┌─────────────────────────┐                                     ┌─────────────────────────┐
│      bench-client       │◄──────────── TCP :3000 ────────────►│   bench-asd × 3 nodes   │
│   n4-standard-8 (8 vCPU)│                                     │   n4-standard-8 (8 vCPU)│
│   32 GB RAM, 30 GB disk │                                     │   32 GB RAM, 30 GB disk │
│   Ubuntu 24.04 LTS      │                                     │   Ubuntu 24.04 LTS      │
│                         │                                     │                         │
│   Python 3.14t (free-   │                                     │   Aerospike Enterprise  │
│     threaded, no GIL)   │                                     │   8.x.x                 │
│   Rust 1.96+            │                                     │   in-memory storage     │
│   PAC, PSDK from source │                                     │   (4 GB, namespace test)│
└─────────────────────────┘                                     └─────────────────────────┘
```

`bench-asd` is a 3-node Aerospike cluster (each node a separate `n4-standard-8` VM, dedicated 8 vCPU per ASD process — critical for measured server-side ceilings). All four VMs (1 client + 3 server nodes) run within the same VPC/subnet, giving sub-millisecond network RTT.

### Why dedicated, isolated VMs?

Local benchmarking on macOS via Podman / Docker Desktop hits several bottlenecks that distort results:

- **Userspace TCP proxy** (Docker Desktop's `gvproxy`) — adds 2-5 ms per hop, capping TPS at ~15K regardless of client capability.
- **CPU contention** — co-locating `asd` and the Python client on a shared VM creates resource competition that masks true scaling behavior. Server-side: running 3 ASDs as containers on a single 8-vCPU host (vs each on its own 8-vCPU VM) caps `aerospike-core` direct at ~280K TPS because the 3 server processes share 8 vCPUs (~2.7 vCPU each). On dedicated 8-vCPU-per-ASD VMs, the cluster sustains ≥580K TPS — well above where any default-config Python client lands. (Earlier writeups quoted the 3-VM ceiling as 810K and then 405K, then ~290-300K rust-core direct; all three were client-side artifacts — services-alternate routing errors, then the Tokio timer wheel + the default 256-conn pool — masquerading as the cluster.)
- **uvloop + free-threading** — uvloop 0.22.x has a libuv FT race on `loop._ready_len` (MagicStack/uvloop issues #720, #721) that triggers when many threads concurrently call `loop.call_soon_threadsafe()`. PSDK / PAC fully mitigates this via a single persistent waker thread inside PAC: all Tokio-side `call_soon_threadsafe` invocations funnel through one dedicated thread, eliminating the multi-threaded access pattern the race needs. The fix is empirically stable across 20+ minutes of stress (z=128 single-loop + AsyncPool 8×64, 241M ops, zero stalls). uvloop is installed by default on FT and non-FT Linux/macOS builds. (uvloop has no Windows wheel; PAC falls back to the asyncio default selector loop there.) An `AEROSPIKE_NO_UVLOOP=1` env-var safety valve is available to opt out without uninstalling the dependency.

Dedicated VMs on isolated CPU cores with direct, low-latency networking between client and server eliminate all of these issues. GCP `n4-standard-8` (8 dedicated vCPUs each) on the same VPC is the reference setup. Equivalent isolation on AWS (`c7i.2xlarge` / dedicated tenancy / placement groups), Azure (`Fsv2-series`), or on-prem (two adjacent physical hosts on a quiet switch) reproduces the numbers within run-to-run noise.

## Environment

| Component | Version |
|-----------|---------|
| GCP machine type | `n4-standard-8` (8 vCPU, 32 GB) |
| OS | Ubuntu 24.04 LTS, kernel 6.17.0-gcp |
| Python | 3.14.6 free-threaded build (e.g. 3.14t) |
| Rust | 1.96.0 |
| PyO3 | 0.29.0 |
| PAC | `aerospike-async` 0.6.0-alpha (built from source with `mimalloc` global allocator; uvloop installed by default) |
| PSDK | `aerospike-sdk` 0.9.0-alpha (built from source) |
| Legacy Python client | `aerospike` 19.2.1 (single-threaded, sync, C client; published PyPI wheel) |
| Aerospike server | Enterprise 8.x, 3-node cluster, in-memory, 4 GB per node, RF=1 |

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
| **PSDK sync, fast-path** (`session.get` / `session.put`) | 32 | **214,489** | 50,857 |
| **PSDK sync, fast-path, ct_runtime** | 32 | **265,971** | 57,200 |
| PSDK sync, builder (chained API) | 32 | 149,428 | 31,564 |
| PSDK sync, builder, ct_runtime | 32 | 187,209 | 33,444 |
| **PSDK async AsyncPool, fast-path** | 4×64 | **260,325** | 108,327 |
| **PSDK async AsyncPool, fast-path** | 8×64 | **~292,000** | (FT only) |
| PSDK async AsyncPool, builder | 4×64 | 181,838 | 61,281 |
| **PSDK async single-loop, fast-path** | 32 tasks | **118,220** | 105,959 |
| PSDK async single-loop, builder | 32 tasks | 68,402 | 64,496 |
| PSDK sync, fast-path | 1 | 10,946 | 9,730 |
| PSDK sync, builder | 1 | 10,053 | 8,826 |
| **PAC sync direct** (`pac-blocking`) | 32 | **209,426** | 50,194 |
| **PAC sync direct, ct_runtime** | 32 | **271,066** | 60,730 |
| **PAC async direct** (`pac-async`) | 32 tasks | **124,001** | 114,020 |
| PAC sync | 1 | 12,221 | 11,812 |
| PAC async | 1 task | 7,773 | 8,304 |
| **Rust core, async** (default settings) | 32 tasks | **~290,000** | n/a (no GIL) |
| Rust core, sync (default settings) | 32 | ~246,000 | n/a (no GIL) |
| Rust core, async, with timer fix + pool sized | 512 tasks | **~580,000** | n/a (no GIL) |
| Rust core, async | 1 task | 12,627 | n/a (no GIL) |
| Rust core, sync | 1 | 11,960 | n/a (no GIL) |
| Python legacy (sync, C client) | 1 | (FT N/A, no wheel) | ~15,000 |

The Rust-core rows here are on the 3-VM ASD topology. At default settings, rust-core async hits ~290K at t=32 and scales with concurrency — but the apparent plateau between t=32 and t=512 is **client-side**, not the cluster. Two `aerospike-core` defaults stack to cap throughput:

- **Per-op Tokio timer-wheel registration.** Every `aerospike_rt::timeout(...)` insert/remove goes through a shared mutex in Tokio's global time driver; under contention this serializes per-op work. Bypassing it (A2 — measurement hack) lifts rust-core async at t=256 from ~381K to ~551K.
- **`max_conns_per_node = 256` default**, fail-fast on exhaustion. With the timer also out of the way, t=512 collapses with ~92% errors as the pool refuses past 256 concurrent ops per node. Sizing the pool to match concurrency (`MAX_CONNS_PER_NODE = 512`) takes t=512 to **580K @ 0 errors** — the real ceiling.

Python clients (PAC, PSDK) hit their own client-side ceilings (PyO3 boundary, asyncio/Tokio bridge, builder allocations) well below 580K, so they don't see either of these two artifacts. Earlier versions of this doc quoted 810K and 405K as "the cluster ceiling"; both were artifacts of the two issues above plus an older services-alternate routing bug. There is no real cluster constraint visible from any default-config Python client.

:::{admonition} `ct_runtime` is experimental — measurement-only on this table
:class: warning

The `ct_runtime` rows above use PAC's `--current-thread-runtime` mode (sync only): each Python thread gets its own Tokio current-thread runtime via PAC's `_LocalClient` proxy. This sidesteps the multi-thread Tokio worker-pool hop and raises the sync ceiling (PAC sync 207K → 277K; PSDK sync fp 210K → 265K).

**But ct_runtime is not production-ready.** Each per-thread runtime owns its own `Cluster`, which means:
- **N× cluster-tend threads** (32 Python threads = 32 tend loops polling the cluster every second)
- **N× connection pools** (~384 connections per process at default settings)
- **Incomplete `_with_overrides` surface** — some PAC methods still hit the shared runtime even when ct_runtime is on

These numbers are included for measurement transparency; treat them as an experimental performance lever, not a recommended deployment.
:::

## Cross-client latency

p50 / p99 / p99.9 in microseconds, sampled 1-in-100 ops during measurement. Framework rows are rounded to 100 µs precision (the per-second histogram bucket size); Rust-core rows are exact.

| Client / Mode | Threads / Tasks | FT (µs) | non-FT (µs) |
|---|---|---|---|
| PSDK sync, fast-path | 32 | **100 / 300 / 500** | 500 / 2,500 / 3,200 |
| PSDK sync, fast-path, ct_runtime | 32 | 100 / 200 / 400 | 500 / 2,400 / 3,400 |
| PSDK sync, builder | 32 | 100 / 900 / 3,700 | 1,400 / 5,000 / 5,800 |
| PSDK sync, fast-path | 1 | 100 / 100 / 200 | 100 / 100 / 100 |
| PSDK async single-loop, fast-path | 32 tasks | 200 / 300 / 600 | 500 / 600 / 700 |
| PSDK async single-loop, builder | 32 tasks | 500 / 600 / 800 | 1,000 / 1,200 / 1,300 |
| PSDK async AsyncPool, fast-path | 4×64 | **900 / 2,500 / 3,500** | 4,800 / 13,300 / 16,400 |
| PSDK async AsyncPool, fast-path | 8×64 | 1,700 / 4,100 / 5,800 | (FT only) |
| PSDK async AsyncPool, builder | 4×64 | 1,400 / 2,600 / 3,800 | 7,800 / 25,300 / 26,000 |
| PAC sync | 32 | 100 / 300 / 500 | 600 / 2,800 / 3,800 |
| PAC sync, ct_runtime | 32 | 100 / 200 / 300 | 500 / 2,300 / 3,300 |
| PAC sync | 1 | 100 / 100 / 100 | 100 / 100 / 100 |
| PAC async | 32 tasks | **200 / 300 / 500** | 400 / 400 / 500 |
| PAC async | 1 task | 100 / 200 / 200 | 100 / 200 / 200 |
| **Rust core, async** (default) | 32 tasks | (sampled) p99 ~190 | n/a (no GIL) |
| Rust core, sync (default) | 32 | (sampled) p99 ~200 | n/a (no GIL) |
| Rust core, async | 1 task | p99 ~140 | n/a (no GIL) |
| Rust core, sync | 1 | p99 ~115 | n/a (no GIL) |

Framework latency is histogram-bucketed at 100 µs granularity (`--with-telemetry`'s sampling resolution); Rust-core latency is sampled exactly. Framework cells with reported p50 under 100 µs round up to the 100 µs bucket boundary.

(batch-sweeps)=
## Batch sweeps

The single-key cells above measure one record per `execute()`. Real applications often batch multiple keys per call to amortize network and per-op overhead. The sweeps below hold concurrency constant (32 threads / tasks) and vary `--batch-size`. Free-threaded only.

### PSDK sync builder

`session.query([keys]).execute()` and `session.batch().upsert(k).put(b).execute()`. Routes through PAC's `batch_read_blocking` / `batch_operate_blocking` directly — no asyncio loop in the path.

| Batch size | Total TPS | × b=1 |
|---|---|---|
| 1 | 145,898 | 1.00× |
| 4 | 142,895 | 0.98× |
| 16 | 328,253 | 2.25× |
| 32 | 401,467 | 2.75× |
| 64 | 470,720 | 3.23× |
| **128** | **485,056** | **3.32×** |

### PSDK async single-loop builder

`await session.query([keys]).execute()` and friends — one event loop, 32 concurrent tasks.

| Batch size | Total TPS | × b=1 |
|---|---|---|
| 1 | 64,569 | 1.00× |
| 4 | 59,514 | 0.92× |
| 16 | 121,155 | 1.88× |
| 32 | 144,885 | 2.24× |
| 64 | 174,080 | 2.70× |
| **128** | **204,736** | **3.17×** |

### PSDK async AsyncPool builder

Four event loops × 64 tasks per loop. Free-threaded only.

| Batch size | Total TPS | × b=1 (pool) |
|---|---|---|
| 1 | 190,278 | 1.00× |
| 4 | 156,954 | 0.83× |
| 16 | 265,443 | 1.40× |
| 32 | 310,901 | 1.63× |
| **64** | **336,469** | **1.77×** |

**Headline**: the **PSDK sync builder scales through batch=128 to ~485K TPS** — the highest framework number in the matrix. Sync batch routes via PAC's `batch_*_blocking` entries with one PyO3 boundary per batch, so doubling the batch size keeps amortizing the per-call Python cost. The b=128 peak is 3.3× the single-key sync builder.

The async single-loop sweep tops out around 205K (batch=128) — the asyncio ↔ Tokio bridge cost per `execute()` doesn't go away just because each call moves more data. AsyncPool recovers most of that by running 4 loops in parallel, hitting 336K at batch=64.

## Stack cost analysis

Layering the headline single-key TPS numbers across clients shows where every transition costs. The Rust-core figures below are at the same default settings as the Python clients; Rust-core's *real* cluster-side ceiling is ≥580K (with the per-op Tokio timer wheel bypassed AND `max_conns_per_node` sized to match concurrency — see ["Per-language baselines"](#per-language-baselines) above). Python clients hit their own client-side ceilings well below 580K, so they aren't sensitive to the Rust-core defaults that gate the higher number.

| Layer | TPS | Note |
|---|---|---|
| **Rust core async, default settings** | **~290,000** | `aerospike-core` via Tokio tasks; at default settings (timer wheel + 256-conn pool both active) |
| Rust core async, timer + pool sized | ~580,000 | Real cluster-side ceiling; current `aerospike-core` defaults stack to cap below this |
| Rust core sync, default settings | ~246,000 | `aerospike-core` via OS threads + `block_on` |
| **PSDK async AsyncPool, fast-path (8×64)** | **~292,000** | 8 event loops × 64 tasks (FT only, uvloop) |
| **PAC sync direct, ct_runtime** | **271,066** | PyO3 wrapper, per-thread Tokio current-thread runtime |
| **PSDK sync, fast-path, ct_runtime** | **265,971** | SDK fast-path + ct_runtime |
| **PSDK async AsyncPool, fast-path (4×64)** | **260,325** | 4 event loops × 64 tasks (FT only, uvloop) |
| **PSDK sync, fast-path** | **214,489** | SDK `session.get` / `session.put` → PAC blocking |
| **PAC sync direct (multi-thread Tokio)** | **209,426** | PyO3 wrapper, shared Tokio multi-thread runtime |
| PSDK async AsyncPool, builder (4×64) | 181,838 | 4 loops, full builder path |
| PSDK sync, builder | 149,428 | SDK chained builder → execute → stream |
| **PAC async direct, 32 tasks** | **124,001** | PyO3 wrapper, asyncio ↔ Tokio bridge (with drainer + uvloop) |
| **PSDK async single-loop, fast-path** | **118,220** | One event loop, `session.get` / `session.put` |
| PSDK async single-loop, builder | 68,402 | One event loop, full builder path |
| Python legacy (sync, non-FT) | ~15,000 | Single-thread C client baseline |

### Sync stack — boundary cost is small

| Transition | TPS | Δ |
|---|---|---|
| Rust core sync (default settings) | ~246,000 | reference (default `aerospike-core`) |
| → PAC sync direct (multi-thread Tokio) | 209,426 | **−15%** (PyO3 + Python boundary; Tokio thread-handoff in the per-op path) |
| → PSDK sync, fast-path | 214,489 | flat — SDK layer is essentially free |
| → PSDK sync, builder | 149,428 | **−30%** vs fp (chained builder + stream wrap in Python) |

The PyO3 + per-op Python ↔ Tokio thread handoff costs ~15% over the equivalent direct rust-core sync number. The PSDK SDK layer is essentially free over PAC direct. (The cluster sustains higher absolute throughput than rust-core sync default — see ["Per-language baselines"](#per-language-baselines) — but with the default `aerospike-core` settings active, both Python and Rust-direct paths land in the same band.)

### Async stack — closer to sync than it used to be

| Transition | TPS | Δ |
|---|---|---|
| PSDK sync, fast-path (sync reference) | 214,489 | — |
| → PAC async direct (single loop, drainer + uvloop) | 124,001 | **−42%** (asyncio loop thread is the gating step) |
| → PSDK async single-loop, fast-path | 118,220 | **−5%** vs PAC async (PSDK SDK layer) |
| → PSDK async AsyncPool, fast-path (4×64) | 260,325 | **+120%** vs single-loop (parallelism across loops + uvloop inside pool, FT only) — **+21% above sync** |
| → PSDK async AsyncPool, fast-path (8×64) | ~292,000 | **+147%** vs single-loop, **+36% over sync** |

**Async key insight**: post-drainer-thread + uvloop, the single-loop async ceiling sits around 120-130K. The bottleneck is now the **asyncio loop thread doing per-op `set_result` and task wakeup work, single-threaded**. `AsyncPool` (multi-loop) breaks past that ceiling by running 4-8 loops in parallel — at 8×64 it actually **exceeds** the sync fast-path ceiling. Only useful under free-threaded Python; under regular CPython the GIL serializes the loops and the pool is slower than a single client (see [AsyncPool note](#asyncpool-is-a-free-threading-feature)).

### Practical takeaway

- **PSDK SDK layer is essentially free** on both sync and async paths — ~3-8% over PAC direct on either side. Most cost is below PSDK in PAC + PyO3.
- **PAC's drainer thread** moves all asyncio-loop wake-ups onto a single persistent waker thread, eliminating per-batch `Python::attach` churn on Tokio workers. This is what lifted async TPS substantially over earlier reference numbers (e.g., AsyncPool 4×64 went from 173K → 246K).
- **uvloop is installed by default** under FT and non-FT Linux/macOS. It lifts single-loop async ~15% on top of the drainer; multi-loop (AsyncPool) sees ~0-3% extra because the per-loop work is already parallelized.
- **The chained-builder API pays a per-op Python tax** on single-key calls (~30% vs fast-path on sync). On batch calls, that cost amortizes across keys: at batch=128 the sync builder reaches ~484K TPS — far above any single-key cell.
- **For maximum throughput**: use the **sync builder with batches** (`session.batch()` or multi-key `session.query([keys])`) on free-threaded Python when the workload tolerates batching — ~484K TPS at batch=128. For single-key sync workloads, the **fast-path** (`session.get` / `session.put`) gives ~210K TPS. For async workloads, **AsyncPool 4-8 loops** delivers 246-273K TPS — above the sync fast-path ceiling. Reserve `--current-thread-runtime` (experimental — see the warning above) for tightly-controlled benchmarking, not production.

## Fast-path vs builder

PSDK exposes two API shapes for single-key reads and writes:

- **Builder** (chained): `session.query(key).execute()` and `session.upsert(key).put(bins).execute()`. Returns a `RecordStream` of wrapped `RecordResult`s. Supports filter expressions, error handlers, TTL overrides, generation checks, batch operations, and secondary-index queries.
- **Fast-path** (direct): `session.get(key)` and `session.put(key, bins)`. Bypasses the builder + stream wrap and calls PAC's native `_blocking` / async entry points directly with the session-cached policy. Single-key only; no filter / error-handler / TTL hooks. Errors raise directly (cache misses raise `RecordNotFound`).

Speedup of fast-path over builder on **single-key** dispatch at 32 threads / 4×64 tasks, FT:

| Config | Builder TPS | Fast-path TPS | Speedup |
|---|---|---|---|
| PSDK async, single client | 68,402 | 118,220 | **1.73×** |
| PSDK async, AsyncPool 4×64 | 181,838 | 260,325 | **1.43×** |
| PSDK sync | 149,428 | 214,489 | **1.44×** |

These speedups are for single-key dispatch. With batching, the builder amortizes its per-op overhead across many keys per call — at batch=128 the sync builder reaches 484K TPS (vs 210K for sync fast-path). The fast-path stays single-key only; for any workload that can batch, the builder eventually wins.

The builder has irreducible Python overhead per op (builder object allocation, `_OperationSpec` finalization, `RecordResult` wrapping, generator-based stream iteration). The fast-path skips all of it.

See [`performance.md`](performance.md) for the user-facing decision guide.

(asyncpool-is-a-free-threading-feature)=
## AsyncPool is a free-threading feature

`AsyncPool` runs N event loops on N OS threads with one PAC client each. Its value is **multi-thread parallelism across CPU cores** — which only materializes under free-threaded Python (`PYTHON_GIL=0`).

Under non-FT Python the GIL still serializes all Python execution. AsyncPool ends up with 256 outstanding tasks across 4 threads competing for one interpreter, plus the per-loop orchestration overhead — typically net flat or slightly slower than a single-client async setup on the same Python binary:

| Config | non-FT TPS | vs single-loop non-FT |
|---|---|---|
| async single-loop, fast-path, 32 tasks | 105,959 | baseline |
| async AsyncPool 4×64, fast-path | 108,327 | **+2%** (uvloop in pool roughly recovers the overhead) |
| async single-loop, builder, 32 tasks | 64,496 | baseline |
| async AsyncPool 4×64, builder | 61,281 | **−5%** |

**AsyncPool is roughly on par with single-client async under GIL-on Python** now that pool loops use uvloop too. Pick the one that fits your code shape; the real AsyncPool win is reserved for free-threaded runs.

## Error classification

The framework treats `RecordNotFound` (cache miss on a point read) as a successful read with no record — not an error. This matches the semantics used by other Aerospike SDKs. Real errors (timeouts, connection failures, server-side errors, etc.) are counted separately as either `Errors:` or `Timeouts:` in the per-second ticker and the summary block.

To verify error accounting on a fresh dataset, pass `--truncate` to the bench command; with the fix in place all modes report `Errors: 0` even when half the early reads cache-miss.

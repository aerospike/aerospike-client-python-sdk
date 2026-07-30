# Benchmarking Guide

This guide documents the architecture, setup, and measured TPS / latency for the Aerospike Python SDK (`PSDK`) and the Aerospike Python Async Client (`PAC`). The reference setup uses several VMs on Google Cloud Platform; the same methodology works on any other cloud provider (AWS EC2, Azure VMs, etc.) or on dedicated on-prem hardware — only the VM provisioning steps would change.

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
- **uvloop + free-threading** — **PAC installs uvloop at import** (`uvloop.install()` in `aerospike_async/__init__`, Linux/macOS, both FT and non-FT; opt out with `AEROSPIKE_NO_UVLOOP=1`; Windows has no uvloop wheel and falls back to the stdlib selector loop). So importing PSDK/PAC sets uvloop as the process loop policy, and **plain async (`asyncio.run`) and the AsyncPool both run on uvloop** — every async cell in this doc is a uvloop run. uvloop 0.22.x has a libuv free-threading race on `loop._ready_len` (MagicStack/uvloop #720, #721) that stalls a *multi-loop* pool when threads hit the shared ready-queue via `loop.call_soon_threadsafe()`; PAC dodges it with the **pipe-wake transport** — cross-thread completion wakeups go through a self-pipe watched by an `add_reader` callback instead of `call_soon_threadsafe`, so the racy path is never exercised under a pool. On the FT pool this is worth ~**+8%** vs the selector fallback (measured, 4×64 fast-path, interleaved median-of-3, latency-neutral). (A separate persistent drainer thread batches Tokio→asyncio wakeups for throughput; a single loop can't trigger the race regardless.) Empirically stable across 20+ minutes of stress (z=128 single-loop + AsyncPool 8×64, 241M+ ops, zero stalls). Under free-threading `AsyncPool` keeps uvloop only when that mitigation is active — pipe-wake on (`AEROSPIKE_PIPE_WAKE` ≠ `0`; default `auto`) or a #721-fixed uvloop release — otherwise it uses the selector loop for its loops; override with `AsyncPool(..., use_uvloop=...)`.

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

# PSDK async window API — get_many/put_many, N independent point ops fused per call
PYTHON_GIL=0 python -m benchmarks.benchmark \
  -H <bench-asd>:3100 --services-alternate \
  -n test -s test -k 100000 -o I8 -w RU,50 \
  -d 15 --warmup 3 --cooldown 0 \
  --mode async-many --many-size 16 -z 32

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

(per-language-baselines)=
## Cross-client Performance — single-key

50/50 RW, 100K keys, 10–15 s measured, each mode at a representative concurrency (see the Threads / Tasks column). Free-threaded runs use `PYTHON_GIL=0`; non-FT runs use `PYTHON_GIL=1 ALLOW_GIL_ON=1`. The Rust core has no GIL — one number applies, shown in the FT column. **Re-measured 2026-07-28 on core `4dd1a93` (v3) / PAC `0.6.0a7.dev6`, 0 errors across every cell.** The pooled-window rows and the **FT p99** column are from the 2026-07-29 published-build runs (PAC `0.6.0a7.dev7` / PSDK `0.9.0a6.dev2`, same v3 core; `matrix-pub` + the window×pool frontier sweep + a targeted fill for the 8×64 / single-thread rows), 0 errors; overlapping single-key modes reproduced the dev6 TPS within ~2%, so the columns are comparable. This table lists modes at **usable tail latency (p99 < 2.8 ms)**; the window×pool *peak* (~442K) trades a ~6 ms tail for only ~4% more TPS and is covered in the window-API section below. Rust-core p99 (`—`) was not captured — it is a comparison baseline, not re-run for tail latency.

| Client / Mode | Threads / Tasks | FT TPS | FT p99 | non-FT TPS |
|---|---|---|---|---|
| **PSDK async window API × AsyncPool** (`get_many` / `put_many`, 4 loops, z=16, k=8) | 4×16×8 | **425,808** | 2.6ms | (FT only) |
| **PSDK async window API × AsyncPool** (low-latency: 4 loops, z=8, k=4) | 4×8×4 | **342,946** | 0.9ms | (FT only) |
| **PSDK async window API, single loop** (`get_many` / `put_many`, k=8, no pool) | 32×8 | **332,396** | 1.4ms | 316,372 |
| **PSDK async AsyncPool, fast-path** | 4×64 | **280,726** | 1.8ms | 116,544 |
| **PSDK sync, fast-path, ct_runtime** | 32 | **272,612** | 0.2ms | 56,326 |
| **PSDK sync, fast-path** (`session.get` / `session.put`) | 32 | **241,104** | 0.2ms | 51,876 |
| PSDK sync, builder, ct_runtime | 32 | 186,338 | 0.4ms | 31,924 |
| PSDK async AsyncPool, builder | 4×64 | 180,187 | 2.6ms | 58,142 |
| **PSDK async single-loop, fast-path** | 128 tasks | **172,150** | 1.2ms | 161,660 |
| PSDK sync, builder (chained API) | 32 | 153,934 | 0.4ms | 30,813 |
| PSDK async single-loop, builder | 32 tasks | 66,322 | 0.6ms | 63,891 |
| PSDK sync, fast-path | 1 | 10,875 | 0.1ms | 10,507 |
| PSDK sync, builder | 1 | 9,534 | 0.1ms | 9,642 |
| **PAC sync direct, ct_runtime** | 32 | **280,828** | 0.2ms | 63,570 |
| **PAC sync direct** (`pac-blocking`) | 32 | **250,197** | 0.2ms | 52,462 |
| **PAC async direct** (`pac-async`) | 32 tasks | **130,641** | 0.3ms | 115,696 |
| PAC sync | 1 | 11,352 | 0.1ms | 10,291 |
| PAC async | 1 task | 7,989 | 0.2ms | 8,452 |
| Rust core, async, pool sized (`MAX_CONNS=512`) | 512 tasks | **575,592** | — | n/a (no GIL) |
| **Rust core, async** (default settings) | 32 tasks | **305,741** | — | n/a (no GIL) |
| Rust core, sync (default settings) | 32 | 245,729 | — | n/a (no GIL) |
| Rust core, async | 1 task | 12,254 | — | n/a (no GIL) |
| Rust core, sync | 1 | 10,650 | — | n/a (no GIL) |

The Rust-core rows here are on the 3-VM ASD topology. At default settings, rust-core async hits ~306K at t=32 and scales with concurrency — but the apparent plateau between t=32 and t=512 is **client-side**, not the cluster. Historically two `aerospike-core` defaults stacked to cap throughput; one is now fixed in core:

- **Per-op Tokio timer-wheel registration — now fixed in core (`CLIENT-4990`, present in `4dd1a93`).** Every `aerospike_rt::timeout(...)` insert/remove used to go through a shared mutex in Tokio's global time driver, serializing per-op work under contention. The reusable-`Sleep`-per-`Connection` rewrite eliminated it, so the default core already carries this win — no measurement hack needed.
- **`max_conns_per_node = 256` default**, fail-fast on exhaustion — still the operative default cap. At high concurrency the pool refuses past 256 concurrent ops per node. Sizing the pool to match concurrency (`MAX_CONNS_PER_NODE = 512`) takes t=512 to **575,592 @ 0 errors** — the real ceiling (the "pool sized" row above).

Python clients (PAC, PSDK) hit their own client-side ceilings (PyO3 boundary, asyncio/Tokio bridge, builder allocations) well below 580K, so they don't see either of these two artifacts. Earlier versions of this doc quoted 810K and 405K as "the cluster ceiling"; both were artifacts of the two issues above plus an older services-alternate routing bug. There is no real cluster constraint visible from any default-config Python client.

:::{admonition} `ct_runtime` is experimental — measurement-only on this table
:class: warning

The `ct_runtime` rows above use PAC's `--current-thread-runtime` mode (sync only): each Python thread gets its own Tokio current-thread runtime via PAC's `_LocalClient` proxy. This sidesteps the multi-thread Tokio worker-pool hop and raises the sync ceiling (PAC sync 250K → 281K; PSDK sync fp 241K → 273K).

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
| PSDK sync, fast-path | 32 | **100 / 200 / 500** | 600 / 2,600 / 3,700 |
| PSDK sync, fast-path, ct_runtime | 32 | 100 / 200 / 300 | 500 / 2,400 / 3,400 |
| PSDK sync, builder | 32 | 200 / 400 / 900 | 1,000 / 3,800 / 6,400 |
| PSDK sync, fast-path | 1 | 100 / 100 / 200 | 100 / 100 / 100 |
| PSDK async single-loop, fast-path | 32 tasks | 200 / 400 / 500 | 300 / 400 / 500 |
| PSDK async single-loop, builder | 32 tasks | 500 / 600 / 700 | 500 / 600 / 700 |
| PSDK async AsyncPool, fast-path | 4×64 | **900 / 2,200 / 3,200** | 2,100 / 4,900 / 6,100 |
| PSDK async AsyncPool, fast-path | 8×64 | 1,700 / 4,100 / 5,800 | (FT only) |
| PSDK async AsyncPool, builder | 4×64 | 1,400 / 2,600 / 3,600 | 4,300 / 9,600 / 10,200 |
| PAC sync | 32 | 100 / 200 / 400 | 600 / 2,600 / 4,000 |
| PAC sync, ct_runtime | 32 | 100 / 200 / 300 | 500 / 2,300 / 3,300 |
| PAC sync | 1 | 100 / 100 / 100 | 100 / 100 / 100 |
| PAC async | 32 tasks | **200 / 300 / 500** | 300 / 400 / 600 |
| PAC async | 1 task | 100 / 200 / 200 | 100 / 200 / 200 |
| **Rust core, async** (default) | 32 tasks | 100 / 170 / 270 | n/a (no GIL) |
| Rust core, sync (default) | 32 | 130 / 190 / 900 | n/a (no GIL) |
| Rust core, async | 1 task | 80 / 100 / 140 | n/a (no GIL) |
| Rust core, sync | 1 | 90 / 120 / 160 | n/a (no GIL) |

Framework latency is histogram-bucketed at 100 µs granularity (`--with-telemetry`'s sampling resolution); Rust-core latency is sampled exactly. Framework cells with reported p50 under 100 µs round up to the 100 µs bucket boundary.

(batch-sweeps)=
## Batch sweeps

The single-key cells above measure one record per `execute()`. Real applications often batch multiple keys per call to amortize network and per-op overhead. The sweeps below hold concurrency constant (32 threads / tasks) and vary `--batch-size`. Free-threaded only.

### PSDK sync builder

`session.query([keys]).execute()` and `session.upsert([keys]).put(b).execute()`. Routes through PAC's `batch_read_blocking` / `batch_operate_blocking` directly — no asyncio loop in the path.

| Batch size | Total TPS | × b=1 |
|---|---|---|
| 1 | 154,516 | 1.00× |
| 4 | 154,942 | 1.00× |
| 16 | 349,128 | 2.26× |
| 32 | 444,176 | 2.87× |
| 64 | 505,440 | 3.27× |
| **128** | **506,432** | **3.28×** |

### PSDK async single-loop builder

`await session.query([keys]).execute()` and friends — one event loop, 32 concurrent tasks.

| Batch size | Total TPS | × b=1 |
|---|---|---|
| 1 | 64,484 | 1.00× |
| 4 | 60,582 | 0.94× |
| 16 | 116,664 | 1.81× |
| 32 | 134,496 | 2.09× |
| 64 | 163,552 | 2.54× |
| **128** | **173,888** | **2.70×** |

### PSDK async AsyncPool builder

Four event loops × 64 tasks per loop. Free-threaded only.

| Batch size | Total TPS | × b=1 (pool) |
|---|---|---|
| 1 | 182,546 | 1.00× |
| 4 | 147,270 | 0.81× |
| 16 | 252,696 | 1.38× |
| 32 | 292,912 | 1.60× |
| **64** | **332,256** | **1.77×** |

### PSDK async window API (`get_many`/`put_many`)

`await session.get_many([keys])` / `session.put_many([keys], bins)` — **client-side** fusion of N independent point ops per call (NOT a server batch: each key stays its own wire op; only the client submission + completion are fused). Single event loop, 32 concurrent windows. *All tables in this doc were re-measured 2026-07-28 on core `4dd1a93` (v3) / PAC `0.6.0a7.dev6`, 0 errors across every cell — so cross-table comparisons are now apples-to-apples on one core.*

| Window size | Total TPS | p99 | × single-op (z512) |
|---|---|---|---|
| single-op fast-path (z512) | 139,310 | 5.9 ms | 1.00× |
| 8 | 332,352 | 1.4 ms | 2.39× |
| **16** | **376,016** | 5.7 ms | **2.70×** |
| 32 | 347,376 | 12.2 ms | 2.49× |

At a 16-key window the single-loop window API hits **~376K TPS — above the AsyncPool fast-path (~317K at 8×64)**: it collapses N task-resumes into one `await` and fuses submission into a single crossing, breaking past the single-loop single-op submission ceiling (~139K raw; the coalescer already lifts the plain fast path to ~172–184K, and the window goes further). It stays N wire ops, so it's a *client-overhead* win, not a network one — server batch (which sends one request per node) still wins on WAN links and on the sync path.

**Across `AsyncPool` loops (`--pool-loops N`) the window API goes higher still** — each loop runs its own windows, so in-flight = `loops × z × k`, and *both* throughput and tail latency track that depth. Fixing loops=4 (the sweet count — 8 over-saturates) and sweeping `z × k` maps the throughput/latency frontier (published build, 50/50, single run/cell):

| Config (4 loops) | in-flight | Total TPS | p99 |
|---|---|---|---|
| z8, k4 | 128 | 342,946 | **0.9 ms** |
| z8, k8 | 256 | 402,036 | 1.5 ms |
| **z16, k8** | **512** | **425,808** | **2.6 ms** |
| z32, k8 (peak) | 1024 | 442,104 | ~6 ms |

The top of the curve is **flat**: going 256→1024 in-flight adds only ~8% TPS but ~4× the p99. So the raw peak (`z32·k8`, ~442K @ ~6 ms) is rarely the right operating point:

- **Balanced default — `z16·k8` (512 in-flight): ~426K @ p99 2.6 ms.** Within ~2% of the peak (statistically tied) at *under half* the tail — the recommended high-throughput window×pool config.
- **Low-latency — `z8·k4` (128 in-flight): ~343K @ p99 0.9 ms.** The highest-throughput mode that stays **under 1 ms**; still above the sync fast-path (241K @ 0.2 ms) and pool fast-path (285K @ 1.8 ms), so window×pool remains the top async mode even held to sub-millisecond tails.
- **No pool / not free-threaded — single-loop window, `k=8`: ~332K @ p99 1.4 ms** (non-FT ~316K). `AsyncPool` requires the GIL off, so on regular CPython — or whenever you just want one event loop — the single-loop window is the path. It's the *only* window config with a non-FT number, and a k=8 window keeps the tail well under the k=16 peak's ~5 ms.

**Shape rule:** at a fixed in-flight budget, prefer **larger windows with fewer tasks** (higher `k`, lower `z`) — fewer task-resumes and more fusion per crossing give higher TPS *and* lower latency (`z16·k8` beats `z32·k4` at 512 in-flight on both axes). And 4 loops beats 8 — eight over-saturates (k=32 collapses to ~247K @ 23 ms). Free-threaded only (`AsyncPool` needs the GIL off).

**Headline**: the **PSDK sync builder scales through batch=128 to ~506K TPS** — the highest framework number in the matrix. Sync batch routes via PAC's `batch_*_blocking` entries with one PyO3 boundary per batch, so doubling the batch size keeps amortizing the per-call Python cost. The b=128 peak is 3.3× the single-key sync builder.

The async single-loop sweep tops out around 174K (batch=128) — the asyncio ↔ Tokio bridge cost per `execute()` doesn't go away just because each call moves more data. AsyncPool recovers most of that by running 4 loops in parallel, hitting 332K at batch=64.

## Stack cost analysis

Layering the headline single-key TPS numbers across clients shows where every transition costs. The Rust-core figures below are at the same default settings as the Python clients; Rust-core's *real* cluster-side ceiling is ≥580K (with the per-op Tokio timer wheel bypassed AND `max_conns_per_node` sized to match concurrency — see ["Per-language baselines"](#per-language-baselines) above). Python clients hit their own client-side ceilings well below 580K, so they aren't sensitive to the Rust-core defaults that gate the higher number.

| Layer | TPS | Note |
|---|---|---|
| **Rust core async, pool sized** | **575,592** | Real cluster-side ceiling; `MAX_CONNS=512` + 8 pools/node (per-op timer fix now in-core, `CLIENT-4990`) |
| **PSDK async window API × AsyncPool (4 loops, z16·k8)** | **425,808** | `get_many`/`put_many` across pool loops — top async mode (FT), p99 2.6ms; low-latency z8·k4 ~343K @ 0.9ms; peak ~442K @ ~6ms |
| PSDK async window API, single loop (k=16) | 376,016 | `get_many`/`put_many` — N single-key ops fused per submission (FT, uvloop) |
| **PSDK async AsyncPool, fast-path (8×64)** | **316,812** | 8 event loops × 64 tasks (FT only, uvloop) |
| **Rust core async, default settings** | **305,741** | `aerospike-core` via Tokio tasks; default 256-conn pool caps below the sized ceiling |
| **PAC sync direct, ct_runtime** | **280,828** | PyO3 wrapper, per-thread Tokio current-thread runtime |
| **PSDK async AsyncPool, fast-path (4×64)** | **280,726** | 4 event loops × 64 tasks (FT only, uvloop) |
| **PSDK sync, fast-path, ct_runtime** | **272,612** | SDK fast-path + ct_runtime |
| **PAC sync direct (multi-thread Tokio)** | **250,197** | PyO3 wrapper, shared Tokio multi-thread runtime |
| Rust core sync, default settings | 245,729 | `aerospike-core` via OS threads + `block_on` |
| **PSDK sync, fast-path** | **241,104** | SDK `session.get` / `session.put` → PAC blocking |
| PSDK async AsyncPool, builder (4×64) | 180,187 | 4 loops, full builder path |
| PSDK sync, builder | 153,934 | SDK chained builder → execute → stream |
| **PAC async direct, 32 tasks** | **130,641** | PyO3 wrapper, asyncio ↔ Tokio bridge (with drainer + uvloop) |
| **PSDK async single-loop, fast-path** | **128,073** | One event loop, `session.get` / `session.put` |
| PSDK async single-loop, builder | 66,322 | One event loop, full builder path |

### Sync stack — boundary cost is small

| Transition | TPS | Δ |
|---|---|---|
| Rust core sync (default settings) | 245,729 | reference (default `aerospike-core`) |
| → PAC sync direct (multi-thread Tokio) | 250,197 | **~flat** (PyO3 + Python boundary cost now within noise of rust-core) |
| → PSDK sync, fast-path | 241,104 | **−4%** — SDK layer is essentially free |
| → PSDK sync, builder | 153,934 | **−36%** vs fp (chained builder + stream wrap in Python) |

On v3/`dev6` the PyO3 + per-op Python ↔ Tokio handoff cost has closed to within noise of the direct rust-core sync number (PAC sync ≈ rust-core sync, both ~246-250K). The PSDK SDK layer is essentially free over PAC direct. (The cluster sustains higher absolute throughput than rust-core sync default — see ["Per-language baselines"](#per-language-baselines) — but with the default `aerospike-core` settings active, both Python and Rust-direct paths land in the same band.)

### Async stack — closer to sync than it used to be

| Transition | TPS | Δ |
|---|---|---|
| PSDK sync, fast-path (sync reference) | 241,104 | — |
| → PAC async direct (single loop, drainer + uvloop) | 130,641 | **−46%** (asyncio loop thread is the gating step) |
| → PSDK async single-loop, fast-path | 128,073 | **−2%** vs PAC async (PSDK SDK layer) |
| → PSDK async AsyncPool, fast-path (4×64) | 280,726 | **+119%** vs single-loop (parallelism across loops + uvloop inside pool, FT only) — **+16% above sync** |
| → PSDK async AsyncPool, fast-path (8×64) | 316,812 | **+147%** vs single-loop, **+31% over sync** |
| → PSDK async window API × AsyncPool (`get_many`, balanced 4×z16×k8) | 425,808 | **the top async mode** — p99 2.6ms (sub-1ms option ~343K; peak ~442K @ ~6ms) |
| → PSDK async window API, single loop (`get_many`, k=16) | 376,016 | **+194%** vs single-op single-loop, **+56% over sync** |

**Async key insight**: post-drainer-thread + uvloop, the single-loop async ceiling sits around 120-130K. The bottleneck is now the **asyncio loop thread doing per-op `set_result` and task wakeup work, single-threaded**. `AsyncPool` (multi-loop) breaks past that ceiling by running 4-8 loops in parallel — at 8×64 it actually **exceeds** the sync fast-path ceiling. Only useful under free-threaded Python; under regular CPython the GIL serializes the loops and the pool is slower than a single client (see [AsyncPool note](#asyncpool-is-a-free-threading-feature)).

### Practical takeaway

- **PSDK SDK layer is essentially free** on both sync and async paths — ~3-8% over PAC direct on either side. Most cost is below PSDK in PAC + PyO3.
- **PAC's drainer thread** moves all asyncio-loop wake-ups onto a single persistent waker thread, eliminating per-batch `Python::attach` churn on Tokio workers. This is what lifted async TPS substantially over earlier reference numbers (e.g., AsyncPool 4×64 went from 173K → 280K).
- **uvloop is installed by default** under FT and non-FT Linux/macOS. It lifts single-loop async ~15% on top of the drainer; multi-loop (AsyncPool) sees ~0-3% extra because the per-loop work is already parallelized.
- **The chained-builder API pays a per-op Python tax** on single-key calls (~30% vs fast-path on sync). On batch calls, that cost amortizes across keys: at batch=128 the sync builder reaches ~506K TPS — far above any single-key cell.
- **For maximum throughput**: use the **sync builder with batches** (multi-key `session.query([keys])` / multi-key write chains) on free-threaded Python when the workload tolerates batching — ~506K TPS at batch=128. For single-key sync workloads, the **fast-path** (`session.get` / `session.put`) gives ~241K TPS. For async workloads, **AsyncPool 4-8 loops** delivers 280-317K TPS — above the sync fast-path ceiling — and the **async window API** (`get_many`/`put_many`) across an `AsyncPool` is the top async mode — **~426K @ p99 2.6ms** balanced (`4 loops × z16 × k8`), **~343K @ p99 0.9ms** if you need sub-millisecond tails, up to a ~442K peak whose ~6ms tail buys only a few percent more. Reserve `--current-thread-runtime` (experimental — see the warning above) for tightly-controlled benchmarking, not production.

## Fast-path vs builder

PSDK exposes two API shapes for single-key reads and writes:

- **Builder** (chained): `session.query(key).execute()` and `session.upsert(key).put(bins).execute()`. Returns a `RecordStream` of wrapped `RecordResult`s. Supports filter expressions, error handlers, TTL overrides, generation checks, batch operations, and secondary-index queries.
- **Fast-path** (direct): `session.get(key)` and `session.put(key, bins)`. Bypasses the builder + stream wrap and calls PAC's native `_blocking` / async entry points directly with the session-cached policy. Single-key only; no filter / error-handler / TTL hooks. Errors raise directly (cache misses raise `RecordNotFound`).

Speedup of fast-path over builder on **single-key** dispatch at 32 threads / 4×64 tasks, FT:

| Config | Builder TPS | Fast-path TPS | Speedup |
|---|---|---|---|
| PSDK async, single client | 66,322 | 128,073 | **1.93×** |
| PSDK async, AsyncPool 4×64 | 180,187 | 280,726 | **1.56×** |
| PSDK sync | 153,934 | 241,104 | **1.57×** |

These speedups are for single-key dispatch. With batching, the builder amortizes its per-op overhead across many keys per call — at batch=128 the sync builder reaches 506K TPS (vs 241K for sync fast-path). The fast-path stays single-key only; for any workload that can batch, the builder eventually wins.

The builder has irreducible Python overhead per op (builder object allocation, `_OperationSpec` finalization, `RecordResult` wrapping, generator-based stream iteration). The fast-path skips all of it.

See [`performance.md`](performance.md) for the user-facing decision guide.

(asyncpool-is-a-free-threading-feature)=
## AsyncPool is a free-threading feature

`AsyncPool` runs N event loops on N OS threads with one PAC client each. Its value is **multi-thread parallelism across CPU cores** — which only materializes under free-threaded Python (`PYTHON_GIL=0`).

Under non-FT Python the GIL still serializes all Python execution. AsyncPool ends up with 256 outstanding tasks across 4 threads competing for one interpreter, plus the per-loop orchestration overhead — typically net flat or slightly slower than a single-client async setup on the same Python binary:

| Config | non-FT TPS | vs single-loop non-FT |
|---|---|---|
| async single-loop, fast-path, 32 tasks | 110,203 | baseline |
| async AsyncPool 4×64, fast-path | 116,544 | **+6%** (uvloop in pool roughly recovers the overhead) |
| async single-loop, builder, 32 tasks | 63,891 | baseline |
| async AsyncPool 4×64, builder | 58,142 | **−9%** |

**AsyncPool is roughly on par with single-client async under GIL-on Python** now that pool loops use uvloop too. Pick the one that fits your code shape; the real AsyncPool win is reserved for free-threaded runs.

## Error classification

The framework treats `RecordNotFound` (cache miss on a point read) as a successful read with no record — not an error. This matches the semantics used by other Aerospike SDKs. Real errors (timeouts, connection failures, server-side errors, etc.) are counted separately as either `Errors:` or `Timeouts:` in the per-second ticker and the summary block.

To verify error accounting on a fresh dataset, pass `--truncate` to the bench command; with the fix in place all modes report `Errors: 0` even when half the early reads cache-miss.

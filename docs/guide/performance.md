# Performance Modes

**Which API and Python build should I use?** PSDK exposes several execution modes. The right one depends on (1) whether you can run a free-threaded CPython build (e.g., 3.14t) with the GIL disabled, and (2) what your workload looks like — predominantly single-key reads/writes, or complex queries with builders, batches, and error handlers.

This guide is the short, user-facing decision tree. The full numbers and methodology behind every recommendation are in [`benchmarking.md`](benchmarking.md).

## TL;DR decision tree

1. **Single-key reads/writes, want max throughput?** Use [`session.get()` / `session.put()`](#fast-path-sessionget--sessionput) — the fast-path API.
2. **Async, and you have a _window_ of keys to read/write at once?** Use [`session.get_many()` / `session.put_many()`](#async-window-api) — the highest-throughput async mode. On a single loop (no pool; works on regular CPython) it does **~332K @ p99 1.4ms**; across an `AsyncPool` (free-threaded) **~426K @ 2.6ms** balanced, or **~343K @ 0.9ms** for sub-millisecond tails (~2.4–3× single-op async).
3. **Complex queries (secondary index, AEL filters, batch ops, error handlers)?** Use [chained builders](#chained-builder-api) — `session.query(...).where(...).execute()` and friends.
4. **Sync or async?** Pick by your app's concurrency model — **async** to fit an event-loop-based app like a web server (`await session.get()` yields the loop instead of blocking it), **sync** ([`aerospike_sdk.sync`](../api/sync/cluster-definition.md)) when you don't need async (sync is often the faster / lower-latency path — see [Cross-client performance](#per-language-baselines)).
5. **Free-threaded Python (e.g. 3.14t)?** Yes if you need high throughput across many threads. No if you depend on C extensions that aren't FT-safe.
6. **AsyncPool?** Only on free-threaded Python. Slower than single-client on non-FT.

## Free-threaded vs regular Python

PSDK works on both standard CPython and a free-threaded build (e.g., `3.14t`). The choice matters a lot for high-throughput workloads.

| | Regular CPython | Free-threaded CPython (e.g. 3.14t) |
|---|---|---|
| **GIL** | Always on. Threads serialize through one interpreter. | Off by default. Multiple threads run Python in true parallel. |
| **Single-thread perf** | Same | Same (slightly slower for some workloads due to atomic refcounts) |
| **Multi-thread perf** | Capped by GIL — usually 1.5-2× single-thread no matter how many threads | Scales near-linearly with cores for I/O-bound work |
| **C extension support** | Universal | Limited — extensions must declare `Py_mod_gil = Py_MOD_GIL_NOT_USED` |
| **Recommended for PSDK?** | If GIL-on simplicity is fine for your workload | If you want PSDK's high-TPS modes |

### Setup for free-threaded mode

```bash
# Install the free-threaded build (uv or pyenv)
uv python install 3.14.5+freethreaded

# Always launch with PYTHON_GIL=0
PYTHON_GIL=0 python my_app.py
```

**Critical gotcha:** if any imported C extension hasn't opted into free-threading, the interpreter silently re-enables the GIL. Verify with `sys._is_gil_enabled()` returning `False` after all imports. PSDK's dependency PAC (`aerospike-async`) is FT-safe; many other libraries aren't yet.

On the free-threaded build the GIL is **off by default** — `PYTHON_GIL=0` is not what turns it off. Rather, it *forces* the GIL to stay off even when an FT-incompatible extension would otherwise silently re-enable it (the gotcha above), so launching with it is a safeguard, not a switch. `PYTHON_GIL=1` forces the GIL back on.

(fast-path-sessionget--sessionput)=
## Fast-path: `session.get` / `session.put`

For single-key operations where you don't need filters, error handlers, projections, batch semantics, secondary indexes, etc., the fast-path methods bypass the builder + stream wrapping and call PAC's native blocking/async APIs directly with the session-cached policy.

### Sync example

```python
from aerospike_sdk import Behavior, DataSet
from aerospike_sdk.sync import ClusterDefinition

with ClusterDefinition("localhost", 3000).connect() as cluster:
    session = cluster.create_session(Behavior.DEFAULT)
    k = DataSet.of("test", "users").id("alice")
    session.put(k, {"name": "Alice", "age": 28})
    record = session.get(k)
    print(record.bins)
```

### Async example

```python
import asyncio
from aerospike_sdk import Behavior, ClusterDefinition, DataSet

async def main():
    async with await ClusterDefinition("localhost", 3000).connect() as cluster:
        session = cluster.create_session(Behavior.DEFAULT)
        k = DataSet.of("test", "users").id("alice")
        await session.put(k, {"name": "Alice", "age": 28})
        record = await session.get(k)
        print(record.bins)

asyncio.run(main())
```

The fast-path APIs accept an optional `bins=` projection for reads and an arbitrary `bins` dict for writes. Errors raise directly (no `RecordResult` wrapping).

**Async operation coalescing (automatic).** On the async fast path, concurrent `session.get()` calls issued within one event-loop iteration are fused into a single client-side crossing to PAC — one submission drives them all, and each `await` still resolves to its own record (or raises its own exception) the instant that key returns. `session.put()` coalesces the same way, each buffered write carrying its own payload. Reads and writes buffer separately (they are distinct submissions) but share one armed flush, so a mixed tick still costs a single scheduling callback. It is fully transparent: no API change, identical results, and a lone or low-rate op dispatches directly, so it pays nothing.

What does *not* coalesce: a projection read (`session.get(key, ["name"])`), anything inside a `TransactionalSession`, and builder calls (`session.query(...)`, `session.upsert(...)`) — the coalescer lives on the `get`/`put` fast path only. Buffers are also per-`Session`, so ops fuse only with same-tick ops on the *same session object*; a session-per-task pattern gets no fusion. On a single loop the lift scales with per-tick fan-in: at high in-flight (512 tasks) it raises throughput ~**+45–56%** across read-only, mixed, and write workloads alike (measured median-of-3, both free-threaded and GIL-on), with p50 latency roughly halved; at low concurrency the win shrinks toward zero because there is less to fuse per tick. On the multi-loop `AsyncPool` the picture is different: the benefit grows with **loop count**, not per-session fan-in — ~+3% at 4 loops versus ~+10% at 8 loops for a 50/50 mix, measured at equal total in-flight (so equal server load and matched p50), which makes it a client-side contention-relief effect rather than a fusion-volume one. Doubling per-session fan-in at a fixed loop count barely moves it. Reads and writes each contribute a material share of the 8-loop gain and the split between them is not stable run to run, so neither direction is the one carrying it; the write share is also independent of payload width, which places the win in the submission crossing rather than in payload conversion. Latency moves with the same grain: p50 improves slightly at 8 loops, and rises slightly at 4, where a buffered op waits a tick without much contention to relieve. These stay independent wire ops — this is client-side submission fusion, **not** a server batch (that's the builder's batch path, `session.query(...)` over a key list). Disable with `PSDK_COALESCE=0` to A/B the whole thing, or `PSDK_COALESCE_WRITES=0` to keep reads fusing while writes dispatch directly.

**When NOT to use fast-path:**
- Anything that needs `where(...)` filters, `expire_record_after_seconds`, `with_durable_delete`, generation checks, or `record_exists_action` overrides — use the builder.
- Reads from a `DataSet` with a secondary-index query — use the builder.
- Batch reads/writes across multiple keys — use the builder (a verb chain spanning more than one key executes as a single batch).
- `RecordResult.is_ok` / `error` introspection per record — use the builder, which yields wrapped `RecordResult` instances.

(async-window-api)=
## Async window API: `session.get_many` / `session.put_many`

When you already have a *window* of independent keys to read or write together — not a server batch, just many single-record ops you'd otherwise `await` one at a time — the async window API submits the whole window in one client→PAC crossing and delivers each key's result positionally. It amortizes the per-op submission cost that caps the single-op fast path, reaching a throughput tier the transparent coalescer can't.

```python
import asyncio
from aerospike_sdk import ClusterDefinition, DataSet

async def main():
    async with await ClusterDefinition("localhost", 3000).connect() as cluster:
        session = cluster.create_session()
        users = DataSet.of("test", "users")
        keys = [users.id(i) for i in range(64)]

        # Write one payload to a window of keys — a single await.
        outcomes = await session.put_many(keys, {"active": True})
        write_errors = [i for i, e in enumerate(outcomes) if e is not None]

        # Read the window back — a single await, results positional (one slot per key).
        records = await session.get_many(keys)
        active = [r.bins["active"] for r in records if not isinstance(r, Exception)]

        print(f"{len(active)} users read; {len(write_errors)} write errors")

asyncio.run(main())
```

Each returned list is the same length as `keys` and positional: a slot holds that key's `Record` (`get_many`) or `None` on success (`put_many`), **or the exception instance for that key** — returned, never raised, so one missing or failed key never fails its window-mates. Check with `isinstance(slot, Exception)`. `get_many` accepts an optional `bins=` projection shared by the window.

**Window API vs. server batch — pick by where your bottleneck is.** This is *not* a server batch (the builder's batch path — `session.query(...)` over a key list), and the difference is what to optimize for:

- **Server batch** sends **one wire request per node** covering all that node's keys — it amortizes network round-trips *and* client submission. It's the wire-efficient choice.
- **The window API** issues **N independent single-record wire ops**, fused only at the client submission boundary (one crossing, one completion). It amortizes **only the client-side per-op cost** — the wire still carries N requests.

So the window API is a *client-overhead* win, not a *network-efficiency* win. On a fast (LAN/VPC) link its lean direct-submission path actually **outruns the async server-batch path** — roughly 2× on the same run, because the async builder-batch pays the builder + stream overhead the window API skips (and at far lower tail latency: window p99 ~5ms vs async batch=128 p99 ~30ms). But on a high-latency/WAN link, batch's fewer round-trips win decisively; and on the **sync** path the builder batch is the throughput king (~**506K** at batch=128 — none of the async hot-path tax).

Rule of thumb: **window API** when you're async, on a fast network, and want per-op independence (each key its own op, policy, and failure) at low latency; **server batch** when you're on a constrained/high-RTT link, moving very large key sets, want server-batch semantics, or you're on sync.

**Throughput.** A single async loop is *submission-bound*: a raw single-op fast path caps around **~139K TPS**. Two things lift it. First, the transparent coalescer (on by default) fuses same-tick ops, so the plain fast path *does* scale with in-flight — to ~172K at 128 tasks, ~184K at 512. Second, and further, the window API amortizes submission across an explicit window: at 50/50 read/write on a 3-node cluster it reaches **~376K TPS at a 16-key window** — the sweet spot — **~2.7×** the single-op path at equal (512) in-flight. A window of 8 gives ~332K at the lowest tail (p99 ~1.4ms); 32+ trade throughput for tail latency (p99 ~12ms). A single loop running the window API (~376K) already out-throughputs the `AsyncPool` *fast-path* at its ~317K peak; running the window API *across* the pool's loops goes higher still and is the highest-throughput async mode. Throughput and tail latency both track total in-flight (`loops × tasks × window`), so pick a point on the frontier: **~426K @ p99 2.6ms** balanced (4 loops × z16 × k8), **~343K @ p99 0.9ms** for sub-millisecond tails, up to a ~442K peak whose ~6ms tail buys only a few percent more. Four loops beats eight (eight over-saturates); at a fixed in-flight budget, larger windows with fewer tasks win. It is **async-only by design**: the sync path has no event loop and already pays minimal per-op overhead, so there is no `get_many`/`put_many` on `SyncSession`.

**When to reach for it:** you have a set of keys in hand and want maximum async throughput. If keys instead arrive one at a time on the event loop, plain `session.get()` / `session.put()` already fuse them transparently (see *Async operation coalescing* above) — the window API is for when you can hand over the whole set at once.

(chained-builder-api)=
## Chained builder API

The full-featured chainable API that mirrors the Aerospike SDK shape across languages.

```python
from aerospike_sdk import Behavior, ClusterDefinition, DataSet, ErrorStrategy

async with await ClusterDefinition("localhost", 3000).connect() as cluster:
    session = cluster.create_session(Behavior.DEFAULT)
    users = DataSet.of("test", "users")

    # Filtered query — AEL filter expression
    results = await (
        session.query(users)
        .where("$.age > %s and $.country == '%s'", 25, "US")
        .execute()
    )
    async for r in results:
        if r.is_ok:
            print(r.record.bins)

    # Write with TTL + error handler
    stream = await (
        session.upsert(users.id(1))
        .put({"name": "Alice"})
        .expire_record_after_seconds(3600)
        .execute(on_error=ErrorStrategy.IN_STREAM)
    )
    await stream.collect()
```

Use the builder when you need filter expressions, batch operations, secondary-index queries, error handlers, TTL overrides, or generation checks. For plain single-key reads and writes, prefer the fast-path.

## AsyncPool — multi-loop async on free-threaded Python only

`AsyncPool` runs N event loops on N OS threads with one cluster member (one PAC client) each, so async work can use multiple CPU cores in parallel. **It only helps under free-threaded Python.**

```python
from aerospike_sdk import AsyncPool, Behavior, ClusterDefinition

async def per_loop(cluster, loop_idx):
    session = cluster.create_session(Behavior.DEFAULT)
    # ... do work, e.g. asyncio.gather of session.get/put calls ...

async with AsyncPool(ClusterDefinition("localhost", 3000), loop_count=4) as pool:
    await pool.map(per_loop, range(4))
```

**Scaling**: at `loop_count >= 4`, AsyncPool automatically gives each member
its own PAC Tokio runtime (per-Client runtime isolation). This eliminates the
cross-loop scheduler contention that previously capped throughput at 4 loops,
so TPS scales monotonically. Measured on 8-core hardware, FT Python (with
uvloop enabled by default and PAC's drainer thread serializing
`call_soon_threadsafe` wakeups across all pooled Clients):

| Pool size | TPS | p99 latency |
|---|---|---|
| 4 × 64 tasks | **~260K** | 2.5 ms |
| 8 × 64 tasks | **~317K** | 4.1 ms |

The 290K ceiling is now **above** the PSDK sync `ct_runtime` ceiling (~266K)
and well past the production sync fast-path (~241K) — async is the highest-
throughput single-key Python mode on free-threaded hardware. Past 8 loops,
additional loops trade p99 latency for marginal TPS; pick `loop_count` based
on the tail-latency budget your workload tolerates.

You can override the auto-enable threshold via `AsyncPool(..., per_client_runtime=True|False)`.
Forcing it on at low loop counts may be useful on smaller hardware; forcing
it off reverts to the shared global Tokio runtime path. Worker count is
auto-derived as `max(2, os.cpu_count() // loop_count)`.

**Event loop under free-threading.** The pool uses uvloop by default under
free-threading — its per-op savings compound with the multi-loop scaling, worth
~**+8%** FT pool throughput over the stdlib selector fallback (measured, 4×64
fast-path, latency-neutral). This
is safe because PAC routes its cross-thread completion wakeups through a
self-pipe watched by an `add_reader` callback instead of `call_soon_threadsafe`,
sidestepping a libuv free-threading race (MagicStack/uvloop #720) that otherwise
stalls a multi-loop pool on uvloop releases lacking the upstream fix. The
transport engages automatically; set `AEROSPIKE_PIPE_WAKE=0` to disable it (the
pool then falls back to the stdlib selector loop under free-threading to stay
safe) or `=1` to force it on. Override the loop choice directly with
`AsyncPool(..., use_uvloop=True|False)`.

**AsyncPool on regular (GIL-on) Python is now roughly on par with single-client async** after the uvloop-in-pool change — measured ~117K (pool 4×64) vs ~110K (single-loop) on FT-Python forced to GIL-on. The GIL still serializes all Python execution across pool threads, so the multi-loop architecture can't deliver the full FT scaling, but uvloop's per-op savings inside the pool now roughly cancel the orchestration overhead.

On regular Python it's a wash — pick AsyncPool if it fits your code shape (you already write fan-out patterns) or a single cluster connection + `asyncio.gather` if simpler. The real AsyncPool win remains free-threaded Python.

## Sync vs async — when to pick which

- **Sync ([`aerospike_sdk.sync`](../api/sync/cluster-definition.md))** is best when:
  - You're integrating into an existing sync codebase (Django views, scripts, etc.)
  - Per-op latency matters more than concurrency depth
  - You want the absolute lowest per-op overhead — PSDK sync fast-path is roughly at parity with PAC's direct blocking API

- **Async (top-level [`aerospike_sdk`](../api/cluster-definition.md))** is best when:
  - You already have an asyncio event loop (FastAPI, aiohttp, etc.)
  - You need to overlap I/O across many concurrent operations
  - You're willing to use uvloop for higher throughput (default in modern asyncio + free-threaded Python setups)

Both modes share the same `Session` API surface (chained builders + fast-path shortcuts), the same `Behavior` policy model, and the same error semantics.

:::{note}
Both modes leave `conn_pools_per_node` at the underlying default of 4. Sync
drives the client from many caller threads, so the per-node connection-pool
mutex sees contention that a single event loop does not — but raising the pool
count did not pay for it: a 32-thread sync benchmark against a 3-node cluster
measured 4 and 8 as indistinguishable on both throughput and p99.

Treat it as a knob to measure rather than one to raise on principle. If you do
want to try it, supply
`with_system_settings(SystemSettings(conn_pools_per_node=8))` on the
`ClusterDefinition` and compare against your own workload.
:::

## Performance summary table

Numbers from the [Benchmarking Guide](benchmarking.md) — 8-vCPU isolated client VM → 3× 8-vCPU isolated server VMs over a low-latency private network, 100K keys, 50/50 RW, 50-byte payload.

### Single-key dispatch (batch size 1)

| Mode | Threads / Tasks | Free-threaded TPS | Non-FT TPS |
|---|---|---|---|
| **Async window API** (`get_many`/`put_many`, k=16) | 32×16 | **~376K** | ~365K |
| **Async fast-path, AsyncPool 8×64** | 512 tasks | **~317K** | (FT only) |
| **Async fast-path, AsyncPool 4×64** | 256 tasks | **~281K** | ~117K |
| **Sync fast-path** (`session.get`/`put`) | 32 | **~241K** | ~52K |
| Sync builder (`session.query(k).execute()`) | 32 | ~154K | ~31K |
| Async fast-path, single client | 32 tasks | ~128K | ~110K |
| Async builder, AsyncPool 4×64 | 256 tasks | ~180K | ~58K |
| Async builder, single client | 32 tasks | ~66K | ~64K |

:::{admonition} Experimental: `current_thread_runtime` (ct_runtime)
:class: warning

Giving each Python thread its own client on a thread-pinned runtime removes the cross-thread hop that every sync operation otherwise pays, and **measured TPS rises to ~273K (sync fp) / ~186K (sync builder)** on free-threaded Python. It is not available on `ClusterDefinition`, and is **not recommended for application use** — the mode only implements part of the operation surface:

| Works | Raises |
|---|---|
| Single-key reads and writes (`get`, `put`, `delete`, `touch`, `exists`, `operate`) | Dataset queries and scans — `session.query(dataset)` |
| Single-key and multi-key builder chains — `session.query(key)`, `session.query([keys])` | Cluster-wide info fan-out — index and UDF listing |
| Single-node `info` | Connection health checks, so `ClusterDefinition.connect()` cannot validate the cluster |

Operational costs on top of that:

- **N× cluster-tend loops.** Each per-thread client tends the cluster independently. At 32 worker threads that is 32 tend loops polling every second, which multiplies info load on the cluster.
- **N× connection pools.** Each thread maintains its own, so total per-node connections scale with thread count. Set `conn_pools_per_node = 1` to keep the total in the same range as a shared client's.
- **Thread-lifetime coupling.** A thread's client lives until the thread exits, so this suits a long-lived pool and penalizes short-lived threads.

Because the gaps above make it unsafe as a general-purpose setting, the mode is reachable only through the deprecated `SyncClient` and is intentionally not exposed on the `ClusterDefinition` builder. Treat the numbers above as a benchmark data point rather than a tuning recommendation; the default sync path (one shared multi-threaded runtime and one shared connection pool) is the supported production setup.
:::

### With batching (`--batch-size > 1`, free-threaded)

When the workload can group keys per call, both the [async window API](#async-window-api) (`get_many`/`put_many`, client-side fusion) and the chained-builder API amortize per-op overhead and surpass every single-key number above.

| Mode | Window / Batch | Peak TPS |
|---|---|---|
| **Sync builder** (server batch) | 128 | **~506K** |
| **Async window API** (`get_many`/`put_many`) | window=16 | **~376K** |
| AsyncPool builder (server batch), 4×64 | 64 | ~332K |
| Async single-loop builder (server batch), 32 tasks | 128 | ~205K |

The **async window API** is client-side fusion of N independent point ops (not a server batch — see [above](#async-window-api)); it's the top *async* throughput mode. Server batch (`session.query([keys])`) is more wire-efficient (one request per node) and wins on high-latency/WAN links and on the sync path.

**Practical reading:**
- If your workload can batch keys, the **sync builder with multi-key `session.query([keys])` or multi-key write chains** is the highest-throughput mode — scales to ~506K TPS at batch=128. Doubling the batch size keeps amortizing the per-call cost.
- For single-key workloads on free-threaded Python, **AsyncPool fast-path at 4-8 loops** delivers **~280-317K TPS** — above sync fast-path (~241K); the **async window API** (`get_many`/`put_many`) goes higher still at ~376K when you have a window of keys in hand. If you prefer sync, the fast-path is still the best non-experimental sync mode.
- On regular Python (GIL on), AsyncPool 4×64 (~117K) edges out single-client async (~110K) and is roughly 2× sync fast-path (~52K). Under non-FT, AsyncPool is now a slight win rather than a loss thanks to uvloop inside the pool.

## Why sync and async perform similarly now

The cost stacks for sync and async used to diverge sharply — async historically lost ~50% to the asyncio ↔ Tokio bridge per op. With PAC's drainer thread (a single persistent waker thread handling all Tokio→asyncio wakeups) plus uvloop installed by default under FT, the async ceiling has closed substantially:

- **Sync clients pay only the PyO3 boundary cost** plus a per-op thread-handoff between caller and Tokio (~71 µs per op). PSDK fast-path adds ~3-5% on top of PAC direct — the SDK layer is essentially free.
- **Async clients pay PyO3 + asyncio event-loop scheduling**. The drainer thread eliminates per-batch `Python::attach` churn on Tokio workers; uvloop reduces per-op loop-thread cost. With both, single-loop async tops out around 130K TPS (the asyncio loop thread is now the single-threaded bottleneck, doing per-op `set_result` and task wakeup).
- **AsyncPool with N loops** breaks past the single-loop ceiling by parallelizing the loop work across N Python threads. 4-8 loops scale to 280-317K TPS — above the production sync ceiling on the same hardware.
- **The chained-builder API pays an additional Python-interpreter cost** on single-key calls — per-op object allocation, validation, and stream-wrap cost. On batch calls, that cost amortizes across keys; at batch=128 the sync builder reaches ~506K TPS — much higher than any single-key cell. Use the fast-path (`session.get`/`session.put`) for single-key dispatch without filters; use the builder with batching for high-throughput bulk workloads.
